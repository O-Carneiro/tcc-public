import gzip
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

DB_PATH = os.environ.get("SINK_DB_PATH", "/data/telemetry.db")

SCHEMA = """
-- Deliberately NOT WAL: the -shm index does not work across a macOS bind
-- mount, so host-side `sqlite3 data/telemetry.db` would read a stale snapshot.
-- One writer, so the rollback journal costs us nothing.
PRAGMA journal_mode = DELETE;

CREATE TABLE IF NOT EXISTS export_batch (
  id          INTEGER PRIMARY KEY,
  received_at TEXT    NOT NULL,
  body_bytes  INTEGER NOT NULL,
  point_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_point (
  id             INTEGER PRIMARY KEY,
  batch_id       INTEGER NOT NULL REFERENCES export_batch(id),
  received_at    TEXT    NOT NULL,
  service_name   TEXT,
  pipeline_hop   TEXT,
  scope_name     TEXT,
  metric_name    TEXT    NOT NULL,
  metric_unit    TEXT,
  metric_type    TEXT    NOT NULL,
  time_unix_nano INTEGER,
  attributes     TEXT,
  value          REAL,
  count          INTEGER,
  sum            REAL,
  min            REAL,
  max            REAL
);

CREATE INDEX IF NOT EXISTS metric_point_name_idx ON metric_point (metric_name);

CREATE TABLE IF NOT EXISTS log_record (
  id                      INTEGER PRIMARY KEY,
  batch_id                INTEGER NOT NULL REFERENCES export_batch(id),
  received_at             TEXT    NOT NULL,
  service_name            TEXT,
  service_instance_id     TEXT,
  pipeline_hop            TEXT,
  scope_name              TEXT,
  time_unix_nano          INTEGER,
  observed_time_unix_nano INTEGER,
  body                    TEXT,
  event_id                INTEGER,
  attributes              TEXT,
  UNIQUE (service_instance_id, event_id, body)
);

CREATE TABLE IF NOT EXISTS log_record_raw (
  id          INTEGER PRIMARY KEY,
  batch_id    INTEGER NOT NULL REFERENCES export_batch(id),
  received_at TEXT    NOT NULL,
  reason      TEXT,
  raw         TEXT
);
"""

_lock = threading.Lock()
_conn: sqlite3.Connection


def _anyvalue(v):
    for key in ("stringValue", "boolValue", "intValue", "doubleValue"):
        if key in v:
            return v[key]
    if "arrayValue" in v:
        return [_anyvalue(x) for x in v["arrayValue"].get("values", [])]
    if "kvlistValue" in v:
        return _attrs(v["kvlistValue"].get("values", []))
    return None


def _attrs(kvs):
    return {kv["key"]: _anyvalue(kv.get("value", {})) for kv in kvs}


def _num(p):
    if "asDouble" in p:
        return float(p["asDouble"])
    if "asInt" in p:
        return float(p["asInt"])
    return None


def _flatten(payload):
    for rm in payload.get("resourceMetrics", []):
        resource = _attrs(rm.get("resource", {}).get("attributes", []))
        head = (resource.get("service.name"), resource.get("pipeline.hop"))
        for sm in rm.get("scopeMetrics", []):
            scope = sm.get("scope", {}).get("name")
            for m in sm.get("metrics", []):
                common = (*head, scope, m["name"], m.get("unit"))
                for kind in ("sum", "gauge", "histogram"):
                    if kind not in m:
                        continue
                    for p in m[kind].get("dataPoints", []):
                        ts = int(p.get("timeUnixNano", 0))
                        at = json.dumps(_attrs(p.get("attributes", [])), sort_keys=True)
                        if kind == "histogram":
                            yield (
                                *common,
                                kind,
                                ts,
                                at,
                                None,
                                int(p.get("count", 0)),
                                p.get("sum"),
                                p.get("min"),
                                p.get("max"),
                            )
                        else:
                            yield (
                                *common,
                                kind,
                                ts,
                                at,
                                _num(p),
                                None,
                                None,
                                None,
                                None,
                            )


def _i64(v):
    """OTLP/JSON int64 arrives as a decimal string; None when absent."""
    if v is None:
        return None
    n = int(v)
    if not -(2**63) <= n < 2**63:
        raise ValueError("int64 out of range")
    return n


def _log_row(resource, scope, rec):
    """One log_record row; raises ValueError/TypeError/KeyError/AttributeError if rec does not fit."""
    attrs = _attrs(rec.get("attributes") or [])
    body = rec.get("body") or {}
    body = body.get("stringValue") if "stringValue" in body else (
        json.dumps(_anyvalue(body)) if body else None
    )
    event_id = attrs.get("kw.event.id")
    row = (
        resource.get("service.name"),
        resource.get("service.instance.id"),
        resource.get("pipeline.hop"),
        scope.get("name"),
        _i64(rec.get("timeUnixNano")),
        _i64(rec.get("observedTimeUnixNano")),
        body,
        _i64(event_id),
        json.dumps(attrs, sort_keys=True),
    )
    for v in row:
        if isinstance(v, str):
            v.encode("utf-8")  # a lone surrogate raises here, not inside sqlite3 (D28)
    return row


def _split_logs(body):
    """Yield ('ok', row) or ('raw', reason, raw_text); never raises for content."""
    try:
        payload = json.loads(body)
        resource_logs = payload["resourceLogs"]
        if not isinstance(resource_logs, list):
            raise TypeError("resourceLogs is not a list")
    except Exception as e:
        yield ("raw", f"bad request body: {type(e).__name__}", body.decode("utf-8", "replace"))
        return
    for rl in resource_logs:
        try:
            resource = _attrs(rl.get("resource", {}).get("attributes", []))
            scopes = rl.get("scopeLogs", [])
            if not isinstance(scopes, list):
                raise TypeError("scopeLogs is not a list")
        except Exception as e:
            yield ("raw", f"bad resourceLogs entry: {type(e).__name__}", json.dumps(rl))
            continue
        for sl in scopes:
            try:
                scope = sl.get("scope", {})
                records = sl.get("logRecords", [])
                if not isinstance(records, list):
                    raise TypeError("logRecords is not a list")
            except Exception as e:
                yield ("raw", f"bad scopeLogs entry: {type(e).__name__}", json.dumps(sl))
                continue
            for rec in records:
                try:
                    yield ("ok", _log_row(resource, scope, rec))
                except Exception as e:
                    yield ("raw", f"bad record: {type(e).__name__}", json.dumps(rec))


app = FastAPI(title="kw-otel sink")


@app.on_event("startup")
def _startup():
    global _conn
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    for attempt in range(10):
        try:
            _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            break
        except sqlite3.OperationalError:
            if attempt == 9:
                raise
            time.sleep(0.5)
    _conn.executescript(SCHEMA)
    _conn.commit()
    print(f"[sink] writing to {DB_PATH}", flush=True)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/v1/metrics")
async def v1_metrics(request: Request):
    body = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        body = gzip.decompress(body)

    rows = list(_flatten(json.loads(body)))
    received_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with _lock:
        cur = _conn.execute(
            "INSERT INTO export_batch (received_at, body_bytes, point_count) VALUES (?,?,?)",
            (received_at, len(body), len(rows)),
        )
        batch_id = cur.lastrowid
        _conn.executemany(
            """INSERT INTO metric_point (
                 batch_id, received_at, service_name, pipeline_hop, scope_name,
                 metric_name, metric_unit, metric_type, time_unix_nano,
                 attributes, value, count, sum, min, max)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(batch_id, received_at, *r) for r in rows],
        )
        _conn.commit()

    print(f"[sink] batch {batch_id}: {len(rows)} points", flush=True)
    return {"partialSuccess": {}}


@app.get("/stats")
def stats():
    """kw-report-shaped summary rebuilt from SQLite; should match the client."""
    with _lock:
        cmds = _conn.execute(
            """SELECT json_extract(attributes,'$."kw.command"'),
                      json_extract(attributes,'$."kw.status"'),
                      SUM(count), SUM(sum), MIN(min), MAX(max)
                 FROM metric_point
                WHERE metric_name = 'kw.command.duration'
             GROUP BY 1, 2 ORDER BY 1, 2"""
        ).fetchall()
        pomo = _conn.execute(
            """SELECT json_extract(attributes,'$."kw.tag"'),
                      SUM(CASE WHEN metric_name='kw.pomodoro.focus_time' THEN value END),
                      SUM(CASE WHEN metric_name='kw.pomodoro.sessions' THEN value END)
                 FROM metric_point
                WHERE metric_name IN ('kw.pomodoro.focus_time','kw.pomodoro.sessions')
             GROUP BY 1 ORDER BY 1"""
        ).fetchall()
        batches = _conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(point_count),0) FROM export_batch"
        ).fetchone()

    return {
        "batches_received": batches[0],
        "points_stored": batches[1],
        "statistics": [
            {
                "command": c[0],
                "status": c[1],
                "total": c[2],
                "min_secs": c[4],
                "max_secs": c[5],
                "avg_secs": round(c[3] / c[2], 2) if c[2] else None,
            }
            for c in cmds
        ],
        "pomodoro": [{"tag": p[0], "focus_secs": p[1], "sessions": p[2]} for p in pomo],
    }


@app.post("/v1/logs")
async def v1_logs(request: Request):
    body = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except Exception:
            pass  # kept verbatim in log_record_raw; content is never a reason to reject (D28)

    items = list(_split_logs(body))
    ok = [it[1] for it in items if it[0] == "ok"]
    raw = [it[1:] for it in items if it[0] == "raw"]
    received_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with _lock:
        try:
            cur = _conn.execute(
                "INSERT INTO export_batch (received_at, body_bytes, point_count) VALUES (?,?,?)",
                (received_at, len(body), len(ok)),
            )
            batch_id = cur.lastrowid
            _conn.executemany(
                """INSERT OR IGNORE INTO log_record (
                     batch_id, received_at, service_name, service_instance_id,
                     pipeline_hop, scope_name, time_unix_nano,
                     observed_time_unix_nano, body, event_id, attributes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [(batch_id, received_at, *r) for r in ok],
            )
            _conn.executemany(
                "INSERT INTO log_record_raw (batch_id, received_at, reason, raw) VALUES (?,?,?,?)",
                [(batch_id, received_at, *r) for r in raw],
            )
            _conn.commit()
        except sqlite3.Error as e:
            try:
                _conn.rollback()
            except sqlite3.Error:
                pass
            print(f"[sink] logs storage failure: {e}", flush=True)
            return JSONResponse({"error": "storage unavailable"}, status_code=503)
        except BaseException:
            _conn.rollback()  # never leave the export_batch row in an open transaction
            raise

    print(f"[sink] logs batch {batch_id}: {len(ok)} records, {len(raw)} raw", flush=True)
    return {"partialSuccess": {}}


@app.get("/stats/logs")
def stats_logs():
    """kw-report aggregates over log_record. int64 values are stored as JSON strings, hence CAST."""
    elapsed = "CAST(json_extract(attributes,'$.\"kw.executed.elapsed_time_in_secs\"') AS INTEGER)"
    duration = "CAST(json_extract(attributes,'$.\"kw.timebox.duration\"') AS INTEGER)"
    with _lock:
        cmds = _conn.execute(
            f"""SELECT json_extract(attributes,'$."kw.executed.command_label"'),
                       json_extract(attributes,'$."kw.executed.status"'),
                       COUNT(*), SUM({elapsed}), MIN({elapsed}), MAX({elapsed})
                  FROM log_record
                 WHERE body = 'executed'
              GROUP BY 1, 2 ORDER BY 1, 2"""
        ).fetchall()
        pomo = _conn.execute(
            f"""SELECT json_extract(attributes,'$."kw.timebox.tag"'),
                       COUNT(*), SUM({duration})
                  FROM log_record
                 WHERE body = 'timebox'
              GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    return {
        "statistics": [
            {"command": c[0], "status": c[1], "total": c[2],
             "sum_secs": c[3], "min_secs": c[4], "max_secs": c[5]}
            for c in cmds
        ],
        "pomodoro": [{"tag": p[0], "sessions": p[1], "total_duration": p[2]} for p in pomo],
    }


def _kanon_k():
    try:
        k = int(os.environ.get("KANON_K", "5"))
    except ValueError:
        k = 5
    return max(k, 2)


def _suppress(groups, k, merged_label):
    """groups: {label: {"insts": set, **sums}}. Keep groups with >= k installs; merge the rest
    into one group only if the merge itself reaches k. Returns [(label, group)]."""
    kept = [(g, v) for g, v in sorted(groups.items()) if len(v["insts"]) >= k]
    small = [v for v in groups.values() if len(v["insts"]) < k]
    if small:
        merged = {"insts": set().union(*(v["insts"] for v in small))}
        for key in small[0]:
            if key != "insts":
                merged[key] = (max if key.startswith("max") else sum)(v[key] for v in small)
        if len(merged["insts"]) >= k:
            kept.append((merged_label, merged))
    return kept


_MODULE_COUNT = "CAST(json_extract(attributes,'$.\"kw.build_modules.module_count\"') AS INTEGER)"


@app.get("/analysis/modules")
def analysis_modules():
    k = _kanon_k()
    with _lock:
        per_inst = _conn.execute(
            f"""SELECT CASE WHEN n = 0 THEN '0' WHEN n < 50 THEN '1-49' WHEN n < 200 THEN '50-199'
                        WHEN n < 500 THEN '200-499' ELSE '500+' END AS bucket,
                       service_instance_id, COUNT(*)
                  FROM (SELECT {_MODULE_COUNT} AS n, service_instance_id
                          FROM log_record
                         WHERE body = 'build_modules' AND n IS NOT NULL)
              GROUP BY 1, 2"""
        ).fetchall()
        name_rows = _conn.execute(
            """SELECT service_instance_id,
                      json_extract(attributes,'$."kw.build_modules.module_names"')
                 FROM log_record
                WHERE body = 'build_modules'
                  AND json_extract(attributes,'$."kw.build_modules.module_names"') IS NOT NULL"""
        ).fetchall()

    groups = {}
    for bucket, inst, n in per_inst:
        g = groups.setdefault(bucket, {"insts": set(), "builds": 0})
        g["insts"].add(inst)
        g["builds"] += n
    order = ["0", "1-49", "50-199", "200-499", "500+"]
    kept = sorted(_suppress(groups, k, "suppressed"),
                  key=lambda x: order.index(x[0]) if x[0] in order else len(order))

    by_name = {}
    for inst, names in name_rows:
        for name in str(names).split(","):
            if name:
                by_name.setdefault(name, set()).add(inst)
    names = sorted(((n, len(v)) for n, v in by_name.items() if len(v) >= k),
                   key=lambda x: (-x[1], x[0]))
    return {
        "k": k,
        "buckets": [{"bucket": b, "installs": len(v["insts"]), "builds": v["builds"]} for b, v in kept],
        "names": [{"name": n, "installs": c} for n, c in names],
    }


@app.get("/analysis/commands")
def analysis_commands():
    k = _kanon_k()
    secs = "CAST(json_extract(attributes,'$.\"kw.executed.elapsed_time_in_secs\"') AS INTEGER)"
    with _lock:
        per_inst = _conn.execute(
            f"""SELECT json_extract(attributes,'$."kw.executed.command"'),
                       json_extract(attributes,'$."kw.executed.status"'),
                       service_instance_id, COUNT(*), SUM({secs}), MAX({secs}), COUNT({secs})
                  FROM log_record
                 WHERE body = 'executed'
              GROUP BY 1, 2, 3"""
        ).fetchall()

    groups = {}
    for cmd, status, inst, runs, total, mx, timed in per_inst:
        g = groups.setdefault((cmd or "unknown", status or "unknown"),
                              {"insts": set(), "runs": 0, "timed": 0, "sum_secs": 0, "max_secs": None})
        g["insts"].add(inst)
        g["runs"] += runs
        g["timed"] += timed
        g["sum_secs"] += total or 0
        if mx is not None:
            g["max_secs"] = mx if g["max_secs"] is None else max(g["max_secs"], mx)

    out = []
    for label, v in _suppress({key: {**val, "max_secs": val["max_secs"] or 0}
                               for key, val in groups.items()}, k, ("suppressed", "*")):
        out.append({"command": label[0], "status": label[1], "installs": len(v["insts"]),
                    "runs": v["runs"],
                    "mean_secs": v["sum_secs"] / v["timed"] if v["timed"] else None,
                    "max_secs": v["max_secs"]})
    return {"k": k, "commands": out}
