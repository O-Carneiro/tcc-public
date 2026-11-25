import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

DB_PATH = os.environ.get("SINK_DB_PATH", "/data/telemetry.db")

# dedupe_key = hash of (instance, body, time, attributes JSON): a retry is one row, while two rows
# of one event that differ in attributes are two rows. NULL columns cannot serve in a UNIQUE key.
LOG_RECORD_DDL = """CREATE TABLE IF NOT EXISTS log_record (
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
  dedupe_key              TEXT    NOT NULL UNIQUE
)"""

SCHEMA = f"""
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

{LOG_RECORD_DDL};

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


_DIGITS = re.compile(r"-?[0-9]+", re.ASCII)  # fullmatch: no trailing newline slips through
_INT64 = range(-(2**63), 2**63)


def _int64_ok(val):
    if isinstance(val, str) and not _DIGITS.fullmatch(val):
        return False
    return int(val) in _INT64


def _scalar(key, val):
    """Strict check for logs (D28): a value sqlite or json cannot take raises, and the record goes raw."""
    if key in ("stringValue", "bytesValue"):
        ok = isinstance(val, str)
    elif key == "boolValue":
        ok = isinstance(val, bool)
    elif key == "intValue":
        ok = isinstance(val, (int, str)) and not isinstance(val, bool) and _int64_ok(val)
    else:
        ok = isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val)
    if not ok:
        raise ValueError(f"bad {key}")
    return val


def _anyvalue(v, strict=False):
    if strict and not isinstance(v, dict):
        raise TypeError("AnyValue is not an object")
    for key in ("stringValue", "boolValue", "intValue", "doubleValue", "bytesValue"):
        if key in v:  # bytesValue stays its base64 string
            return _scalar(key, v[key]) if strict else v[key]
    if "arrayValue" in v:
        values = v["arrayValue"].get("values", [])
        if strict and not isinstance(values, list):
            raise TypeError("arrayValue.values is not a list")
        return [_anyvalue(x, strict) for x in values]
    if "kvlistValue" in v:
        return _attrs(v["kvlistValue"].get("values", []), strict)
    return None


def _attrs(kvs, strict=False):
    if strict and not isinstance(kvs, list):
        raise TypeError("attributes is not a list")
    out = {}
    for kv in kvs:
        if strict and not (isinstance(kv, dict) and isinstance(kv.get("key"), str)):
            raise TypeError("bad key/value entry")
        out[kv["key"]] = _anyvalue(kv.get("value", {}), strict)
    return out


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
    """OTLP/JSON int64: a JSON int or a decimal-digit string; None when absent."""
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, str)):
        raise TypeError("int64 field has the wrong type")
    if isinstance(v, str) and not _DIGITS.fullmatch(v):
        raise ValueError("int64 string is not decimal")
    n = int(v)
    if not -(2**63) <= n < 2**63:
        raise ValueError("int64 out of range")
    return n


def _check_text(x):
    """A lone surrogate or NUL anywhere in a name or string sends the record to raw (D28);
    json.dumps would ASCII-escape a surrogate and hide it until sqlite or the reader chokes."""
    if isinstance(x, str):
        x.encode("utf-8")
        if "\x00" in x:
            raise ValueError("NUL in text")
    elif isinstance(x, list):
        for v in x:
            _check_text(v)
    elif isinstance(x, dict):
        for k, v in x.items():
            _check_text(k)
            _check_text(v)


def _event_id(rec):
    """kw.event.id is accepted only as an intValue (string or int); None if absent."""
    for kv in rec.get("attributes") or []:
        if kv.get("key") == "kw.event.id":
            value = kv.get("value")
            if not (isinstance(value, dict) and set(value) == {"intValue"}):
                raise TypeError("kw.event.id is not an intValue")
            return _i64(value["intValue"])
    return None


def _dedupe_key(instance, body, time_ns, attrs_json):
    raw = json.dumps([instance, body, time_ns, attrs_json])
    return hashlib.sha256(raw.encode("ascii")).hexdigest()


def _log_row(resource, scope, rec):
    """One log_record row; raises ValueError/TypeError/KeyError/AttributeError if rec does not fit."""
    attrs = _attrs(rec.get("attributes") or [], strict=True)
    body = rec.get("body") or {}
    if not isinstance(body, dict):
        raise TypeError("body is not an object")
    body = _anyvalue(body, strict=True) if "stringValue" in body else (
        json.dumps(_anyvalue(body, strict=True)) if body else None
    )
    _check_text(attrs)
    event_id = _event_id(rec)
    attrs_json = json.dumps(attrs, sort_keys=True)
    texts = (
        resource.get("service.name"),
        resource.get("service.instance.id"),
        resource.get("pipeline.hop"),
        scope.get("name"),
    )
    time_ns = _i64(rec.get("timeUnixNano"))
    row = (
        *texts,
        time_ns,
        _i64(rec.get("observedTimeUnixNano")),
        body,
        event_id,
        attrs_json,
        _dedupe_key(texts[1], body, time_ns, attrs_json),
    )
    for v in (*texts, body):
        if v is not None and not isinstance(v, str):
            raise TypeError("text column is not a string")
        _check_text(v)
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
            resource = _attrs(rl.get("resource", {}).get("attributes", []), strict=True)
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


def _migrate(conn):
    """Old log_record had UNIQUE (instance, event_id, body), which drops a second row of one event.
    SQLite cannot drop a table constraint, so copy into a new table; rolls back whole on failure."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(log_record)")]
    if not cols or "dedupe_key" in cols:
        return
    conn.execute("BEGIN")
    try:
        conn.execute("ALTER TABLE log_record RENAME TO log_record_old")
        conn.execute(LOG_RECORD_DDL)
        old = conn.execute(
            """SELECT id, batch_id, received_at, service_name, service_instance_id, pipeline_hop,
                      scope_name, time_unix_nano, observed_time_unix_nano, body, event_id, attributes
                 FROM log_record_old"""
        )
        conn.executemany(
            """INSERT OR IGNORE INTO log_record (id, batch_id, received_at, service_name,
                 service_instance_id, pipeline_hop, scope_name, time_unix_nano,
                 observed_time_unix_nano, body, event_id, attributes, dedupe_key)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ((*r, _dedupe_key(r[4], r[9], r[7], r[11])) for r in old),
        )
        conn.execute("DROP TABLE log_record_old")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


_NUMBER = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?")


def _kw_num(json_type, value):
    """SQL function: the numeric value of an attribute, or NULL if it is not numeric."""
    if json_type in ("integer", "real"):
        return float(value)
    if json_type == "text" and len(value) <= 64 and _NUMBER.fullmatch(value):
        f = float(value)
        return f if math.isfinite(f) else None
    return None


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
    _conn.create_function("kw_num", 2, _kw_num, deterministic=True)
    _migrate(_conn)
    _conn.executescript(SCHEMA)
    _conn.commit()
    print(f"[sink] writing to {DB_PATH}", flush=True)


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/v1/metrics")
async def v1_metrics(request: Request):
    body = await request.body()
    gz = request.headers.get("content-encoding", "").lower() == "gzip"
    return await run_in_threadpool(_store_metrics, body, gz)


def _store_metrics(body, gz):
    if gz:
        body = gzip.decompress(body)

    rows = list(_flatten(json.loads(body)))
    received_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with _lock:
        try:
            batch_id = _insert_metrics(rows, received_at, len(body))
        except BaseException:
            _conn.rollback()  # never leave the export_batch row in an open transaction
            raise

    print(f"[sink] batch {batch_id}: {len(rows)} points", flush=True)
    return {"partialSuccess": {}}


def _insert_metrics(rows, received_at, size):
    cur = _conn.execute(
        "INSERT INTO export_batch (received_at, body_bytes, point_count) VALUES (?,?,?)",
        (received_at, size, len(rows)),
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
    return batch_id


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
                "min_secs": _finite(c[4]),
                "max_secs": _finite(c[5]),
                "avg_secs": _finite(round(c[3] / c[2], 2)) if c[2] and c[3] is not None else None,
            }
            for c in cmds
        ],
        "pomodoro": [{"tag": p[0], "focus_secs": _finite(p[1]), "sessions": _finite(p[2])} for p in pomo],
    }


@app.post("/v1/logs")
async def v1_logs(request: Request):
    body = await request.body()
    gz = request.headers.get("content-encoding", "").lower() == "gzip"
    return await run_in_threadpool(_store_logs, body, gz)


def _store_logs(body, gz):
    if gz:
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
                     observed_time_unix_nano, body, event_id, attributes, dedupe_key)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
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
        # SUM raises on int64 overflow; TOTAL (float) is the fallback, exact sums stay integers.
        for attempt, total in enumerate((lambda x: f"SUM({x})",
                                         lambda x: f"CASE WHEN COUNT({x}) THEN TOTAL({x}) END")):
            try:
                cmds = _conn.execute(
                    f"""SELECT json_extract(attributes,'$."kw.executed.command_label"'),
                               json_extract(attributes,'$."kw.executed.status"'),
                               COUNT(*), {total(elapsed)}, MIN({elapsed}), MAX({elapsed})
                          FROM log_record
                         WHERE body = 'executed'
                      GROUP BY 1, 2 ORDER BY 1, 2"""
                ).fetchall()
                pomo = _conn.execute(
                    f"""SELECT json_extract(attributes,'$."kw.timebox.tag"'),
                               COUNT(*), {total(duration)}
                          FROM log_record
                         WHERE body = 'timebox'
                      GROUP BY 1 ORDER BY 1"""
                ).fetchall()
                break
            except sqlite3.OperationalError as e:
                if attempt == 1 or "overflow" not in str(e):
                    raise
    return {
        "statistics": [
            {"command": c[0], "status": c[1], "total": c[2],
             "sum_secs": _finite(c[3]), "min_secs": c[4], "max_secs": c[5]}
            for c in cmds
        ],
        "pomodoro": [{"tag": p[0], "sessions": p[1], "total_duration": _finite(p[2])} for p in pomo],
    }


_RESERVED = {"bucket", "records", "installs", "measured", "non_numeric", "sum", "mean", "min",
             "max", "key"}
_MAX_GROUPS = 1000
# SQLite integer division truncates toward zero; this floors, so pre-1970 times land in the right day.
_FLOOR_SECS = ("(time_unix_nano - ((time_unix_nano % 1000000000) + 1000000000) % 1000000000)"
               " / 1000000000")
_BUCKETS = {
    "day": f"date({_FLOOR_SECS}, 'unixepoch')",
    "week": f"date({_FLOOR_SECS}, 'unixepoch', 'weekday 0', '-6 days')",  # the Monday
}
# json_type distinguishes JSON true/false, which json_extract flattens to 1/0.
_AS_TEXT = (
    "CASE json_type(attributes, ?) WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' "
    "ELSE CAST(json_extract(attributes, ?) AS TEXT) END"
)


def _finite(x):
    return None if isinstance(x, float) and not math.isfinite(x) else x


def _path(name):
    # SQLite parses the quoted label as a JSON string, so a backslash must be doubled to stay literal.
    return '$."' + name.replace("\\", "\\\\") + '"'


@app.get("/analysis/schema")
def analysis_schema():
    with _lock:
        kinds = _conn.execute(
            """SELECT body, COUNT(*), COUNT(DISTINCT NULLIF(service_instance_id, ''))
                 FROM log_record WHERE body IS NOT NULL GROUP BY body ORDER BY body"""
        ).fetchall()
        attrs = _conn.execute(
            """SELECT l.body, j.key, COUNT(*)
                 FROM log_record l, json_each(l.attributes) j
                WHERE l.body IS NOT NULL GROUP BY l.body, j.key ORDER BY l.body, j.key"""
        ).fetchall()
    by_kind = {}
    for kind, name, n in attrs:
        by_kind.setdefault(kind, []).append({"name": name, "records": n})
    return {"kinds": [{"kind": k, "records": n, "installs": inst, "attributes": by_kind.get(k, [])}
                      for k, n, inst in kinds]}


def _bad(reason):
    return JSONResponse({"error": reason}, status_code=400)


@app.get("/analysis/group")
def analysis_group(kind: str | None = None, by: str | None = None,
                   measure: str | None = None, bucket: str | None = None):
    if kind is None:
        return _bad("missing kind")
    if bucket is not None and bucket not in _BUCKETS:
        return _bad("bucket must be day or week")
    names = by.split(",") if by else []
    if len(names) > 3:
        return _bad("by takes at most 3 attributes")
    if len(set(names)) != len(names):
        return _bad("duplicate attribute in by")

    with _lock:
        known = {r[0] for r in _conn.execute(
            """SELECT DISTINCT j.key FROM log_record l, json_each(l.attributes) j
                WHERE l.body = ?""", (kind,))}
        present = _conn.execute(
            "SELECT 1 FROM log_record WHERE body = ? LIMIT 1", (kind,)).fetchone()
        if not present:
            return _bad(f"unknown kind: {kind}")
        for n in (*names, *([measure] if measure is not None else [])):
            if n not in known or '"' in n:
                return _bad(f"unknown attribute for {kind}: {n}")

        # Fragments are fixed text chosen by flags; names and kind only ever travel as bound
        # parameters. Group columns: bucket first, then g0..g2.
        inner, params, cols = [], [], []
        if bucket:
            inner.append(f"{_BUCKETS[bucket]} AS bucket")
            cols.append("bucket")
        for n, name in enumerate(names):
            inner.append(f"{_AS_TEXT} AS g{n}")
            params += [_path(name)] * 2
            cols.append(f"g{n}")
        outer = [*cols, "COUNT(*)", "COUNT(DISTINCT inst)"]
        if measure is not None:
            inner.append("kw_num(json_type(attributes, ?), json_extract(attributes, ?)) AS m")
            inner.append("NULLIF(json_type(attributes, ?), 'null') AS p")
            params += [_path(measure)] * 3
            outer += ["COUNT(m)", "COUNT(p)", "SUM(m)", "MIN(m)", "MAX(m)"]
        sql = (f"SELECT {', '.join(outer)} FROM (SELECT NULLIF(service_instance_id, '') AS inst"
               f"{''.join(', ' + c for c in inner)} FROM log_record WHERE body = ?)")
        params.append(kind)
        if cols:
            sql += (" GROUP BY " + ", ".join(cols) + " ORDER BY "
                    + ", ".join(f"{c} IS NULL, {c}" for c in cols) + " LIMIT ?")
            params.append(_MAX_GROUPS + 1)
        rows = _conn.execute(sql, params).fetchall()

    truncated = len(rows) > _MAX_GROUPS
    keyed = any(n in _RESERVED for n in names)  # a reserved name would overwrite a stat field
    groups = []
    for row in rows[:_MAX_GROUPS]:
        cut = len(cols)
        vals = dict(zip(cols, row[:cut]))
        g = {"bucket": vals["bucket"]} if bucket else {}
        group_vals = {name: vals[f"g{n}"] for n, name in enumerate(names)}
        if keyed:
            g["key"] = group_vals
        else:
            g.update(group_vals)
        g["records"], g["installs"] = row[cut], row[cut + 1]
        if measure is not None:
            measured, carried, total, lo, hi = row[cut + 2:]
            total, lo, hi = _finite(total), _finite(lo), _finite(hi)
            g.update(measured=measured, non_numeric=carried - measured, sum=total,
                     mean=_finite(total / measured) if measured and total is not None else None,
                     min=lo, max=hi)
        groups.append(g)
    return {"kind": kind, "by": names, "measure": measure, "bucket": bucket,
            "keyed": keyed, "truncated": truncated, "groups": groups}
