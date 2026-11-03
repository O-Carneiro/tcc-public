import gzip
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request

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
