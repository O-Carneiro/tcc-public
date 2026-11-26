import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import FastAPI, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

DB_PATH = os.environ.get("SINK_DB_PATH", "/data/telemetry.db")

# dedupe_key (D51) = hash of (instance, body, event id); without an event id, of (instance, body, time, attributes).
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

{LOG_RECORD_DDL};

CREATE TABLE IF NOT EXISTS log_record_raw (
  id          INTEGER PRIMARY KEY,
  batch_id    INTEGER NOT NULL REFERENCES export_batch(id),
  received_at TEXT    NOT NULL,
  reason      TEXT,
  raw         TEXT
);

CREATE INDEX IF NOT EXISTS log_record_join ON log_record(body, service_instance_id, event_id);
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


def _now():
    return datetime.now(timezone.utc)


def _week_instant(t):
    """Epoch seconds of the Monday 00:13:00 UTC of the week containing t (floor arithmetic, valid for negative t)."""
    return t - ((t // 86400 + 3) % 7) * 86400 - t % 86400 + 780


def _received_at():
    return datetime.fromtimestamp(_week_instant(int(_now().timestamp())), timezone.utc).isoformat()


def _coarsen_ns(ns):
    return ns if not ns else _week_instant(ns // 10**9) * 10**9


def _dedupe_key(instance, body, event_id, time_ns, attrs_json):
    """D51: an event with an id is keyed by (instance, body, id); otherwise the D42 key."""
    if event_id is not None:
        raw = json.dumps([instance, body, event_id])
    else:
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
    time_ns = _coarsen_ns(_i64(rec.get("timeUnixNano")))
    row = (
        *texts,
        time_ns,
        _coarsen_ns(_i64(rec.get("observedTimeUnixNano"))),
        body,
        event_id,
        attrs_json,
        _dedupe_key(texts[1], body, event_id, time_ns, attrs_json),
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
                 FROM log_record_old ORDER BY id"""
        )
        conn.executemany(
            """INSERT OR IGNORE INTO log_record (id, batch_id, received_at, service_name,
                 service_instance_id, pipeline_hop, scope_name, time_unix_nano,
                 observed_time_unix_nano, body, event_id, attributes, dedupe_key)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ((*r, _dedupe_key(r[4], r[9], r[10], r[7], r[11])) for r in old),
        )
        conn.execute("DROP TABLE log_record_old")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _rekey(conn):
    """Recompute every log_record dedupe_key (D51); of rows that collide keep the lowest id."""
    conn.execute("BEGIN")
    try:
        seen, drop, new = set(), [], []
        rows = conn.execute(
            "SELECT id, service_instance_id, body, event_id, time_unix_nano, attributes, dedupe_key"
            " FROM log_record ORDER BY id"
        )
        for id_, inst, body, event_id, time_ns, attrs, old in rows:
            key = _dedupe_key(inst, body, event_id, time_ns, attrs)
            if key in seen:
                drop.append((id_,))
                continue
            seen.add(key)
            if key != old:
                new.append((key, id_))
        conn.executemany("DELETE FROM log_record WHERE id = ?", drop)
        # keys may be permuted between rows: park them on unique placeholders first (the column is NOT NULL)
        conn.executemany("UPDATE log_record SET dedupe_key = 'rekey:' || id WHERE id = ?", [(i,) for _, i in new])
        conn.executemany("UPDATE log_record SET dedupe_key = ? WHERE id = ?", new)
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
    _rekey(_conn)
    print(f"[sink] writing to {DB_PATH}", flush=True)


@app.get("/healthz")
def healthz():
    return {"ok": True}


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
    received_at = _received_at()

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


_RESERVED = {"week", "install", "records", "installs", "measured", "non_numeric", "sum", "mean",
             "min", "max", "key"}
_MAX_GROUPS = 1000
# SQLite integer division truncates toward zero; this floors, so pre-1970 times land in the right day.
_FLOOR_SECS = ("(time_unix_nano - ((time_unix_nano % 1000000000) + 1000000000) % 1000000000)"
               " / 1000000000")
# The Monday date of the primary record's time (alias p).
_WEEK = f"date({_FLOOR_SECS}, 'unixepoch', 'weekday 0', '-6 days')".replace("time_unix_nano", "p.time_unix_nano")
# json_type distinguishes JSON true/false, which json_extract flattens to 1/0.
_AS_TEXT = (
    "CASE json_type(attributes, ?) WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' "
    "ELSE CAST(json_extract(attributes, ?) AS TEXT) END"
)
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", re.ASCII)
# Install rank: by each non-empty install's lowest log_record id, over the whole table.
_RANKS = ("WITH rk(inst, r) AS (SELECT service_instance_id, ROW_NUMBER() OVER (ORDER BY MIN(id))"
          " FROM log_record WHERE service_instance_id <> '' GROUP BY service_instance_id) ")


def _finite(x):
    return None if isinstance(x, float) and not math.isfinite(x) else x


def _path(name):
    # SQLite parses the quoted label as a JSON string, so a backslash must be doubled to stay literal.
    return '$."' + name.replace("\\", "\\\\") + '"'


@app.get("/analysis/schemas")
def analysis_schemas():
    with _lock:
        rows = _conn.execute(
            """SELECT body, COUNT(*), COUNT(DISTINCT NULLIF(service_instance_id, ''))
                 FROM log_record WHERE body IS NOT NULL GROUP BY body ORDER BY body"""
        ).fetchall()
    return {"schemas": [{"schema": b, "records": n, "installs": inst} for b, n, inst in rows]}


def _attribute_counts(schema):
    return _conn.execute(
        """SELECT j.key, COUNT(*) FROM log_record l, json_each(l.attributes) j
            WHERE l.body = ? GROUP BY j.key ORDER BY j.key""", (schema,)).fetchall()


@app.get("/analysis/schema/{schema}/attributes")
def analysis_attributes(schema: str):
    with _lock:
        present = _conn.execute("SELECT 1 FROM log_record WHERE body = ? LIMIT 1", (schema,)).fetchone()
        rows = _attribute_counts(schema) if present else []
    if not present:
        return JSONResponse({"error": f"unknown schema: {schema}"}, status_code=404)
    return {"schema": schema, "attributes": [{"name": k, "records": n} for k, n in rows]}


def _bad(reason):
    return JSONResponse({"error": reason}, status_code=400)


def _bound(name, value):
    """Epoch seconds of a strict YYYY-MM-DD date (UTC midnight), or None."""
    if value is None:
        return None
    if not _ISO_DATE.fullmatch(value):
        raise ValueError(f"{name} must be YYYY-MM-DD: {value}")
    try:
        return (date.fromisoformat(value) - date(1970, 1, 1)).days * 86400
    except ValueError:
        raise ValueError(f"{name} is not a real date: {value}") from None


@app.get("/analysis/query")
def analysis_query(schema: str | None = None, join: str | None = None, by: str | None = None,
                   measure: str | None = None, bucket: str | None = None,
                   where: Annotated[list[str] | None, Query()] = None,
                   from_: Annotated[str | None, Query(alias="from")] = None, to: str | None = None):
    if schema is None:
        return _bad("missing schema")
    joins = join.split(",") if join is not None else []
    if len(joins) > 63:
        return _bad("join takes at most 63 schemas")
    names = by.split(",") if by else []
    wheres = where or []
    if len(set(names)) != len(names):
        return _bad("duplicate attribute in by")
    items = bucket.split(",") if bucket is not None else []
    if bucket is not None and (len(set(items)) != len(items) or not set(items) <= {"week", "install"}):
        return _bad("bucket must be week and/or install")
    buckets = [b for b in ("week", "install") if b in items]
    conds = []  # (attribute, value)
    for w in wheres:
        name, colon, value = w.partition(":")
        if not colon:
            return _bad(f"where must be attribute:value: {w}")
        conds.append((name, value))
    try:
        lo, hi = _bound("from", from_), _bound("to", to)
    except ValueError as e:
        return _bad(str(e))

    with _lock:
        present = {r[0] for r in _conn.execute("SELECT DISTINCT body FROM log_record")}
        if schema not in present:
            return _bad("unknown schema")
        for n, j in enumerate(joins):
            if j in joins[:n] or j == schema or not j:
                return _bad(f"join must name distinct other schemas: {j!r}")
            if j not in present:
                return _bad("unknown schema (join)")
        # Each name resolves once: the primary schema if it has it, else the first joined one that does.
        keys = {}  # alias -> discovered names, read only when a name needs that schema

        def owner(name):
            for n, s in enumerate([schema, *joins]):
                a = "p" if n == 0 else f"j{n - 1}"
                if a not in keys:
                    keys[a] = {k for k, _ in _attribute_counts(s)}
                if name in keys[a]:
                    return a

        alias = {}
        for name in (*names, *([measure] if measure is not None else []), *(c[0] for c in conds)):
            alias[name] = None if '"' in name else owner(name)
            if alias[name] is None:
                return _bad(f"unknown attribute for {schema}: {name}")

        def col(name, expr):  # qualify the unqualified `attributes` of a fragment
            return expr.replace("attributes", f"{alias[name]}.attributes")

        # Fragments are fixed text; names and schemas only ever travel as bound parameters.
        sel, sel_p, cols = [], [], []
        if "week" in buckets:
            sel.append(f"{_WEEK} AS w")
            cols.append("w")
        if "install" in buckets:
            sel.append("rk.r AS i")
            cols.append("i")
        for n, name in enumerate(names):
            sel.append(f"{col(name, _AS_TEXT)} AS g{n}")
            sel_p += [_path(name)] * 2
            cols.append(f"g{n}")
        outer = [*cols, "COUNT(*)", "COUNT(DISTINCT inst)"]
        if measure is not None:
            sel.append(col(measure, "kw_num(json_type(attributes, ?), json_extract(attributes, ?)) AS m"))
            sel.append(col(measure, "NULLIF(json_type(attributes, ?), 'null') AS pt"))
            sel_p += [_path(measure)] * 3
            outer += ["COUNT(m)", "COUNT(pt)", "SUM(m)", "MIN(m)", "MAX(m)"]
        src, src_p, where_sql, where_p = ["log_record p"], [], ["p.body = ?"], [schema]
        for n, j in enumerate(joins):
            src.append(f"LEFT JOIN log_record j{n} ON j{n}.body = ? AND j{n}.event_id = p.event_id"
                       f" AND j{n}.service_instance_id = p.service_instance_id AND p.service_instance_id <> ''")
            src_p.append(j)
        if "install" in buckets:
            src.append("LEFT JOIN rk ON rk.inst = p.service_instance_id")
        for bound, op in ((lo, ">="), (hi, "<=")):
            if bound is not None:
                where_sql.append(f"CAST(strftime('%s', {_WEEK}) AS INTEGER) {op} ?")
                where_p.append(bound)
        for name, value in conds:
            where_sql.append(f"{col(name, _AS_TEXT)} = ?")
            where_p += [_path(name)] * 2 + [value]
        sql = (f"{_RANKS if 'install' in buckets else ''}SELECT {', '.join(outer)} FROM "
               f"(SELECT NULLIF(p.service_instance_id, '') AS inst{''.join(', ' + c for c in sel)} "
               f"FROM {' '.join(src)} WHERE {' AND '.join(where_sql)})")
        params = [*sel_p, *src_p, *where_p]
        if cols:
            sql += (" GROUP BY " + ", ".join(cols) + " ORDER BY "
                    + ", ".join(f"{c} IS NULL, {c}" for c in cols) + " LIMIT ?")
            params.append(_MAX_GROUPS + 1)
        try:
            rows = _conn.execute(sql, params).fetchall()
        except sqlite3.Error as e:
            if "too many" not in str(e) and "at most" not in str(e):
                raise
            return _bad(f"query too large for sqlite: {e}")

    cut = len(cols)
    rows = [r for r in rows if r[cut]]  # nothing matched: no group
    truncated = len(rows) > _MAX_GROUPS
    keyed = any(n in _RESERVED for n in names)  # a reserved name would overwrite a stat field
    groups = []
    for row in rows[:_MAX_GROUPS]:
        vals = dict(zip(cols, row[:cut]))
        g = {}
        if "week" in buckets:
            g["week"] = vals["w"]
        if "install" in buckets:
            g["install"] = None if vals["i"] is None else f"install {vals['i']}"
        group_vals = {name: vals[f"g{n}"] for n, name in enumerate(names)}
        if keyed:
            g["key"] = group_vals
        else:
            g.update(group_vals)
        g["records"], g["installs"] = row[cut], row[cut + 1]
        if measure is not None:
            measured, carried, total, mn, mx = row[cut + 2:]
            total, mn, mx = _finite(total), _finite(mn), _finite(mx)
            g.update(measured=measured, non_numeric=carried - measured, sum=total,
                     mean=_finite(total / measured) if measured and total is not None else None,
                     min=mn, max=mx)
        groups.append(g)
    return {"schema": schema, "join": joins, "by": names, "bucket": buckets, "measure": measure,
            "where": wheres, "from": from_, "to": to, "keyed": keyed, "groups": groups,
            "truncated": truncated}
