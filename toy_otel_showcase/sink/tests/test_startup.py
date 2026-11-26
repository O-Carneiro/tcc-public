"""Startup and liveness: /healthz while writes block, migration of an old DB, the D51 rekey."""
import hashlib
import json
import os
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from conftest import executed, fresh_rows, payload, post_json, record, s, sink_app

pytestmark = pytest.mark.usefixtures("raw_times")


def count(db_path):
    return fresh_rows(db_path, "SELECT COUNT(*) FROM log_record")[0][0]


def test_healthz_answers_while_a_write_lock_is_held(client, db_path):
    holder = sqlite3.connect(db_path, timeout=0)
    holder.execute("BEGIN EXCLUSIVE")
    done = {}
    t = threading.Thread(target=lambda: done.update(r=post_json(client, payload([executed(1)]))))
    t.start()
    try:
        time.sleep(0.5)  # the post is now waiting on sqlite's busy timeout
        t0 = time.monotonic()
        assert client.get("/healthz").status_code == 200
        elapsed = time.monotonic() - t0
    finally:
        holder.rollback()
        holder.close()
        t.join(30)
    assert elapsed < 1.0, f"/healthz took {elapsed:.2f}s: a handler blocks the event loop"


# --- migration of a database created by an older sink ---

OLD_SCHEMA = """
CREATE TABLE export_batch (id INTEGER PRIMARY KEY, received_at TEXT NOT NULL,
  body_bytes INTEGER NOT NULL, point_count INTEGER NOT NULL);
CREATE TABLE log_record (
  id INTEGER PRIMARY KEY,
  batch_id INTEGER NOT NULL REFERENCES export_batch(id),
  received_at TEXT NOT NULL, service_name TEXT, service_instance_id TEXT,
  pipeline_hop TEXT, scope_name TEXT, time_unix_nano INTEGER,
  observed_time_unix_nano INTEGER, body TEXT, event_id INTEGER, attributes TEXT,
  UNIQUE (service_instance_id, event_id, body)
);
"""


def test_old_schema_db_is_migrated_keeps_rows_and_serves_queries(db_path):
    import os
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(OLD_SCHEMA)
    con.execute("INSERT INTO export_batch VALUES (1, '2026-01-01T00:00:00+00:00', 10, 3)")
    for k in range(3):
        con.execute(
            "INSERT INTO log_record (batch_id, received_at, service_name, service_instance_id,"
            " body, event_id, time_unix_nano, attributes) VALUES (1,'t','kworkflow','old',"
            "'executed',?,?,?)",
            (k, 1700000000000000000, json.dumps({"kw.event.id": str(k), "kw.old": "yes"})))
    # rows that share everything except the event id or the attributes are distinct records
    for row_id, event_id, attrs in [(5, None, {"a": "1"}), (9, None, {"a": "2"}), (12, 1, {}), (20, 2, {})]:
        con.execute(
            "INSERT INTO log_record (id, batch_id, received_at, service_instance_id, body, event_id,"
            " time_unix_nano, attributes) VALUES (?,1,'t','other','executed',?,?,?)",
            (row_id, event_id, 1700000000000000000, json.dumps(attrs)))
    con.commit()
    con.close()

    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        assert fresh_rows(db_path, "SELECT id FROM log_record WHERE service_instance_id = 'other'"
                                   " ORDER BY id") == [(5,), (9,), (12,), (20,)]
        assert count(db_path) == 7
        groups = c.get("/analysis/query", params={"schema": "executed", "by": "kw.old"}).json()["groups"]
        assert groups == [{"kw.old": "yes", "records": 3, "installs": 1},
                          {"kw.old": None, "records": 4, "installs": 1}]
        # the old UNIQUE(instance, event, body) would have dropped the second row
        a = record("executed", 10, {"kw.new": s("a")})
        b = record("executed", 11, {"kw.new": s("b")})
        for _ in range(2):  # the retry adds nothing
            assert post_json(c, payload([a, b], instance="old")).status_code == 200
        assert count(db_path) == 9


# --- D51: a DB keyed the D42 way is rekeyed once at startup ---

def _d42_key(instance, body, time_ns, attrs_json):
    return hashlib.sha256(json.dumps([instance, body, time_ns, attrs_json]).encode("ascii")).hexdigest()


def _d51_key(instance, body, event_id):
    return hashlib.sha256(json.dumps([instance, body, event_id]).encode("ascii")).hexdigest()


def _db_with_schema(db_path, schema):
    import os
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(schema)
    con.execute("INSERT INTO export_batch VALUES (1, '2026-01-01T00:00:00+00:00', 10, 3)")
    return con


def _put(con, row_id, instance, event_id, t, extra, key=None):
    attrs = {"kw.x": extra} | ({} if event_id is None else {"kw.event.id": str(event_id)})
    attrs = json.dumps(attrs, sort_keys=True)
    con.execute(
        "INSERT INTO log_record (id, batch_id, received_at, service_name, service_instance_id,"
        " scope_name, time_unix_nano, body, event_id, attributes, dedupe_key)"
        " VALUES (?,1,'t','kworkflow',?,'sc',?,'executed',?,?,?)",
        (row_id, instance, t, event_id, attrs, key or _d42_key(instance, "executed", t, attrs)))
    return attrs


def _start_twice(db_path, query):
    out = []
    for _ in range(2):
        with TestClient(sink_app.app, raise_server_exceptions=False):
            pass
        out.append(fresh_rows(db_path, query))
    return out


def test_d42_keyed_db_is_rekeyed_at_startup_and_a_second_start_changes_nothing(db_path):
    con = _db_with_schema(db_path, sink_app.SCHEMA)
    T = 1700000000000000000
    # (id, instance, event_id, time): 1,2 one event re-sent with another time; 3 another event;
    # 4,5 same event id and body on two installs; 6,7,8 three-way collision; 9 has no event id
    rows = [(1, "inst", 7, T), (2, "inst", 7, T + 10**12), (3, "inst", 8, T),
            (4, "a", 5, T), (5, "b", 5, T), (6, "inst", 9, T), (7, "inst", 9, T + 1),
            (8, "inst", 9, T + 2), (9, "inst", None, T + 3)]
    attrs = {r[0]: _put(con, *r, extra=str(r[0])) for r in rows}
    con.commit()
    con.close()
    survivors = [r for r in rows if r[0] not in (2, 7, 8)]
    before = fresh_rows(db_path, "SELECT id, service_instance_id, time_unix_nano, event_id, attributes"
                                 " FROM log_record WHERE id IN (1,3,4,5,6,9) ORDER BY id")

    keys = "SELECT id, dedupe_key FROM log_record ORDER BY id"
    expected = [(r[0], _d51_key(r[1], "executed", r[2]) if r[2] is not None
                 else _d42_key(r[1], "executed", r[3], attrs[r[0]])) for r in survivors]
    first, second = _start_twice(db_path, keys)
    assert first == second == expected
    after = fresh_rows(db_path, "SELECT id, service_instance_id, time_unix_nano, event_id, attributes"
                                " FROM log_record ORDER BY id")
    assert after == before


def test_rekey_survives_stored_keys_swapped_between_two_rows(db_path):
    con = _db_with_schema(db_path, sink_app.SCHEMA)
    T = 1700000000000000000
    _put(con, 1, "inst", 7, T, "a", key=_d51_key("inst", "executed", 8))
    _put(con, 2, "inst", 8, T, "b", key=_d51_key("inst", "executed", 7))
    con.commit()
    con.close()
    first, second = _start_twice(db_path, "SELECT id, dedupe_key FROM log_record ORDER BY id")
    want = [(1, _d51_key("inst", "executed", 7)), (2, _d51_key("inst", "executed", 8))]
    assert first == second == want


def test_pre_dedupe_key_db_with_two_rows_of_one_event_keeps_the_lowest_id(db_path):
    # an older schema without the UNIQUE (instance, event_id, body) that later versions added
    con = _db_with_schema(db_path, OLD_SCHEMA.replace(",\n  UNIQUE (service_instance_id, event_id, body)", ""))
    for row_id, t in [(2, 1700000000000000001), (1, 1700000000000000000)]:
        con.execute(
            "INSERT INTO log_record (id, batch_id, received_at, service_name, service_instance_id,"
            " body, event_id, time_unix_nano, attributes) VALUES (?,1,'t','kworkflow','old',"
            "'executed',7,?,?)", (row_id, t, json.dumps({"kw.event.id": "7", "t": str(t)})))
    con.commit()
    con.close()
    first, second = _start_twice(db_path, "SELECT id, dedupe_key FROM log_record ORDER BY id")
    assert first == second == [(1, _d51_key("old", "executed", 7))]
