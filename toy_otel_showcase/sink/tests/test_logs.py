import gzip
import json
import sqlite3
import threading
import time

from fastapi.testclient import TestClient

from conftest import (eid, executed, fresh_rows, i, payload, post_json, post_recs, record, s,
                      sink_app, timebox)

COLUMNS = {
    "id", "batch_id", "received_at", "service_name", "service_instance_id",
    "pipeline_hop", "scope_name", "time_unix_nano", "observed_time_unix_nano",
    "body", "event_id", "attributes",
}


def count(db_path, table="log_record"):
    return fresh_rows(db_path, f"SELECT COUNT(*) FROM {table}")[0][0]


def raw_blob(db_path):
    """All text/blob content of log_record_raw (its column names are not in the contract)."""
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute("SELECT * FROM log_record_raw").fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        for v in r:
            if isinstance(v, bytes):
                v = v.decode("utf-8", "replace")
            if isinstance(v, str):
                out.append(v)
    return "\n".join(out)


def test_fixture_selftest(client, db_path):
    # Fixtures point the app at a temp DB and the existing sink still works.
    assert client.get("/healthz").status_code == 200
    assert fresh_rows(db_path, "SELECT COUNT(*) FROM export_batch")[0][0] == 0


def test_logs_stores_record_with_contract_columns(client, db_path):
    r = post_json(client, payload([executed(7, "build", "success", 12)], instance="abc"))
    assert r.status_code == 200
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM log_record").fetchall()
    con.close()
    assert len(rows) == 1
    row = rows[0]
    assert COLUMNS <= set(row.keys())
    assert row["service_name"] == "kworkflow"
    assert row["service_instance_id"] == "abc"
    assert row["scope_name"] == "kw_tele"
    assert row["body"] == "executed"
    assert int(row["event_id"]) == 7
    assert row["time_unix_nano"] is not None
    attrs = json.loads(row["attributes"])
    assert "kw.executed.command_label" in json.dumps(attrs)


def test_logs_accepts_gzip(client, db_path):
    body = gzip.compress(json.dumps(payload([executed(1)])).encode())
    r = client.post("/v1/logs", content=body,
                    headers={"content-type": "application/json", "content-encoding": "gzip"})
    assert r.status_code == 200
    assert count(db_path) == 1


def test_dedupe_same_instance_event_body(client, db_path):
    p = payload([executed(1)])
    assert post_json(client, p).status_code == 200
    assert post_json(client, p).status_code == 200
    assert count(db_path) == 1


def test_same_event_id_other_instance_is_not_a_duplicate(client, db_path):
    post_json(client, payload([executed(1)], instance="a"))
    post_json(client, payload([executed(1)], instance="b"))
    assert count(db_path) == 2


def test_unknown_attributes_are_stored(client, db_path):
    rec = executed(1)
    rec["attributes"].append({"key": "kw.hardware.gpu", "value": s("rtx")})
    rec["attributes"].append({"key": "kw.future.count", "value": i(5)})
    assert post_json(client, payload([rec])).status_code == 200
    stored = fresh_rows(db_path, "SELECT attributes FROM log_record")[0][0]
    assert "kw.hardware.gpu" in stored and "rtx" in stored
    assert "kw.future.count" in stored


def test_non_json_body_goes_raw_and_returns_200(client, db_path):
    junk = "this is {not json"
    r = client.post("/v1/logs", content=junk, headers={"content-type": "application/json"})
    assert r.status_code == 200
    assert count(db_path) == 0
    assert junk in raw_blob(db_path)


def test_body_without_resource_logs_goes_raw_verbatim(client, db_path):
    text = json.dumps({"hello": "world"})
    r = client.post("/v1/logs", content=text, headers={"content-type": "application/json"})
    assert r.status_code == 200
    assert count(db_path) == 0
    assert text in raw_blob(db_path)


def test_gzip_garbage_does_not_reject(client, db_path):
    r = client.post("/v1/logs", content=b"not gzip at all",
                    headers={"content-encoding": "gzip"})
    assert r.status_code == 200


def test_odd_record_goes_raw_and_good_records_still_land(client, db_path):
    odd = {"body": {"stringValue": "executed"}, "attributes": "not-a-list"}
    r = post_json(client, payload([executed(1), odd, executed(2)]))
    assert r.status_code == 200
    assert count(db_path) == 2
    assert "not-a-list" in raw_blob(db_path)


def test_record_missing_fields_is_never_rejected(client, db_path):
    r = post_json(client, payload([{}, {"body": {}}, {"attributes": []}]))
    assert r.status_code == 200
    # nothing may vanish: every record is in one table or the other
    assert count(db_path) + count(db_path, "log_record_raw") >= 1


def test_storage_failure_returns_503(client):
    sink_app._conn.close()  # simulates an unavailable database
    r = post_json(client, payload([executed(1)]))
    assert r.status_code == 503


def test_committed_before_200(client, db_path):
    r = post_json(client, payload([executed(1), timebox(2)]))
    assert r.status_code == 200
    assert count(db_path) == 2  # read through a brand-new connection


def test_stats_logs_executed_and_pomodoro(client):
    recs = [
        executed(1, "build", "success", 10),
        executed(2, "build", "success", 30),
        executed(3, "build", "failed", 5),
        executed(4, "deploy", "success", 100),
        timebox(5, "tcc", 1500),
        timebox(6, "tcc", 600),
        timebox(7, "mail", 300),
    ]
    assert post_json(client, payload(recs)).status_code == 200
    data = client.get("/stats/logs").json()

    stats = {(x["command"], x["status"]): x for x in data["statistics"]}
    b = stats[("build", "success")]
    assert (b["total"], b["sum_secs"], b["min_secs"], b["max_secs"]) == (2, 40, 10, 30)
    assert stats[("build", "failed")]["total"] == 1
    assert stats[("deploy", "success")]["max_secs"] == 100

    pomo = {x["tag"]: x for x in data["pomodoro"]}
    assert (pomo["tcc"]["sessions"], pomo["tcc"]["total_duration"]) == (2, 2100)
    assert (pomo["mail"]["sessions"], pomo["mail"]["total_duration"]) == (1, 300)


def test_stats_logs_casts_int64_strings_to_integers(client):
    # As text, "9" > "10"; without CAST min/max/sum would be wrong or strings.
    recs = [executed(1, "build", "success", 9), executed(2, "build", "success", 10),
            timebox(3, "t", 9), timebox(4, "t", 10)]
    post_json(client, payload(recs))
    data = client.get("/stats/logs").json()
    row = data["statistics"][0]
    assert (row["min_secs"], row["max_secs"], row["sum_secs"]) == (9, 10, 19)
    assert all(isinstance(row[k], int) for k in ("min_secs", "max_secs", "sum_secs"))
    p = data["pomodoro"][0]
    assert p["total_duration"] == 19 and isinstance(p["total_duration"], int)


# --- regression: lone surrogates (JSON "\ud800" decodes to an unencodable str) ---

LONE = "\ud800"


def batches(db_path):
    return count(db_path, "export_batch")


def test_surrogate_in_body_returns_200(client, db_path):
    r = post_json(client, payload([executed(1, body="bad" + LONE)]))
    assert r.status_code == 200


def test_surrogate_in_instance_id_returns_200(client, db_path):
    r = post_json(client, payload([executed(1)], instance="inst" + LONE))
    assert r.status_code == 200
    assert count(db_path) + count(db_path, "log_record_raw") >= 1


def test_surrogate_in_service_scope_hop_returns_200(client, db_path):
    p = payload([executed(1)])
    res = p["resourceLogs"][0]
    res["resource"]["attributes"][0]["value"] = s("kw" + LONE)
    res["resource"]["attributes"].append({"key": "pipeline.hop", "value": s("h" + LONE)})
    res["scopeLogs"][0]["scope"]["name"] = "sc" + LONE
    assert post_json(client, p).status_code == 200


def test_good_records_land_beside_surrogate_record(client, db_path):
    bad = executed(2, body="bad" + LONE)
    r = post_json(client, payload([executed(1), bad, executed(3)]))
    assert r.status_code == 200
    assert count(db_path) >= 2
    assert count(db_path) + count(db_path, "log_record_raw") >= 3


def test_no_orphan_batch_after_odd_request_and_next_request_works(client, db_path):
    post_json(client, payload([executed(1, body="bad" + LONE)]))
    n = batches(db_path)
    assert n <= 1
    if n == 1:  # a batch row is only legitimate if something was stored for it
        assert count(db_path) + count(db_path, "log_record_raw") >= 1
    assert post_json(client, payload([executed(5)])).status_code == 200
    assert count(db_path) >= 1


def test_storage_failure_503_leaves_no_batch_row(client, db_path):
    # The trigger fails the insert after the export_batch row was written,
    # so the handler must roll back that transaction.
    con = sqlite3.connect(db_path)
    con.execute("CREATE TRIGGER boom BEFORE INSERT ON log_record "
                "BEGIN SELECT RAISE(FAIL, 'disk full'); END")
    con.commit()
    con.close()
    assert post_json(client, payload([executed(1)])).status_code == 503
    assert batches(db_path) == 0


# --- D28 poison records, bytesValue, dedupe key, migration, event loop ---

def kv(key, value):
    return {"key": key, "value": value}


def test_poison_stringvalue_dict_or_list_goes_raw_and_rest_lands(client, db_path):
    poison_dict = record("executed", 10)
    poison_dict["body"] = {"stringValue": {"poison_marker_d": 1}}
    poison_list = record("executed", 11)
    poison_list["body"] = {"stringValue": ["poison_marker_l"]}
    r = post_json(client, payload([executed(1), poison_dict, poison_list, executed(2)]))
    assert r.status_code == 200
    assert count(db_path) == 2
    blob = raw_blob(db_path)
    assert "poison_marker_d" in blob and "poison_marker_l" in blob
    assert count(db_path, "log_record_raw") == 2


def test_poison_int_over_int64_attribute_goes_raw(client, db_path):
    bad = executed(5)
    bad["attributes"].append(kv("kw.big", {"intValue": 10**20}))
    r = post_json(client, payload([executed(1), bad, executed(2)]))
    assert r.status_code == 200
    assert count(db_path) == 2
    assert "kw.big" in raw_blob(db_path)


def test_poison_int_over_int64_event_id_goes_raw(client, db_path):
    for big in (str(10**20), 10**20):
        bad = executed(5)
        bad["attributes"][0] = kv("kw.event.id", {"intValue": big})
        assert post_json(client, payload([executed(1), bad])).status_code == 200
    assert count(db_path) == 1
    assert count(db_path, "log_record_raw") == 2


def test_int64_boundary_values_are_stored(client, db_path):
    ok_rec = executed(2**63 - 1)
    assert post_json(client, payload([ok_rec])).status_code == 200
    assert count(db_path) == 1


def test_attribute_stringvalue_dict_never_loses_the_batch(client, db_path):
    rec = executed(5)
    rec["attributes"].append(kv("kw.odd", {"stringValue": {"a": 1}}))
    assert post_json(client, payload([executed(1), rec])).status_code == 200
    assert count(db_path) >= 1
    assert count(db_path) + count(db_path, "log_record_raw") == 2


def test_bytesvalue_stored_as_base64_string(client, db_path):
    rec = executed(1)
    rec["attributes"].append(kv("kw.blob", {"bytesValue": "AAEC/w=="}))
    assert post_json(client, payload([rec])).status_code == 200
    attrs = json.loads(fresh_rows(db_path, "SELECT attributes FROM log_record")[0][0])
    assert attrs["kw.blob"] == "AAEC/w=="


def ext_rows(event_id, kind, **attrs):
    return record(kind, event_id, {k: s(v) for k, v in attrs.items()})


def test_same_kind_rows_of_one_event_with_different_attributes_both_stored(client, db_path):
    a = ext_rows(50, "build_modules", **{"kw.m.name": "alpha"})
    b = ext_rows(50, "build_modules", **{"kw.m.name": "beta"})
    p = payload([a, b])
    assert post_json(client, p).status_code == 200
    assert count(db_path) == 2
    assert post_json(client, p).status_code == 200  # exact retry
    assert count(db_path) == 2
    assert post_json(client, payload([b])).status_code == 200
    assert count(db_path) == 2


def test_retry_in_one_batch_twice_is_one_row(client, db_path):
    rec = ext_rows(51, "build_modules", **{"kw.m.name": "alpha"})
    assert post_json(client, payload([rec, rec])).status_code == 200
    assert count(db_path) == 1


def test_same_event_different_instance_still_distinct_with_ext_rows(client, db_path):
    rec = ext_rows(52, "build_modules", **{"kw.m.name": "alpha"})
    post_json(client, payload([rec], instance="a"))
    post_json(client, payload([rec], instance="b"))
    assert count(db_path) == 2


def no_event_id(rec):
    rec["attributes"] = [a for a in rec["attributes"] if a["key"] != "kw.event.id"]
    return rec


def test_records_without_event_id_deduped_on_retry(client, db_path):
    rec = no_event_id(executed(1))
    p = payload([rec])
    assert post_json(client, p).status_code == 200
    assert post_json(client, p).status_code == 200
    assert count(db_path) == 1


def test_records_without_event_id_but_different_content_both_stored(client, db_path):
    a = no_event_id(executed(1, "build"))
    b = no_event_id(executed(1, "deploy"))
    c = no_event_id(executed(1, "build", t="1700000000000000001"))
    assert post_json(client, payload([a, b, c])).status_code == 200
    assert count(db_path) == 3


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


def make_old_db(path, n=3):
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(OLD_SCHEMA)
    con.execute("INSERT INTO export_batch VALUES (1, '2026-01-01T00:00:00+00:00', 10, ?)", (n,))
    for k in range(n):
        con.execute(
            "INSERT INTO log_record (batch_id, received_at, service_name, service_instance_id,"
            " body, event_id, time_unix_nano, attributes) VALUES (1,'t','kworkflow','old',"
            "'executed',?,?,?)",
            (k, 1700000000000000000, json.dumps({"kw.event.id": str(k), "kw.old": "yes"})))
    con.commit()
    con.close()


def test_old_schema_db_is_migrated_and_keeps_rows(db_path):
    make_old_db(db_path)
    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        assert c.get("/healthz").status_code == 200
        assert count(db_path) == 3
        rows = fresh_rows(db_path, "SELECT attributes FROM log_record ORDER BY event_id")
        assert all(json.loads(r[0])["kw.old"] == "yes" for r in rows)
        # the old UNIQUE(instance, event, body) would have dropped the second row
        a = ext_rows(0, "executed", **{"kw.new": "a"})
        b = ext_rows(0, "executed", **{"kw.new": "b"})
        assert post_json(c, payload([a, b], instance="old")).status_code == 200
        assert count(db_path) == 5
        assert post_json(c, payload([a, b], instance="old")).status_code == 200
        assert count(db_path) == 5
    # starting again on the migrated DB is a no-op
    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        assert c.get("/healthz").status_code == 200
        assert count(db_path) == 5


def test_old_schema_db_served_by_group_endpoint(db_path):
    make_old_db(db_path)
    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        r = c.get("/analysis/group", params={"kind": "executed", "by": "kw.old"})
        assert r.status_code == 200
        assert [(g["kw.old"], g["records"]) for g in r.json()["groups"]] == [("yes", 3)]


def test_healthz_answers_while_a_write_lock_is_held(client, db_path):
    holder = sqlite3.connect(db_path, timeout=0)
    holder.execute("BEGIN EXCLUSIVE")
    done = {}

    def slow_post():
        done["r"] = post_json(client, payload([executed(1)]))

    t = threading.Thread(target=slow_post)
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


# --- review regressions: unstorable text, regex cost, metrics transaction, floor, strict types ---

def analysis_all_ok(client):
    assert client.get("/analysis/schema").status_code == 200
    assert client.get("/stats/logs").status_code == 200
    for by in (None, "v", "kw.event.id"):
        params = {"kind": "executed"} | ({"by": by} if by else {})
        assert client.get("/analysis/group", params=params).status_code in (200, 400)
    assert client.get("/analysis/group", params={"kind": "executed"}).status_code == 200


import pytest  # noqa: E402


@pytest.mark.parametrize("attrs,marker", [
    ({"v": s("\ud800")}, "ud800"),
    ({"\ud800n": s("val")}, "ud800"),
    ({"v": s("a\x00b")}, "u0000"),
    ({"a\x00b": s("val")}, "u0000"),
])
def test_unstorable_text_in_attribute_goes_raw_and_analysis_stays_200(client, db_path, attrs, marker):
    bad = record("executed", 90, attrs)
    r = post_json(client, payload([executed(1), bad, executed(2)]))
    assert r.status_code == 200
    assert count(db_path) == 2
    assert count(db_path, "log_record_raw") == 1
    assert marker in raw_blob(db_path)
    analysis_all_ok(client)


def test_long_non_numeric_string_measure_is_fast(client):
    post_recs(client, [executed(1), record("executed", 2, {
        "kw.executed.elapsed_time_in_secs": s("0" * 10000 + "x")})])
    t0 = time.monotonic()
    r = client.get("/analysis/group", params={
        "kind": "executed", "measure": "kw.executed.elapsed_time_in_secs"})
    elapsed = time.monotonic() - t0
    assert r.status_code == 200
    g = r.json()["groups"][0]
    assert g["non_numeric"] == 1 and g["measured"] == 1
    assert elapsed < 2.0, f"measure took {elapsed:.1f}s on a long non-numeric string"


def test_failed_metrics_insert_leaves_no_open_transaction(client, db_path):
    bad = {"resourceMetrics": [{"scopeMetrics": [{"metrics": [{
        "name": None, "gauge": {"dataPoints": [{"asInt": "1", "timeUnixNano": "1"}]}}]}]}]}
    r = client.post("/v1/metrics", content=json.dumps(bad),
                    headers={"content-type": "application/json"})
    assert r.status_code >= 400
    assert post_json(client, payload([executed(1)])).status_code == 200
    # the failed request's export_batch row must not ride along with the logs commit
    assert count(db_path, "export_batch") == 1
    assert count(db_path, "metric_point") == 0
    assert count(db_path) == 1


@pytest.mark.parametrize("t,day,week", [
    ("-1", "1969-12-31", "1969-12-29"),
    ("-86400000000000", "1969-12-31", "1969-12-29"),
    ("-86400000000001", "1969-12-30", "1969-12-29"),
    ("0", "1970-01-01", "1969-12-29"),
])
def test_bucket_floors_negative_times(client, t, day, week):
    post_recs(client, [record("tt", 1, {}, t=t)])
    for b, want in (("day", day), ("week", week)):
        r = client.get("/analysis/group", params={"kind": "tt", "bucket": b})
        assert r.status_code == 200
        assert r.json()["groups"][0]["bucket"] == want


@pytest.mark.parametrize("make", [
    lambda: record("ev", 1, {"x": s("1")}) | {"timeUnixNano": 1.5},
    lambda: record("ev", 1, {"x": s("1")}) | {"timeUnixNano": True},
    lambda: record("ev", 1, {"x": s("1")}) | {"observedTimeUnixNano": 2.5},
    lambda: no_eid(record("ev", 1, {"kw.event.id": {"doubleValue": 7.0}})),
    lambda: no_eid(record("ev", 1, {"kw.event.id": {"boolValue": True}})),
    lambda: no_eid(record("ev", 1, {"kw.event.id": s("7")})),
])
def test_wrong_typed_numeric_fields_go_raw_not_truncated(client, db_path, make):
    r = post_json(client, payload([executed(1), make()]))
    assert r.status_code == 200
    assert count(db_path) == 1
    assert count(db_path, "log_record_raw") == 1


def no_eid(rec):
    """Drop the helper's own kw.event.id so the test's typed one is the only one."""
    seen = [a for a in rec["attributes"] if a["key"] == "kw.event.id"]
    rec["attributes"].remove(seen[0])
    return rec


@pytest.mark.parametrize("text", [" 12 ", "1_0", "+5", "12\n", "\t7", "１２", "1.0", "0x1", ""])
def test_non_canonical_int_strings_go_raw(client, db_path, text):
    bad = executed(5)
    bad["attributes"].append(kv("kw.n", {"intValue": text}))
    assert post_json(client, payload([executed(1), bad])).status_code == 200
    assert count(db_path) == 1
    assert count(db_path, "log_record_raw") == 1


@pytest.mark.parametrize("val", ["12", "-12", "0", "007", str(2**63 - 1), str(-(2**63)), 12, -12])
def test_canonical_int_values_still_accepted(client, db_path, val):
    rec = executed(5)
    rec["attributes"].append(kv("kw.n", {"intValue": val}))
    assert post_json(client, payload([rec])).status_code == 200
    assert count(db_path) == 1
    assert count(db_path, "log_record_raw") == 0


# --- non-finite metrics values and the /stats/logs overflow retry ---

def post_metrics_raw(client, raw):
    return client.post("/v1/metrics", content=raw, headers={"content-type": "application/json"})


def metric(kind, point, name="kw.command.duration", attrs=None):
    pt = {"timeUnixNano": "1", "attributes": attrs or [], **point}
    return json.dumps({"resourceMetrics": [{"scopeMetrics": [{"metrics": [
        {"name": name, "unit": "s", kind: {"dataPoints": [pt]}}]}]}]})


def no_nonfinite(text):
    return not any(w in text for w in ("Infinity", "NaN"))


@pytest.mark.parametrize("raw", [
    '{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"kw.command.duration",'
    '"sum":{"dataPoints":[{"timeUnixNano":"1","asDouble":1e999}]}}]}]}]}',
    '{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"kw.command.duration",'
    '"histogram":{"dataPoints":[{"timeUnixNano":"1","count":"1","sum":1e999,"min":-1e999,'
    '"max":NaN}]}}]}]}]}',
])
def test_stats_with_infinite_metric_values_returns_200_with_nulls(client, raw):
    posted = post_metrics_raw(client, raw).status_code
    r = client.get("/stats")
    assert r.status_code == 200, r.text
    assert no_nonfinite(r.text)
    assert r.json()["batches_received"] == (1 if posted == 200 else 0)


def test_stats_overflowing_sum_and_average_return_200(client):
    # Each value is finite; their sum overflows to inf.
    for _ in range(2):
        assert post_metrics_raw(client, metric("histogram", {
            "count": "1", "sum": 1e308, "min": 1e308, "max": 1e308},
            attrs=[{"key": "kw.command", "value": s("build")},
                   {"key": "kw.status", "value": s("ok")}])).status_code == 200
    r = client.get("/stats")
    assert r.status_code == 200, r.text
    row = r.json()["statistics"][0]
    assert row["total"] == 2 and row["min_secs"] == 1e308
    assert row["avg_secs"] is None


def test_stats_pomodoro_overflow_returns_200(client):
    for _ in range(2):
        post_metrics_raw(client, metric("gauge", {"asDouble": 1e308},
                                        name="kw.pomodoro.focus_time",
                                        attrs=[{"key": "kw.tag", "value": s("t")}]))
    r = client.get("/stats")
    assert r.status_code == 200, r.text
    assert r.json()["pomodoro"][0]["focus_secs"] is None


def test_stats_int64_overflow_asint_does_not_break_stats(client):
    posted = post_metrics_raw(client, metric("gauge", {"asInt": str(2**70)})).status_code
    assert posted in (200, 400, 500)  # status of the POST itself is not under test
    assert client.get("/stats").status_code == 200
    assert client.get("/healthz").status_code == 200


def test_stats_logs_retry_keeps_null_sum_for_group_without_elapsed(client):
    big = str(2**63 - 1)
    recs = [
        executed(1, "huge", "ok", big), executed(2, "huge", "ok", big),
        record("executed", 3, {"kw.executed.command_label": s("untimed"),
                               "kw.executed.status": s("ok")}),
    ]
    assert post_json(client, payload(recs)).status_code == 200
    r = client.get("/stats/logs")
    assert r.status_code == 200, r.text
    rows = {x["command"]: x for x in r.json()["statistics"]}
    assert rows["untimed"]["sum_secs"] is None
    assert rows["untimed"]["min_secs"] is None and rows["untimed"]["total"] == 1
    huge = rows["huge"]["sum_secs"]
    assert huge is None or (isinstance(huge, float) and huge > 1e19)


def test_stats_logs_retry_keeps_null_total_for_timebox_without_duration(client):
    big = str(2**63 - 1)
    recs = [executed(1, "huge", "ok", big), executed(2, "huge", "ok", big),
            record("timebox", 3, {"kw.timebox.tag": s("nodur")})]
    assert post_json(client, payload(recs)).status_code == 200
    r = client.get("/stats/logs")
    assert r.status_code == 200, r.text
    p = {x["tag"]: x for x in r.json()["pomodoro"]}
    assert p["nodur"]["total_duration"] is None
