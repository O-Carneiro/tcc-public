import gzip
import json
import sqlite3

from conftest import (executed, fresh_rows, i, payload, post_json, record, s,
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
