"""POST /v1/logs: what is stored, what goes to the raw table, dedupe, storage failure."""
import gzip
import json
import sqlite3

import pytest

from conftest import executed, fresh_rows, payload, post_json, record, s

pytestmark = pytest.mark.usefixtures("raw_times")


EDGES_STORED = ["12", "-12", "0", "007", str(2**63 - 1), str(-(2**63)), 12, -12]  # JSON ints stay ints
LONE = "\ud800"  # a JSON "\ud800" decodes to a str that cannot be encoded as UTF-8


def count(db_path, table="log_record"):
    return fresh_rows(db_path, f"SELECT COUNT(*) FROM {table}")[0][0]


def raw_blob(db_path):
    """All text/blob content of log_record_raw (its column names are not in the contract)."""
    out = []
    for row in fresh_rows(db_path, "SELECT * FROM log_record_raw"):
        for v in row:
            if isinstance(v, bytes):
                v = v.decode("utf-8", "replace")
            if isinstance(v, str):
                out.append(v)
    return "\n".join(out)


def kv(key, value):
    return {"key": key, "value": value}


def with_attr(rec, key, value):
    rec["attributes"].append(kv(key, value))
    return rec


def without_event_id(rec):
    rec["attributes"] = [a for a in rec["attributes"] if a["key"] != "kw.event.id"]
    return rec


def with_event_id_value(rec, value):
    """Replace the helper's kw.event.id so the given (possibly mistyped) value is the only one."""
    rec["attributes"] = [kv("kw.event.id", value)] + without_event_id(rec)["attributes"]
    return rec


def odd_text_in_resource():
    p = payload([executed(1)])
    res = p["resourceLogs"][0]
    res["resource"]["attributes"][0]["value"] = s("kw" + LONE)
    res["resource"]["attributes"].append(kv("pipeline.hop", s("h" + LONE)))
    res["scopeLogs"][0]["scope"]["name"] = "sc" + LONE
    return p


# --- what is stored ---

def test_record_is_stored_whole_and_committed_before_200(client, db_path):
    base = executed(2**63 - 1, "build", "success", 12) | {"observedTimeUnixNano": "1700000000000000099"}
    rec = with_attr(with_attr(with_attr(base, "kw.hardware.gpu", s("rtx")),
                              "kw.blob", {"bytesValue": "AAEC/w=="}),
                    "kw.ok", {"boolValue": True})
    assert post_json(client, payload([rec], instance="abc")).status_code == 200

    # a new connection: only committed data is visible to it
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM log_record").fetchall()
    con.close()
    assert len(rows) == 1
    row = rows[0]
    assert {k: row[k] for k in ("service_name", "service_instance_id", "pipeline_hop", "scope_name",
                                "time_unix_nano", "observed_time_unix_nano", "body", "event_id")} == {
        "service_name": "kworkflow", "service_instance_id": "abc", "pipeline_hop": None,
        "scope_name": "kw_tele", "time_unix_nano": 1700000000000000000,
        "observed_time_unix_nano": 1700000000000000099, "body": "executed", "event_id": 2**63 - 1}
    # int64 stays a decimal string, bytes become base64; unknown attributes need no code change
    assert json.loads(row["attributes"]) == {
        "kw.event.id": str(2**63 - 1), "kw.event.kind": "executed",
        "kw.executed.command_label": "build", "kw.executed.status": "success",
        "kw.executed.elapsed_time_in_secs": "12", "kw.hardware.gpu": "rtx",
        "kw.blob": "AAEC/w==", "kw.ok": True}
    assert count(db_path, "log_record_raw") == 0


def test_int64_edge_values_are_stored_not_sent_to_raw(client, db_path):
    edges = ["12", "-12", "0", "007", str(2**63 - 1), str(-(2**63)), 12, -12]
    rec = executed(1)
    for n, val in enumerate(edges):
        with_attr(rec, f"kw.n{n}", {"intValue": val})
    assert post_json(client, payload([rec])).status_code == 200
    assert count(db_path, "log_record_raw") == 0
    attrs = json.loads(fresh_rows(db_path, "SELECT attributes FROM log_record")[0][0])
    assert {k: v for k, v in attrs.items() if k.startswith("kw.n")} == {
        f"kw.n{n}": EDGES_STORED[n] for n in range(len(edges))}


def test_pipeline_hop_resource_attribute_is_stored(client, db_path):
    p = payload([executed(1)])
    p["resourceLogs"][0]["resource"]["attributes"].append(kv("pipeline.hop", s("collector")))
    assert post_json(client, p).status_code == 200
    assert fresh_rows(db_path, "SELECT pipeline_hop FROM log_record") == [("collector",)]


def test_gzip_body_is_decoded(client, db_path):
    body = gzip.compress(json.dumps(payload([executed(1)])).encode())
    r = client.post("/v1/logs", content=body,
                    headers={"content-type": "application/json", "content-encoding": "gzip"})
    assert r.status_code == 200
    assert count(db_path) == 1


# --- dedupe: each record is stored once ---

def test_event_id_and_body_dedupe_across_retries_batches_and_attributes(client, db_path):
    """D51: the key is install + body + event id; the first row seen is the one kept."""
    def post(recs, instance="i"):
        assert post_json(client, payload(recs, instance=instance)).status_code == 200

    a = executed(1)
    later = executed(1, t="1700000000000000001") | {
        "attributes": a["attributes"] + [kv("kw.extra", s("x"))]}
    post([a]); post([a]); post([a, a])  # re-sent, and twice in one batch
    post([later])  # same id and body, other time and attributes
    assert count(db_path) == 1
    assert fresh_rows(db_path, "SELECT time_unix_nano FROM log_record") == [(1700000000000000000,)]
    post([a], instance="other")  # the same event from another install is a new row
    post([executed(1, body="other")])  # same id, different body
    assert count(db_path) == 3


def test_records_without_event_id_dedupe_on_their_content(client, db_path):
    """D42: the key is the whole record when the id is absent."""
    same = without_event_id(executed(1, "build"))
    others = [without_event_id(executed(1, "deploy")),
              without_event_id(executed(1, "build", t="1700000000000000001"))]
    for _ in range(2):
        assert post_json(client, payload([same] + others)).status_code == 200
    assert count(db_path) == 3
    # records missing every field are valid empty records, never rejected, and share one key
    assert post_json(client, payload([{}, {"body": {}}, {"attributes": []}])).status_code == 200
    assert count(db_path) == 4


def test_event_id_zero_is_an_id_not_an_absent_one(client, db_path):
    for rec in (executed(0), executed(0, t="1700000000000000001")):
        assert post_json(client, payload([rec])).status_code == 200
    assert count(db_path) == 1


# --- malformed or poison input goes to log_record_raw, never 4xx/5xx (D28) ---

def _poison_body(value):
    rec = record("executed", 80)
    rec["body"] = {"stringValue": value}
    return rec


def _arr(values):
    return with_attr(executed(5), "kw.a", {"arrayValue": {"values": values}})


# name: (record, text that must be recoverable from log_record_raw, or None)
POISON = {
    "attributes not a list": ({"body": {"stringValue": "executed"}, "attributes": "not-a-list"}, None),
    "attribute value not a dict": (record("e", 1) | {"attributes": [kv("k", "plain")]}, None),
    "body stringValue list": (_poison_body(["poison_l"]), "poison_l"),
    "body not a dict": (record("e", 1) | {"body": "executed"}, None),
    "attribute over int64": (with_attr(executed(5), "kw.big", {"intValue": 10**20}), "kw.big"),
    "event id over int64": (with_event_id_value(executed(5), {"intValue": str(10**20)}), None),
    "non-canonical int string": (with_attr(executed(5), "kw.n", {"intValue": "1_0"}), None),
    "boolean time": (record("ev", 1, {}) | {"timeUnixNano": True}, None),
    "time over int64": (record("ev", 1, {}) | {"timeUnixNano": str(10**20)}, None),
    "event id string": (with_event_id_value(record("ev", 1), s("7")), None),
    "double NaN": (with_attr(executed(5), "kw.d", {"doubleValue": float("nan")}), None),
    "bool as string": (with_attr(executed(5), "kw.b", {"boolValue": "yes"}), None),
    "int given a bool": (with_attr(executed(5), "kw.b", {"intValue": True}), None),
    "double given a bool": (with_attr(executed(5), "kw.b", {"doubleValue": True}), None),
    "array values a dict": (_arr({"a": 1}), None),
    "NUL inside an array": (_arr([s("a\x00")]), None),
    "NUL inside a kvlist": (with_attr(executed(5), "kw.a",
                                      {"kvlistValue": {"values": [kv("k", s("a\x00"))]}}), None),
    "lone surrogate in value": (record("executed", 90, {"v": s(LONE)}), "ud800"),
    "lone surrogate in name": (record("executed", 90, {LONE + "n": s("val")}), None),
    "NUL in value": (record("executed", 90, {"v": s("a\x00b")}), "u0000"),
}


@pytest.mark.parametrize("poison,marker", POISON.values(), ids=POISON.keys())
def test_unstorable_record_goes_raw_and_its_neighbours_land(client, db_path, poison, marker):
    r = post_json(client, payload([executed(1), poison, executed(3)]))
    assert r.status_code == 200
    assert (count(db_path), count(db_path, "log_record_raw"), count(db_path, "export_batch")) == (2, 1, 1)
    if marker:
        assert marker in raw_blob(db_path)


# a malformed batch is never rejected; whatever part of it is unusable is kept raw
BAD_SHAPES = {
    "resourceLogs [null]": {"resourceLogs": [None]},
    "scopeLogs a number": {"resourceLogs": [{"scopeLogs": 5}]},
    "logRecords a number": {"resourceLogs": [{"scopeLogs": [{"logRecords": 5}]}]},
    "logRecords of non-dicts": {"resourceLogs": [{"scopeLogs": [{"logRecords": [None, 5, "x"]}]}]},
    "resource attributes null": {"resourceLogs": [{"resource": {"attributes": None}}]},
    "top level null": None,
}


@pytest.mark.parametrize("body", BAD_SHAPES.values(), ids=BAD_SHAPES.keys())
def test_malformed_batch_shapes_are_200_and_recorded(client, db_path, body):
    r = client.post("/v1/logs", content=json.dumps(body), headers={"content-type": "application/json"})
    assert r.status_code == 200
    assert count(db_path, "export_batch") == 1


def test_unstorable_resource_text_sends_the_whole_batch_to_raw(client, db_path):
    assert post_json(client, payload([executed(1)], instance="inst" + LONE)).status_code == 200
    assert post_json(client, odd_text_in_resource()).status_code == 200
    assert (count(db_path), count(db_path, "log_record_raw")) == (0, 2)


def test_retrieval_keeps_working_after_poison(client, db_path):
    for poison, _ in POISON.values():
        assert post_json(client, payload([executed(1), poison])).status_code == 200
    assert post_json(client, odd_text_in_resource()).status_code == 200
    assert client.get("/analysis/schemas").status_code == 200
    assert client.get("/analysis/query", params={"schema": "executed"}).status_code == 200


def test_unparseable_request_bodies_are_kept_verbatim_in_raw(client, db_path):
    bodies = [("this is {not json", {}), (json.dumps({"hello": "world"}), {}),
              (b"not gzip at all", {"content-encoding": "gzip"})]
    for raw_body, headers in bodies:
        r = client.post("/v1/logs", content=raw_body,
                        headers={"content-type": "application/json", **headers})
        assert r.status_code == 200
    assert count(db_path) == 0
    blob = raw_blob(db_path)
    assert all(m in blob for m in ("this is {not json", '"hello"', "not gzip at all"))


# --- D23: 503 only on storage failure, and then nothing is left behind ---

def test_storage_failure_returns_503_and_rolls_back_the_batch_row(client, db_path):
    # The trigger fails the insert after the export_batch row was written.
    con = sqlite3.connect(db_path)
    con.execute("CREATE TRIGGER boom BEFORE INSERT ON log_record "
                "BEGIN SELECT RAISE(FAIL, 'disk full'); END")
    con.commit()
    con.close()
    assert post_json(client, payload([executed(1)])).status_code == 503
    assert count(db_path, "export_batch") == 0
    # the failed attempt left nothing that a later commit could make visible
    con = sqlite3.connect(db_path)
    con.execute("DROP TRIGGER boom")
    con.commit()
    con.close()
    assert post_json(client, payload([executed(1)])).status_code == 200
    assert (count(db_path, "export_batch"), count(db_path)) == (1, 1)


@pytest.mark.parametrize("method,path", [("post", "/v1/metrics"), ("get", "/stats"), ("get", "/stats/logs")])
def test_legacy_metrics_routes_are_gone(client, method, path):
    assert getattr(client, method)(path).status_code == 404
