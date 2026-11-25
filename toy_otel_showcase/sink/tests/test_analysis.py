import json
import sqlite3

from conftest import (bl, d, eid, ex, fresh_rows, i, ns, payload, post_json, post_recs,
                      record, s)

LABEL = "kw.executed.command_label"


def schema(client):
    r = client.get("/analysis/schema")
    assert r.status_code == 200
    return r.json()


def test_helpers_selftest(client, db_path):
    # ns() is UTC; ex() carries the three executed attributes and a unique event id.
    assert ns("1970-01-01T00:00:01") == "1000000000"
    a, b = ex(), ex(t="2026-11-16T00:00:00")
    assert a["attributes"][0]["value"] != b["attributes"][0]["value"]
    post_recs(client, [a, b])
    rows = fresh_rows(db_path, "SELECT time_unix_nano, body, attributes FROM log_record "
                               "ORDER BY id")
    assert rows[1][0] == int(ns("2026-11-16T00:00:00"))
    assert rows[0][1] == "executed"
    assert json.loads(rows[0][2])["kw.executed.status"] == "success"


def test_helpers_typed_values_stored_as_json_types(client, db_path):
    rec = record("x", eid(), {"a.b": bl(True), "a.d": d(1.5), "a.i": i(7)})
    post_recs(client, [rec])
    attrs = json.loads(fresh_rows(db_path, "SELECT attributes FROM log_record")[0][0])
    assert attrs["a.b"] is True and attrs["a.d"] == 1.5 and attrs["a.i"] == "7"


def test_schema_empty_db(client):
    assert schema(client) == {"kinds": []}


def test_schema_kinds_attributes_counts(client):
    post_recs(client, [ex(), ex(), record("timebox", eid(), {"kw.timebox.tag": s("t")})],
              instance="a")
    post_recs(client, [ex(extra={"kw.hardware.gpu": s("rtx")})], instance="b")
    post_recs(client, [ex()], instance="b")
    data = schema(client)
    assert [k["kind"] for k in data["kinds"]] == ["executed", "timebox"]
    ex_kind, tb = data["kinds"]
    assert (ex_kind["records"], ex_kind["installs"]) == (4, 2)
    assert (tb["records"], tb["installs"]) == (1, 1)
    attrs = ex_kind["attributes"]
    assert [a["name"] for a in attrs] == sorted(a["name"] for a in attrs)
    by_name = {a["name"]: a["records"] for a in attrs}
    assert by_name[LABEL] == 4
    assert by_name["kw.hardware.gpu"] == 1
    assert "kw.timebox.tag" not in by_name
    assert {a["name"] for a in tb["attributes"]} >= {"kw.timebox.tag"}


def test_schema_discovers_new_source_without_code_change(client):
    post_recs(client, [record("mailing_list", eid(), {"kw.ml.subject": s("hi")})])
    kinds = {k["kind"]: k for k in schema(client)["kinds"]}
    assert "mailing_list" in kinds
    assert "kw.ml.subject" in {a["name"] for a in kinds["mailing_list"]["attributes"]}


def test_schema_skips_null_body(client):
    rec = record("executed", eid(), {})
    rec["body"] = {}
    post_recs(client, [rec, ex()])
    assert [k["kind"] for k in schema(client)["kinds"]] == ["executed"]


def test_schema_has_no_install_ids(client):
    post_recs(client, [ex()], instance="secret-install-zzz")
    resp = client.get("/analysis/schema")
    assert "secret-install-zzz" not in resp.text
    assert "service_instance_id" not in resp.text
    assert isinstance(resp.json()["kinds"][0]["installs"], int)
