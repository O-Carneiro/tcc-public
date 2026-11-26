"""GET /analysis/query: response shape, schema, by, measure, bucket (week, install), reserved names, limits."""
import hashlib
import json
import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

from conftest import (bl, d, eid, ex, fresh_rows, i, no_time, payload, post_json, post_recs,
                      record, s, sink_app)

pytestmark = pytest.mark.usefixtures("raw_times")

LABEL = "kw.executed.command_label"
STATUS = "kw.executed.status"
SECS = "kw.executed.elapsed_time_in_secs"
BIG = "9223372036854775807"
MON = "2026-11-16T00:00:00"  # a Monday


def query(client, **params):
    return client.get("/analysis/query", params=params)


def ok(client, **params):
    r = query(client, **params)
    assert r.status_code == 200, r.text
    return r.json()


def err(client, prefix=None, **params):
    r = query(client, **params)
    assert r.status_code == 400, r.text
    msg = r.json()["error"]
    assert isinstance(msg, str) and msg
    if prefix:
        assert msg.startswith(prefix), msg


def response(schema="executed", by=(), bucket=(), measure=None, keyed=False, groups=(),
             truncated=False):
    return {"schema": schema, "join": [], "by": list(by), "bucket": list(bucket),
            "measure": measure, "where": [], "from": None, "to": None, "keyed": keyed,
            "groups": list(groups), "truncated": truncated}


# --- response shape and schema ---

def test_response_has_exactly_the_documented_keys_with_empty_echoes(client):
    post_recs(client, [ex("a")])
    data = ok(client, schema="executed")
    assert set(data) == {"schema", "join", "by", "bucket", "measure", "where", "from", "to",
                         "keyed", "groups", "truncated"}
    assert data == response(groups=[{"records": 1, "installs": 1}])


def test_by_measure_and_bucket_are_echoed(client):
    post_recs(client, [ex("a", t=MON)])
    assert ok(client, schema="executed", by=LABEL, measure=SECS, bucket="install,week") == response(
        by=[LABEL], bucket=["week", "install"], measure=SECS,
        groups=[{"week": "2026-11-16", "install": "install 1", LABEL: "a", "records": 1,
                 "installs": 1, "measured": 1, "non_numeric": 0, "sum": 10, "mean": 10,
                 "min": 10, "max": 10}])


@pytest.mark.parametrize("params, prefix", [
    pytest.param({}, "missing schema", id="nothing"),
    pytest.param({"by": LABEL}, "missing schema", id="by-only"),
    pytest.param({"kind": "executed"}, "missing schema", id="old-kind-parameter"),
    pytest.param({"schema": "nope"}, "unknown schema", id="unknown-schema"),
    pytest.param({"schema": "kw.executed"}, "unknown schema", id="not-a-body"),
])
def test_schema_is_required_and_must_exist(client, params, prefix):
    post_recs(client, [ex()])
    err(client, prefix, **params)


def test_query_on_an_empty_database_is_400(client):
    err(client, "unknown schema", schema="executed")


def test_old_group_path_is_gone(client):
    post_recs(client, [ex()])
    assert client.get("/analysis/group", params={"kind": "executed"}).status_code == 404


def test_schema_filters_and_missing_attribute_groups_as_null_last(client):
    post_recs(client, [ex("a"), record("executed", eid(), {STATUS: s("ok")}), ex("b"),
                       record("timebox", eid(), {"kw.timebox.tag": s("t")})])
    got = ok(client, schema="executed", by=LABEL)["groups"]
    assert [(g[LABEL], g["records"]) for g in got] == [("a", 1), ("b", 1), (None, 1)]


# --- by ---

def test_by_none_one_and_two_attributes(client):
    post_recs(client, [ex("build"), ex("build", "failed"), ex("deploy")], instance="a")
    post_recs(client, [ex("build")], instance="b")
    assert ok(client, schema="executed") == response(groups=[{"records": 4, "installs": 2}])
    assert ok(client, schema="executed", by=LABEL) == response(by=[LABEL], groups=[
        {LABEL: "build", "records": 3, "installs": 2},
        {LABEL: "deploy", "records": 1, "installs": 1}])
    assert ok(client, schema="executed", by=f"{LABEL},{STATUS}") == response(
        by=[LABEL, STATUS], groups=[
            {LABEL: "build", STATUS: "failed", "records": 1, "installs": 1},
            {LABEL: "build", STATUS: "success", "records": 2, "installs": 2},
            {LABEL: "deploy", STATUS: "success", "records": 1, "installs": 1}])


def test_by_accepts_more_than_three_attributes(client):
    names = [f"k.{n}" for n in range(5)]
    post_recs(client, [ex(extra={n: s(f"{n}-{v}") for n in names}) for v in (2, 1)])
    assert ok(client, schema="executed", by=",".join(names)) == response(by=names, groups=[
        {**{n: f"{n}-{v}" for n in names}, "records": 1, "installs": 1} for v in (1, 2)])


def test_values_are_returned_as_text(client):
    post_recs(client, [record("c", eid(), {"v": v}) for v in (
        bl(True), bl(False), d(1.5), i(7), s("txt"),
        {"arrayValue": {"values": [s("a"), s("b")]}})])
    got = {g["v"] for g in ok(client, schema="c", by="v")["groups"]}
    arrays = {v for v in got if v.startswith("[")}
    assert got - arrays == {"true", "false", "1.5", "7", "txt"}
    assert [json.loads(v) for v in arrays] == [["a", "b"]]


def test_installs_ignore_null_and_empty_ids(client):
    post_recs(client, [ex("g")], instance="a")
    post_recs(client, [ex("g")], instance="")
    p = payload([ex("g")])
    p["resourceLogs"][0]["resource"]["attributes"].pop()  # drop service.instance.id
    assert post_json(client, p).status_code == 200
    assert ok(client, schema="executed", by=LABEL)["groups"] == [
        {LABEL: "g", "records": 3, "installs": 1}]


# --- measure ---

def test_measure_statistics_over_numeric_values(client):
    # As text "9" > "10"; only numeric handling gives min 9 / max 100.
    post_recs(client, [ex(secs=9), ex(secs=10), ex(secs=100), ex(secs=-3)])
    assert ok(client, schema="executed", measure=SECS) == response(
        measure=SECS, groups=[{"records": 4, "installs": 1, "measured": 4, "non_numeric": 0,
                               "sum": 116, "mean": 29, "min": -3, "max": 100}])


def test_measure_with_by_and_mean_weighted_by_records_not_installs(client):
    post_recs(client, [ex("a", secs=2), ex("a", secs=2), ex("a", secs=2)], instance="x")
    post_recs(client, [ex("a", secs=8), ex("b", secs=10)], instance="y")
    got = {g[LABEL]: (g["sum"], g["mean"]) for g in
           ok(client, schema="executed", by=LABEL, measure=SECS)["groups"]}
    assert got == {"a": (14, 3.5), "b": (10, 10)}


def test_measure_string_is_numeric_only_if_it_is_a_finite_decimal(client):
    numeric = ["1e3", "-2.5e-1", "+7", ".5", "5."]
    # the long one must not make the query slow
    other = [" 12 ", "", "1,5", "0x10", "1e999", "nan", "Infinity", "1_000", "abc", "0" * 10000 + "x"]
    post_recs(client, [ex(extra={SECS: s(t)}) for t in numeric + other])
    g = ok(client, schema="executed", measure=SECS)["groups"][0]
    assert (g["measured"], g["non_numeric"]) == (len(numeric), len(other))
    assert g["sum"] == pytest.approx(1000 - 0.25 + 7 + 0.5 + 5) and g["max"] == 1000


def test_measure_accepts_int_values_and_a_string_of_at_most_64_characters(client):
    post_recs(client, [ex(extra={SECS: {"intValue": 7}}), ex(extra={SECS: s("1" * 64)}),
                       ex(extra={SECS: s("1" * 65)})])
    g = ok(client, schema="executed", measure=SECS)["groups"][0]
    assert (g["records"], g["measured"], g["non_numeric"]) == (3, 2, 1)
    assert g["min"] == 7


def test_measure_mixed_types_and_missing_attribute(client):
    post_recs(client, [ex(secs=10), ex(extra={SECS: s("abc")}), ex(extra={SECS: s("2.5")}),
                       ex(extra={SECS: d(1.5)}), record("executed", eid(), {LABEL: s("build")})])
    g = ok(client, schema="executed", measure=SECS)["groups"][0]
    assert (g["records"], g["measured"], g["non_numeric"]) == (5, 3, 1)
    assert g["sum"] == pytest.approx(14) and g["mean"] == pytest.approx(14 / 3)
    # an explicit null is neither measured nor non-numeric
    post_recs(client, [ex(extra={SECS: {}})])
    g = ok(client, schema="executed", measure=SECS)["groups"][0]
    assert (g["records"], g["measured"], g["non_numeric"]) == (6, 3, 1)


def test_measure_with_nothing_measured_gives_null_statistics(client):
    post_recs(client, [ex("none", extra={SECS: s("abc")}), ex("some", secs=2)])
    got = {x[LABEL]: x for x in ok(client, schema="executed", by=LABEL, measure=SECS)["groups"]}
    assert [got["none"][k] for k in ("records", "measured", "non_numeric", "sum", "mean", "min", "max")] == [
        1, 0, 1, None, None, None, None]
    assert [got["some"][k] for k in ("measured", "sum", "mean", "min", "max")] == [1, 2, 2, 2, 2]


def test_overflow_gives_null_in_that_group_only_and_never_fails(client):
    post_recs(client, [ex("big", extra={SECS: d(1e308)}), ex("big", extra={SECS: s("1e308")}),
                       ex("ok", secs=4), ex("i64", secs=BIG), ex("i64", secs=BIG), ex("i64", secs=BIG)])
    got = {g[LABEL]: g for g in ok(client, schema="executed", by=LABEL, measure=SECS)["groups"]}
    assert (got["big"]["sum"], got["big"]["mean"], got["big"]["min"]) == (None, None, 1e308)
    assert got["big"]["measured"] == 2
    assert (got["ok"]["sum"], got["ok"]["mean"]) == (4, 4)
    assert got["i64"]["measured"] == 3 and got["i64"]["max"] == pytest.approx(9.223372036854775807e18)


# --- bucket=week (on the record's own time, not the received time) ---

def test_bucket_week_on_record_time(client):
    times = ["2026-11-16T00:00:00",  # Monday
             "2026-11-18T12:30:00",
             "2026-11-22T23:59:59",  # Sunday, same week
             "2026-11-23T00:00:00"]  # next Monday
    post_recs(client, [ex(t=t) for t in times] + [no_time(ex())])
    assert ok(client, schema="executed", bucket="week") == response(bucket=["week"], groups=[
        {"week": "2026-11-16", "records": 3, "installs": 1},
        {"week": "2026-11-23", "records": 1, "installs": 1},
        {"week": None, "records": 1, "installs": 1}])


def test_bucket_week_orders_before_by_values(client):
    post_recs(client, [ex("b", t="2026-11-16T01:00:00"), ex("a", t="2026-11-16T02:00:00"),
                       ex("z", t="2026-11-15T23:00:00"), no_time(ex("a"))])
    groups = ok(client, schema="executed", by=LABEL, bucket="week")["groups"]
    assert [(g["week"], g[LABEL]) for g in groups] == [
        ("2026-11-09", "z"), ("2026-11-16", "a"), ("2026-11-16", "b"), (None, "a")]
    assert all("bucket" not in g for g in groups)


def test_bucket_week_floors_times_before_1970(client):
    for t in ("-1", "-86400000000001", "0"):
        post_recs(client, [record("tt", eid(), {}, t=t)])
    assert [g["week"] for g in ok(client, schema="tt", bucket="week")["groups"]] == ["1969-12-29"]


# --- bucket=install ---

def first_arrival(client, instances):
    """Post one record per instance, in this order, so install N is the Nth instance."""
    for inst in instances:
        post_recs(client, [ex("a")], instance=inst)


def test_install_labels_follow_first_arrival_and_installs_is_one_per_group(client):
    first_arrival(client, ["zzz", "aaa", "mmm"])
    post_recs(client, [ex("a"), ex("a")], instance="aaa")
    assert ok(client, schema="executed", bucket="install") == response(bucket=["install"], groups=[
        {"install": "install 1", "records": 1, "installs": 1},
        {"install": "install 2", "records": 3, "installs": 1},
        {"install": "install 3", "records": 1, "installs": 1}])


def test_install_labels_are_stable_across_queries_and_filters(client):
    first_arrival(client, ["i1", "i2", "i3"])
    post_recs(client, [ex("only-i3")], instance="i3")
    plain = {g["install"] for g in ok(client, schema="executed", bucket="install")["groups"]}
    assert plain == {"install 1", "install 2", "install 3"}
    got = ok(client, schema="executed", by=LABEL, bucket="install")["groups"]
    assert {(g[LABEL], g["install"]) for g in got} == {
        ("a", "install 1"), ("a", "install 2"), ("a", "install 3"), ("only-i3", "install 3")}
    # an install that first appeared in another schema still counts in the numbering
    post_recs(client, [record("timebox", eid(), {})], instance="i0")
    post_recs(client, [ex("late")], instance="i0")
    late = ok(client, schema="executed", by=LABEL, bucket="install")["groups"]
    assert [g["install"] for g in late if g[LABEL] == "late"] == ["install 4"]


def test_install_labels_sort_numerically(client):
    first_arrival(client, [f"i{n}" for n in range(11)])
    labels = [g["install"] for g in ok(client, schema="executed", bucket="install")["groups"]]
    assert labels == [f"install {n}" for n in range(1, 12)]


def test_empty_or_null_instance_has_a_null_label_sorted_last(client):
    post_recs(client, [ex("a")], instance="")
    p = payload([ex("a")])
    p["resourceLogs"][0]["resource"]["attributes"].pop()  # no service.instance.id
    assert post_json(client, p).status_code == 200
    post_recs(client, [ex("a")], instance="real")
    assert ok(client, schema="executed", bucket="install")["groups"] == [
        {"install": "install 1", "records": 1, "installs": 1},
        {"install": None, "records": 2, "installs": 0}]


def test_bucket_week_then_install_has_a_fixed_key_and_sort_order(client):
    post_recs(client, [ex("a", t="2026-11-23T00:00:00")], instance="first")
    post_recs(client, [ex("a", t=MON)], instance="second")
    post_recs(client, [ex("a", t=MON)], instance="first")
    post_recs(client, [no_time(ex("a"))], instance="second")
    expected = [
        {"week": "2026-11-16", "install": "install 1", "records": 1, "installs": 1},
        {"week": "2026-11-16", "install": "install 2", "records": 1, "installs": 1},
        {"week": "2026-11-23", "install": "install 1", "records": 1, "installs": 1},
        {"week": None, "install": "install 2", "records": 1, "installs": 1}]
    for order in ("week,install", "install,week"):
        data = ok(client, schema="executed", bucket=order)
        assert data["bucket"] == ["week", "install"], order
        assert data["groups"] == expected, order
        assert [list(g)[:2] for g in data["groups"]] == [["week", "install"]] * 4, order


def test_install_sorts_by_rank_before_by_values(client):
    post_recs(client, [ex("z")], instance="one")
    post_recs(client, [ex("a")], instance="two")
    groups = ok(client, schema="executed", by=LABEL, bucket="install")["groups"]
    assert [(g["install"], g[LABEL]) for g in groups] == [("install 1", "z"), ("install 2", "a")]


def _d42_key(instance, body, time_ns, attrs_json):
    return hashlib.sha256(json.dumps([instance, body, time_ns, attrs_json]).encode("ascii")).hexdigest()


def test_install_labels_are_unchanged_by_a_restart_that_rekeys_duplicates(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(sink_app.SCHEMA)
    con.execute("INSERT INTO export_batch VALUES (1, '2026-01-01T00:00:00+00:00', 10, 3)")
    T = 1700000000000000000
    # (id, instance, event id, time): ids 3 and 5 re-send events of ids 1 and 4 with another time
    for row_id, inst, event, t in [(1, "b", 1, T), (2, "a", 2, T), (3, "b", 1, T + 10**12),
                                   (4, "c", 3, T), (5, "c", 3, T + 10**12), (6, "a", 4, T)]:
        attrs = json.dumps({"kw.event.id": str(event), "kw.x": str(row_id)}, sort_keys=True)
        con.execute(
            "INSERT INTO log_record (id, batch_id, received_at, service_name, service_instance_id,"
            " scope_name, time_unix_nano, body, event_id, attributes, dedupe_key)"
            " VALUES (?,1,'t','kworkflow',?,'sc',?,'executed',?,?,?)",
            (row_id, inst, t, event, attrs, _d42_key(inst, "executed", t, attrs)))
    con.commit()
    con.close()
    want = [{"install": "install 1", "records": 1, "installs": 1},  # b
            {"install": "install 2", "records": 2, "installs": 1},  # a
            {"install": "install 3", "records": 1, "installs": 1}]  # c
    for _ in range(2):
        with TestClient(sink_app.app, raise_server_exceptions=False) as c:
            assert fresh_rows(db_path, "SELECT COUNT(*) FROM log_record")[0][0] == 4
            assert ok(c, schema="executed", bucket="install")["groups"] == want
    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        post_recs(c, [ex("new")], instance="d")
        got = ok(c, schema="executed", by=LABEL, bucket="install")["groups"]
        assert [g["install"] for g in got if g[LABEL] == "new"] == ["install 4"]


def test_no_real_install_id_in_any_response(client):
    secret = "secret-install-id-7f3a"
    post_recs(client, [ex("a", t=MON, extra={"kw.x": s("v")})], instance=secret)
    texts = [query(client, schema="executed", **p).text for p in (
        {}, {"by": LABEL}, {"by": "kw.x,kw.event.kind", "measure": SECS},
        {"bucket": "install"}, {"bucket": "week,install"}, {"by": "service.instance.id"},
        {"by": f"{LABEL},nope"}, {"bucket": secret})]
    texts.append(client.get("/analysis/query", params={"schema": secret}).text)
    assert all(secret not in t for t in texts)
    assert query(client, schema="executed", bucket="install").status_code == 200


# --- name and bucket validation ---

@pytest.mark.parametrize("params, prefix", [
    pytest.param({"by": "kw.nope"}, "unknown attribute", id="by-unknown"),
    pytest.param({"by": f"{LABEL},kw.nope"}, "unknown attribute", id="by-one-unknown"),
    pytest.param({"measure": "kw.nope"}, "unknown attribute", id="measure-unknown"),
    pytest.param({"by": "kw.timebox.tag"}, "unknown attribute", id="by-other-schema"),
    pytest.param({"measure": "kw.timebox.tag"}, "unknown attribute", id="measure-other-schema"),
    pytest.param({"by": f"{LABEL},{LABEL}"}, None, id="by-duplicate"),
    pytest.param({"by": f"{LABEL},"}, None, id="by-empty-item"),
    pytest.param({"by": 'x"y'}, None, id="by-double-quote"),
    pytest.param({"measure": 'x"y'}, None, id="measure-double-quote"),
    pytest.param({"bucket": "day"}, "bucket", id="bucket-day"),
    pytest.param({"bucket": "month"}, "bucket", id="bucket-unknown"),
    pytest.param({"bucket": "WEEK"}, "bucket", id="bucket-case"),
    pytest.param({"bucket": ""}, "bucket", id="bucket-empty"),
    pytest.param({"bucket": "week,week"}, "bucket", id="bucket-duplicate"),
    pytest.param({"bucket": "install,install"}, "bucket", id="bucket-install-duplicate"),
    pytest.param({"bucket": "week,"}, "bucket", id="bucket-empty-item"),
    pytest.param({"bucket": "week,day"}, "bucket", id="bucket-one-bad-item"),
])
def test_invalid_request_is_400_with_an_error_message(client, params, prefix):
    # present in the data, but a double quote is never allowed in a name
    post_recs(client, [ex(extra={"a": s("1"), 'x"y': s("1")}),
                       record("timebox", eid(), {"kw.timebox.tag": s("t")})])
    err(client, prefix, schema="executed", **params)


def test_sql_looking_and_path_looking_names(client, db_path):
    post_recs(client, [ex(), ex()])
    for evil in ("x') OR 1=1 --", "'; DELETE FROM log_record; --", "$.a"):
        for field in ("by", "measure", "schema"):
            err(client, **{"schema": "executed", field: evil})
    assert fresh_rows(db_path, "SELECT COUNT(*) FROM log_record")[0][0] == 2
    # a name that is present in the data is a literal key, never a JSON path
    for name in ["a.b", "x[0]", "$.a", "back\\slash", "a b", "ünï", "*", "$..a"]:
        decoys = {"a": s("DECOY"), "x": {"arrayValue": {"values": [s("DECOY")]}},
                  "a.b" if name != "a.b" else "zz": {"kvlistValue": {"values": [
                      {"key": "b", "value": s("DECOY")}]}}}
        post_recs(client, [ex(extra={**decoys, name: s("lit")})])
        r = ok(client, schema="executed", by=name)
        assert [g[name] for g in r["groups"] if g[name] is not None] == ["lit"]


def test_a_very_long_by_list_is_400_not_500(client):
    names = [f"k.{n}" for n in range(3000)]
    post_recs(client, [ex(extra={n: s("v") for n in names})])
    r = query(client, schema="executed", by=",".join(names))
    assert r.status_code == 400, r.text
    assert isinstance(r.json()["error"], str)


# --- reserved names ---

RESERVED = ["week", "install", "records", "installs", "measured", "non_numeric", "sum", "mean",
            "min", "max", "key"]


@pytest.mark.parametrize("name", RESERVED)
def test_reserved_by_names_nest_the_values_under_key(client, name):
    post_recs(client, [ex("a", extra={name: s("v1")}), ex("a", extra={name: s("v1")}),
                       ex("b", extra={name: s("v2")})])
    assert ok(client, schema="executed", by=f"{LABEL},{name}", measure=SECS) == response(
        by=[LABEL, name], measure=SECS, keyed=True, groups=[
            {"key": {LABEL: "a", name: "v1"}, "records": 2, "installs": 1, "measured": 2,
             "non_numeric": 0, "sum": 20, "mean": 10, "min": 10, "max": 10},
            {"key": {LABEL: "b", name: "v2"}, "records": 1, "installs": 1, "measured": 1,
             "non_numeric": 0, "sum": 10, "mean": 10, "min": 10, "max": 10}])


def test_reserved_names_with_bucket_keep_bucket_keys_top_level_and_plain_names_are_not_keyed(client):
    post_recs(client, [ex(t=MON, extra={"week": s("v"), "install": s("w")})])
    assert ok(client, schema="executed", by="week,install", bucket="week,install") == response(
        by=["week", "install"], bucket=["week", "install"], keyed=True, groups=[
            {"week": "2026-11-16", "install": "install 1",
             "key": {"week": "v", "install": "w"}, "records": 1, "installs": 1}])
    plain = ok(client, schema="executed", by=LABEL)  # a reserved name only matters when in by
    assert plain["keyed"] is False and "key" not in plain["groups"][0]


# --- limit ---

def test_truncation_starts_above_1000_groups(client):
    post_recs(client, [ex(f"l{n:04d}") for n in range(1000)])
    data = ok(client, schema="executed", by=LABEL)
    assert data["truncated"] is False and len(data["groups"]) == 1000
    post_recs(client, [ex("l1000")])
    data = ok(client, schema="executed", by=LABEL)
    assert data["truncated"] is True and len(data["groups"]) == 1000
    assert data["groups"][-1][LABEL] == "l0999"
