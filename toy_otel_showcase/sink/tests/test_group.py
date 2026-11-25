import json
from datetime import date

import pytest

from conftest import (bl, d, eid, ex, fresh_rows, i, no_time, payload, post_json, post_recs,
                      record, s)

LABEL = "kw.executed.command_label"
STATUS = "kw.executed.status"
SECS = "kw.executed.elapsed_time_in_secs"


def group(client, **params):
    return client.get("/analysis/group", params=params)


def ok(client, **params):
    r = group(client, **params)
    assert r.status_code == 200, r.text
    return r.json()


def err(client, **params):
    r = group(client, **params)
    assert r.status_code == 400, r.text
    assert isinstance(r.json()["error"], str) and r.json()["error"]
    return r.json()


def test_calendar_assumptions():
    # The bucket tests rely on 2026-11-16 being a Monday.
    assert date(2026, 11, 16).weekday() == 0


# --- by ---

def test_by_one_attribute(client):
    post_recs(client, [ex("build"), ex("build"), ex("deploy")], instance="a")
    post_recs(client, [ex("build")], instance="b")
    data = ok(client, kind="executed", by=LABEL)
    assert data["kind"] == "executed" and data["by"] == [LABEL]
    assert data["truncated"] is False
    got = [(g[LABEL], g["records"], g["installs"]) for g in data["groups"]]
    assert got == [("build", 3, 2), ("deploy", 1, 1)]
    assert "bucket" not in data["groups"][0]
    assert "measured" not in data["groups"][0]


def test_by_two_attributes(client):
    post_recs(client, [ex("build", "success"), ex("build", "failed"), ex("build", "success")])
    data = ok(client, kind="executed", by=f"{LABEL},{STATUS}")
    got = [(g[LABEL], g[STATUS], g["records"]) for g in data["groups"]]
    assert got == [("build", "failed", 1), ("build", "success", 2)]


def test_by_three_attributes(client):
    post_recs(client, [ex("a", "x", extra={"k.z": s("1")}), ex("a", "x", extra={"k.z": s("2")})])
    data = ok(client, kind="executed", by=f"{LABEL},{STATUS},k.z")
    assert [(g["k.z"], g["records"]) for g in data["groups"]] == [("1", 1), ("2", 1)]


def test_no_by_no_bucket_is_one_total_group(client):
    post_recs(client, [ex(secs=3), ex(secs=4)], instance="a")
    post_recs(client, [ex(secs=5)], instance="b")
    data = ok(client, kind="executed")
    assert len(data["groups"]) == 1
    g = data["groups"][0]
    assert (g["records"], g["installs"]) == (3, 2)
    assert LABEL not in g


def test_kind_is_filtered(client):
    post_recs(client, [ex(), record("timebox", eid(), {"kw.timebox.tag": s("t")})])
    assert ok(client, kind="executed")["groups"][0]["records"] == 1
    assert ok(client, kind="timebox")["groups"][0]["records"] == 1


def test_new_kind_works_without_code_change(client):
    post_recs(client, [record("distro", eid(), {"kw.distro.id": s("arch")}),
                       record("distro", eid(), {"kw.distro.id": s("arch")})])
    data = ok(client, kind="distro", by="kw.distro.id")
    assert [(g["kw.distro.id"], g["records"]) for g in data["groups"]] == [("arch", 2)]


def test_installs_ignore_null_and_empty_ids(client, db_path):
    post_recs(client, [ex("g")], instance="a")
    post_recs(client, [ex("g")], instance="")
    p = payload([ex("g")])
    p["resourceLogs"][0]["resource"]["attributes"].pop()  # drop service.instance.id
    assert post_json(client, p).status_code == 200
    g = ok(client, kind="executed", by=LABEL)["groups"][0]
    assert (g["records"], g["installs"]) == (3, 1)


# --- values and missing attributes ---

def test_values_coerced_to_text(client):
    post_recs(client, [
        record("c", eid(), {"v": bl(True)}),
        record("c", eid(), {"v": bl(False)}),
        record("c", eid(), {"v": d(1.5)}),
        record("c", eid(), {"v": i(7)}),
        record("c", eid(), {"v": s("txt")}),
    ])
    data = ok(client, kind="c", by="v")
    vals = {g["v"] for g in data["groups"]}
    assert vals == {"true", "false", "1.5", "7", "txt"}
    assert all(isinstance(v, str) for v in vals)


def test_array_value_returned_as_json_text(client):
    rec = record("c", eid(), {"v": {"arrayValue": {"values": [s("a"), s("b")]}}})
    post_recs(client, [rec])
    v = ok(client, kind="c", by="v")["groups"][0]["v"]
    assert isinstance(v, str) and json.loads(v) == ["a", "b"]


def test_missing_attribute_groups_as_null_last(client):
    post_recs(client, [ex("a"), record("executed", eid(), {STATUS: s("ok")}), ex("b")])
    data = ok(client, kind="executed", by=LABEL)
    assert [(g[LABEL], g["records"]) for g in data["groups"]] == [("a", 1), ("b", 1), (None, 1)]


# --- measure ---

def test_measure_int_strings_sum_mean_min_max(client):
    # As text "9" > "10"; only numeric handling gives min 9 / max 100.
    post_recs(client, [ex(secs=9), ex(secs=10), ex(secs=100), ex(secs=-3)])
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert (g["records"], g["measured"], g["non_numeric"]) == (4, 4, 0)
    assert g["sum"] == 116 and g["min"] == -3 and g["max"] == 100
    assert g["mean"] == pytest.approx(29)


def test_measure_counts_non_numeric_and_missing(client):
    post_recs(client, [
        ex(secs=10),
        ex(extra={SECS: s("abc")}),
        ex(extra={SECS: s("2.5")}),
        ex(extra={SECS: d(1.5)}),
        ex(extra={SECS: s("nan")}),
        ex(extra={SECS: s("inf")}),
        record("executed", eid(), {LABEL: s("build")}),  # no measure attribute
    ])
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert g["records"] == 7
    assert g["measured"] == 3
    assert g["non_numeric"] == 3
    assert g["sum"] == pytest.approx(14)
    assert g["mean"] == pytest.approx(14 / 3)
    assert (g["min"], g["max"]) == (pytest.approx(1.5), pytest.approx(10))


def test_mean_null_when_nothing_measured(client):
    post_recs(client, [ex(extra={SECS: s("abc")}), ex(extra={SECS: s("")})])
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert g["measured"] == 0 and g["non_numeric"] == 2
    assert [g[k] for k in ("sum", "mean", "min", "max")] == [None] * 4


def test_measure_mean_weighted_by_records_not_installs(client):
    post_recs(client, [ex(secs=2), ex(secs=2), ex(secs=2)], instance="a")
    post_recs(client, [ex(secs=8)], instance="b")
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert g["mean"] == pytest.approx(3.5)


def test_measure_with_by(client):
    post_recs(client, [ex("a", secs=1), ex("a", secs=3), ex("b", secs=10)])
    data = ok(client, kind="executed", by=LABEL, measure=SECS)
    assert data["measure"] == SECS
    got = {g[LABEL]: (g["sum"], g["mean"]) for g in data["groups"]}
    assert got == {"a": (4, 2), "b": (10, 10)}


# --- bucket ---

def test_bucket_day_and_week_labels(client):
    times = ["2026-11-16T00:00:00",  # Monday
             "2026-11-18T12:30:00",
             "2026-11-22T23:59:59",  # Sunday, same week
             "2026-11-23T00:00:00"]  # next Monday
    post_recs(client, [ex(t=t) for t in times])
    day = ok(client, kind="executed", bucket="day")
    assert day["bucket"] == "day"
    assert [(g["bucket"], g["records"]) for g in day["groups"]] == [
        ("2026-11-16", 1), ("2026-11-18", 1), ("2026-11-22", 1), ("2026-11-23", 1)]
    week = ok(client, kind="executed", bucket="week")
    assert [(g["bucket"], g["records"]) for g in week["groups"]] == [
        ("2026-11-16", 3), ("2026-11-23", 1)]


def test_bucket_null_time_goes_to_null_bucket_last(client):
    post_recs(client, [ex(t="2026-11-17T08:00:00"), no_time(ex())])
    groups = ok(client, kind="executed", bucket="day")["groups"]
    assert [(g["bucket"], g["records"]) for g in groups] == [("2026-11-17", 1), (None, 1)]


def test_bucket_with_by_orders_by_bucket_then_values(client):
    post_recs(client, [ex("b", t="2026-11-16T01:00:00"), ex("a", t="2026-11-16T02:00:00"),
                       ex("z", t="2026-11-15T23:00:00"), ex("a", t="2026-11-23T00:00:00"),
                       no_time(ex("a"))])
    groups = ok(client, kind="executed", by=LABEL, bucket="week")["groups"]
    assert [(g["bucket"], g[LABEL]) for g in groups] == [
        ("2026-11-09", "z"), ("2026-11-16", "a"), ("2026-11-16", "b"),
        ("2026-11-23", "a"), (None, "a")]


def test_bucket_uses_record_time_not_received_time(client):
    post_recs(client, [ex(t="2001-09-09T01:46:40")])
    assert ok(client, kind="executed", bucket="day")["groups"][0]["bucket"] == "2001-09-09"


# --- limits ---

def test_truncation_at_1000_groups(client):
    post_recs(client, [ex(f"l{n:04d}") for n in range(1001)])
    data = ok(client, kind="executed", by=LABEL)
    assert data["truncated"] is True
    assert len(data["groups"]) == 1000
    assert data["groups"][0][LABEL] == "l0000" and data["groups"][-1][LABEL] == "l0999"


def test_exactly_1000_groups_not_truncated(client):
    post_recs(client, [ex(f"l{n:04d}") for n in range(1000)])
    data = ok(client, kind="executed", by=LABEL)
    assert data["truncated"] is False and len(data["groups"]) == 1000


# --- errors ---

def test_400_missing_kind(client):
    post_recs(client, [ex()])
    err(client)
    err(client, by=LABEL)


def test_400_unknown_kind(client):
    post_recs(client, [ex()])
    err(client, kind="nope")


def test_400_unknown_kind_on_empty_db(client):
    err(client, kind="executed")


def test_400_unknown_by_attribute(client):
    post_recs(client, [ex()])
    err(client, kind="executed", by="kw.nope")
    err(client, kind="executed", by=f"{LABEL},kw.nope")


def test_400_attribute_of_other_kind(client):
    post_recs(client, [ex(), record("timebox", eid(), {"kw.timebox.tag": s("t")})])
    err(client, kind="executed", by="kw.timebox.tag")
    err(client, kind="executed", measure="kw.timebox.tag")


def test_400_unknown_measure(client):
    post_recs(client, [ex()])
    err(client, kind="executed", measure="kw.nope")


def test_400_bad_bucket(client):
    post_recs(client, [ex()])
    for b in ("month", "", "DAY", "day;"):
        err(client, kind="executed", bucket=b)


def test_400_more_than_three_by(client):
    post_recs(client, [ex(extra={"a": s("1"), "b": s("1")})])
    err(client, kind="executed", by=f"{LABEL},{STATUS},a,b")


def test_400_duplicate_by(client):
    post_recs(client, [ex()])
    err(client, kind="executed", by=f"{LABEL},{LABEL}")


def test_400_attribute_name_with_double_quote_even_if_present(client):
    post_recs(client, [ex(extra={'x"y': s("1")})])
    err(client, kind="executed", by='x"y')
    err(client, kind="executed", measure='x"y')


@pytest.mark.parametrize("evil", [
    "x') OR 1=1 --",
    'a" ) ; DROP TABLE log_record; --',
    "$.a",
    "'; DELETE FROM log_record; --",
])
def test_sql_injection_looking_names_rejected_and_data_intact(client, db_path, evil):
    post_recs(client, [ex(), ex()])
    err(client, kind="executed", by=evil)
    err(client, kind="executed", measure=evil)
    err(client, kind=evil)
    assert fresh_rows(db_path, "SELECT COUNT(*) FROM log_record")[0][0] == 2


def test_attribute_name_with_dot_and_unusual_chars_is_a_plain_name(client):
    post_recs(client, [ex(extra={"weird name.v[0]": s("q")})])
    data = ok(client, kind="executed", by="weird name.v[0]")
    assert data["groups"][0]["weird name.v[0]"] == "q"


# --- privacy of the response ---

def test_no_install_ids_anywhere(client):
    ids = ["secret-install-aaa", "secret-install-bbb"]
    for n, inst in enumerate(ids):
        post_recs(client, [ex("build", t="2026-11-16T00:00:00")], instance=inst)
    for params in (dict(kind="executed"),
                   dict(kind="executed", by=LABEL, measure=SECS, bucket="week"),
                   dict(kind="executed", by=f"{LABEL},{STATUS}", bucket="day")):
        resp = client.get("/analysis/group", params=params)
        assert resp.status_code == 200
        assert not any(x in resp.text for x in ids)
        assert "service_instance_id" not in resp.text
        assert "service.instance.id" not in resp.text
    assert client.get("/analysis/group", params=dict(kind="nope")).text.count("secret") == 0


# --- reserved names, odd attribute names, null values, numeric strings ---

RESERVED = ["bucket", "records", "installs", "measured", "non_numeric", "sum", "mean", "min",
            "max", "key"]


def test_keyed_false_for_plain_names(client):
    post_recs(client, [ex("a")])
    assert ok(client, kind="executed", by=LABEL)["keyed"] is False
    assert ok(client, kind="executed")["keyed"] is False
    assert ok(client, kind="executed", bucket="day", measure=SECS)["keyed"] is False


@pytest.mark.parametrize("name", RESERVED)
def test_reserved_by_name_nests_all_values_under_key(client, name):
    post_recs(client, [ex("a", extra={name: s("v1")}), ex("a", extra={name: s("v1")}),
                       ex("b", extra={name: s("v2")})])
    data = ok(client, kind="executed", by=f"{LABEL},{name}")
    assert data["keyed"] is True
    got = [(g["key"][LABEL], g["key"][name], g["records"]) for g in data["groups"]]
    assert got == [("a", "v1", 2), ("b", "v2", 1)]
    for g in data["groups"]:
        assert LABEL not in g  # no group value at top level
        assert set(g["key"]) == {LABEL, name}


def test_reserved_name_does_not_overwrite_stats(client):
    post_recs(client, [ex(extra={"records": s("x"), "sum": s("y")}) for _ in range(3)])
    g = ok(client, kind="executed", by="records,sum", measure=SECS)["groups"][0]
    assert g["records"] == 3 and g["sum"] == 30
    assert g["key"] == {"records": "x", "sum": "y"}


def test_reserved_name_with_bucket_keeps_bucket_top_level(client):
    post_recs(client, [ex(t="2026-11-16T00:00:00", extra={"bucket": s("v")})])
    g = ok(client, kind="executed", by="bucket", bucket="day")["groups"][0]
    assert g["bucket"] == "2026-11-16" and g["key"] == {"bucket": "v"}


def test_reserved_name_only_matters_when_in_by(client):
    post_recs(client, [ex(extra={"sum": s("v")})])
    data = ok(client, kind="executed", by=LABEL)
    assert data["keyed"] is False and "key" not in data["groups"][0]


def test_keyed_present_on_all_200_responses(client):
    post_recs(client, [ex()])
    for params in (dict(kind="executed"), dict(kind="executed", by=LABEL),
                   dict(kind="executed", bucket="week")):
        assert isinstance(ok(client, **params)["keyed"], bool)


@pytest.mark.parametrize("name", ["a.b", "x[0]", "$.a", "$", "a$b", "back\\slash", "a\\", "[0]",
                                  "a b", "ünï", "*", "a.*", "$..a"])
def test_odd_attribute_names_are_literal_or_400(client, name):
    # A name is a literal key, never a JSON path: the decoys below must not be reached.
    decoys = {"a": s("DECOY"), "x": {"arrayValue": {"values": [s("DECOY")]}},
              "": s("DECOY")}
    post_recs(client, [ex(extra={**decoys, name: s("lit"), "other": s("o")})])
    r = group(client, kind="executed", by=name)
    if r.status_code == 400:
        assert r.json()["error"]
        return
    assert r.status_code == 200, r.text
    g = r.json()["groups"]
    assert [x.get("key", x).get(name) for x in g] == ["lit"]
    assert "DECOY" not in r.text
    m = group(client, kind="executed", measure=name)
    assert m.status_code in (200, 400)
    if m.status_code == 200:
        assert m.json()["groups"][0]["non_numeric"] == 1


def test_dotted_name_not_confused_with_nested_path(client):
    # "a.b" literal key vs the path a -> b: only the literal key exists.
    rec = ex(extra={"a.b": s("flat"), "a": {"kvlistValue": {"values": [
        {"key": "b", "value": s("nested")}]}}})
    post_recs(client, [rec])
    g = ok(client, kind="executed", by="a.b")["groups"]
    assert [x["a.b"] for x in g] == ["flat"]


def test_null_attribute_value_consistent(client):
    # An explicit JSON null: the key shows in the schema, groups as null, and counts as
    # neither measured nor non_numeric (same as an absent attribute).
    post_recs(client, [ex(secs=4), ex(extra={SECS: {}}), ex(extra={SECS: s("x")})])
    names = {a["name"]: a["records"] for a in next(
        k for k in client.get("/analysis/schema").json()["kinds"]
        if k["kind"] == "executed")["attributes"]}
    assert names[SECS] == 3  # schema counts the null-valued key (it is present in the JSON)
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert (g["records"], g["measured"], g["non_numeric"]) == (3, 1, 1)
    by = ok(client, kind="executed", by=SECS)["groups"]
    assert (None, 1) in [(x[SECS], x["records"]) for x in by]


@pytest.mark.parametrize("text,numeric", [
    ("1e3", True), ("1E3", True), ("-2.5e-1", True), ("+7", True), (".5", True), ("5.", True),
    (" 12 ", False), ("12 ", False), ("", False), ("1,5", False), ("0x10", False),
    ("1e999", False), ("nan", False), ("Infinity", False), ("1_000", False), ("--1", False),
])
def test_measure_string_parsing(client, text, numeric):
    post_recs(client, [ex(extra={SECS: s(text)})])
    g = ok(client, kind="executed", measure=SECS)["groups"][0]
    assert (g["measured"], g["non_numeric"]) == ((1, 0) if numeric else (0, 1))
    if text == "1e3":
        assert g["sum"] == 1000


# --- non-finite and overflowing aggregates ---

BIG = "9223372036854775807"


def test_double_overflow_sum_returns_200_with_nulls(client):
    post_recs(client, [ex(extra={SECS: d(1e308)}), ex(extra={SECS: d(1e308)})])
    r = group(client, kind="executed", measure=SECS)
    assert r.status_code == 200, r.text
    g = r.json()["groups"][0]
    assert g["measured"] == 2 and g["records"] == 2
    assert g["sum"] is None and g["mean"] is None
    assert g["min"] == 1e308 and g["max"] == 1e308  # finite stats are unaffected


def test_numeric_string_overflow_sum_returns_200(client):
    post_recs(client, [ex(extra={SECS: s("1e308")}), ex(extra={SECS: s("1e308")})])
    r = group(client, kind="executed", measure=SECS)
    assert r.status_code == 200, r.text
    g = r.json()["groups"][0]
    assert g["measured"] == 2 and g["sum"] is None and g["mean"] is None
    assert g["min"] == 1e308


def test_overflow_in_one_group_does_not_hide_the_others(client):
    post_recs(client, [ex("big", extra={SECS: d(1e308)}), ex("big", extra={SECS: d(1e308)}),
                       ex("ok", secs=4)])
    r = group(client, kind="executed", by=LABEL, measure=SECS)
    assert r.status_code == 200, r.text
    got = {g[LABEL]: g for g in r.json()["groups"]}
    assert got["big"]["sum"] is None
    assert got["ok"]["sum"] == 4 and got["ok"]["mean"] == 4


def test_huge_int64_strings_sum_does_not_overflow(client):
    post_recs(client, [ex(secs=BIG), ex(secs=BIG), ex(secs=BIG)])
    r = group(client, kind="executed", measure=SECS)
    assert r.status_code == 200, r.text
    g = r.json()["groups"][0]
    assert g["measured"] == 3
    assert g["sum"] is None or g["sum"] == pytest.approx(3 * 9.223372036854775807e18)
    assert g["max"] == pytest.approx(9.223372036854775807e18)


def test_stats_logs_huge_elapsed_does_not_500(client):
    post_recs(client, [ex("big", secs=BIG), ex("big", secs=BIG)])
    r = client.get("/stats/logs")
    assert r.status_code == 200, r.text
    row = r.json()["statistics"][0]
    assert row["total"] == 2 and row["max_secs"] is not None


def test_stats_logs_huge_timebox_duration_does_not_500(client):
    post_recs(client, [timebox_(BIG), timebox_(BIG)])
    r = client.get("/stats/logs")
    assert r.status_code == 200, r.text
    assert r.json()["pomodoro"][0]["sessions"] == 2


def timebox_(duration):
    return record("timebox", eid(), {"kw.timebox.tag": s("t"), "kw.timebox.duration": i(duration)})
