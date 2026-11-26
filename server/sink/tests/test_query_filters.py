"""GET /analysis/query filters: where, from/to, join (D58)."""
import sqlite3

import pytest

from conftest import (bl, build_modules, d, eid, ex, fresh_rows, i, no_time, ns, post_recs, record,
                      s, sink_app)

pytestmark = pytest.mark.usefixtures("raw_times")

LABEL = "kw.executed.command_label"
STATUS = "kw.executed.status"
SECS = "kw.executed.elapsed_time_in_secs"
MODULES = "kw.build_modules.module_count"


def query(client, **params):
    return client.get("/analysis/query", params=params)


def ok(client, **params):
    r = query(client, **params)
    assert r.status_code == 200, r.text
    return r.json()


def err(client, prefix, **params):
    r = query(client, **params)
    assert r.status_code == 400, r.text
    assert r.json()["error"].startswith(prefix), r.json()


def counts(client, by, **params):
    """{by value: records} for a one-attribute grouping."""
    return {g[by]: g["records"] for g in ok(client, by=by, **params)["groups"]}


def total(client, **params):
    # With nothing to group by, an empty selection may be no group or one group of zero records.
    return sum(g["records"] for g in ok(client, **params)["groups"])


def response(schema="executed", join=(), by=(), bucket=(), measure=None, where=(), frm=None,
             to=None, groups=()):
    return {"schema": schema, "join": list(join), "by": list(by), "bucket": list(bucket),
            "measure": measure, "where": list(where), "from": frm, "to": to, "keyed": False,
            "groups": list(groups), "truncated": False}


# --- where ---

def test_where_equality_and_repeats_are_anded(client):
    post_recs(client, [ex("build", "success"), ex("build", "failed"), ex("deploy", "success")])
    assert ok(client, schema="executed", where=f"{LABEL}:build") == response(
        where=[f"{LABEL}:build"], groups=[{"records": 2, "installs": 1}])
    both = [f"{LABEL}:build", f"{STATUS}:failed"]
    assert ok(client, schema="executed", where=both) == response(
        where=both, groups=[{"records": 1, "installs": 1}])
    assert total(client, schema="executed", where=[f"{LABEL}:deploy", f"{STATUS}:failed"]) == 0


def test_where_splits_at_the_first_colon_only(client):
    post_recs(client, [ex("a:b"), ex("a"), ex("b")])
    assert total(client, schema="executed", where=f"{LABEL}:a:b") == 1


def test_where_round_trips_every_by_group_value(client):
    # Mixed JSON types in one attribute; each by-displayed text must select exactly its group.
    post_recs(client, [record("c", eid(), {"v": v}) for v in (
        s("txt"), s("txt"), s("7"), i(8), bl(True), bl(False), d(1.5), s(""))])
    shown = counts(client, "v", schema="c")
    assert shown == {"": 1, "1.5": 1, "7": 1, "8": 1, "false": 1, "true": 1, "txt": 2}
    for text, n in shown.items():
        assert total(client, schema="c", where=f"v:{text}") == n


@pytest.mark.parametrize("stored, text, other", [
    pytest.param(i(42), "42", "42.0", id="int-as-decimal-text"),
    pytest.param(i(-5), "-5", "5", id="negative-int"),
    pytest.param(bl(True), "true", "1", id="bool-true"),
    pytest.param(bl(False), "false", "0", id="bool-false"),
    pytest.param(s("Abc"), "Abc", "abc", id="case-sensitive"),
])
def test_where_matches_the_shown_text_only(client, stored, text, other):
    post_recs(client, [record("c", eid(), {"v": stored})])
    assert total(client, schema="c", where=f"v:{text}") == 1
    assert total(client, schema="c", where=f"v:{other}") == 0


def test_null_and_absent_never_match_but_empty_string_does(client):
    post_recs(client, [record("c", eid(), {"v": s("")}), record("c", eid(), {"v": {}}),
                       record("c", eid(), {"w": s("x")}), record("c", eid(), {"v": s("x")})])
    assert total(client, schema="c", where="v:") == 1
    assert total(client, schema="c", where="v:null") == 0


@pytest.mark.parametrize("where, prefix", [
    pytest.param("nocolon", "where", id="no-colon"),
    pytest.param("", "where", id="empty"),
    pytest.param("no.such.attr:x", "unknown attribute", id="unknown-attribute"),
    pytest.param('bad"name:x', "unknown attribute", id="quote-in-name"),
])
def test_where_malformed_or_unknown_is_400(client, where, prefix):
    post_recs(client, [ex()])
    err(client, prefix, schema="executed", where=where)


@pytest.mark.parametrize("value", [
    pytest.param("x' OR '1'='1", id="quote-or"),
    pytest.param("x'; DROP TABLE log_record; --", id="drop-table"),
    pytest.param("%", id="like-wildcard"),
    pytest.param('x" OR 1=1 --', id="double-quote"),
])
def test_where_sql_metacharacters_stay_inert(client, db_path, value):
    post_recs(client, [ex("build"), ex(value)])
    assert total(client, schema="executed", where=f"{LABEL}:{value}") == 1
    assert fresh_rows(db_path, "SELECT COUNT(*) FROM log_record")[0][0] == 2


# --- from / to ---

# The week of Monday 2026-11-16 runs to Sunday the 22nd; 2026-11-15 belongs to the week of the 9th.
TIMES = {"sun-prev": "2026-11-15T12:00:00", "mon": "2026-11-16T00:00:00",
         "wed": "2026-11-18T12:00:00", "sun": "2026-11-22T23:59:59",
         "next-mon": "2026-11-23T00:00:00"}


def weekly(client):
    post_recs(client, [ex(label, t=t) for label, t in TIMES.items()])


def labels(client, **params):
    return sorted(counts(client, LABEL, schema="executed", **params))


def test_from_to_compare_the_weeks_monday_inclusive_at_both_ends(client):
    weekly(client)
    assert labels(client, **{"from": "2026-11-16"}) == ["mon", "next-mon", "sun", "wed"]
    assert labels(client, to="2026-11-16") == ["mon", "sun", "sun-prev", "wed"]
    assert labels(client, **{"from": "2026-11-16", "to": "2026-11-16"}) == ["mon", "sun", "wed"]
    assert labels(client, **{"from": "2026-11-17", "to": "2026-11-22"}) == []
    assert labels(client, **{"from": "2026-11-23", "to": "2026-11-23"}) == ["next-mon"]
    assert labels(client, **{"from": "2026-11-09", "to": "2026-11-09"}) == ["sun-prev"]


def test_from_to_are_echoed(client):
    weekly(client)
    assert ok(client, schema="executed", **{"from": "2026-11-16", "to": "2026-11-16"}) == response(
        frm="2026-11-16", to="2026-11-16", groups=[{"records": 3, "installs": 1}])


def test_time_zero_is_the_week_of_1969_12_29(client):
    zero = ex("epoch")
    zero["timeUnixNano"] = zero["observedTimeUnixNano"] = "0"
    post_recs(client, [zero, ex("later", t="2026-11-18T00:00:00")])
    assert labels(client, **{"from": "1969-12-29", "to": "1969-12-29"}) == ["epoch"]
    assert labels(client, to="1969-12-28") == []
    assert labels(client, to="1969-12-30") == ["epoch"]
    assert ok(client, schema="executed", by=LABEL, bucket="week", to="1969-12-29")["groups"] == [
        {"week": "1969-12-29", LABEL: "epoch", "records": 1, "installs": 1}]


@pytest.mark.parametrize("bound", [{"from": "2000-01-01"}, {"to": "2100-01-01"},
                                   {"from": "2000-01-01", "to": "2100-01-01"}],
                         ids=["from", "to", "both"])
def test_null_time_is_excluded_when_either_bound_is_given(client, bound):
    post_recs(client, [no_time(ex("none")), ex("some", t="2026-11-18T00:00:00")])
    assert labels(client, **bound) == ["some"]
    assert labels(client) == ["none", "some"]


def test_from_after_to_gives_200_and_no_groups(client):
    weekly(client)
    assert ok(client, schema="executed", by=LABEL, **{"from": "2026-11-23", "to": "2026-11-16"}
              ) == response(by=[LABEL], frm="2026-11-23", to="2026-11-16")


@pytest.mark.parametrize("name, value", [
    pytest.param("from", "2026-1-5", id="from-unpadded"),
    pytest.param("from", "2026-02-30", id="from-not-a-date"),
    pytest.param("from", "20261116", id="from-compact"),
    pytest.param("from", "2026-11-16T00:00:00", id="from-datetime"),
    pytest.param("from", " 2026-11-16", id="from-space"),
    pytest.param("from", "", id="from-empty"),
    pytest.param("to", "yesterday", id="to-word"),
    pytest.param("to", "2026-13-01", id="to-month-13"),
    pytest.param("to", "2026-11-16\n", id="to-newline"),
])
def test_invalid_dates_are_400(client, name, value):
    weekly(client)
    err(client, name, schema="executed", **{name: value})


# --- join ---

def pair(client, status="success", count=3, t=None, instance="inst-1"):
    """A kw-shaped executed + build_modules pair sharing install and kw.event.id."""
    e = eid()
    recs = [ex("build", status, event_id=e, t=t), build_modules(e, count)]
    if t:
        for r in recs:
            r["timeUnixNano"] = r["observedTimeUnixNano"] = ns(t)
    post_recs(client, recs, instance=instance)


def test_modules_versus_failures(client):
    pair(client, "success", 4)
    pair(client, "success", 6)
    pair(client, "failed", 20)
    assert ok(client, schema="build_modules", join="executed", by=STATUS, measure=MODULES
              ) == response(
        schema="build_modules", join=["executed"], by=[STATUS], measure=MODULES, groups=[
            {STATUS: "failed", "records": 1, "installs": 1, "measured": 1, "non_numeric": 0,
             "sum": 20, "mean": 20, "min": 20, "max": 20},
            {STATUS: "success", "records": 2, "installs": 1, "measured": 2, "non_numeric": 0,
             "sum": 10, "mean": 5, "min": 4, "max": 6}])


def test_event_without_a_matching_row_gives_null(client):
    pair(client, "success", 4)
    post_recs(client, [build_modules(eid(), 9)])
    assert ok(client, schema="build_modules", join="executed", by=STATUS)["groups"] == [
        {STATUS: "success", "records": 1, "installs": 1},
        {STATUS: None, "records": 1, "installs": 1}]


def test_join_needs_the_same_non_empty_install(client):
    e1, e2 = eid(), eid()
    post_recs(client, [ex(status="a", event_id=e1)], instance="inst-1")
    post_recs(client, [build_modules(e1, 1)], instance="inst-2")  # same event id, other install
    post_recs(client, [ex(status="b", event_id=e2)], instance="")
    post_recs(client, [build_modules(e2, 1)], instance="")
    assert counts(client, STATUS, schema="build_modules", join="executed") == {None: 2}


def test_join_ignores_records_without_event_id(client):
    recs = [ex(status="x"), build_modules(0, 1)]
    for r in recs:
        r["attributes"] = [a for a in r["attributes"] if a["key"] != "kw.event.id"]
    post_recs(client, recs)
    assert counts(client, STATUS, schema="build_modules", join="executed") == {None: 1}


def test_name_resolution_prefers_primary_then_join_order(client):
    e = eid()

    def rec(kind, **a):
        return record(kind, e, {k: s(v) for k, v in a.items()})

    post_recs(client, [rec("p", own="p", dup="from-p"), rec("a", dup="from-a", only_a="A"),
                       rec("b", dup="from-b", only_a="B", only_b="B")])

    def first(**kw):
        return ok(client, schema="p", **kw)["groups"][0]

    assert first(join="a,b", by="dup")["dup"] == "from-p"
    assert first(join="a,b", by="only_a")["only_a"] == "A"
    assert first(join="b,a", by="only_a")["only_a"] == "B"
    assert first(join="b,a", by="only_b")["only_b"] == "B"


def test_attribute_of_a_non_joined_schema_is_400(client):
    pair(client)
    post_recs(client, [record("timebox", eid(), {"kw.timebox.tag": s("t")})])
    err(client, "unknown attribute", schema="build_modules", by=STATUS)
    err(client, "unknown attribute", schema="build_modules", measure=SECS)
    err(client, "unknown attribute", schema="build_modules", where=f"{STATUS}:success")
    err(client, "unknown attribute", schema="build_modules", join="executed", by="kw.timebox.tag")


@pytest.mark.parametrize("join, prefix", [
    pytest.param("nope", "unknown schema", id="unknown"),
    pytest.param("executed,nope", "unknown schema", id="one-unknown"),
    pytest.param("executed,executed", "join", id="duplicate"),
    pytest.param("build_modules", "join", id="self"),
    pytest.param("executed,", "join", id="empty-item"),
    pytest.param("", "join", id="empty-value"),
])
def test_bad_join_is_400(client, join, prefix):
    pair(client)
    err(client, prefix, schema="build_modules", join=join)


def test_where_on_a_joined_attribute_drops_events_with_no_match(client):
    pair(client, "success", 4)
    pair(client, "failed", 5)
    post_recs(client, [build_modules(eid(), 6)])
    assert ok(client, schema="build_modules", join="executed", where=f"{STATUS}:success"
              )["groups"] == [{"records": 1, "installs": 1}]
    assert total(client, schema="build_modules", join="executed", where=f"{STATUS}:") == 0


def test_join_with_filters_and_bucket_still_hides_install_ids(client):
    pair(client, "success", 4, t="2026-11-17T00:00:00", instance="secret-install-a")
    pair(client, "failed", 8, t="2026-11-24T00:00:00", instance="secret-install-b")
    r = query(client, schema="build_modules", join="executed", by=STATUS, bucket="week,install",
              **{"from": "2026-11-16", "to": "2026-11-23"})
    assert r.status_code == 200, r.text
    assert "secret-install" not in r.text
    assert r.json()["groups"] == [
        {"week": "2026-11-16", "install": "install 1", STATUS: "success", "records": 1,
         "installs": 1},
        {"week": "2026-11-23", "install": "install 2", STATUS: "failed", "records": 1,
         "installs": 1}]


def test_more_than_63_joins_is_400_not_500(client):
    names = [f"s{n}" for n in range(64)]
    post_recs(client, [record(name, eid(), {f"a.{name}": s("x")}) for name in names] + [ex()])
    err(client, "join", schema="executed", join=",".join(names))


def test_group_cap_with_join_and_week_install_bucket(client):
    n = 1001
    ids = [eid() for _ in range(n)]
    post_recs(client, [ex(event_id=e) for e in ids] + [build_modules(e, k) for k, e in enumerate(ids)])
    r = ok(client, schema="build_modules", join="executed", by=MODULES, bucket="week,install")
    assert (len(r["groups"]), r["truncated"]) == (1000, True)
    assert {(g["week"], g["install"], g["installs"]) for g in r["groups"]} == {
        ("2023-11-13", "install 1", 1)}


# --- index and query plan ---

class Spy:
    """Stands in for the app's connection: records every statement, optionally raising."""

    def __init__(self, real, fail=None):
        self.real, self.fail, self.seen = real, fail, []

    def execute(self, sql, params=()):
        self.seen.append((sql, tuple(params)))
        if self.fail and self.fail(sql):
            raise sqlite3.OperationalError("disk I/O error")
        return self.real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self.real, name)


def test_join_search_uses_all_three_index_columns(client, monkeypatch):
    pair(client, "success", 4)
    spy = Spy(sink_app._conn)
    monkeypatch.setattr(sink_app, "_conn", spy)
    ok(client, schema="build_modules", join="executed", by=STATUS)
    sql, params = [x for x in spy.seen if "JOIN" in x[0].upper()][-1]
    plan = [row[3] for row in spy.real.execute("EXPLAIN QUERY PLAN " + sql, params)]
    searches = [p for p in plan if p.startswith("SEARCH") and "log_record_join" in p]
    assert any(all(c in p for c in ("body=?", "service_instance_id=?", "event_id=?")) for p in searches), plan


def test_non_limit_sqlite_error_is_a_500(client, monkeypatch):
    pair(client)
    monkeypatch.setattr(sink_app, "_conn", Spy(sink_app._conn, fail=lambda sql: "COUNT(DISTINCT inst)" in sql))
    r = query(client, schema="executed", by=STATUS)
    assert r.status_code == 500, r.text


# --- index ---

def test_join_index_exists_after_startup(client, db_path):
    assert fresh_rows(db_path, "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                      ("log_record_join",)) == [("log_record_join",)]
    cols = fresh_rows(db_path,
                      "SELECT name FROM pragma_index_info('log_record_join') ORDER BY seqno")
    assert cols == [("body",), ("service_instance_id",), ("event_id",)]
