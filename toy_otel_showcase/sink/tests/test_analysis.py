import json
from itertools import count as _counter

import pytest

from conftest import build_modules, i, payload, post_json, record, s

_ids = _counter(1)


def post_build(client, instance, modules=None, names=None, command=None,
               status="success", secs=10, elapsed=None):
    """One kw build = an `executed` record and a `build_modules` record with one shared event id."""
    eid = next(_ids)
    recs = []
    if command is not None or modules is None:
        recs.append(record("executed", eid, {
            "kw.executed.command": s(command or "build"),
            "kw.executed.status": s(status),
            "kw.executed.elapsed_time_in_secs": i(secs),
        }))
    if modules is not None:
        recs.append(build_modules(eid, modules, names))
    assert post_json(client, payload(recs, instance=instance)).status_code == 200


def rows(resp):
    """Rows list: bare list, or the first list value of the response object."""
    if isinstance(resp, list):
        return resp
    return next(v for v in resp.values() if isinstance(v, list) and v != resp.get("names"))


@pytest.fixture
def setk(monkeypatch):
    # Assumption: K is read from os.environ at request time.
    return lambda k: monkeypatch.setenv("KANON_K", str(k))


def modules(client):
    r = client.get("/analysis/modules")
    assert r.status_code == 200
    return r.json()


def commands(client):
    r = client.get("/analysis/commands")
    assert r.status_code == 200
    return r.json()


def buckets(data):
    return {b["bucket"]: b for b in data["buckets"]}


@pytest.mark.parametrize("n,label", [
    (0, "0"), (1, "1-49"), (49, "1-49"), (50, "50-199"), (199, "50-199"),
    (200, "200-499"), (499, "200-499"), (500, "500+"), (5000, "500+"),
])
def test_bucket_boundaries(client, setk, n, label):
    setk(2)
    post_build(client, "a", modules=n)
    post_build(client, "b", modules=n)
    b = buckets(modules(client))
    assert b[label]["installs"] == 2 and b[label]["builds"] == 2
    assert set(b) == {label}


def test_installs_distinct_builds_total(client, setk):
    setk(2)
    for _ in range(3):
        post_build(client, "a", modules=10)
    post_build(client, "b", modules=10)
    row = buckets(modules(client))["1-49"]
    assert (row["installs"], row["builds"]) == (2, 4)


def test_small_buckets_merge_into_suppressed(client, setk):
    setk(2)
    post_build(client, "a", modules=0)
    post_build(client, "b", modules=100)
    post_build(client, "c", modules=10)
    post_build(client, "d", modules=10)
    b = buckets(modules(client))
    assert set(b) == {"1-49", "suppressed"}
    assert (b["suppressed"]["installs"], b["suppressed"]["builds"]) == (2, 2)


def test_merged_group_omitted_when_still_under_k(client, setk):
    setk(3)
    post_build(client, "a", modules=0)
    post_build(client, "b", modules=100)
    for inst in "cde":
        post_build(client, inst, modules=10)
    assert set(buckets(modules(client))) == {"1-49"}


def test_names_below_k_dropped_without_merge_and_ordered(client, setk):
    setk(2)
    post_build(client, "a", modules=3, names="zeta,alpha,,rare")
    post_build(client, "b", modules=3, names="alpha,zeta")
    post_build(client, "c", modules=3, names="alpha")
    names = modules(client)["names"]
    assert names == [{"name": "alpha", "installs": 3}, {"name": "zeta", "installs": 2}]


def test_names_empty_when_no_record_has_names(client, setk):
    setk(2)
    post_build(client, "a", modules=3)
    post_build(client, "b", modules=3)
    assert modules(client)["names"] == []


def test_no_service_instance_id_in_any_response(client, setk):
    setk(2)
    ids = ["secret-install-aaa", "secret-install-bbb"]
    for inst in ids:
        post_build(client, inst, modules=7, names="x", command="build")
    for ep in ("/analysis/modules", "/analysis/commands"):
        resp = client.get(ep)
        assert resp.status_code == 200
        text = resp.text
        assert not any(x in text for x in ids)
        assert len(json.loads(text)) > 0


def test_k_default_is_5(client, monkeypatch):
    monkeypatch.delenv("KANON_K", raising=False)
    for inst in "abcd":
        post_build(client, inst, modules=10)
    d = modules(client)
    assert d["k"] == 5 and d["buckets"] == []
    post_build(client, "e", modules=10)
    assert buckets(modules(client))["1-49"]["installs"] == 5


def test_k_below_2_is_clamped_to_2(client, setk):
    setk(1)
    post_build(client, "a", modules=10)
    d = modules(client)
    assert d["k"] == 2 and d["buckets"] == []
    post_build(client, "b", modules=10)
    assert buckets(modules(client))["1-49"]["installs"] == 2


def test_string_int64_values_are_cast(client, setk):
    setk(2)
    # As text "100" < "50"; only an integer comparison puts both in 50-199.
    post_build(client, "a", modules=100)
    post_build(client, "b", modules=50)
    assert set(buckets(modules(client))) == {"50-199"}


def test_two_records_per_build_count_once(client, setk):
    setk(2)
    post_build(client, "a", modules=10, command="build")
    post_build(client, "b", modules=10, command="build")
    assert buckets(modules(client))["1-49"]["builds"] == 2
    r = rows(commands(client))
    assert len(r) == 1 and r[0]["runs"] == 2


def test_commands_groups_and_aggregates(client, setk):
    setk(2)
    post_build(client, "a", command="build", status="success", secs=9)
    post_build(client, "b", command="build", status="success", secs=10)
    post_build(client, "b", command="build", status="success", secs=11)
    r = rows(commands(client))
    assert len(r) == 1
    row = r[0]
    assert (row["command"], row["status"]) == ("build", "success")
    assert (row["installs"], row["runs"], row["max_secs"]) == (2, 3, 11)
    assert row["mean_secs"] == pytest.approx(10)
    assert isinstance(row["max_secs"], int)


def test_commands_suppression_merges_to_star(client, setk):
    setk(2)
    post_build(client, "a", command="deploy", status="failed")
    post_build(client, "b", command="mail", status="success")
    post_build(client, "c", command="build")
    post_build(client, "d", command="build")
    got = {(x["command"], x["status"]): x for x in rows(commands(client))}
    assert set(got) == {("build", "success"), ("suppressed", "*")}
    assert (got[("suppressed", "*")]["installs"], got[("suppressed", "*")]["runs"]) == (2, 2)


def test_commands_merged_omitted_when_under_k(client, setk):
    setk(3)
    post_build(client, "a", command="deploy")
    post_build(client, "b", command="mail")
    for inst in "cde":
        post_build(client, inst, command="build")
    assert [x["command"] for x in rows(commands(client))] == ["build"]


def test_commands_executed_without_attributes_returns_200(client, monkeypatch):
    monkeypatch.delenv("KANON_K", raising=False)
    for n in range(6):
        post_build(client, f"i{n}", command="build", status="success")
    bare = record("executed", next(_ids), {})
    assert post_json(client, payload([bare], instance="i0")).status_code == 200
    r = client.get("/analysis/commands")
    assert r.status_code == 200
    got = {(x["command"], x["status"]) for x in rows(r.json())}
    assert ("build", "success") in got


def test_modules_record_without_module_count_returns_200(client, monkeypatch):
    monkeypatch.delenv("KANON_K", raising=False)
    for n in range(6):
        post_build(client, f"i{n}", modules=10)
    bare = record("build_modules", next(_ids), {})
    assert post_json(client, payload([bare], instance="i0")).status_code == 200
    r = client.get("/analysis/modules")
    assert r.status_code == 200
    assert "1-49" in buckets(r.json())


def test_mean_secs_ignores_runs_with_missing_elapsed(client, setk):
    setk(2)
    post_build(client, "a", command="build", secs=10)
    post_build(client, "b", command="build", secs=20)
    no_elapsed = record("executed", next(_ids), {
        "kw.executed.command": s("build"), "kw.executed.status": s("success")})
    assert post_json(client, payload([no_elapsed], instance="a")).status_code == 200
    row = rows(commands(client))[0]
    assert row["runs"] == 3
    assert row["mean_secs"] == pytest.approx(15)
