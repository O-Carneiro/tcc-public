"""GET /analysis/schemas and /analysis/schema/{schema}/attributes (D58)."""
import pytest

from conftest import eid, ex, get_ok, payload, post_json, post_recs, record, s

LABEL = "kw.executed.command_label"
NOT_FOUND = {"detail": "Not Found"}


def attrs(*pairs):
    return [{"name": n, "records": c} for n, c in sorted(pairs)]


def no_body_record():
    rec = record("executed", eid(), {})
    rec["body"] = {}  # a record without a schema is not a schema of its own
    return rec


# ---- /analysis/schemas

def test_schemas_empty_store(client):
    assert get_ok(client, "/analysis/schemas") == {"schemas": []}


def test_schemas_lists_records_and_installs_sorted_without_attributes(client):
    post_recs(client, [ex(), ex(), record("timebox", eid(), {"kw.timebox.tag": s("t")}),
                       no_body_record()], instance="a")
    post_recs(client, [ex(extra={"kw.hardware.gpu": s("rtx")}), ex()], instance="b")

    assert get_ok(client, "/analysis/schemas") == {"schemas": [
        {"schema": "executed", "records": 4, "installs": 2},
        {"schema": "timebox", "records": 1, "installs": 1},
    ]}


def test_schemas_installs_ignore_empty_and_null_ids(client):
    post_recs(client, [ex()], instance="a")
    post_recs(client, [ex()], instance="")
    # A resource with no service.instance.id at all gives a NULL install id.
    body = payload([ex()])
    body["resourceLogs"][0]["resource"]["attributes"] = []
    assert post_json(client, body).status_code == 200

    assert get_ok(client, "/analysis/schemas") == {
        "schemas": [{"schema": "executed", "records": 3, "installs": 1}]}


@pytest.mark.parametrize("path", ["/analysis/schemas", "/analysis/schema/executed/attributes"])
def test_no_install_id_in_any_schema_response(client, path):
    post_recs(client, [ex()], instance="secret-install-zzz")
    r = client.get(path)
    assert r.status_code == 200
    text = r.text
    assert "secret-install-zzz" not in text
    assert "service_instance_id" not in text and "service.instance.id" not in text


# ---- /analysis/schema/{schema}/attributes

def test_attributes_sorted_with_record_counts(client):
    post_recs(client, [ex(), ex(extra={"kw.hardware.gpu": s("rtx")}),
                       record("timebox", eid(), {"kw.timebox.tag": s("t")})])

    assert get_ok(client, "/analysis/schema/executed/attributes") == {
        "schema": "executed",
        "attributes": attrs(("kw.event.id", 2), ("kw.event.kind", 2), (LABEL, 2),
                            ("kw.executed.status", 2), ("kw.executed.elapsed_time_in_secs", 2),
                            ("kw.hardware.gpu", 1)),
    }


def test_attributes_of_other_schema_are_separate(client):
    post_recs(client, [ex(), record("timebox", eid(), {"kw.timebox.tag": s("t")})])

    assert get_ok(client, "/analysis/schema/timebox/attributes") == {
        "schema": "timebox",
        "attributes": attrs(("kw.event.id", 1), ("kw.event.kind", 1), ("kw.timebox.tag", 1)),
    }


@pytest.mark.parametrize("name", ["nope", "executed "], ids=["absent", "near-miss"])
def test_attributes_unknown_schema_is_404(client, name):
    post_recs(client, [ex()])
    r = client.get(f"/analysis/schema/{name}/attributes")
    assert r.status_code == 404
    assert r.json() == {"error": f"unknown schema: {name}"}


def test_attributes_on_empty_store_is_404(client):
    r = client.get("/analysis/schema/executed/attributes")
    assert r.status_code == 404
    assert r.json()["error"].startswith("unknown schema")


# ---- removed paths

@pytest.mark.parametrize("path,params", [
    ("/analysis/schema", {}),
    ("/analysis/group", {"kind": "executed"}),
], ids=["schema", "group"])
def test_old_paths_are_gone(client, path, params):
    post_recs(client, [ex()])
    r = client.get(path, params=params)
    assert r.status_code == 404
    assert r.json() == NOT_FOUND


def test_old_plural_attributes_path_is_gone(client):
    post_recs(client, [ex()])
    assert client.get("/analysis/schemas/executed/attributes").status_code == 404


# ---- extensibility

def test_new_schema_appears_in_both_routes_without_sink_change(client):
    post_recs(client, [ex()])
    post_recs(client, [record("mailing_list", eid(), {"kw.mailing_list.name": s("lkml"),
                                                       "kw.mailing_list.posts": s("3")}),
                       record("mailing_list", eid(), {"kw.mailing_list.name": s("kw")})],
              instance="b")

    assert get_ok(client, "/analysis/schemas") == {"schemas": [
        {"schema": "executed", "records": 1, "installs": 1},
        {"schema": "mailing_list", "records": 2, "installs": 1},
    ]}
    assert get_ok(client, "/analysis/schema/mailing_list/attributes") == {
        "schema": "mailing_list",
        "attributes": attrs(("kw.event.id", 2), ("kw.event.kind", 2),
                            ("kw.mailing_list.name", 2), ("kw.mailing_list.posts", 1)),
    }
