"""A made-up data source plugs in from kw_tele to retrieval with no sink change."""
import contextlib
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import pytest

from conftest import post_json

ROOT = Path(__file__).resolve().parents[3]
KWDB_SQL = ROOT / "kworkflow" / "database" / "kwdb.sql"
sys.path.insert(0, str(ROOT / "client" / "kw_tele" / "src"))

from kw_tele import payload as kw_payload, reflect  # noqa: E402

CPUS = ["alpha-1", "beta-2", "gamma-3"]
DISTROS = ["debian", "arch"]


def make_hw_db(path):
    if not KWDB_SQL.exists():
        pytest.skip("real kwdb.sql not available: %s" % KWDB_SQL)
    with contextlib.closing(sqlite3.connect(str(path))) as conn:
        conn.executescript(KWDB_SQL.read_text())
        conn.execute(
            "CREATE TABLE hw (id INTEGER PRIMARY KEY REFERENCES event(id),"
            " cpu_model TEXT, ram_mb INTEGER)"
        )
        conn.commit()


def add_hw(path, cpu, ram, distro=None):
    with contextlib.closing(sqlite3.connect(str(path))) as conn:
        cur = conn.execute("INSERT INTO event (date, time) VALUES ('2026-03-04', '05:06:07')")
        if distro is None:
            conn.execute("INSERT INTO hw (id, cpu_model, ram_mb) VALUES (?,?,?)",
                         (cur.lastrowid, cpu, ram))
        else:
            conn.execute("INSERT INTO hw (id, cpu_model, ram_mb, distro) VALUES (?,?,?,?)",
                         (cur.lastrowid, cpu, ram, distro))
        conn.commit()


def client_payload(path, instance):
    """The document kw_tele would upload for this database."""
    with contextlib.closing(sqlite3.connect(str(path))) as conn:
        schema = reflect.load(conn)
        resource = kw_payload.Resource(instance_id=instance, kw_version="9.9.9", now=1_800_000_000)
        return kw_payload.pending(conn, schema, resource, 100).document


def upload(client, path, instance):
    r = post_json(client, client_payload(path, instance))
    assert r.status_code == 200, r.text


def group(client, **params):
    r = client.get("/analysis/group", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def populate(client, tmp_path, with_distro=False):
    """12 installs, 1 or 2 hw events each; returns the (cpu, ram, distro) rows posted."""
    sent = []
    for n in range(12):
        db = tmp_path / ("install%d.db" % n)
        make_hw_db(db)
        if with_distro:
            with contextlib.closing(sqlite3.connect(str(db))) as conn:
                conn.execute("ALTER TABLE hw ADD COLUMN distro TEXT")
                conn.commit()
        cpu = CPUS[n % 3]
        for k in range(1 + n % 2):
            ram = 4096 * (1 + (n + k) % 4)
            distro = DISTROS[n % 2] if with_distro else None
            add_hw(db, cpu, ram, distro)
            sent.append((n, cpu, ram, distro))
        upload(client, db, "install-%02d" % n)
    return sent


def expected_by_cpu(sent):
    acc = defaultdict(lambda: {"rams": [], "installs": set()})
    for n, cpu, ram, _ in sent:
        acc[cpu]["rams"].append(ram)
        acc[cpu]["installs"].add(n)
    return acc


def test_new_source_reaches_retrieval(client, tmp_path):
    sent = populate(client, tmp_path)

    schema = client.get("/analysis/schema")
    kinds = {k["kind"]: k for k in schema.json()["kinds"]}
    assert "hw" in kinds
    names = {a["name"] for a in kinds["hw"]["attributes"]}
    assert {"kw.hw.cpu_model", "kw.hw.ram_mb"} <= names
    assert kinds["hw"]["records"] == len(sent)
    assert kinds["hw"]["installs"] == 12

    resp = client.get("/analysis/group", params={
        "kind": "hw", "by": "kw.hw.cpu_model", "measure": "kw.hw.ram_mb"})
    assert resp.status_code == 200
    body = resp.json()
    expected = expected_by_cpu(sent)
    assert [g["kw.hw.cpu_model"] for g in body["groups"]] == sorted(expected)
    for g in body["groups"]:
        want = expected[g["kw.hw.cpu_model"]]
        assert g["records"] == len(want["rams"])
        assert g["installs"] == len(want["installs"])
        assert g["measured"] == len(want["rams"])
        assert g["non_numeric"] == 0
        assert g["sum"] == sum(want["rams"])
        assert g["mean"] == pytest.approx(sum(want["rams"]) / len(want["rams"]))
        assert g["min"] == min(want["rams"]) and g["max"] == max(want["rams"])

    for text in (schema.text, resp.text):
        assert "service_instance_id" not in text
        assert "install-0" not in text and "install-1" not in text


def test_new_column_appears_without_code_change(client, tmp_path):
    sent = populate(client, tmp_path, with_distro=True)

    kinds = {k["kind"]: k for k in client.get("/analysis/schema").json()["kinds"]}
    names = {a["name"] for a in kinds["hw"]["attributes"]}
    assert {"kw.hw.cpu_model", "kw.hw.ram_mb", "kw.hw.distro"} <= names

    body = group(client, kind="hw", by="kw.hw.distro", measure="kw.hw.ram_mb")
    acc = defaultdict(lambda: {"rams": [], "installs": set()})
    for n, _, ram, distro in sent:
        acc[distro]["rams"].append(ram)
        acc[distro]["installs"].add(n)
    assert [g["kw.hw.distro"] for g in body["groups"]] == sorted(acc)
    for g in body["groups"]:
        want = acc[g["kw.hw.distro"]]
        assert g["records"] == len(want["rams"])
        assert g["installs"] == len(want["installs"])
        assert g["mean"] == pytest.approx(sum(want["rams"]) / len(want["rams"]))

    two = group(client, kind="hw", by="kw.hw.distro,kw.hw.cpu_model")
    assert sum(g["records"] for g in two["groups"]) == len(sent)
    assert "service_instance_id" not in str(two) and "install-" not in str(two)
