import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "sink"))
import app as sink_app  # noqa: E402
import generate  # noqa: E402


def test_same_seed_same_payloads():
    assert generate.digest(generate.build_payloads(7, 40)) == generate.digest(generate.build_payloads(7, 40))
    assert generate.digest(generate.build_payloads(7, 40)) != generate.digest(generate.build_payloads(8, 40))


def test_rare_names_are_suppressed(tmp_path, monkeypatch):
    monkeypatch.setattr(sink_app, "DB_PATH", str(tmp_path / "t" / "telemetry.db"))
    monkeypatch.delenv("KANON_K", raising=False)
    payloads = generate.build_payloads(42)
    with TestClient(sink_app.app) as c:
        for p in payloads:
            assert c.post("/v1/logs", json=p).status_code == 200
        res = c.get("/analysis/modules").json()
        cmds = c.get("/analysis/commands").json()
    assert res["k"] == 5
    shown = {n["name"] for n in res["names"]}
    assert shown, "common names should be visible"
    assert not shown & set(generate.RARE)
    assert all(n["installs"] >= 5 for n in res["names"])
    assert sum(b["installs"] for b in res["buckets"]) > 0
    blob = json.dumps(cmds) + json.dumps(res)
    assert not any(n in blob for n in generate.RARE)
    ids = [rl["resource"]["attributes"][1]["value"]["stringValue"] for p in payloads for rl in p["resourceLogs"]]
    assert not any(i in blob for i in ids)
