import json
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app as sink_app  # noqa: E402


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    path = str(tmp_path / "t" / "telemetry.db")
    # app.py reads DB_PATH at import time and connects in its startup hook, so
    # patching the module attribute before entering TestClient is enough.
    monkeypatch.setattr(sink_app, "DB_PATH", path)
    return path


@pytest.fixture
def client(db_path):
    with TestClient(sink_app.app, raise_server_exceptions=False) as c:
        yield c


def fresh_rows(db_path, sql, args=()):
    """Query through a new connection, so only committed data is visible."""
    con = sqlite3.connect(db_path)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def s(v):
    return {"stringValue": v}


def i(v):
    # OTLP/JSON encodes int64 as a decimal string.
    return {"intValue": str(v)}


def record(kind="executed", event_id=1, attrs=None, body=None, t="1700000000000000000"):
    base = [
        {"key": "kw.event.id", "value": i(event_id)},
        {"key": "kw.event.kind", "value": s(kind)},
    ]
    extra = [{"key": k, "value": v} for k, v in (attrs or {}).items()]
    return {
        "timeUnixNano": t,
        "observedTimeUnixNano": t,
        "severityNumber": 9,
        "severityText": "INFO",
        "body": {"stringValue": body if body is not None else kind},
        "attributes": base + extra,
    }


def payload(records, instance="inst-1"):
    return {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": s("kworkflow")},
                        {"key": "service.instance.id", "value": s(instance)},
                    ]
                },
                "scopeLogs": [
                    {"scope": {"name": "kw_tele", "version": "1"}, "logRecords": records}
                ],
            }
        ]
    }


def executed(event_id, label="build", status="success", secs=10, **kw):
    return record(
        "executed",
        event_id,
        {
            "kw.executed.command_label": s(label),
            "kw.executed.status": s(status),
            "kw.executed.elapsed_time_in_secs": i(secs),
        },
        **kw,
    )


def timebox(event_id, tag="tcc", duration=1500):
    return record(
        "timebox",
        event_id,
        {
            "kw.timebox.tag": s(tag),
            "kw.timebox.duration": i(duration),
            "kw.timebox.description": s("x"),
        },
    )


def post_json(client, obj, **kw):
    return client.post(
        "/v1/logs",
        content=json.dumps(obj),
        headers={"content-type": "application/json", **kw.pop("headers", {})},
    )


def build_modules(event_id, count, names=None):
    attrs = {"kw.build_modules.module_count": i(count)}
    if names is not None:
        attrs["kw.build_modules.module_names"] = s(names)
    return record("build_modules", event_id, attrs)
