"""D52: incoming times and received_at are stored as their week instant, whatever the client sent."""
import json
from datetime import datetime, timezone

from conftest import eid, ex, executed, fresh_rows, ns, no_time, payload, post_json, post_recs, sink_app

# (sent, stored): the shared vectors of .scratch/time-coarsening-seams.md, in ns; a week instant stays
VECTORS = [
    ("2026-03-04T15:20:00", "2026-03-02T00:13:00"),
    ("2026-03-01T23:59:59", "2026-02-23T00:13:00"),
    ("2026-03-02T00:00:00", "2026-03-02T00:13:00"),
    ("2026-03-02T00:12:59", "2026-03-02T00:13:00"),
    ("2026-03-02T00:13:00", "2026-03-02T00:13:00"),
    ("1970-01-01T00:00:01", "1969-12-29T00:13:00"),
]


def _ns(iso):
    return int(ns(iso))


def test_incoming_times_are_stored_as_week_instants(client, db_path):
    recs, want = [], {}
    for sent, stored in VECTORS:
        rec = executed(eid())
        rec["timeUnixNano"], rec["observedTimeUnixNano"] = ns(sent), ns(sent)
        recs.append(rec)
        want[rec["attributes"][0]["value"]["intValue"]] = (_ns(stored), _ns(stored))
    zero = executed(eid()) | {"timeUnixNano": "0", "observedTimeUnixNano": "0"}
    absent = no_time(executed(eid()))
    recs += [zero, absent]
    want[zero["attributes"][0]["value"]["intValue"]] = (0, 0)
    want[absent["attributes"][0]["value"]["intValue"]] = (None, None)
    assert post_json(client, payload(recs)).status_code == 200
    got = {str(e): (t, o) for e, t, o in fresh_rows(
        db_path, "SELECT event_id, time_unix_nano, observed_time_unix_nano FROM log_record")}
    assert got == want


def test_received_at_is_the_week_instant_of_the_clock_in_every_table(client, db_path, monkeypatch):
    for now, week_instant in VECTORS:
        monkeypatch.setattr(sink_app, "_now",
                            lambda now=now: datetime.fromisoformat(now).replace(tzinfo=timezone.utc))
        assert sink_app._received_at() == week_instant + "+00:00"
    monkeypatch.setattr(sink_app, "_now", lambda: datetime(2026, 3, 4, 15, 20, tzinfo=timezone.utc))
    poison = {"body": {"stringValue": "executed"}, "attributes": "not-a-list"}
    assert post_json(client, payload([executed(1), poison])).status_code == 200
    got = {t: fresh_rows(db_path, f"SELECT DISTINCT received_at FROM {t}")
           for t in ("export_batch", "log_record", "log_record_raw")}
    assert got == {t: [("2026-03-02T00:13:00+00:00",)] for t in got}


def test_grouping_by_week_after_coarsening(client):
    # Wed and Sun of one week, then Mon 00:05 of the next (inside the 00:13 shift)
    post_recs(client, [ex(t=t) for t in ("2026-03-04T15:20:00", "2026-03-08T23:00:00", "2026-03-09T00:05:00")])
    r = client.get("/analysis/query", params={"schema": "executed", "bucket": "week"}).json()
    assert sorted((g["week"], g["records"]) for g in r["groups"]) == [("2026-03-02", 2), ("2026-03-09", 1)]


def test_id_less_records_in_one_week_dedupe_on_the_coarsened_time(client, db_path):
    recs = []
    for t in ("2026-03-04T15:20:00", "2026-03-06T08:00:00"):
        rec = executed(1) | {"timeUnixNano": ns(t)}
        rec["attributes"] = [a for a in rec["attributes"] if a["key"] != "kw.event.id"]
        recs.append(rec)
    assert post_json(client, payload(recs)).status_code == 200
    assert fresh_rows(db_path, "SELECT COUNT(*) FROM log_record") == [(1,)]
