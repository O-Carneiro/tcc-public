"""§3 `info`."""

import json
import time

import pytest

from helpers.cli_case import CliCase
from helpers.otlp import attrs, records
from helpers import constants, kwdb, otlp
from helpers.constants import NOW

EP = "http://collector.test:4318/v1/logs"


class TestSummary(CliCase):
    def setup_method(self, method):
        super().setup_method(method)
        self.seed(0, endpoint=EP)
        db = self.sb.db
        for _ in range(4):
            kwdb.add_executed(db)
        kwdb.add_timebox(db)
        kwdb.set_sent_at(db, 1, 1_700_000_000)
        kwdb.set_sent_at(db, 2, 1_700_000_000)
        kwdb.set_sent_at(db, 3, -1)

    def test_summary_has_consent_endpoint_instance_and_counts(self):
        res = self.sb.run("info")
        assert res.code == 0, res
        out = res.out
        assert "yes" in out
        assert EP in out
        assert constants.INSTANCE in out
        assert all(word in out.lower() for word in ("pending", "sent", "excluded"))
        lines = [l for l in out.splitlines() if "pending" in l.lower()]
        assert any("2" in l for l in lines), lines
        lines = [l for l in out.splitlines() if "excluded" in l.lower()]
        assert any("1" in l for l in lines), lines

    def test_summary_says_none_without_instance_id_and_never_when_no_attempts(self):
        self.sb.write_config("telemetry_consent=ask\nendpoint=%s\n" % EP)
        out = self.sb.run("info").out
        assert "none" in out
        assert "never" in out
        assert "ask" in out

    def test_summary_shows_last_status_and_local_iso_times(self):
        self.sb.write_state(
            last_attempt_at=NOW, last_success_at=NOW - 86400, last_status="http-503"
        )
        out = self.sb.run("info", env={"TZ": "UTC"}).out
        assert "http-503" in out
        stamp = time.strftime("%Y-%m-%d", time.gmtime(NOW))
        assert stamp in out
        assert time.strftime("%Y-%m-%d", time.gmtime(NOW - 86400)) in out

    @pytest.mark.parametrize(
        "key",
        [
            "executed",
            "timebox",
            "kw.executed.command_label",
            "kw.executed.status",
            "kw.executed.elapsed_time_in_secs",
            "kw.timebox.tag",
            "kw.timebox.duration",
            "kw.timebox.description",
            "kw.event.date",
        ],
    )
    def test_summary_lists_each_kind_with_its_attribute_keys(self, key):
        assert key in self.sb.run("info").out

    def test_summary_never_lists_sent_at_as_collected(self):
        assert "sent_at" not in self.sb.run("info").out

    def test_collected_keys_follow_the_schema(self):
        kwdb.sql(self.sb.db, "ALTER TABLE executed ADD COLUMN exit_code INTEGER")
        assert "kw.executed.exit_code" in self.sb.run("info").out

    def test_summary_writes_nothing(self):
        before = self.snapshot()
        self.sb.run("info")
        self.assert_no_side_effects(before)

    def test_summary_works_without_consent(self):
        self.sb.config_path.unlink()
        assert self.sb.run("info").code == 0

    def test_flags_are_mutually_exclusive(self):
        assert self.sb.run("info", "--export", "--pending").code == 2


class TestPending(CliCase):
    def test_prints_exactly_two_lines(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-09")
        kwdb.add_executed(self.sb.db, date="2026-02-01")
        kwdb.add_executed(self.sb.db, date="2026-01-15")
        kwdb.set_sent_at(self.sb.db, 3, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        res = self.sb.run("info", "--pending")
        assert res.code == 0, res
        assert res.out == "pending_events=1\npending_since=2026-03-09\n"

    def test_empty_when_nothing_is_pending(self):
        self.seed(0)
        res = self.sb.run("info", "--pending")
        assert res.out == "pending_events=0\npending_since=\n"

    def test_works_without_consent_and_writes_nothing(self):
        self.seed(2, consent=False)
        before = self.snapshot()
        res = self.sb.run("info", "--pending")
        assert res.out == "pending_events=2\npending_since=2026-03-01\n"
        self.assert_no_side_effects(before)

    def test_exit_6_without_db_and_4_without_column(self):
        assert self.sb.run("info", "--pending").code == 6
        self.sb.make_db(with_sent_at=False)
        assert self.sb.run("info", "--pending").code == 4


class TestExport(CliCase):
    def test_only_sent_rows_with_sent_at_attribute(self):
        self.seed(0)
        db = self.sb.db
        for _ in range(3):
            kwdb.add_executed(db)
        kwdb.add_timebox(db)
        kwdb.set_sent_at(db, 4, 1_700_000_500)
        kwdb.set_sent_at(db, 1, 1_700_000_100)
        kwdb.set_sent_at(db, 3, -1)
        res = self.sb.run("info", "--export")
        assert res.code == 0, res
        doc = json.loads(res.out)
        assert otlp.event_ids(doc) == [1, 4]
        recs = records(doc)
        assert attrs(recs[0])["kw.event.sent_at"] == {"intValue": "1700000100"}
        assert attrs(recs[1])["kw.event.sent_at"] == {"intValue": "1700000500"}
        assert recs[1]["body"] == {"stringValue": "timebox"}
        assert "kw.timebox.tag" in attrs(recs[1])
        assert "pending" not in res.out

    def test_export_shape_matches_upload_payload(self):
        self.seed(1)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        doc = json.loads(self.sb.run("info", "--export").out)
        assert otlp.resource_attrs(doc)[0]["key"] == "service.name"
        assert doc["resourceLogs"][0]["scopeLogs"][0]["scope"]["name"] == "kw_tele"

    def test_export_with_nothing_sent_is_valid_json(self):
        self.seed(2)
        res = self.sb.run("info", "--export")
        assert res.code == 0, res
        assert records(json.loads(res.out)) == []

    def test_export_writes_nothing_and_needs_no_consent(self):
        self.seed(2, consent=False)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        before = self.snapshot()
        res = self.sb.run("info", "--export")
        assert res.code == 0, res
        assert otlp.event_ids(json.loads(res.out)) == [1]
        self.assert_no_side_effects(before)

    def test_exit_6_without_db_and_4_without_column(self):
        assert self.sb.run("info", "--export").code == 6
        self.sb.make_db(with_sent_at=False)
        assert self.sb.run("info", "--export").code == 4


class TestInfoError(CliCase):
    def test_plain_info_prints_config_then_exits_6_without_db(self):
        self.sb.configure(endpoint=EP)
        res = self.sb.run("info")
        assert res.code == 6, res
        assert EP in res.out
        assert constants.INSTANCE in res.out

    def test_plain_info_prints_config_then_exits_4_without_column(self):
        self.sb.make_db(with_sent_at=False)
        self.sb.configure(endpoint=EP)
        res = self.sb.run("info")
        assert res.code == 4, res
        assert EP in res.out
