"""§3 `info`."""

import json

import pytest

from helpers.cli_case import CliCase
from helpers import constants, kwdb
from helpers.otlp import attrs, records
from helpers.constants import NOW

EP = "http://collector.test:4318/v1/logs"


def parse_collected(out):
    collected = {}
    for line in out.split("collected:")[1].splitlines():
        if line.strip():
            kind, keys = line.split(":", 1)
            collected[kind.strip()] = set(keys.replace(" ", "").split(","))
    return collected


EVENT_KEYS = {"kw.event.id", "kw.event.date", "kw.event.kind"}


class TestInfo(CliCase):
    def seed_mixed(self):
        # 1 sent, 2 excluded, 3 and 4 pending, 5 (timebox) sent.
        self.seed(0, endpoint=EP)
        for _ in range(4):
            kwdb.add_executed(self.sb.db)
        kwdb.add_timebox(self.sb.db)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        kwdb.set_sent_at(self.sb.db, 5, 1_700_000_500)

    def seed_described(self):
        self.seed_mixed()
        kwdb.sql(self.sb.db, "ALTER TABLE executed ADD COLUMN exit_code INTEGER")
        self.sb.write_state(
            last_attempt_at=NOW, last_success_at=NOW - 86400, last_status="http-503"
        )

    def test_summary_header_reports_config_state_and_counts(self):
        self.seed_described()
        res = self.sb.run("info")
        assert res.code == 0, res
        assert res.out.split("collected:")[0] == (
            "consent: yes\n"
            f"endpoint: {EP}\n"
            f"instance_id: {constants.INSTANCE}\n"
            "last success: 2027-01-14T08:00:00+00:00\n"
            "last attempt: 2027-01-15T08:00:00+00:00\n"
            "last status: http-503\n"
            "pending events: 2\n"
            "sent events: 2\n"
            "excluded events: 1\n"
        )

    def test_summary_lists_the_collected_keys_per_kind_without_internal_columns(self):
        self.seed_described()
        out = self.sb.run("info").out
        collected = parse_collected(out)
        assert collected["executed"] == EVENT_KEYS | {
            "kw.executed.command_label",
            "kw.executed.status",
            "kw.executed.elapsed_time_in_secs",
            "kw.executed.exit_code",
        }
        assert collected["timebox"] == EVENT_KEYS | {
            "kw.timebox.tag",
            "kw.timebox.duration",
            "kw.timebox.description",
        }
        assert "sent_at" not in out
        assert "kw.event.time" not in out

    def test_pending_reports_the_count_and_the_oldest_date(self):
        self.seed_described()
        assert self.sb.run("info", "--pending").out == (
            "pending_events=2\npending_since=2026-03-04\n"
        )

    def test_pending_since_is_empty_when_nothing_is_pending(self):
        self.seed(1)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        assert self.sb.run("info", "--pending").out == (
            "pending_events=0\npending_since=\n"
        )

    def test_export_has_the_sent_rows_with_their_sent_at(self):
        self.seed_described()
        exported = json.loads(self.sb.run("info", "--export").out)
        assert [(r["body"]["stringValue"], attrs(r)) for r in records(exported)] == [
            (
                "executed",
                {
                    "kw.event.id": {"intValue": "1"},
                    "kw.event.date": {"stringValue": "2026-03-02"},
                    "kw.event.kind": {"stringValue": "executed"},
                    "kw.executed.command_label": {"stringValue": "build"},
                    "kw.executed.status": {"stringValue": "success"},
                    "kw.executed.elapsed_time_in_secs": {"intValue": "42"},
                    "kw.event.sent_at": {"intValue": "1700000100"},
                },
            ),
            (
                "timebox",
                {
                    "kw.event.id": {"intValue": "5"},
                    "kw.event.date": {"stringValue": "2026-03-02"},
                    "kw.event.kind": {"stringValue": "timebox"},
                    "kw.timebox.tag": {"stringValue": "work"},
                    "kw.timebox.duration": {"intValue": "1500"},
                    "kw.timebox.description": {"stringValue": "write tests"},
                    "kw.event.sent_at": {"intValue": "1700000500"},
                },
            ),
        ]
        assert exported["resourceLogs"][0]["scopeLogs"][0]["scope"]["name"] == "kw_tele"

    def test_summary_without_instance_id_or_attempts(self):
        self.sb.write_config(f"telemetry_consent=ask\nendpoint={EP}\n")
        assert self.sb.run("info").out.split("collected:")[0].splitlines() == [
            "consent: ask",
            f"endpoint: {EP}",
            "instance_id: none",
            "last success: never",
            "last attempt: never",
            "last status: none",
        ]


class TestReadOnly(CliCase):
    @pytest.mark.parametrize(
        "args",
        [
            ["info"],
            ["info", "--pending"],
            ["info", "--export"],
            ["upload", "--dry-run"],
            ["config"],
        ],
        ids=" ".join,
    )
    def test_read_only_commands_write_nothing_and_need_no_consent(self, args):
        self.seed(2, consent=False)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        before = self.snapshot()
        res = self.sb.run(*args)
        assert res.code == 0, res
        self.assert_no_side_effects(before)
        assert not self.sb.state_dir.exists()
