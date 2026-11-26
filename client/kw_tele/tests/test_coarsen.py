"""D50 time coarsening and D52 `exclude_fields`, observed through the CLI and the `coarsen` module."""

import datetime
import json
import os
import time
from unittest import mock

import pytest

from helpers import constants, kwdb, otlp
from helpers.cli_case import CliCase
from helpers.otlp import attrs, records

UTC = datetime.timezone.utc


def epoch(text):
    return int(datetime.datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


# t (UTC), week instant (UTC), week date: the same vectors as the sink's tests.
VECTORS = [
    ("2026-03-04T15:20:00", "2026-03-02T00:13:00", "2026-03-02"),
    ("2026-03-01T23:59:59", "2026-02-23T00:13:00", "2026-02-23"),
    ("2026-03-02T00:00:00", "2026-03-02T00:13:00", "2026-03-02"),
    ("2026-03-02T00:12:59", "2026-03-02T00:13:00", "2026-03-02"),
    ("2026-03-02T00:13:00", "2026-03-02T00:13:00", "2026-03-02"),
    ("1970-01-01T00:00:01", "1969-12-29T00:13:00", "1969-12-29"),
]


@pytest.fixture
def utc():
    with mock.patch.dict(os.environ, {"TZ": "UTC"}):
        time.tzset()
        yield
    time.tzset()


class TestCoarsenModule:
    @pytest.mark.parametrize("moment, instant, date", VECTORS)
    def test_week_instant_and_week_date_vectors(self, moment, instant, date):
        from kw_tele import coarsen

        assert coarsen.week_instant(epoch(moment)) == epoch(instant)
        assert coarsen.week_date(epoch(moment)) == date

    @pytest.mark.parametrize(
        "row, expected",
        [
            ({"date": "1970-01-05", "time": "00:00:00"}, 345600),
            ({"date": "2026-03-04", "time": None}, epoch("2026-03-04T00:00:00")),
            ({"date": "1970-01-02", "time": "12:00:00"}, None),
            ({"date": "garbage", "time": "00:00:00"}, None),
            ({"date": "9999-12-31", "time": "12:00:00"}, None),
        ],
    )
    def test_event_moment_is_the_local_epoch_or_none_when_unrepresentable(
        self, utc, row, expected
    ):
        from kw_tele import coarsen

        assert coarsen.event_moment(row) == expected

    def test_export_value_coarsens_the_date_and_omits_the_time(self, utc):
        from kw_tele import coarsen

        row = {"id": 1, "date": "2026-03-04", "time": "15:20:00"}
        assert coarsen.export_value("date", row) == "2026-03-02"
        assert coarsen.export_value("time", row) is coarsen.OMIT
        assert coarsen.export_value("id", row) == 1
        assert (
            coarsen.export_value("date", {"date": "garbage", "time": None})
            is coarsen.OMIT
        )
        assert set(coarsen.OMITTED_COLUMNS) == {"time"}
        assert {"kw.event.id", "kw.event.kind"} <= set(coarsen.PROTECTED_KEYS)


class TestCoarsenedPayload(CliCase):
    def test_week_instants_and_week_date_leave_the_machine_not_the_event_time(self):
        self.seed(0)
        for moment, _, _ in VECTORS[:5]:
            day, clock = moment.split("T")
            kwdb.add_executed(self.sb.db, date=day, time=clock)
        recs = records(self.dry_run())
        assert [r["timeUnixNano"] for r in recs] == [
            str(epoch(instant) * 10**9) for _, instant, _ in VECTORS[:5]
        ]
        assert [attrs(r)["kw.event.date"]["stringValue"] for r in recs] == [
            d for _, _, d in VECTORS[:5]
        ]
        assert all("kw.event.time" not in attrs(r) for r in recs)
        assert {r["observedTimeUnixNano"] for r in recs} == {
            str(otlp.week_instant(constants.NOW) * 10**9)
        }


class TestExcludeFields(CliCase):
    def seed_mixed(self, exclude):
        self.seed(0, exclude_fields=exclude)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time="05:06:07")
        kwdb.sql(
            self.sb.db,
            "INSERT INTO build_modules_report "
            "(label_name, status, date, time, elapsed_time_in_secs, module_count, module_names) "
            "VALUES ('build', 'success', '2026-03-05', '05:06:07', 42, 3, 'a,b,c')",
        )

    def all_keys(self, *args):
        res = self.sb.run(*args)
        assert res.code == 0, res
        return {k for r in records(json.loads(res.out)) for k in attrs(r)}

    def test_excluded_keys_are_absent_from_upload_dry_run_export_and_info(self):
        stub = self.serve()
        self.seed_mixed("kw.build_modules.module_names, kw.executed.status ,,kw.nope")
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_100)
        gone = {"kw.build_modules.module_names", "kw.executed.status"}
        dry = self.all_keys("upload", "--dry-run")
        assert {"kw.build_modules.module_count", "kw.executed.command_label"} <= dry
        assert not dry & gone
        exported = self.all_keys("info", "--export")
        assert "kw.event.sent_at" in exported
        assert not exported & gone
        info = self.sb.run("info").out
        assert "kw.build_modules.module_count" in info
        assert not any(k in info for k in gone)
        self.sb.configure(
            endpoint=stub.url, exclude_fields="kw.build_modules.module_names"
        )
        assert self.sb.run("upload", "--force").code == 0
        sent = {k for r in records(stub.posts()[0].json()) for k in attrs(r)}
        assert "kw.build_modules.module_names" not in sent
        assert "kw.executed.status" in sent

    def test_identity_keys_cannot_be_excluded_and_time_exclusion_is_a_noop(self):
        self.seed(1, exclude_fields="kw.event.id,kw.event.kind,kw.event.time")
        (rec,) = records(self.dry_run())
        assert {"kw.event.id", "kw.event.kind"} <= set(attrs(rec))
        info = self.sb.run("info").out
        assert "kw.event.id" in info and "kw.event.kind" in info

    def test_excluding_the_date_keeps_the_coarse_time_unix_nano(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time="05:06:07")
        (with_date,) = records(self.dry_run())
        self.sb.configure(exclude_fields="kw.event.date")
        (without,) = records(self.dry_run())
        assert "kw.event.date" in attrs(with_date)
        assert "kw.event.date" not in attrs(without)
        assert (
            without["timeUnixNano"]
            == with_date["timeUnixNano"]
            == otlp.week_ns("2026-03-04", "05:06:07")
        )

    def test_marking_uses_the_raw_row_whatever_is_excluded(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url, exclude_fields="kw.event.date")
        kwdb.add_executed(self.sb.db, date="2026-03-04", time="05:06:07")
        assert self.sb.run("upload", "--force").code == 0
        assert kwdb.sent_map(self.sb.db) == {1: constants.NOW}
        assert self.sb.run("upload", "--force").code == 0
        assert len(stub.posts()) == 1


class TestNoRawTimeLeaks(CliCase):
    MOMENTS = [
        ("2026-03-04", "15:20:07"),
        ("2026-03-08", "23:59:58"),
    ]  # Wednesday, Sunday

    @pytest.mark.parametrize("output", ["upload", "dry-run", "export"])
    def test_no_raw_date_time_or_exact_epoch_leaves_the_machine(self, output):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        for date, clock in self.MOMENTS:
            kwdb.add_executed(self.sb.db, date=date, time=clock)
        if output == "upload":
            assert self.sb.run("upload", "--force").code == 0
            text = stub.posts()[0].body.decode()
        else:
            if output == "export":
                for event_id in (1, 2):
                    kwdb.set_sent_at(self.sb.db, event_id, 1_700_000_100)
            args = (
                ("upload", "--dry-run") if output == "dry-run" else ("info", "--export")
            )
            text = self.sb.run(*args).out
        assert "kw.event.date" in text  # the output under test is not empty
        raw = [d for d, _ in self.MOMENTS] + [c for _, c in self.MOMENTS]
        raw += [otlp.utc_ns(d, c) for d, c in self.MOMENTS]
        assert [needle for needle in raw if needle in text] == []
