"""§4.1 payload shape and §4.2 reflection, observed through `upload --dry-run`."""

import base64
import calendar
import copy
import json
import time

import pytest

from helpers.cli_case import CliCase
from helpers.otlp import attrs, records
from helpers import constants, kwdb, otlp

TODAY_EXECUTED = {
    "kw.event.id",
    "kw.event.date",
    "kw.event.kind",
    "kw.executed.command_label",
    "kw.executed.status",
    "kw.executed.elapsed_time_in_secs",
}
NEW_EVENT = "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')"
NOTE_DDL = "CREATE TABLE note (id INTEGER PRIMARY KEY REFERENCES event(id), text TEXT)"


def s(value):
    return {"stringValue": value}


def i(value):
    return {"intValue": str(value)}


def normalized(doc):
    """Attribute lists become dicts, so the comparison does not depend on attribute order."""
    doc = copy.deepcopy(doc)
    for resource in doc["resourceLogs"]:
        resource["resource"]["attributes"] = {
            a["key"]: a["value"] for a in resource["resource"]["attributes"]
        }
        for scope in resource["scopeLogs"]:
            for rec in scope["logRecords"]:
                rec["attributes"] = attrs(rec)
    return doc


def record(kind, ts, observed, attributes):
    return {
        "timeUnixNano": ts,
        "observedTimeUnixNano": observed,
        "severityNumber": 9,
        "severityText": "INFO",
        "body": s(kind),
        "attributes": attributes,
    }


class TestPayloadShape(CliCase):
    def test_dry_run_document_has_the_whole_expected_structure(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        eid = kwdb.add_executed(
            self.sb.db,
            label="build",
            status="failure",
            date="2026-03-04",
            time="05:06:07",
            elapsed=42,
        )
        tid = kwdb.add_timebox(
            self.sb.db,
            tag="deep",
            date="2026-03-04",
            time="07:08:09",
            duration=1500,
            description="tests",
        )
        doc = self.dry_run()
        version = doc["resourceLogs"][0]["scopeLogs"][0]["scope"]["version"]
        assert version
        observed = str(otlp.week_instant(constants.NOW) * 10**9)
        assert normalized(doc) == {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": {
                            "service.name": s("kworkflow"),
                            "service.version": s(constants.KW_VERSION),
                            "service.instance.id": s(constants.INSTANCE),
                            "telemetry.sdk.name": s("kw_tele"),
                            "telemetry.sdk.language": s("python"),
                            "telemetry.sdk.version": s(version),
                        }
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "kw_tele", "version": version},
                            "logRecords": [
                                record(
                                    "executed",
                                    otlp.week_ns("2026-03-04", "05:06:07"),
                                    observed,
                                    {
                                        "kw.event.id": i(eid),
                                        "kw.event.date": s("2026-03-02"),
                                        "kw.event.kind": s("executed"),
                                        "kw.executed.command_label": s("build"),
                                        "kw.executed.status": s("failure"),
                                        "kw.executed.elapsed_time_in_secs": i(42),
                                    },
                                ),
                                record(
                                    "timebox",
                                    otlp.week_ns("2026-03-04", "07:08:09"),
                                    observed,
                                    {
                                        "kw.event.id": i(tid),
                                        "kw.event.date": s("2026-03-02"),
                                        "kw.event.kind": s("timebox"),
                                        "kw.timebox.tag": s("deep"),
                                        "kw.timebox.duration": i(1500),
                                        "kw.timebox.description": s("tests"),
                                    },
                                ),
                            ],
                        }
                    ],
                }
            ]
        }


class TestValueEncoding(CliCase):
    def test_null_columns_are_omitted_and_a_null_time_means_midnight(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time=None)
        kwdb.add_timebox(self.sb.db, description=None)
        executed, timebox = records(self.dry_run())
        assert set(attrs(executed)) == TODAY_EXECUTED
        assert executed["timeUnixNano"] == otlp.week_ns("2026-03-04", "00:00:00")
        assert "kw.timebox.description" not in attrs(timebox)

    @pytest.mark.parametrize(
        "tz, offset_hours, date, time_, week",
        [
            ("UTC", 0, "2026-03-04", "05:06:07", "2026-03-02"),
            ("Etc/GMT-3", 3, "2026-03-04", "05:06:07", "2026-03-02"),
            # Local Monday 02:00 at UTC+3 is Sunday 23:00 UTC, the previous week.
            ("Etc/GMT-3", 3, "2026-03-02", "02:00:00", "2026-02-23"),
        ],
    )
    def test_the_event_moment_is_read_in_the_process_timezone_then_coarsened_in_utc(
        self, tz, offset_hours, date, time_, week
    ):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date=date, time=time_)
        res = self.sb.run("upload", "--dry-run", env={"TZ": tz})
        stamp = time.strptime(f"{date} {time_}", "%Y-%m-%d %H:%M:%S")
        epoch = calendar.timegm(stamp) - offset_hours * 3600
        rec = records(json.loads(res.out))[0]
        assert rec["timeUnixNano"] == str(otlp.week_instant(epoch) * 10**9)
        assert attrs(rec)["kw.event.date"] == s(week)

    @pytest.mark.parametrize(
        "date, time_, expected_ns, expected_date",
        [
            ("1970-01-05", "00:00:00", str(346380 * 10**9), "1970-01-05"),
            ("1970-01-02", "12:00:00", "0", None),
            ("9999-12-31", "12:00:00", "0", None),
            ("garbage", "25:61:61", "0", None),
        ],
    )
    def test_unknown_or_unrepresentable_moment_gives_zero_and_no_date_attribute(
        self, date, time_, expected_ns, expected_date
    ):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date=date, time=time_)
        (rec,) = records(self.dry_run())
        assert rec["timeUnixNano"] == expected_ns
        assert attrs(rec).get("kw.event.date") == (
            s(expected_date) if expected_date else None
        )
        assert "kw.event.time" not in attrs(rec)

    def test_sqlite_types_map_to_otlp_types(self):
        self.seed(1)
        db = self.sb.db
        for column in (
            "ratio REAL",
            "huge REAL",
            "raw BLOB",
            "digits TEXT",
            "words TEXT",
        ):
            kwdb.sql(db, f"ALTER TABLE executed ADD COLUMN {column}")
        kwdb.sql(
            db,
            "UPDATE executed SET ratio = 1.5, huge = 9e999, raw = X'00ff10', digits = '123', "
            "words = ?, elapsed_time_in_secs = 9007199254740993",
            ('revisão ✓ "aspas"\nlinha',),
        )
        a = attrs(records(self.dry_run())[0])
        assert {k: v for k, v in a.items() if k.startswith("kw.executed.")} == {
            "kw.executed.command_label": s("build"),
            "kw.executed.status": s("success"),
            "kw.executed.elapsed_time_in_secs": i(9007199254740993),
            "kw.executed.ratio": {"doubleValue": 1.5},
            "kw.executed.huge": s("inf"),
            "kw.executed.raw": {
                "bytesValue": base64.b64encode(b"\x00\xff\x10").decode()
            },
            "kw.executed.digits": s("123"),
            "kw.executed.words": s('revisão ✓ "aspas"\nlinha'),
        }


class TestReflection(CliCase):
    @pytest.mark.parametrize(
        "ddl",
        [
            (
                'CREATE TABLE ext ("id" INTEGER NOT NULL UNIQUE, "files" INTEGER NOT NULL, '
                'PRIMARY KEY("id"), FOREIGN KEY("id") REFERENCES "event"("id"))'
            ),
            "CREATE TABLE ext (id INTEGER PRIMARY KEY REFERENCES event(id), files INTEGER)",
        ],
        ids=["table-level-foreign-key", "inline-references"],
    )
    def test_new_extension_table_becomes_a_new_kind(self, ddl):
        self.seed(1)
        db = self.sb.db
        kwdb.sql(db, ddl)
        kwdb.sql(db, NEW_EVENT)
        # Only event.sent_at is kw_tele's own marker; an extension column of that name is data.
        kwdb.sql(db, "ALTER TABLE ext ADD COLUMN sent_at INTEGER")
        kwdb.sql(db, "INSERT INTO ext(id, files, sent_at) VALUES (2, 12, 5)")
        executed, ext = records(self.dry_run())
        assert [executed["body"], ext["body"]] == [s("executed"), s("ext")]
        assert set(attrs(executed)) == TODAY_EXECUTED
        assert attrs(ext) == {
            "kw.event.id": i(2),
            "kw.event.date": s("2026-05-04"),
            "kw.event.kind": s("ext"),
            "kw.ext.files": i(12),
            "kw.ext.sent_at": i(5),
        }

    @pytest.mark.parametrize(
        "lookup_ddl, lookup_row, expected",
        [
            (
                "CREATE TABLE tool (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                "INSERT INTO tool(name) VALUES ('vim')",
                {"kw.ping.tool": s("vim")},
            ),
            (
                "CREATE TABLE tool (id INTEGER PRIMARY KEY, os TEXT)",
                "INSERT INTO tool(os) VALUES ('linux')",
                {"kw.ping.tool_id": i(1)},
            ),
        ],
        ids=["lookup-with-name-column", "lookup-without-name-column-keeps-raw-id"],
    )
    def test_lookup_resolves_the_name_column_or_keeps_the_raw_value(
        self, lookup_ddl, lookup_row, expected
    ):
        self.seed(0)
        db = self.sb.db
        kwdb.sql(db, lookup_ddl)
        kwdb.sql(db, lookup_row)
        kwdb.sql(
            db,
            "CREATE TABLE ping (id INTEGER PRIMARY KEY REFERENCES event(id), "
            "tool_id INTEGER REFERENCES tool(id))",
        )
        kwdb.sql(db, NEW_EVENT)
        kwdb.sql(db, "INSERT INTO ping(id, tool_id) VALUES (1, 1)")
        a = attrs(records(self.dry_run())[0])
        assert {k: v for k, v in a.items() if k.startswith("kw.ping")} == expected

    @pytest.mark.parametrize(
        "table, column, value, expected",
        [
            ("executed", "exit_code INTEGER", "2", i(2)),
            ("event", "host TEXT", "'box'", s("box")),
        ],
    )
    def test_new_column_becomes_a_new_attribute(self, table, column, value, expected):
        self.seed(1)
        name = column.split()[0]
        kwdb.sql(self.sb.db, f"ALTER TABLE {table} ADD COLUMN {column}")
        kwdb.sql(self.sb.db, f"UPDATE {table} SET {name} = {value}")
        key = f"kw.{table}.{name}"
        a = attrs(records(self.dry_run())[0])
        assert a[key] == expected
        assert set(a) == TODAY_EXECUTED | {key}

    def test_event_without_an_extension_row_is_kind_event(self):
        self.seed(0)
        kwdb.sql(self.sb.db, NEW_EVENT)
        (rec,) = records(self.dry_run())
        assert rec["body"] == s("event")
        assert set(attrs(rec)) == {"kw.event.id", "kw.event.date", "kw.event.kind"}

    def test_an_event_in_two_extension_tables_gives_two_records_and_batch_size_counts_events(
        self,
    ):
        self.seed(3, batch_size=2)
        kwdb.sql(self.sb.db, NOTE_DDL)
        kwdb.sql(self.sb.db, "INSERT INTO note(id, text) VALUES (1, 'a'), (2, 'b')")
        got = sorted(
            (int(attrs(r)["kw.event.id"]["intValue"]), r["body"]["stringValue"])
            for r in records(self.dry_run())
        )
        assert got == [(1, "executed"), (1, "note"), (2, "executed"), (2, "note")]

    def test_only_tables_with_a_foreign_key_to_event_are_kinds(self):
        self.seed(1)
        kwdb.sql(self.sb.db, "CREATE TABLE scratch (id INTEGER PRIMARY KEY, v TEXT)")
        kwdb.sql(self.sb.db, "INSERT INTO scratch(id, v) VALUES (1, 'leak')")
        text = self.sb.run("upload", "--dry-run").out
        for name in ("scratch", "active_timebox", "pomodoro_report", "kw.executed.id"):
            assert name not in text


class TestExtensibility(CliCase):
    """A new table or column reaches the payload with no code change."""

    def test_real_build_modules_table_exports(self):
        self.seed(0)
        kwdb.sql(
            self.sb.db,
            "INSERT INTO build_modules_report "
            "(label_name, status, date, time, elapsed_time_in_secs, module_count, module_names) "
            "VALUES ('build', 'success', '2026-03-04', '05:06:07', 42, 3, 'a,b,c')",
        )
        by_kind = {r["body"]["stringValue"]: attrs(r) for r in records(self.dry_run())}
        assert set(by_kind) == {"executed", "build_modules"}
        assert (
            by_kind["build_modules"]["kw.event.id"]
            == by_kind["executed"]["kw.event.id"]
        )
        got = {
            k: v
            for k, v in by_kind["build_modules"].items()
            if k.startswith("kw.build_modules")
        }
        assert got == {
            "kw.build_modules.module_count": i(3),
            "kw.build_modules.module_names": s("a,b,c"),
        }
        assert by_kind["executed"]["kw.executed.command_label"] == s("build")
