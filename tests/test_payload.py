"""§4.1 payload shape and §4.2 reflection, observed through `upload --dry-run`."""

import base64
import calendar
import json
import time

from helpers.cli_case import CliCase
from helpers.otlp import attrs, records
from helpers import constants, kwdb, otlp

TODAY_EXECUTED = {
    "kw.event.id",
    "kw.event.date",
    "kw.event.time",
    "kw.event.kind",
    "kw.executed.command_label",
    "kw.executed.status",
    "kw.executed.elapsed_time_in_secs",
}
TODAY_TIMEBOX = {
    "kw.event.id",
    "kw.event.date",
    "kw.event.time",
    "kw.event.kind",
    "kw.timebox.tag",
    "kw.timebox.duration",
    "kw.timebox.description",
}


class TestShape(CliCase):
    def setup_method(self, method):
        super().setup_method(method)
        self.seed(0)
        self.eid = kwdb.add_executed(
            self.sb.db,
            label="build",
            status="failure",
            date="2026-03-04",
            time="05:06:07",
            elapsed=42,
        )
        self.tid = kwdb.add_timebox(
            self.sb.db,
            tag="deep",
            date="2026-03-04",
            time="07:08:09",
            duration=1500,
            description="tests",
        )
        self.doc = self.dry_run()
        self.recs = records(self.doc)

    def test_top_level_structure(self):
        assert list(self.doc) == ["resourceLogs"]
        assert len(self.doc["resourceLogs"]) == 1
        rl = self.doc["resourceLogs"][0]
        assert len(rl["scopeLogs"]) == 1
        scope = rl["scopeLogs"][0]["scope"]
        assert scope["name"] == "kw_tele"
        assert scope["version"]

    def test_resource_attributes(self):
        got = {a["key"]: a["value"] for a in otlp.resource_attrs(self.doc)}
        assert got["service.name"] == {"stringValue": "kworkflow"}
        assert got["service.version"] == {"stringValue": constants.KW_VERSION}
        assert got["service.instance.id"] == {"stringValue": constants.INSTANCE}
        assert got["telemetry.sdk.name"] == {"stringValue": "kw_tele"}
        assert got["telemetry.sdk.language"] == {"stringValue": "python"}
        sdk = got["telemetry.sdk.version"]["stringValue"]
        assert sdk
        assert sdk == self.doc["resourceLogs"][0]["scopeLogs"][0]["scope"]["version"]
        assert len(got) == 6

    def test_service_version_is_unknown_without_kw_version(self):
        res = self.sb.run("upload", "--dry-run", env={"KW_VERSION": None})
        got = {a["key"]: a["value"] for a in otlp.resource_attrs(json.loads(res.out))}
        assert got["service.version"] == {"stringValue": "unknown"}

    def test_one_record_per_event_in_id_order(self):
        assert otlp.event_ids(self.doc) == [self.eid, self.tid]

    def test_record_envelope(self):
        for rec in self.recs:
            assert rec["severityNumber"] == 9
            assert rec["severityText"] == "INFO"
            assert rec["observedTimeUnixNano"] == str(constants.NOW * 10**9)
            assert isinstance(rec["timeUnixNano"], str)
        assert [r["body"] for r in self.recs] == [
            {"stringValue": "executed"},
            {"stringValue": "timebox"},
        ]

    def test_kind_attribute_matches_body(self):
        assert attrs(self.recs[0])["kw.event.kind"] == {"stringValue": "executed"}
        assert attrs(self.recs[1])["kw.event.kind"] == {"stringValue": "timebox"}

    def test_attribute_names_for_todays_schema(self):
        assert set(attrs(self.recs[0])) == TODAY_EXECUTED
        assert set(attrs(self.recs[1])) == TODAY_TIMEBOX

    def test_lookups_resolve_to_names_without_the_id_suffix(self):
        a = attrs(self.recs[0])
        assert a["kw.executed.command_label"] == {"stringValue": "build"}
        assert a["kw.executed.status"] == {"stringValue": "failure"}
        assert attrs(self.recs[1])["kw.timebox.tag"] == {"stringValue": "deep"}

    def test_integers_are_decimal_strings(self):
        a = attrs(self.recs[0])
        assert a["kw.executed.elapsed_time_in_secs"] == {"intValue": "42"}
        assert a["kw.event.id"] == {"intValue": str(self.eid)}
        assert attrs(self.recs[1])["kw.timebox.duration"] == {"intValue": "1500"}

    def test_text_values(self):
        a = attrs(self.recs[1])
        assert a["kw.event.date"] == {"stringValue": "2026-03-04"}
        assert a["kw.event.time"] == {"stringValue": "07:08:09"}
        assert a["kw.timebox.description"] == {"stringValue": "tests"}

    def test_sent_at_never_appears_in_upload_payloads(self):
        text = self.sb.run("upload", "--dry-run").out
        assert "sent_at" not in text

    def test_sent_at_is_not_in_the_payload_actually_posted(self):
        stub = self.serve()
        self.sb.configure(endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert b"sent_at" not in stub.posts()[0].body
        assert otlp.event_ids(stub.posts()[0].json()) == [self.eid, self.tid]

    def test_posted_payload_matches_dry_run_except_observed_time(self):
        stub = self.serve()
        self.sb.configure(endpoint=stub.url)
        self.sb.run("upload", "--force")
        assert stub.posts()[0].json() == self.doc


class TestNullAndEncoding(CliCase):
    def test_null_time_is_omitted_and_means_midnight(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time=None)
        rec = records(self.dry_run())[0]
        assert "kw.event.time" not in attrs(rec)
        assert rec["timeUnixNano"] == otlp.utc_ns("2026-03-04", "00:00:00")

    def test_null_description_is_omitted(self):
        self.seed(0)
        kwdb.add_timebox(self.sb.db, description=None)
        assert "kw.timebox.description" not in attrs(records(self.dry_run())[0])

    def test_time_unix_nano_is_local_date_and_time(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time="05:06:07")
        rec = records(self.dry_run())[0]
        assert rec["timeUnixNano"] == otlp.utc_ns("2026-03-04", "05:06:07")

    def test_time_unix_nano_follows_the_process_timezone(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="2026-03-04", time="05:06:07")
        res = self.sb.run("upload", "--dry-run", env={"TZ": "Etc/GMT-3"})
        rec = records(json.loads(res.out))[0]
        expected = (
            calendar.timegm(time.strptime("2026-03-04 05:06:07", "%Y-%m-%d %H:%M:%S")) - 3 * 3600
        ) * 10**9
        assert rec["timeUnixNano"] == str(expected)

    def test_malformed_date_and_time_give_zero_and_keep_raw_strings(self):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date="garbage", time="25:61:61")
        kwdb.add_executed(self.sb.db, date="2026-02-30", time="00:00:00")
        kwdb.add_executed(self.sb.db, date="9999999999-01-01", time="00:00:00")
        recs = records(self.dry_run())
        assert [r["timeUnixNano"] for r in recs] == ["0", "0", "0"]
        assert attrs(recs[0])["kw.event.date"] == {"stringValue": "garbage"}

    def test_text_value_that_looks_like_a_number_stays_a_string(self):
        self.seed(0)
        kwdb.add_timebox(self.sb.db, description="123")
        assert attrs(records(self.dry_run())[0])["kw.timebox.description"] == {"stringValue": "123"}

    def test_unicode_text_round_trips(self):
        self.seed(0)
        kwdb.add_timebox(self.sb.db, description='revisão ✓ "aspas"\nlinha')
        assert attrs(records(self.dry_run())[0])["kw.timebox.description"] == {
            "stringValue": 'revisão ✓ "aspas"\nlinha'
        }

    def test_real_blob_and_non_finite_values_use_their_otlp_types(self):
        self.seed(1)
        db = self.sb.db
        kwdb.sql(db, "ALTER TABLE executed ADD COLUMN ratio REAL")
        kwdb.sql(db, "ALTER TABLE executed ADD COLUMN raw BLOB")
        kwdb.sql(db, "UPDATE executed SET ratio = 1.5, raw = X'00ff10'")
        a = attrs(records(self.dry_run())[0])
        assert a["kw.executed.ratio"] == {"doubleValue": 1.5}
        assert a["kw.executed.raw"] == {"bytesValue": base64.b64encode(b"\x00\xff\x10").decode()}
        kwdb.sql(db, "UPDATE executed SET ratio = 9e999")
        a = attrs(records(self.dry_run())[0])
        assert a["kw.executed.ratio"] == {"stringValue": "inf"}

    def test_large_integers_stay_exact_as_strings(self):
        self.seed(1)
        kwdb.sql(self.sb.db, "UPDATE executed SET elapsed_time_in_secs = 9007199254740993")
        assert attrs(records(self.dry_run())[0])["kw.executed.elapsed_time_in_secs"] == {
            "intValue": "9007199254740993"
        }


class TestReflection(CliCase):
    def test_new_extension_table_becomes_a_new_kind(self):
        self.seed(0)
        db = self.sb.db
        kwdb.add_executed(db)
        kwdb.sql(
            db,
            'CREATE TABLE "compile_unit" ('
            '"id" INTEGER NOT NULL UNIQUE, "files" INTEGER NOT NULL, '
            'PRIMARY KEY("id"), FOREIGN KEY("id") REFERENCES "event"("id"))',
        )
        kwdb.sql(db, "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')")
        new_id = kwdb.sql(db, "SELECT max(id) FROM event")[0][0]
        kwdb.sql(db, "INSERT INTO compile_unit(id, files) VALUES (?, 12)", (new_id,))
        recs = records(self.dry_run())
        assert [r["body"]["stringValue"] for r in recs] == ["executed", "compile_unit"]
        a = attrs(recs[1])
        assert a["kw.compile_unit.files"] == {"intValue": "12"}
        assert a["kw.event.kind"] == {"stringValue": "compile_unit"}
        assert set(attrs(recs[0])) == TODAY_EXECUTED

    def test_new_extension_table_with_inline_references_clause(self):
        self.seed(0)
        db = self.sb.db
        kwdb.sql(
            db, "CREATE TABLE note (id INTEGER PRIMARY KEY REFERENCES event(id), " "text TEXT)"
        )
        kwdb.sql(db, "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')")
        kwdb.sql(db, "INSERT INTO note(id, text) VALUES (1, 'hi')")
        rec = records(self.dry_run())[0]
        assert rec["body"] == {"stringValue": "note"}
        assert attrs(rec)["kw.note.text"] == {"stringValue": "hi"}

    def test_new_table_with_a_lookup_resolves_the_name_column(self):
        self.seed(0)
        db = self.sb.db
        kwdb.sql(db, "CREATE TABLE editor (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        kwdb.sql(db, "INSERT INTO editor(name) VALUES ('vim')")
        kwdb.sql(
            db,
            "CREATE TABLE edit_session (id INTEGER PRIMARY KEY REFERENCES event(id), "
            "editor_id INTEGER REFERENCES editor(id))",
        )
        kwdb.sql(db, "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')")
        kwdb.sql(db, "INSERT INTO edit_session(id, editor_id) VALUES (1, 1)")
        a = attrs(records(self.dry_run())[0])
        assert a["kw.edit_session.editor"] == {"stringValue": "vim"}
        assert "kw.edit_session.editor_id" not in a

    def test_lookup_without_name_column_uses_the_raw_value(self):
        self.seed(0)
        db = self.sb.db
        kwdb.sql(db, "CREATE TABLE host (id INTEGER PRIMARY KEY, os TEXT)")
        kwdb.sql(db, "INSERT INTO host(os) VALUES ('linux')")
        kwdb.sql(
            db,
            "CREATE TABLE ping (id INTEGER PRIMARY KEY REFERENCES event(id), "
            "host_id INTEGER REFERENCES host(id))",
        )
        kwdb.sql(db, "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')")
        kwdb.sql(db, "INSERT INTO ping(id, host_id) VALUES (1, 1)")
        assert attrs(records(self.dry_run())[0])["kw.ping.host_id"] == {"intValue": "1"}

    def test_new_column_on_executed_becomes_a_new_attribute(self):
        self.seed(1)
        assert "kw.executed.exit_code" not in attrs(records(self.dry_run())[0])
        kwdb.sql(self.sb.db, "ALTER TABLE executed ADD COLUMN exit_code INTEGER")
        kwdb.sql(self.sb.db, "UPDATE executed SET exit_code = 2")
        a = attrs(records(self.dry_run())[0])
        assert a["kw.executed.exit_code"] == {"intValue": "2"}
        assert set(a) - {"kw.executed.exit_code"} == TODAY_EXECUTED

    def test_new_column_on_event_becomes_an_event_attribute(self):
        self.seed(1)
        kwdb.sql(self.sb.db, "ALTER TABLE event ADD COLUMN host TEXT")
        kwdb.sql(self.sb.db, "UPDATE event SET host = 'box'")
        a = attrs(records(self.dry_run())[0])
        assert a["kw.event.host"] == {"stringValue": "box"}
        assert "kw.event.sent_at" not in a

    def test_event_with_no_extension_row_is_kind_event(self):
        self.seed(0)
        kwdb.sql(self.sb.db, "INSERT INTO event(date, time) VALUES ('2026-05-05','10:00:00')")
        recs = records(self.dry_run())
        assert len(recs) == 1
        assert recs[0]["body"] == {"stringValue": "event"}
        assert set(attrs(recs[0])) == {
            "kw.event.id",
            "kw.event.date",
            "kw.event.time",
            "kw.event.kind",
        }

    def test_event_matched_by_two_extension_tables_gives_two_records(self):
        self.seed(1)
        db = self.sb.db
        kwdb.sql(
            db, "CREATE TABLE note (id INTEGER PRIMARY KEY REFERENCES event(id), " "text TEXT)"
        )
        kwdb.sql(db, "INSERT INTO note(id, text) VALUES (1, 'both')")
        recs = records(self.dry_run())
        assert sorted(r["body"]["stringValue"] for r in recs) == ["executed", "note"]
        assert otlp.event_ids(self.dry_run()) == [1, 1]

    def test_batch_size_counts_events_not_records(self):
        self.seed(3, batch_size=2)
        db = self.sb.db
        kwdb.sql(
            db, "CREATE TABLE note (id INTEGER PRIMARY KEY REFERENCES event(id), " "text TEXT)"
        )
        kwdb.sql(db, "INSERT INTO note(id, text) VALUES (1, 'a'), (2, 'b')")
        doc = self.dry_run()
        assert len(records(doc)) == 4
        assert sorted(set(otlp.event_ids(doc))) == [1, 2]

    def test_tables_without_a_foreign_key_to_event_are_not_kinds(self):
        self.seed(1)
        kwdb.sql(self.sb.db, "CREATE TABLE scratch (id INTEGER PRIMARY KEY, v TEXT)")
        kwdb.sql(self.sb.db, "INSERT INTO scratch(id, v) VALUES (1, 'leak')")
        text = self.sb.run("upload", "--dry-run").out
        assert "scratch" not in text
        assert "kernel_config" not in text

    def test_views_are_not_candidates(self):
        self.seed(1)
        text = self.sb.run("upload", "--dry-run").out
        assert "active_timebox" not in text
        assert "pomodoro_report" not in text

    def test_extension_attribute_columns_never_include_id(self):
        self.seed(1)
        assert "kw.executed.id" not in attrs(records(self.dry_run())[0])


class TestIncludePredicate:
    def test_only_event_sent_at_is_excluded_today(self):
        from kw_tele.reflect import include

        assert not include("event", "sent_at")
        assert include("event", "date")
        assert include("executed", "status_id")
        assert include("timebox", "description")
        assert include("executed", "sent_at")

    def test_table_level_call_with_none_column(self):
        from kw_tele.reflect import include

        assert include("executed", None)
        assert include("event", None)
