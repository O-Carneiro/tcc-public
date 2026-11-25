"""§3 `upload`: consent gate, dry-run, batching, state machine, failure handling."""

import contextlib
import fcntl
import json
import math
import re

from helpers.cli_case import CliCase
import pytest
from helpers import constants, kvfile, kwdb, otlp
from helpers.constants import NOW


def ids_by_post(stub):
    return [otlp.event_ids(r.json()) for r in stub.posts()]


class HoldLock:
    """Holds telemetry.lock from this process, the way a running upload would."""

    def __init__(self, sandbox):
        sandbox.state_dir.mkdir(parents=True, exist_ok=True)
        self.fh = open(sandbox.lock_path, "a+")
        fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self):
        fcntl.flock(self.fh, fcntl.LOCK_UN)
        self.fh.close()


class TestConsentGate(CliCase):
    @pytest.mark.parametrize("args", [["upload"], ["upload", "--force"]])
    def test_exit_3_without_consent_and_nothing_written(self, args):
        stub = self.serve()
        self.seed(3, endpoint=stub.url, consent=False)
        before = self.snapshot()
        res = self.sb.run(*args)
        assert res.code == 3, res
        self.assert_no_side_effects(before)
        assert stub.requests == []
        assert not self.sb.state_dir.exists()

    def test_exit_3_when_consent_is_no(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url, consent="no", instance_id="")
        assert self.sb.run("upload", "--force").code == 3
        assert stub.requests == []
        assert not self.sb.state_path.exists()

    def test_exit_3_when_endpoint_was_edited_after_consent(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.sb.write_config(
            "telemetry_consent=yes\ninstance_id=i\n"
            "consented_endpoint=http://old.test/v1/logs\nendpoint=%s\n" % stub.url
        )
        assert self.sb.run("upload", "--force").code == 3
        assert stub.requests == []

    def test_force_and_dry_run_are_mutually_exclusive(self):
        self.seed(1)
        res = self.sb.run("upload", "--force", "--dry-run")
        assert res.code == 2, res


class TestDryRun(CliCase):
    def test_works_without_consent_with_placeholder_instance_id(self):
        self.seed(2, consent=False)
        before = self.snapshot()
        doc = self.dry_run()
        attrs = {a["key"]: a["value"] for a in otlp.resource_attrs(doc)}
        assert attrs["service.instance.id"] == {"stringValue": "<assigned on opt-in>"}
        assert otlp.event_ids(doc) == [1, 2]
        self.assert_no_side_effects(before)
        assert not self.sb.state_dir.exists()

    def test_uses_real_instance_id_when_present(self):
        self.seed(1)
        attrs = {a["key"]: a["value"] for a in otlp.resource_attrs(self.dry_run())}
        assert attrs["service.instance.id"] == {"stringValue": constants.INSTANCE}

    def test_prints_only_json_on_stdout_and_does_not_touch_rows(self):
        self.seed(3)
        res = self.sb.run("upload", "--dry-run")
        json.loads(res.out)
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None, 3: None}

    def test_takes_at_most_batch_size_events_lowest_ids_first(self):
        self.seed(5, batch_size=2)
        assert otlp.event_ids(self.dry_run()) == [1, 2]

    def test_skips_sent_and_excluded_rows(self):
        self.seed(5, batch_size=3)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        assert otlp.event_ids(self.dry_run()) == [3, 4, 5]

    def test_no_pending_prints_empty_document(self):
        self.seed(0)
        res = self.sb.run("upload", "--dry-run")
        assert res.code == 0, res
        assert json.loads(res.out) == {"resourceLogs": []}

    def test_takes_no_lock(self):
        self.seed(2)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        res = self.sb.run("upload", "--dry-run")
        assert res.code == 0, res
        assert otlp.event_ids(json.loads(res.out)) == [1, 2]

    def test_does_not_create_lock_or_state_files(self):
        self.seed(2)
        self.dry_run()
        assert not self.sb.lock_path.exists()
        assert not self.sb.state_path.exists()

    def test_does_not_open_the_network(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.dry_run()
        assert stub.requests == []

    def test_exit_6_without_db_and_exit_4_without_column(self):
        assert self.sb.run("upload", "--dry-run").code == 6
        self.sb.make_db(with_sent_at=False)
        assert self.sb.run("upload", "--dry-run").code == 4


class TestForceUpload(CliCase):
    def test_sends_everything_pending_in_batches_of_batch_size(self):
        stub = self.serve()
        self.seed(5, endpoint=stub.url, batch_size=2)
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert ids_by_post(stub) == [[1, 2], [3, 4], [5]]
        assert kwdb.sent_map(self.sb.db) == {i: NOW for i in range(1, 6)}
        assert res.out.strip() == "sent 5 events to %s" % stub.url

    def test_request_is_a_json_post(self):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        self.sb.run("upload", "--force")
        req = stub.posts()[0]
        assert req.headers["Content-Type"] == "application/json"
        assert req.path == "/v1/logs"

    def test_success_updates_state(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.sb.write_state(
            last_attempt_at=5, last_success_at=6, consecutive_failures=4, last_status="http-503"
        )
        assert self.sb.run("upload", "--force").code == 0
        assert self.sb.read_state() == (
            {
                "last_attempt_at": str(NOW),
                "last_success_at": str(NOW),
                "consecutive_failures": "0",
                "last_status": "ok",
            }
        )

    def test_nothing_pending_is_a_success(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert res.out.strip() == "nothing to send"
        assert stub.requests == []
        assert self.sb.read_state()["last_status"] == "ok"
        assert self.sb.read_state()["last_success_at"] == str(NOW)

    def test_excluded_rows_are_never_sent(self):
        stub = self.serve()
        self.seed(4, endpoint=stub.url)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        kwdb.set_sent_at(self.sb.db, 3, 1_700_000_000)
        assert self.sb.run("upload", "--force").code == 0
        assert ids_by_post(stub) == [[1, 4]]
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: -1, 3: 1_700_000_000, 4: NOW}

    def test_sent_rows_are_not_sent_twice(self):
        stub = self.serve()
        self.seed(3, endpoint=stub.url)
        self.sb.run("upload", "--force")
        self.sb.run("upload", "--force", now=NOW + 5)
        assert len(stub.posts()) == 1
        assert set(kwdb.sent_map(self.sb.db).values()) == {NOW}

    @pytest.mark.parametrize(
        "body",
        [b"", b"not json", b"\xff\xfe", b'{"partialSuccess": {"rejectedLogRecords": 3}}'],
    )
    def test_response_body_is_never_required(self, body):
        stub = self.serve(respond=lambda req: (200, {}, body))
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert set(kwdb.sent_map(self.sb.db).values()) == {NOW}

    def test_any_2xx_is_success(self):
        stub = self.serve(codes=[202])
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert set(kwdb.sent_map(self.sb.db).values()) == {NOW}

    def test_background_mode_prints_nothing(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        res = self.sb.run("upload")
        assert res.code == 0, res
        assert (res.out, res.err) == ("", "")
        assert len(stub.posts()) == 1

    def test_background_failure_prints_nothing(self):
        stub = self.serve(codes=[503])
        self.seed(2, endpoint=stub.url)
        res = self.sb.run("upload")
        assert res.code == 5, res
        assert (res.out, res.err) == ("", "")


class TestFailure(CliCase):
    def test_503_exits_5_and_counts_the_failure(self):
        stub = self.serve(codes=[503])
        self.seed(3, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=2, last_success_at=77)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None, 3: None}
        state = self.sb.read_state()
        assert state["consecutive_failures"] == "3"
        assert state["last_status"] == "http-503"
        assert state["last_attempt_at"] == str(NOW)
        assert state["last_success_at"] == "77"
        assert res.err.strip(), "error goes to stderr"
        assert res.out == ""

    def test_400_is_recorded_like_any_other_non_2xx(self):
        stub = self.serve(codes=[400])
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 5
        assert self.sb.read_state()["last_status"] == "http-400"

    def test_redirect_to_a_200_is_still_a_failure(self):
        stub = self.serve(codes=["redirect"])
        self.seed(2, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}
        assert self.sb.read_state()["last_status"] == "http-302"
        assert [r.path for r in stub.requests] == ["/v1/logs"]

    def test_connection_refused(self):
        self.seed(2, endpoint=constants.DEAD_URL)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert self.sb.read_state()["last_status"] == "error:connection"
        assert self.sb.read_state()["consecutive_failures"] == "1"
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}

    def test_socket_timeout_is_error_timeout(self):
        stub = self.serve(codes=["hang"])
        self.seed(2, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert self.sb.read_state()["last_status"] == "error:timeout"
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}

    def test_earlier_batches_stay_marked_when_a_later_one_fails(self):
        stub = self.serve(codes=[200, 503])
        self.seed(5, endpoint=stub.url, batch_size=2)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: None, 4: None, 5: None}
        state = self.sb.read_state()
        assert (state["consecutive_failures"], state["last_status"]) == ("1", "http-503")

    def test_failed_batch_is_retried_from_the_lowest_id(self):
        stub = self.serve(codes=[503, 200])
        self.seed(3, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 5
        assert self.sb.run("upload", "--force", now=NOW + 10).code == 0
        assert ids_by_post(stub) == [[1, 2, 3], [1, 2, 3]]
        assert self.sb.read_state()["consecutive_failures"] == "0"

    def test_no_db_exits_6_and_records_error_nodb(self):
        self.sb.configure(endpoint=constants.DEAD_URL)
        self.sb.write_state(consecutive_failures=1)
        res = self.sb.run("upload", "--force")
        assert res.code == 6, res
        state = self.sb.read_state()
        assert (
            (state["last_status"], state["consecutive_failures"], state["last_attempt_at"])
        ) == ("error:nodb", "2", str(NOW))

    def test_missing_column_exits_4_and_records_error_schema(self):
        stub = self.serve()
        self.sb.make_db(with_sent_at=False)
        kwdb.add_executed(self.sb.db)
        self.sb.configure(endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 4, res
        assert self.sb.read_state()["last_status"] == "error:schema"
        assert self.sb.read_state()["consecutive_failures"] == "1"
        assert stub.requests == []
        cols = [r[1] for r in kwdb.sql(self.sb.db, "PRAGMA table_info(event)")]
        assert "sent_at" not in cols

    def test_internal_error_when_state_cannot_be_written_sends_nothing(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.sb.state_dir.parent.mkdir(parents=True, exist_ok=True)
        self.sb.state_dir.write_text("a file where the state dir should be")
        res = self.sb.run("upload", "--force")
        assert res.code == 1, res
        assert stub.requests == []
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}


class TestLock(CliCase):
    def test_lock_held_by_another_process_exits_0_without_sending(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert "upload already in progress" in res.err
        assert stub.requests == []
        assert not self.sb.state_path.exists()
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}

    def test_lock_held_in_background_mode_is_silent(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        res = self.sb.run("upload")
        assert (res.code, res.out, res.err) == (0, "", "")
        assert stub.requests == []

    def test_lock_is_released_after_a_run(self):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert self.sb.lock_path.exists()
        with open(self.sb.lock_path, "a+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_consent_check_comes_before_the_lock(self):
        self.seed(1, consent=False)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        assert self.sb.run("upload", "--force").code == 3


class TestSpacing(CliCase):
    def attempt(self, attempt_at, failures, success=0, **cfg):
        stub = self.serve()
        self.seed(2, endpoint=stub.url, **cfg)
        self.sb.write_state(
            last_attempt_at=attempt_at,
            last_success_at=success,
            consecutive_failures=failures,
            last_status="ok",
        )
        before = self.snapshot()
        res = self.sb.run("upload")
        return stub, res, before

    def assert_skipped(self, stub, res, before):
        assert res.code == 0, res
        assert stub.requests == []
        self.assert_no_side_effects(before)

    def assert_sent(self, stub, res):
        assert res.code == 0, res
        assert len(stub.posts()) == 1
        assert self.sb.read_state()["last_attempt_at"] == str(NOW)

    def test_inside_backoff_after_success_exits_0_and_changes_nothing(self):
        self.assert_skipped(*self.attempt(NOW - 3599, 0))

    def test_at_backoff_boundary_it_runs(self):
        self.assert_sent(*self.attempt(NOW - 3600, 0)[:2])

    def test_gap_doubles_per_failure(self):
        self.assert_skipped(*self.attempt(NOW - 14399, 3))

    def test_gap_boundary_after_three_failures(self):
        self.assert_sent(*self.attempt(NOW - 14400, 3)[:2])

    def test_gap_is_capped_at_the_upload_interval(self):
        self.assert_skipped(*self.attempt(NOW - 86399, 30))

    def test_capped_gap_boundary(self):
        self.assert_sent(*self.attempt(NOW - 86400, 30)[:2])

    def test_cap_is_never_below_the_backoff(self):
        self.assert_skipped(*self.attempt(NOW - 3599, 5, upload_interval=100))

    def test_cap_never_below_backoff_boundary(self):
        self.assert_sent(*self.attempt(NOW - 3600, 5, upload_interval=100)[:2])

    def test_custom_backoff_is_honoured(self):
        self.assert_skipped(*self.attempt(NOW - 59, 0, upload_backoff=60))

    def test_time_due_is_not_rechecked(self):
        stub, res, _ = self.attempt(NOW - 4000, 0, success=NOW - 10)
        self.assert_sent(stub, res)

    def test_force_ignores_spacing(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.sb.write_state(last_attempt_at=NOW - 1)
        assert self.sb.run("upload", "--force").code == 0
        assert len(stub.posts()) == 1


class TestStateOrdering(CliCase):
    def test_failure_is_written_before_anything_is_sent(self):
        seen = []
        stub = self.serve(before=lambda req: seen.append(self.sb.read_state()))
        self.seed(2, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=2, last_success_at=9, last_status="ok")
        assert self.sb.run("upload", "--force").code == 0
        assert stub.errors == []
        assert seen[0] == (
            {
                "last_attempt_at": str(NOW),
                "last_success_at": "9",
                "consecutive_failures": "3",
                "last_status": "error:internal",
            }
        )

    def test_a_killed_run_leaves_the_failure_counted(self):
        import signal
        import subprocess
        import sys

        stub = self.serve(codes=["hang"])
        self.seed(2, endpoint=stub.url)
        proc = subprocess.Popen(
            [sys.executable, "-m", "kw_tele", "upload", "--force"],
            env=self.sb.env(),
            cwd=str(self.sb.root),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        import time

        deadline = time.monotonic() + 15
        while not stub.requests and time.monotonic() < deadline:
            time.sleep(0.05)
        assert stub.requests, "uploader never reached the server"
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        state = self.sb.read_state()
        assert (
            (state["consecutive_failures"], state["last_status"], state["last_attempt_at"])
        ) == ("1", "error:internal", str(NOW))

    def test_sqlite_is_not_locked_while_the_request_is_in_flight(self):
        probe = []

        def before(req):
            kwdb.sql(self.sb.db, "INSERT INTO event(date, time) VALUES ('2030-01-01','00:00:00')")
            probe.append(True)

        stub = self.serve(before=before)
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert stub.errors == []
        assert probe == [True]


class TestMidRunConsent(CliCase):
    def test_consent_withdrawn_between_batches_exits_3_and_restores_failures(self):
        def withdraw(req):
            if req.index == 0:
                self.sb.write_config(
                    "telemetry_consent=no\ninstance_id=\n"
                    "consented_endpoint=\nendpoint=%s\n" % stub.url
                )

        stub = self.serve(before=withdraw)
        self.seed(3, endpoint=stub.url, batch_size=1)
        self.sb.write_state(
            last_attempt_at=NOW - 9000,
            last_success_at=111,
            consecutive_failures=2,
            last_status="http-503",
        )
        res = self.sb.run("upload")
        assert res.code == 3, res
        assert len(stub.posts()) == 1
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: None, 3: None}
        assert self.sb.read_state() == (
            {
                "last_attempt_at": str(NOW),
                "last_success_at": "111",
                "consecutive_failures": "2",
                "last_status": "http-503",
            }
        )

    def test_endpoint_edit_between_batches_also_stops_the_run(self):
        def edit(req):
            if req.index == 0:
                cfg = re.sub(
                    r"^endpoint=.*$",
                    "endpoint=http://other.test/x",
                    self.sb.read_config_text(),
                    flags=re.M,
                )
                kvfile.atomic_write(self.sb.config_path, cfg)

        stub = self.serve(before=edit)
        self.seed(3, endpoint=stub.url, batch_size=1)
        res = self.sb.run("upload", "--force")
        assert res.code == 3, res
        assert len(stub.posts()) == 1


class TestRowIdentity(CliCase):
    def test_reused_id_with_different_content_is_not_marked(self):
        def swap(req):
            if req.index == 0:
                kwdb.sql(self.sb.db, "DELETE FROM statistics_report WHERE id = 3")
                new = kwdb.add_executed(self.sb.db, date="2031-12-31", time="23:59:59")
                assert new == 3

        stub = self.serve(before=swap)
        self.seed(3, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert stub.errors == []
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: None}

    def test_row_excluded_while_in_flight_is_recorded_as_sent(self):
        def exclude(req):
            kwdb.set_sent_at(self.sb.db, 2, -1)

        stub = self.serve(before=exclude)
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW}

    def test_row_with_null_time_is_marked(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        kwdb.add_executed(self.sb.db, time=None)
        assert self.sb.run("upload", "--force").code == 0
        assert kwdb.sent_map(self.sb.db) == {1: NOW}

    def test_row_sent_by_a_concurrent_run_keeps_its_original_timestamp(self):
        def other_run_marks(req):
            kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)

        stub = self.serve(before=other_run_marks)
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert kwdb.sent_map(self.sb.db) == {1: 1_700_000_000, 2: NOW}


class TestDirtyData(CliCase):
    def test_undecodable_text_does_not_block_the_batch(self):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        kwdb.add_timebox(self.sb.db, description="x")
        kwdb.sql(self.sb.db, "UPDATE timebox SET description = CAST(X'66ff6f' AS TEXT)")
        kwdb.add_executed(self.sb.db, date="2026-04-01")
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: NOW}
        recs = otlp.records(stub.posts()[0].json())
        desc = otlp.attrs(recs[1])["kw.timebox.description"]
        assert desc == {"stringValue": "f�o"}

    def test_malformed_date_gives_time_zero_and_the_record_is_sent(self):
        stub = self.serve()
        self.seed(3, endpoint=stub.url)
        kwdb.sql(self.sb.db, "UPDATE event SET date='not-a-date' WHERE id=1")
        kwdb.sql(self.sb.db, "UPDATE event SET date='2026-13-45' WHERE id=2")
        kwdb.sql(self.sb.db, "UPDATE event SET time='soon' WHERE id=3")
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        recs = otlp.records(stub.posts()[0].json())
        assert [r["timeUnixNano"] for r in recs] == ["0", "0", "0"]
        assert otlp.attrs(recs[0])["kw.event.date"] == {"stringValue": "not-a-date"}
        assert otlp.attrs(recs[2])["kw.event.time"] == {"stringValue": "soon"}
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: NOW}


class TestDeadline(CliCase):
    @pytest.mark.slow
    def test_deadline_after_progress_is_partial_and_resets_failures(self):
        stub = self.serve(codes=[200, "trickle"])
        self.seed(2, endpoint=stub.url, batch_size=1)
        self.sb.write_state(consecutive_failures=3, last_success_at=42)
        res = self.sb.run("upload", "--force", timeout=85)
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: None}
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == ("partial", "0")
        assert state["last_success_at"] == "42"
        assert state["last_attempt_at"] == str(NOW)

    @pytest.mark.slow
    def test_deadline_without_progress_records_timeout_and_keeps_the_failure(self):
        stub = self.serve(codes=["trickle"])
        self.seed(2, endpoint=stub.url, batch_size=1)
        self.sb.write_state(consecutive_failures=1)
        res = self.sb.run("upload", "--force", timeout=85)
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}
        state = self.sb.read_state()
        assert state["consecutive_failures"] == "2"
        assert state["last_status"] == "error:timeout"


def _ids(req):
    return [int(otlp.attrs(r)["kw.event.id"]["intValue"]) for r in otlp.records(req.json())]


class Poison:
    """Stub responder: a POST holding a bad id gets `code`; `transient` ids get 503 (bad ids win)."""

    def __init__(self, bad, code=413, transient=()):
        self.bad, self.code, self.transient = set(bad), code, set(transient)
        self.accepted, self.rejected = [], []

    def __call__(self, req):
        ids = _ids(req)
        if self.bad & set(ids):
            self.rejected.append(ids)
            return self.code, {}, b"{}"
        if self.transient & set(ids):
            return 503, {}, b"{}"
        self.accepted.extend(ids)
        return 200, {}, b"{}"


def _bulk(db, count):
    with contextlib.closing(kwdb.connect(db)) as conn:
        conn.executemany(
            "INSERT INTO statistics_report "
            "(label_name, status, date, time, elapsed_time_in_secs) VALUES (?,?,?,?,?)",
            [("build", "success", "2026-03-04", "05:06:07", i) for i in range(count)],
        )
        conn.commit()


def _marked(db):
    return sorted(i for i, v in kwdb.sent_map(db).items() if v == NOW)


class TestPoisonBatch(CliCase):
    """A permanent 4xx must not block newer rows: halve the batch, skip a lone bad row."""

    @pytest.mark.parametrize("code", [400, 413, 422])
    def test_bad_row_is_skipped_and_the_others_are_sent(self, code):
        stub = self.serve(respond=Poison({3}, code=code))
        self.seed(5, endpoint=stub.url)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: None, 4: NOW, 5: NOW}
        assert self.sb.read_state()["last_status"] == "http-%d" % code

    def test_each_row_is_accepted_at_most_once_and_matches_what_is_marked(self):
        poison = Poison({3})
        stub = self.serve(respond=poison)
        self.seed(5, endpoint=stub.url)
        self.sb.run("upload", "--force", timeout=30)
        assert sorted(poison.accepted) == [1, 2, 4, 5]
        assert _marked(self.sb.db) == sorted(poison.accepted)
        assert [3] in poison.rejected
        assert len(stub.posts()) <= 2 * 5

    def test_bad_row_at_the_front_does_not_stall_small_batches(self):
        stub = self.serve(respond=Poison({1}, code=400))
        self.seed(5, endpoint=stub.url, batch_size=2)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: NOW, 3: NOW, 4: NOW, 5: NOW}
        assert len(stub.posts()) <= 12

    def test_all_rows_bad_sends_nothing_and_stops(self):
        stub = self.serve(codes=[400])
        self.seed(4, endpoint=stub.url)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert set(kwdb.sent_map(self.sb.db).values()) == {None}
        assert len(stub.posts()) <= 2 * 4

    def test_info_reports_the_skipped_row_as_pending(self):
        stub = self.serve(respond=Poison({3}))
        self.seed(5, endpoint=stub.url)
        self.sb.run("upload", "--force", timeout=30)
        out = self.sb.run("info").out
        assert "pending events: 1" in out
        assert "last status: http-413" in out
        assert self.sb.run("info", "--pending").out.splitlines()[0] == "pending_events=1"

    @pytest.mark.parametrize("code", [408, 429, 500, 503])
    def test_transient_codes_are_not_bisected(self, code):
        stub = self.serve(codes=[code])
        self.seed(4, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert len(stub.posts()) == 1
        assert set(kwdb.sent_map(self.sb.db).values()) == {None}


class TestPoisonBatchLimits(CliCase):
    """D41: only 400/413/422 blame the rows; skips are capped; partial progress is recorded."""

    @pytest.mark.parametrize("code", [401, 403, 404, 405, 415, 421])
    def test_endpoint_level_4xx_stops_at_the_first_failure(self, code):
        stub = self.serve(codes=[code])
        self.seed(0, endpoint=stub.url)
        _bulk(self.sb.db, 3000)
        res = self.sb.run("upload", "--force", timeout=15)
        assert res.code == 5, res
        assert len(stub.posts()) <= 2
        assert self.sb.read_state()["last_status"] == "http-%d" % code
        assert self.sb.read_state()["consecutive_failures"] == "1"
        assert _marked(self.sb.db) == []
        assert kwdb.sql(self.sb.db, "SELECT count(*) FROM event WHERE sent_at IS NULL")[0][0] == 3000

    def test_at_most_20_rows_are_skipped_per_run(self):
        bad = set(range(3, 91, 3))  # 30 interleaved bad rows among 100
        poison = Poison(bad)
        stub = self.serve(respond=poison)
        self.seed(0, endpoint=stub.url)
        _bulk(self.sb.db, 100)
        res = self.sb.run("upload", "--force", timeout=60)
        assert res.code == 5, res
        assert set(_marked(self.sb.db)) <= set(range(1, 101)) - bad
        assert len(poison.accepted) == len(set(poison.accepted))
        singles = [r for r in poison.rejected if len(r) == 1]
        assert 20 <= len(singles) <= 21
        good_batches = len(stub.posts()) - len(poison.rejected)
        assert len(stub.posts()) <= 20 * (math.log2(500) + 2) + good_batches
        assert self.sb.read_state()["last_status"] == "http-413"

    def test_more_than_999_pending_rows_with_skips_is_not_an_error(self):
        poison = Poison({5})
        stub = self.serve(respond=poison)
        self.seed(0, endpoint=stub.url, batch_size=1200)
        _bulk(self.sb.db, 1500)
        res = self.sb.run("upload", "--force", timeout=60)
        assert res.code == 5, res
        assert "Traceback" not in res.err and "OperationalError" not in res.err, res
        assert _marked(self.sb.db) == [i for i in range(1, 1501) if i != 5]
        assert self.sb.read_state()["last_status"] == "http-413"

    def test_5xx_while_bisecting_keeps_accepted_rows_and_reports_the_transient_status(self):
        poison = Poison({3}, transient={4})
        stub = self.serve(respond=poison)
        self.seed(5, endpoint=stub.url)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert _marked(self.sb.db) == sorted(poison.accepted) == [1, 2]
        assert self.sb.read_state()["last_status"] == "http-503"

    def test_accepted_rows_with_refused_rows_count_as_partial_progress(self):
        stub = self.serve(respond=Poison({3}))
        self.seed(5, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=3, last_success_at=42)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        state = self.sb.read_state()
        assert state["last_status"] == "http-413"
        assert state["consecutive_failures"] == "0"
        # Same as the D30 deadline path: partial progress is not a success.
        assert state["last_success_at"] == "42"
        assert state["last_attempt_at"] == str(NOW)

    def test_nothing_accepted_keeps_counting_the_failure(self):
        stub = self.serve(codes=[400])
        self.seed(4, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=3, last_success_at=42)
        assert self.sb.run("upload", "--force", timeout=30).code == 5
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == ("http-400", "4")
        assert state["last_success_at"] == "42"

    def test_second_run_retries_the_bad_row_first_and_delivers_the_others(self):
        poison = Poison({3})
        stub = self.serve(respond=poison)
        self.seed(5, endpoint=stub.url)
        self.sb.run("upload", "--force", timeout=30)
        kwdb.add_executed(self.sb.db, date="2026-04-01")
        kwdb.add_executed(self.sb.db, date="2026-04-02")
        before = len(stub.posts())
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert _ids(stub.posts()[before])[0] == 3
        assert _marked(self.sb.db) == [1, 2, 4, 5, 6, 7]
        assert sorted(poison.accepted) == [1, 2, 4, 5, 6, 7]


class TestLockedDbOtherCommands(CliCase):
    def _locked(self, *args):
        with contextlib.closing(kwdb.connect(self.sb.db)) as holder:
            holder.execute("BEGIN EXCLUSIVE")
            return self.sb.run(*args)

    def test_config_on_a_locked_db_has_no_traceback(self):
        self.seed(2)
        res = self._locked("config")
        assert "Traceback" not in res.err, res
        # Either listing with unknown counts (0) or an error exit (1) is clean; a crash is not.
        assert res.code in (0, 1), res

    @pytest.mark.parametrize("extra", [[], ["--include-history"]])
    def test_consent_yes_on_a_locked_db_fails_and_changes_nothing(self, extra):
        self.seed(2, consent=False)
        res = self._locked("consent", "--set", "yes", *extra)
        assert "Traceback" not in res.err, res
        assert res.code == 1, res
        assert not self.sb.config_path.exists()


class TestSizeLimit(CliCase):
    """A 413 that halving resolves is not a failure: only skipped rows make a run fail."""

    @staticmethod
    def _limit(max_records, bad=()):
        accepted = []

        def respond(req):
            ids = _ids(req)
            if len(ids) > max_records or set(ids) & set(bad):
                return 413, {}, b"{}"
            accepted.extend(ids)
            return 200, {}, b"{}"

        respond.accepted = accepted
        return respond

    def test_size_only_413_ends_in_success(self):
        respond = self._limit(50)
        stub = self.serve(respond=respond)
        self.seed(0, endpoint=stub.url, batch_size=200)
        _bulk(self.sb.db, 300)
        self.sb.write_state(consecutive_failures=2, last_success_at=42)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 0, res
        assert "sent 300 events" in res.out
        assert _marked(self.sb.db) == list(range(1, 301))
        assert sorted(respond.accepted) == list(range(1, 301))
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == ("ok", "0")
        assert state["last_success_at"] == str(NOW)

    def test_later_run_with_a_backlog_still_ends_in_success(self):
        respond = self._limit(50)
        stub = self.serve(respond=respond)
        self.seed(0, endpoint=stub.url, batch_size=200)
        _bulk(self.sb.db, 300)
        assert self.sb.run("upload", "--force", timeout=30).code == 0
        _bulk(self.sb.db, 120)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 0, res
        assert _marked(self.sb.db) == list(range(1, 421))
        assert self.sb.read_state()["last_status"] == "ok"

    def test_size_limit_plus_one_bad_row_fails_and_skips_only_that_row(self):
        respond = self._limit(50, bad={120})
        stub = self.serve(respond=respond)
        self.seed(0, endpoint=stub.url, batch_size=200)
        _bulk(self.sb.db, 300)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert _marked(self.sb.db) == [i for i in range(1, 301) if i != 120]
        assert self.sb.read_state()["last_status"] == "http-413"


class TestFutureState(CliCase):
    """D40 for kw_tele: a time in the future (clock change) reads as 0, so the gate does not block."""

    TEN_DAYS = 10 * 86400

    @pytest.mark.parametrize(
        "fields",
        [
            ["last_attempt_at"],
            ["last_success_at"],
            ["last_attempt_at", "last_success_at"],
        ],
    )
    def test_future_time_does_not_block_a_due_upload(self, fields):
        stub = self.serve()
        self.seed(3, endpoint=stub.url)
        state = {"last_attempt_at": NOW - self.TEN_DAYS, "last_success_at": NOW - self.TEN_DAYS}
        state.update({f: NOW + self.TEN_DAYS for f in fields})
        self.sb.write_state(consecutive_failures=0, last_status="ok", **state)
        res = self.sb.run("upload")
        assert res.code == 0, res
        assert len(stub.posts()) == 1
        assert set(kwdb.sent_map(self.sb.db).values()) == {NOW}
        after = self.sb.read_state()
        assert int(after["last_attempt_at"]) <= NOW
        assert int(after["last_success_at"]) <= NOW

    def test_future_times_are_rewritten_even_when_the_upload_fails(self):
        stub = self.serve(codes=[503])
        self.seed(2, endpoint=stub.url)
        future = NOW + self.TEN_DAYS
        self.sb.write_state(
            last_attempt_at=future, last_success_at=future, consecutive_failures=0, last_status="ok"
        )
        assert self.sb.run("upload").code == 5
        after = self.sb.read_state()
        assert int(after["last_attempt_at"]) <= NOW
        assert int(after["last_success_at"]) <= NOW


class TestLockedDbDuringUpload(CliCase):
    def test_locked_db_is_a_clean_exit_1_and_keeps_rows_pending(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        # kw_tele waits 10 s for the lock.
        with contextlib.closing(kwdb.connect(self.sb.db)) as holder:
            holder.execute("BEGIN EXCLUSIVE")
            res = self.sb.run("upload", "--force")
        assert "Traceback" not in res.err, res
        assert res.code == 1, res
        assert res.err.strip(), res
        assert stub.posts() == []
        assert set(kwdb.sent_map(self.sb.db).values()) == {None}
