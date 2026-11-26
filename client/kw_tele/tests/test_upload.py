"""§3 `upload`: consent gate, dry-run, batching, state machine, failure handling."""

import contextlib
import fcntl
import json
import math

from helpers.cli_case import CliCase
import pytest
from helpers import constants, kwdb, otlp
from kw_tele import commands
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
    @pytest.mark.parametrize(
        "args, config",
        [
            (["upload"], None),
            (["upload", "--force"], None),
            (
                ["upload", "--force"],
                "telemetry_consent=no\ninstance_id=\nconsented_endpoint=\n",
            ),
            (
                ["upload", "--force"],
                "telemetry_consent=yes\ninstance_id=i\nconsented_endpoint=http://old.test/v1/logs\n",
            ),
        ],
        ids=[
            "background-no-config",
            "force-no-config",
            "consent-no",
            "endpoint-changed-since-consent",
        ],
    )
    def test_exit_3_without_effective_consent_sends_and_writes_nothing(
        self, args, config
    ):
        stub = self.serve()
        self.seed(3, endpoint=stub.url, consent=False)
        if config:
            self.sb.write_config(config + f"endpoint={stub.url}\n")
        before = self.snapshot()
        res = self.sb.run(*args)
        assert res.code == 3, res
        self.assert_no_side_effects(before)
        assert stub.requests == []
        assert not self.sb.state_dir.exists()


class TestDryRun(CliCase):
    def test_without_consent_uses_a_placeholder_instance_id_and_touches_nothing(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url, consent=False)
        before = self.snapshot()
        res = self.sb.run("upload", "--dry-run")
        doc = json.loads(res.out)
        attrs = {a["key"]: a["value"] for a in otlp.resource_attrs(doc)}
        assert attrs["service.instance.id"] == {"stringValue": "<assigned on opt-in>"}
        assert otlp.event_ids(doc) == [1, 2]
        self.assert_no_side_effects(before)
        assert not self.sb.state_dir.exists()
        assert stub.requests == []

    def test_takes_no_lock_and_uses_the_real_instance_id(self):
        self.seed(2)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        doc = self.dry_run()
        attrs = {a["key"]: a["value"] for a in otlp.resource_attrs(doc)}
        assert attrs["service.instance.id"] == {"stringValue": constants.INSTANCE}
        assert otlp.event_ids(doc) == [1, 2]
        assert not self.sb.state_path.exists()

    def test_takes_at_most_batch_size_unsent_events_lowest_ids_first(self):
        self.seed(6, batch_size=3)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        assert otlp.event_ids(self.dry_run()) == [3, 4, 5]
        assert kwdb.sent_map(self.sb.db)[3] is None


class TestForceUpload(CliCase):
    def test_sends_pending_in_batches_marks_rows_and_updates_state(self):
        stub = self.serve()
        self.seed(5, endpoint=stub.url, batch_size=2)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        kwdb.set_sent_at(self.sb.db, 3, 1_700_000_000)
        self.sb.write_state(
            last_attempt_at=5,
            last_success_at=6,
            consecutive_failures=4,
            last_status="http-503",
        )
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert ids_by_post(stub) == [[1, 4], [5]]
        assert stub.posts()[0].headers["Content-Type"] == "application/json"
        assert stub.posts()[0].path == "/v1/logs"
        assert kwdb.sent_map(self.sb.db) == {
            1: NOW,
            2: -1,
            3: 1_700_000_000,
            4: NOW,
            5: NOW,
        }
        assert res.out.strip() == f"sent 3 events to {stub.url}"
        assert self.sb.read_state() == {
            "last_attempt_at": str(NOW),
            "last_success_at": str(NOW),
            "consecutive_failures": "0",
            "last_status": "ok",
        }

    def test_sent_rows_are_not_sent_again(self):
        stub = self.serve()
        self.seed(3, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        res = self.sb.run("upload", "--force", now=NOW + 5)
        assert (res.code, res.out.strip()) == (0, "nothing to send")
        assert len(stub.posts()) == 1

    def test_the_post_is_the_dry_run_document_and_carries_no_sent_at(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        doc = self.dry_run()
        assert self.sb.run("upload", "--force").code == 0
        post = stub.posts()[0]
        assert post.json() == doc
        assert b"sent_at" not in post.body

    def test_events_of_one_week_share_a_coarse_time_but_are_each_marked(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        for day in ("2026-03-02", "2026-03-04", "2026-03-08"):
            kwdb.add_executed(self.sb.db, date=day, time="23:59:59")
        assert self.sb.run("upload", "--force").code == 0
        recs = otlp.records(stub.posts()[0].json())
        assert {r["timeUnixNano"] for r in recs} == {
            otlp.week_ns("2026-03-02", "23:59:59")
        }
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: NOW}

    def test_nothing_pending_is_a_success(self):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert res.out.strip() == "nothing to send"
        assert stub.requests == []
        state = self.sb.read_state()
        assert (state["last_status"], state["last_success_at"]) == ("ok", str(NOW))

    @pytest.mark.parametrize(
        "code, body",
        [
            (200, b"not json"),
            (200, b'{"partialSuccess": {"rejectedLogRecords": 3}}'),
            (202, b"{}"),
        ],
    )
    def test_any_2xx_marks_rows_whatever_the_body(self, code, body):
        stub = self.serve(respond=lambda req: (code, {}, body))
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert set(kwdb.sent_map(self.sb.db).values()) == {NOW}

    @pytest.mark.parametrize(
        "respond, exit_code, sent",
        [
            (lambda req: (200, {}, b"{}"), 0, {1: NOW, 2: NOW, 3: NOW}),
            (lambda req: (503, {}, b"{}"), 5, {1: None, 2: None, 3: None}),
            (
                lambda req: (413, {}, b"{}") if 2 in _ids(req) else (200, {}, b"{}"),
                5,
                {1: NOW, 2: None, 3: NOW},
            ),
        ],
        ids=["success", "failure", "partial-with-a-skipped-row"],
    )
    def test_background_mode_prints_nothing(self, respond, exit_code, sent):
        stub = self.serve(respond=respond)
        self.seed(3, endpoint=stub.url)
        res = self.sb.run("upload")
        assert (res.code, res.out, res.err) == (exit_code, "", "")
        assert kwdb.sent_map(self.sb.db) == sent


class TestFailure(CliCase):
    def test_503_exits_5_leaves_rows_pending_and_counts_the_failure(self):
        stub = self.serve(codes=[503])
        self.seed(3, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=2, last_success_at=77)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None, 3: None}
        assert self.sb.read_state() == {
            "last_attempt_at": str(NOW),
            "last_success_at": "77",
            "consecutive_failures": "3",
            "last_status": "http-503",
        }
        assert res.err.strip(), "error goes to stderr"
        assert res.out == ""

    @pytest.mark.parametrize(
        "codes, endpoint, status",
        [
            ([400], None, "http-400"),
            (["redirect"], None, "http-302"),
            (["hang"], None, "error:timeout"),
            (None, constants.DEAD_URL, "error:connection"),
            (None, "http://" + "a" * 70 + ".test/v1/logs", "error:connection"),
            (None, "http://collector.test/v1 logs", "error:connection"),
        ],
        ids=[
            "400",
            "redirect-to-a-200",
            "socket-timeout",
            "connection-refused",
            "hostname-label-too-long",
            "space-in-url",
        ],
    )
    def test_each_failure_kind_exits_5_with_its_status_and_keeps_rows_pending(
        self, codes, endpoint, status
    ):
        stub = self.serve(codes=codes) if codes else None
        self.seed(2, endpoint=endpoint or stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert "Traceback" not in res.err, res
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == (status, "1")
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}
        if codes == ["redirect"]:
            assert [r.path for r in stub.requests] == ["/v1/logs"]

    def test_earlier_batches_stay_marked_and_the_failed_batch_is_retried_from_the_lowest_id(
        self,
    ):
        stub = self.serve(codes=[200, 503, 200])
        self.seed(5, endpoint=stub.url, batch_size=2)
        assert self.sb.run("upload", "--force").code == 5
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: None, 4: None, 5: None}
        state = self.sb.read_state()
        assert (state["consecutive_failures"], state["last_status"]) == (
            "1",
            "http-503",
        )
        assert self.sb.run("upload", "--force", now=NOW + 10).code == 0
        assert ids_by_post(stub) == [[1, 2], [3, 4], [3, 4], [5]]
        assert self.sb.read_state()["consecutive_failures"] == "0"

    @pytest.mark.parametrize(
        "db_state, code, status",
        [("none", 6, "error:nodb"), ("no-column", 4, "error:schema")],
    )
    def test_unusable_database_records_the_failure_and_sends_nothing(
        self, db_state, code, status
    ):
        stub = self.serve()
        if db_state == "no-column":
            self.sb.make_db(with_sent_at=False)
            kwdb.add_executed(self.sb.db)
        self.sb.configure(endpoint=stub.url)
        self.sb.write_state(consecutive_failures=1)
        res = self.sb.run("upload", "--force")
        assert res.code == code, res
        assert "Traceback" not in res.err, res
        state = self.sb.read_state()
        assert (
            state["last_status"],
            state["consecutive_failures"],
            state["last_attempt_at"],
        ) == (
            status,
            "2",
            str(NOW),
        )
        assert stub.requests == []


class TestLock(CliCase):
    @pytest.mark.parametrize("args", [["--force"], []], ids=["force", "background"])
    def test_lock_held_by_another_process_exits_0_without_sending(self, args):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        res = self.sb.run("upload", *args)
        assert res.code == 0, res
        assert ("upload already in progress" in res.err) == bool(args)
        assert res.out == ""
        assert stub.requests == []
        assert not self.sb.state_path.exists()
        assert kwdb.sent_map(self.sb.db) == {1: None, 2: None}

    def test_consent_check_comes_before_the_lock(self):
        self.seed(1, consent=False)
        held = HoldLock(self.sb)
        self.addCleanup(held.release)
        assert self.sb.run("upload", "--force").code == 3


class TestSpacing(CliCase):
    @pytest.mark.parametrize(
        "age, failures, cfg, sends",
        [
            (3599, 0, {}, False),
            (3600, 0, {}, True),
            (14399, 3, {}, False),
            (14400, 3, {}, True),
            (86399, 30, {}, False),
            (86400, 30, {}, True),
            (3599, 5, {"upload_interval": 100}, False),
            (3600, 5, {"upload_interval": 100}, True),
            (59, 0, {"upload_backoff": 60}, False),
            (60, 0, {"upload_backoff": 60}, True),
        ],
        ids=[
            "backoff-1s-short",
            "backoff-boundary",
            "doubled-3-failures-1s-short",
            "doubled-3-failures-boundary",
            "capped-at-interval-1s-short",
            "capped-at-interval-boundary",
            "cap-never-below-backoff-1s-short",
            "cap-never-below-backoff-boundary",
            "custom-backoff-1s-short",
            "custom-backoff-boundary",
        ],
    )
    def test_background_upload_runs_only_once_the_gap_has_elapsed(
        self, age, failures, cfg, sends
    ):
        stub = self.serve()
        self.seed(2, endpoint=stub.url, **cfg)
        self.sb.write_state(
            last_attempt_at=NOW - age,
            last_success_at=0,
            consecutive_failures=failures,
            last_status="ok",
        )
        before = self.snapshot()
        res = self.sb.run("upload")
        assert res.code == 0, res
        if sends:
            assert len(stub.posts()) == 1
            assert self.sb.read_state()["last_attempt_at"] == str(NOW)
        else:
            assert stub.requests == []
            self.assert_no_side_effects(before)

    def test_force_ignores_spacing(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        self.sb.write_state(last_attempt_at=NOW - 1)
        assert self.sb.run("upload", "--force").code == 0
        assert len(stub.posts()) == 1


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
        assert otlp.attrs(recs[1])["kw.timebox.description"] == {"stringValue": "f�o"}

    def test_row_with_invalid_utf8_in_a_core_column_is_marked_and_not_resent(self):
        # The cap turns an endless re-POST loop into a fast, visible failure.
        stub = self.serve(
            respond=lambda req: (200 if req.index < 4 else 503, {}, b"{}")
        )
        self.seed(2, endpoint=stub.url, batch_size=1)
        kwdb.sql(
            self.sb.db,
            "UPDATE event SET date = CAST(x'32303236ff' AS TEXT) WHERE id = 1",
        )
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW}
        assert len(stub.posts()) == 2


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

    def test_sqlite_is_not_locked_while_the_request_is_in_flight(self):
        probe = []

        def before(req):
            kwdb.sql(
                self.sb.db,
                "INSERT INTO event(date, time) VALUES ('2030-01-01','00:00:00')",
            )
            probe.append(True)

        stub = self.serve(before=before)
        self.seed(2, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0
        assert stub.errors == []
        assert probe == [True]


class TestMidRunConsent(CliCase):
    @pytest.mark.parametrize(
        "new_config",
        [
            "telemetry_consent=no\ninstance_id=\nconsented_endpoint=\nendpoint={url}\n",
            (
                "telemetry_consent=yes\ninstance_id=i\nconsented_endpoint={url}\n"
                "endpoint=http://other.test/x\n"
            ),
        ],
        ids=["consent-withdrawn", "endpoint-edited"],
    )
    def test_consent_lost_between_batches_exits_3_and_restores_failures(
        self, new_config
    ):
        def change(req):
            if req.index == 0:
                self.sb.write_config(new_config.format(url=stub.url))

        stub = self.serve(before=change)
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
        assert self.sb.read_state() == {
            "last_attempt_at": str(NOW),
            "last_success_at": "111",
            "consecutive_failures": "2",
            "last_status": "http-503",
        }


class TestRowIdentity(CliCase):
    def test_rows_changed_while_in_flight_are_marked_only_when_still_the_same_row(self):
        def change(req):
            if req.index == 0:
                kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)  # another run sent it
                kwdb.set_sent_at(self.sb.db, 2, -1)  # excluded meanwhile
                kwdb.sql(self.sb.db, "DELETE FROM statistics_report WHERE id = 4")
                assert (
                    kwdb.add_executed(self.sb.db, date="2031-12-31", time="23:59:59")
                    == 4
                )

        stub = self.serve(before=change)
        self.seed(4, endpoint=stub.url)
        kwdb.sql(self.sb.db, "UPDATE event SET time = NULL WHERE id = 3")
        assert self.sb.run("upload", "--force").code == 0
        assert stub.errors == []
        assert kwdb.sent_map(self.sb.db) == {
            1: 1_700_000_000,
            2: NOW,
            3: NOW,  # a NULL time still matches itself
            4: None,
        }


class TestDeadline(CliCase):
    @pytest.mark.parametrize(
        "codes, batches, status, failures, committed",
        [
            ([200, "trickle"], 1, "partial", "0", {1: NOW, 2: None}),
            (["trickle"], 0, "error:timeout", "2", {1: None, 2: None}),
        ],
        ids=["after-progress-is-partial", "without-progress-is-a-timeout"],
    )
    def test_deadline_exits_5_and_records_partial_progress_or_timeout(
        self, monkeypatch, codes, batches, status, failures, committed
    ):
        # A shortened deadline keeps this fast; the next test pins the real 60 s.
        monkeypatch.setattr(commands, "UPLOAD_DEADLINE_SECONDS", 1)
        stub = self.serve(codes=codes)
        self.seed(2, endpoint=stub.url, batch_size=1)
        self.sb.write_state(consecutive_failures=1, last_success_at=42)
        code, _, err = self.call(["upload", "--force"])
        assert code == 5, err
        assert "deadline" in err
        assert kwdb.sent_map(self.sb.db) == committed
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == (
            status,
            failures,
        )
        assert (state["last_success_at"], state["last_attempt_at"]) == ("42", str(NOW))

    @pytest.mark.slow
    def test_real_deadline_is_60_seconds(self):
        stub = self.serve(codes=["trickle"])
        self.seed(2, endpoint=stub.url, batch_size=1)
        res = self.sb.run("upload", "--force", timeout=85)
        assert res.code == 5, res
        assert self.sb.read_state()["last_status"] == "error:timeout"


def _ids(req):
    return [
        int(otlp.attrs(r)["kw.event.id"]["intValue"]) for r in otlp.records(req.json())
    ]


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
        poison = Poison({3}, code=code)
        stub = self.serve(respond=poison)
        self.seed(5, endpoint=stub.url)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert kwdb.sent_map(self.sb.db) == {1: NOW, 2: NOW, 3: None, 4: NOW, 5: NOW}
        assert sorted(poison.accepted) == [1, 2, 4, 5], "each good row accepted once"
        assert self.sb.read_state()["last_status"] == f"http-{code}"

    @pytest.mark.parametrize("code", [408, 503])
    def test_transient_codes_are_not_bisected(self, code):
        stub = self.serve(codes=[code])
        self.seed(4, endpoint=stub.url)
        res = self.sb.run("upload", "--force")
        assert res.code == 5, res
        assert len(stub.posts()) == 1
        assert set(kwdb.sent_map(self.sb.db).values()) == {None}


class TestPoisonBatchLimits(CliCase):
    """D41: only 400/413/422 blame the rows; skips are capped; partial progress is recorded."""

    @pytest.mark.parametrize("code", [401, 404, 415])
    def test_endpoint_level_4xx_stops_at_the_first_failure(self, code):
        stub = self.serve(codes=[code])
        self.seed(0, endpoint=stub.url)
        _bulk(self.sb.db, 3000)
        res = self.sb.run("upload", "--force", timeout=15)
        assert res.code == 5, res
        assert len(stub.posts()) <= 2
        assert self.sb.read_state()["last_status"] == f"http-{code}"
        assert self.sb.read_state()["consecutive_failures"] == "1"
        assert _marked(self.sb.db) == []
        assert (
            kwdb.sql(self.sb.db, "SELECT count(*) FROM event WHERE sent_at IS NULL")[0][
                0
            ]
            == 3000
        )

    @pytest.mark.parametrize("bad_count", [20, 30])
    def test_the_run_stops_at_the_20th_skipped_row(self, bad_count):
        every_third = list(range(3, 91, 3))
        bad = set(every_third[:bad_count])
        poison = Poison(bad)
        stub = self.serve(respond=poison)
        self.seed(0, endpoint=stub.url)
        _bulk(self.sb.db, 100)
        res = self.sb.run("upload", "--force", timeout=60)
        assert res.code == 5, res
        # Rows after the 20th bad row (id 60) are left for the next run, even when no 21st bad row exists.
        assert _marked(self.sb.db) == [i for i in range(1, 60) if i not in bad]
        assert len(poison.accepted) == len(set(poison.accepted))
        assert len([r for r in poison.rejected if len(r) == 1]) == 20
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

    def test_5xx_while_bisecting_keeps_accepted_rows_and_reports_the_transient_status(
        self,
    ):
        poison = Poison({3}, transient={4})
        stub = self.serve(respond=poison)
        self.seed(5, endpoint=stub.url)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        assert _marked(self.sb.db) == sorted(poison.accepted) == [1, 2]
        assert self.sb.read_state()["last_status"] == "http-503"

    @pytest.mark.parametrize(
        "respond, status, failures",
        [
            (Poison({3}), "http-413", "0"),
            (lambda req: (400, {}, b"{}"), "http-400", "4"),
        ],
        ids=["accepted-rows-are-partial-progress", "nothing-accepted-keeps-counting"],
    )
    def test_failure_count_resets_only_when_some_rows_were_accepted(
        self, respond, status, failures
    ):
        stub = self.serve(respond=respond)
        self.seed(5, endpoint=stub.url)
        self.sb.write_state(consecutive_failures=3, last_success_at=42)
        res = self.sb.run("upload", "--force", timeout=30)
        assert res.code == 5, res
        state = self.sb.read_state()
        assert (state["last_status"], state["consecutive_failures"]) == (
            status,
            failures,
        )
        # Same as the D30 deadline path: partial progress is not a success.
        assert state["last_success_at"] == "42"
        assert state["last_attempt_at"] == str(NOW)


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
        ],
    )
    def test_future_time_does_not_block_a_due_upload(self, fields):
        stub = self.serve()
        self.seed(3, endpoint=stub.url)
        state = {
            "last_attempt_at": NOW - self.TEN_DAYS,
            "last_success_at": NOW - self.TEN_DAYS,
        }
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
            last_attempt_at=future,
            last_success_at=future,
            consecutive_failures=0,
            last_status="ok",
        )
        assert self.sb.run("upload").code == 5
        after = self.sb.read_state()
        assert int(after["last_attempt_at"]) <= NOW
        assert int(after["last_success_at"]) <= NOW
