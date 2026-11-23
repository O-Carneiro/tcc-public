"""§3 exit-code table, plus in-process `main()` and `python3 -m kw_tele` parity."""

import contextlib
import io
import os
from unittest import mock

import pytest

from helpers.cli_case import CliCase
from helpers import constants, kwdb, otlp


class TestExitCodes(CliCase):
    def test_0_success(self):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 0

    def test_0_nothing_pending(self):
        self.seed(0)
        assert self.sb.run("upload", "--force").code == 0

    def test_0_not_due(self):
        self.seed(1)
        self.sb.write_state(last_attempt_at=constants.NOW - 1)
        assert self.sb.run("upload").code == 0

    @pytest.mark.parametrize(
        "args",
        [
            [],
            ["frobnicate"],
            ["upload", "--nope"],
            ["consent", "--set"],
            ["upload", "--force", "--dry-run"],
            ["info", "--bogus"],
        ],
    )
    def test_2_usage_errors(self, args):
        assert self.sb.run(*args).code == 2

    def test_3_refused(self):
        self.seed(1, consent=False)
        assert self.sb.run("upload", "--force").code == 3

    def test_4_schema(self):
        self.sb.make_db(with_sent_at=False)
        self.sb.configure()
        assert self.sb.run("upload", "--force").code == 4
        assert self.sb.run("info", "--pending").code == 4

    def test_5_failed_upload(self):
        stub = self.serve(codes=[503])
        self.seed(1, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 5

    def test_6_no_db(self):
        self.sb.configure()
        assert self.sb.run("upload", "--force").code == 6
        assert self.sb.run("info", "--export").code == 6

    def test_1_internal_error_is_not_a_traceback_exit(self):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        self.sb.state_dir.write_text("not a directory")
        res = self.sb.run("upload", "--force")
        assert res.code == 1, res
        assert "Traceback" not in res.err


class TestInProcess(CliCase):
    def call(self, argv, env):
        out, err = io.StringIO(), io.StringIO()
        from kw_tele import cli

        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(
            out
        ), contextlib.redirect_stderr(err):
            try:
                code = cli.main(argv)
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue(), err.getvalue()

    def test_main_returns_an_int_for_each_documented_outcome(self):
        self.seed(2)
        env = self.sb.env()
        assert self.call(["consent"], env)[:2] == (0, "yes\n")
        code, out, _ = self.call(["info", "--pending"], env)
        assert (code, out) == (0, "pending_events=2\npending_since=2026-03-01\n")
        assert self.call(["upload", "--dry-run"], env)[0] == 0

    def test_main_usage_error_is_exit_2(self):
        code = self.call(["upload", "--force", "--dry-run"], self.sb.env())[0]
        assert code == 2

    def test_main_uploads_in_process(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        code, out, _ = self.call(["upload", "--force"], self.sb.env())
        assert code == 0
        assert "sent 2 events" in out
        assert kwdb.sent_map(self.sb.db) == {1: constants.NOW, 2: constants.NOW}

    def test_kw_tele_now_is_ignored_without_the_test_flag(self):
        self.seed(1)
        env = self.sb.env(now=1)
        env.pop("KW_TELE_TEST")
        code, out, _ = self.call(["upload", "--dry-run"], env)
        assert code == 0
        import json

        obs = otlp.records(json.loads(out))[0]["observedTimeUnixNano"]
        assert obs != str(10**9)
        assert int(obs) > 1_700_000_000 * 10**9


class TestModuleEntry(CliCase):
    def test_python_m_kw_tele_matches_main(self):
        self.seed(1)
        res = self.sb.run("info", "--pending")
        assert (res.code, res.out) == (0, "pending_events=1\npending_since=2026-03-01\n")
