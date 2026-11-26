"""§3 exit-code table, plus in-process `main()` and `python3 -m kw_tele` parity."""

import contextlib
import json
import subprocess

import pytest

from helpers.cli_case import CliCase
from helpers import constants, kwdb
from helpers.constants import ROOT

EP = "http://collector.test:4318/v1/logs"


class TestExitCodes(CliCase):
    @pytest.mark.parametrize(
        "args",
        [
            [],
            ["frobnicate"],
            ["upload", "--force", "--dry-run"],
            ["info", "--export", "--pending"],
            ["consent", "--set"],
            ["consent", "--set", "no", "--include-history"],
            ["consent", "--set", "maybe"],
            ["consent", "--include-history"],
        ],
        ids=lambda a: " ".join(a) or "no-arguments",
    )
    def test_2_usage_errors_change_nothing(self, args):
        self.sb.make_db()
        res = self.sb.run(*args)
        assert res.code == 2, res
        assert not self.sb.config_path.exists()

    def test_3_refused_without_consent(self):
        self.seed(1, consent=False)
        assert self.sb.run("upload", "--force").code == 3

    @pytest.mark.parametrize(
        "args",
        [
            ["info"],
            ["info", "--pending"],
            ["info", "--export"],
            ["upload", "--dry-run"],
            ["upload", "--force"],
        ],
        ids=" ".join,
    )
    @pytest.mark.parametrize(
        "db_state, code",
        [("no-db", 6), ("no-sent_at-column", 4), ("not-a-database", 6)],
    )
    def test_4_and_6_database_problems(self, args, db_state, code):
        if db_state == "no-sent_at-column":
            self.sb.make_db(with_sent_at=False)
        elif db_state == "not-a-database":
            self.sb.data_dir.mkdir(parents=True)
            self.sb.db.write_bytes(b"this is not a sqlite database\n" * 20)
        self.sb.configure(endpoint=EP)
        res = self.sb.run(*args)
        assert "Traceback" not in res.err, res
        assert res.code == code, res

    def test_5_failed_upload(self):
        stub = self.serve(codes=[503])
        self.seed(1, endpoint=stub.url)
        assert self.sb.run("upload", "--force").code == 5

    @pytest.mark.parametrize(
        "args", [["upload", "--force"], ["upload"]], ids=["force", "background"]
    )
    def test_1_internal_error_is_not_a_traceback_exit(self, args):
        stub = self.serve()
        self.seed(1, endpoint=stub.url)
        self.sb.state_dir.write_text("not a directory")
        res = self.sb.run(*args)
        assert res.code == 1, res
        assert "Traceback" not in res.err
        if args == ["upload"]:
            assert (res.out, res.err) == ("", "")
        assert stub.requests == []
        assert kwdb.sent_map(self.sb.db) == {1: None}

    @pytest.mark.parametrize(
        "args, consent",
        [
            (["upload", "--force"], True),
            (["consent", "--set", "yes"], False),
        ],
        ids=["upload", "consent-set-yes"],
    )
    def test_1_locked_database_is_reported_not_a_traceback(self, args, consent):
        """D31: a database kw holds locked; kw_tele waits 10 s for the lock before giving up."""
        stub = self.serve()
        self.seed(2, endpoint=stub.url, consent=consent)
        with contextlib.closing(kwdb.connect(self.sb.db)) as holder:
            holder.execute("BEGIN EXCLUSIVE")
            res = self.sb.run(*args)
        assert "Traceback" not in res.err, res
        assert res.code == 1, res
        assert res.err.strip(), "the reason goes to stderr"
        assert stub.requests == []
        assert set(kwdb.sent_map(self.sb.db).values()) == {None}
        assert self.sb.config_path.exists() == consent


class TestZipapp(CliCase):
    def test_built_archive_runs_without_the_source_tree(self, tmp_path):
        out = tmp_path / "kw_tele"
        build = subprocess.run(
            ["sh", str(ROOT / "build.sh"), str(out)],
            capture_output=True,
            timeout=60,
            check=False,
        )
        assert build.returncode == 0, build
        env = self.sb.env()
        # With src/ on the path the test would exercise the source tree, not the archive.
        env.pop("PYTHONPATH")

        def run(*args):
            proc = subprocess.run(
                [str(out), *args],
                env=env,
                cwd=str(self.sb.root),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=60,
                check=False,
            )
            return proc.returncode, proc.stdout.decode().strip()

        assert run("bogus")[0] == 2
        assert run("upload", "--dry-run")[0] == 6
        self.seed(1, consent=False)
        assert run("upload", "--force")[0] == 3
        self.sb.configure()
        assert run("consent") == (0, "yes")


class TestInProcess(CliCase):
    def test_main_returns_an_int_for_each_documented_outcome(self):
        stub = self.serve()
        self.seed(2, endpoint=stub.url)
        env = self.sb.env()
        assert self.call(["consent"], env)[:2] == (0, "yes\n")
        assert self.call(["info", "--pending"], env)[:2] == (
            0,
            "pending_events=2\npending_since=2026-03-01\n",
        )
        assert self.call(["upload", "--dry-run"], env)[0] == 0
        assert self.call(["upload", "--force", "--dry-run"], env)[0] == 2
        code, out, _ = self.call(["upload", "--force"], env)
        assert (code, "sent 2 events" in out) == (0, True)
        assert kwdb.sent_map(self.sb.db) == {1: constants.NOW, 2: constants.NOW}

    def test_kw_tele_now_is_ignored_without_the_test_flag(self):
        self.seed(1)
        env = self.sb.env(now=1)
        env.pop("KW_TELE_TEST")
        code, out, _ = self.call(["upload", "--dry-run"], env)
        assert code == 0
        observed = json.loads(out)["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
        assert int(observed["observedTimeUnixNano"]) > 1_700_000_000 * 10**9
