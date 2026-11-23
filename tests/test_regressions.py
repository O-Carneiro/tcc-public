"""Bugs found in review; each test pins the contract the implementation broke."""

import json
import subprocess

import pytest

from helpers import kwdb, otlp
from helpers.cli_case import CliCase
from helpers.constants import ROOT


class TestZipapp(CliCase):
    @pytest.fixture(autouse=True)
    def _binary(self, tmp_path_factory):
        out = tmp_path_factory.mktemp("zipapp") / "kw_tele"
        build = subprocess.run(
            ["sh", str(ROOT / "build.sh"), str(out)], capture_output=True, timeout=60
        )
        assert build.returncode == 0, build
        self.binary = out

    def run_binary(self, *args):
        env = self.sb.env()
        # With src/ on the path the test would exercise the source tree, not the archive.
        env.pop("PYTHONPATH")
        proc = subprocess.run(
            [str(self.binary), *args],
            env=env,
            cwd=str(self.sb.root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
        )
        return proc.returncode, proc.stdout.decode(), proc.stderr.decode()

    def test_exit_0_consent_print(self):
        self.sb.configure()
        code, out, err = self.run_binary("consent")
        assert (code, out.strip()) == (0, "yes"), err

    def test_exit_2_bogus_subcommand(self):
        assert self.run_binary("bogus")[0] == 2

    def test_exit_6_dry_run_without_db(self):
        code, out, err = self.run_binary("upload", "--dry-run")
        assert code == 6, (out, err)

    def test_exit_3_upload_without_consent(self):
        self.seed(1, consent=False)
        code, out, err = self.run_binary("upload", "--force")
        assert code == 3, (out, err)


class TestUnreadableUtf8Row(CliCase):
    def test_row_with_invalid_utf8_date_is_marked_and_not_resent(self):
        # The cap turns an endless re-POST loop into a fast, visible failure.
        stub = self.serve(respond=lambda req: (200 if req.index < 4 else 503, {}, b"{}"))
        self.seed(2, endpoint=stub.url, batch_size=1)
        kwdb.sql(self.sb.db, "UPDATE event SET date = CAST(x'32303236ff' AS TEXT) WHERE id = 1")
        res = self.sb.run("upload", "--force")
        sent = kwdb.sent_map(self.sb.db)
        assert res.code == 0, (res, sent)
        assert all(v and v > 0 for v in sent.values()), sent
        assert len(stub.posts()) == 2
        marked = sum(1 for v in sent.values() if v and v > 0)
        assert res.out.strip() == "sent %d events to %s" % (marked, stub.url)

    def test_row_with_invalid_utf8_time_is_marked_and_not_resent(self):
        stub = self.serve(respond=lambda req: (200 if req.index < 4 else 503, {}, b"{}"))
        self.seed(1, endpoint=stub.url, batch_size=1)
        kwdb.sql(self.sb.db, "UPDATE event SET time = CAST(x'3035ff' AS TEXT) WHERE id = 1")
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        assert len(stub.posts()) == 1
        assert kwdb.sent_map(self.sb.db)[1] > 0


class TestUnrepresentableTimestamps(CliCase):
    @pytest.mark.parametrize("date", ["1960-01-01", "9999-12-31"])
    def test_out_of_fixed64_range_gives_zero_and_is_still_sent(self, date):
        stub = self.serve()
        self.seed(0, endpoint=stub.url)
        kwdb.add_executed(self.sb.db, date=date, time="12:00:00")
        res = self.sb.run("upload", "--force")
        assert res.code == 0, res
        recs = otlp.records(stub.posts()[0].json())
        assert [r["timeUnixNano"] for r in recs] == ["0"]
        assert kwdb.sent_map(self.sb.db)[1] > 0

    @pytest.mark.parametrize("date", ["1960-01-01", "9999-12-31"])
    def test_dry_run_agrees(self, date):
        self.seed(0)
        kwdb.add_executed(self.sb.db, date=date, time="12:00:00")
        assert otlp.records(self.dry_run())[0]["timeUnixNano"] == "0"


class TestMalformedEndpoint(CliCase):
    def test_empty_dns_label_is_a_connection_failure_not_a_crash(self):
        self.seed(1, endpoint="http://a..b/")
        res = self.sb.run("upload", "--force")
        assert "Traceback" not in res.err, res
        assert res.code == 5, res
        state = self.sb.read_state()
        assert state["last_status"] == "error:connection"
        assert state["consecutive_failures"] == "1"
        assert kwdb.sent_map(self.sb.db)[1] is None


class TestHugeStateTimes(CliCase):
    @pytest.mark.parametrize("key", ["last_success_at", "last_attempt_at"])
    def test_info_survives_an_18_digit_time(self, key):
        self.seed(1)
        self.sb.write_state(**{key: "999999999999999999"})
        res = self.sb.run("info")
        assert "Traceback" not in res.err, res
        assert res.code == 0, res


class TestNotADatabase(CliCase):
    @pytest.fixture(autouse=True)
    def _garbage_db(self):
        self.sb.data_dir.mkdir(parents=True)
        self.sb.db.write_bytes(b"this is not a sqlite database\n" * 20)
        self.sb.configure()

    def test_config_reports_unknown_counts(self):
        res = self.sb.run("config")
        assert "Traceback" not in res.err, res
        assert res.code == 0, res
        out = dict(line.split("=", 1) for line in res.out.splitlines())
        assert [out[k] for k in ("pending_events", "sent_events", "excluded_events")] == [
            "unknown"
        ] * 3

    @pytest.mark.parametrize(
        "args",
        [["info"], ["info", "--pending"], ["info", "--export"], ["upload", "--dry-run"]],
    )
    def test_read_only_commands_exit_6_without_a_traceback(self, args):
        res = self.sb.run(*args)
        assert "Traceback" not in res.err, res
        assert res.code == 6, res

    def test_upload_force_fails_cleanly(self):
        res = self.sb.run("upload", "--force")
        assert "Traceback" not in res.err, res
        assert res.code == 6, res
        assert json.dumps(self.sb.read_state()["last_status"]) == '"error:nodb"'
