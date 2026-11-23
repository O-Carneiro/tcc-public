"""§1.1: parsing and rewriting of telemetry.config / telemetry.state, observed through
`consent` and `config`."""

import os

import pytest

from helpers.cli_case import CliCase
from helpers import kvfile

DEFAULTS = {
    "upload_interval": "86400",
    "upload_backoff": "3600",
    "batch_size": "500",
    "endpoint": "http://localhost:4318/v1/logs",
}
YES = (
    "telemetry_consent=yes\ninstance_id=abc\n" "consented_endpoint=http://h/x\nendpoint=http://h/x"
)


def listing(res):
    return kvfile.parse_kv(res.out)


class TestParsing(CliCase):
    def test_missing_files_mean_defaults(self):
        res = self.sb.run("config")
        assert res.code == 0, res
        out = listing(res)
        for key, value in DEFAULTS.items():
            assert out[key] == value
        assert out["last_attempt_at"] == "0"
        assert out["consecutive_failures"] == "0"

    def test_last_line_without_newline_counts(self):
        self.sb.write_config(YES)
        assert self.sb.run("consent").out.strip() == "yes"

    def test_crlf_line_endings(self):
        self.sb.write_config(YES.replace("\n", "\r\n") + "\r\n")
        assert self.sb.run("consent").out.strip() == "yes"

    def test_crlf_on_numeric_value(self):
        self.sb.write_config("batch_size=7\r\nupload_backoff=9")
        out = listing(self.sb.run("config"))
        assert (out["batch_size"], out["upload_backoff"]) == ("7", "9")

    def test_duplicate_keys_last_wins(self):
        self.sb.write_config("batch_size=5\nbatch_size=7\n")
        assert listing(self.sb.run("config"))["batch_size"] == "7"

    def test_duplicate_keys_last_wins_even_if_last_is_invalid(self):
        self.sb.write_config("batch_size=5\nbatch_size=08\n")
        assert listing(self.sb.run("config"))["batch_size"] == "500"

    def test_comments_and_blank_lines_are_ignored(self):
        self.sb.write_config("# batch_size=9\n\n   \n  # upload_backoff=9\nupload_interval=77\n")
        out = listing(self.sb.run("config"))
        assert (out["batch_size"], out["upload_backoff"], out["upload_interval"]) == (
            "500",
            "3600",
            "77",
        )

    def test_inline_comments_are_not_supported(self):
        self.sb.write_config("batch_size=9 # nine\n")
        assert listing(self.sb.run("config"))["batch_size"] == "500"

    def test_whitespace_around_key_and_value_is_trimmed(self):
        self.sb.write_config("  batch_size \t=\t 12  \n")
        assert listing(self.sb.run("config"))["batch_size"] == "12"

    def test_value_is_split_on_first_equals_only(self):
        url = "http://h/x?a=b&c=d"
        self.sb.write_config(
            "telemetry_consent=yes\ninstance_id=i\n"
            "consented_endpoint=%s\nendpoint=%s\n" % (url, url)
        )
        assert self.sb.run("consent").out.strip() == "yes"
        assert listing(self.sb.run("config"))["endpoint"] == url

    @pytest.mark.parametrize(
        "bad",
        [
            "08",
            "-1",
            "+5",
            "0",
            "5001",
            "1e3",
            "1.5",
            "",
            "abc",
            "1234567890123456789",
            "99999999999999999999",
        ],
    )
    def test_invalid_batch_size_falls_back_to_default(self, bad):
        self.sb.write_config("batch_size=%s\n" % bad)
        assert listing(self.sb.run("config"))["batch_size"] == "500"

    @pytest.mark.parametrize("good", ["1", "5000", "10"])
    def test_valid_batch_size_boundaries(self, good):
        self.sb.write_config("batch_size=%s\n" % good)
        assert listing(self.sb.run("config"))["batch_size"] == good

    @pytest.mark.parametrize(
        "key, raw, expected",
        [
            ("upload_interval", "0", "86400"),
            ("upload_interval", "31536001", "86400"),
            ("upload_interval", "31536000", "31536000"),
            ("upload_interval", "1", "1"),
            ("upload_backoff", "0", "3600"),
            ("upload_backoff", "31536001", "3600"),
            ("upload_backoff", "007", "3600"),
            ("upload_backoff", "31536000", "31536000"),
            ("upload_backoff", "999999999999999999", "3600"),
        ],
    )
    def test_interval_and_backoff_ranges(self, key, raw, expected):
        self.sb.write_config("%s=%s\n" % (key, raw))
        assert listing(self.sb.run("config"))[key] == expected

    @pytest.mark.parametrize(
        "bad", ["ftp://host/x", "localhost:4318", "http://", "https:///path", "", "x"]
    )
    def test_invalid_endpoint_falls_back_to_default(self, bad):
        self.sb.write_config("endpoint=%s\n" % bad)
        assert listing(self.sb.run("config"))["endpoint"] == DEFAULTS["endpoint"]

    def test_https_endpoint_is_accepted(self):
        self.sb.write_config("endpoint=https://collector.example.org:4318/v1/logs\n")
        assert (
            listing(self.sb.run("config"))["endpoint"]
            == "https://collector.example.org:4318/v1/logs"
        )

    @pytest.mark.parametrize("raw", ["YES", "Yes", "true", "1", "maybe", ""])
    def test_values_are_case_sensitive(self, raw):
        self.sb.write_config(YES.replace("telemetry_consent=yes", "telemetry_consent=%s" % raw))
        assert self.sb.run("consent").out.strip() == "ask"

    def test_state_integers_follow_the_same_grammar(self):
        self.sb.write_state(
            last_attempt_at="08",
            last_success_at="-5",
            consecutive_failures="1234567890123456789",
            last_status="ok",
        )
        out = listing(self.sb.run("config"))
        assert ((out["last_attempt_at"], out["last_success_at"], out["consecutive_failures"])) == (
            "0",
            "0",
            "0",
        )
        assert out["last_status"] == "ok"

    def test_state_unknown_status_falls_back_to_empty(self):
        self.sb.write_state(last_status="exploded")
        assert listing(self.sb.run("config"))["last_status"] == ""

    @pytest.mark.parametrize(
        "status",
        [
            "ok",
            "partial",
            "http-503",
            "error:timeout",
            "error:connection",
            "error:schema",
            "error:nodb",
            "error:internal",
        ],
    )
    def test_state_accepts_every_documented_status(self, status):
        self.sb.write_state(last_status=status)
        assert listing(self.sb.run("config"))["last_status"] == status

    def test_state_last_line_without_newline_and_crlf(self):
        self.sb.write_state()
        self.sb.state_path.write_bytes(b"last_attempt_at=12\r\nconsecutive_failures=3")
        out = listing(self.sb.run("config"))
        assert (out["last_attempt_at"], out["consecutive_failures"]) == ("12", "3")


class TestRewrite(CliCase):
    def test_unknown_lines_and_comments_survive_and_keys_stay_in_place(self):
        self.sb.write_config("# keep me\nfoo=bar\nbatch_size=5\n\nzzz = 1\nupload_backoff=60\n")
        res = self.sb.run("config", "--set-batch-size", "9")
        assert res.code == 0, res
        lines = self.sb.read_config_text().splitlines()
        assert lines[:6] == (
            ["# keep me", "foo=bar", "batch_size=9", "", "zzz = 1", "upload_backoff=60"]
        )
        assert self.sb.read_config()["batch_size"] == "9"

    def test_missing_keys_are_appended(self):
        self.sb.write_config("# header\nfoo=bar\n")
        self.sb.run("config", "--set-upload-interval", "100")
        lines = self.sb.read_config_text().splitlines()
        assert lines[:2] == ["# header", "foo=bar"]
        assert "upload_interval=100" in lines

    def test_append_when_last_line_has_no_newline(self):
        self.sb.write_config("foo=bar")
        self.sb.run("config", "--set-batch-size", "3")
        cfg = self.sb.read_config()
        assert (cfg["foo"], cfg["batch_size"]) == ("bar", "3")

    def test_rewrite_creates_missing_parent_directories(self):
        assert not self.sb.config_path.parent.exists()
        res = self.sb.run("config", "--set-batch-size", "3")
        assert res.code == 0, res
        assert self.sb.read_config()["batch_size"] == "3"
        assert self.sb.state_dir.is_dir()

    def test_write_is_atomic_replace_not_in_place(self):
        self.sb.write_config("batch_size=5\n")
        alias = self.sb.root / "alias.config"
        os.link(self.sb.config_path, alias)
        self.sb.run("config", "--set-batch-size", "9")
        assert alias.read_text() == "batch_size=5\n"
        assert self.sb.read_config()["batch_size"] == "9"

    def test_no_temp_files_are_left_behind(self):
        self.sb.write_config("batch_size=5\n")
        self.sb.run("config", "--set-batch-size", "9")
        assert sorted(p.name for p in self.sb.config_path.parent.iterdir()) == ["telemetry.config"]

    def test_consent_rewrite_keeps_unknown_lines_too(self):
        self.sb.write_config("# note\nfoo=bar\nendpoint=http://h/x\n")
        assert self.sb.run("consent", "--set", "yes").code == 0
        text = self.sb.read_config_text()
        assert "# note" in text
        assert "foo=bar" in text
        assert self.sb.read_config()["consented_endpoint"] == "http://h/x"
