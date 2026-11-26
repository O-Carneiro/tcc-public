"""§1.1: parsing and rewriting of telemetry.config / telemetry.state, observed through
`consent` and `config`."""

import os

import pytest

from helpers.cli_case import CliCase
from helpers import kvfile
from helpers.constants import CONFIG_KEYS

DEFAULTS = {
    "upload_interval": "86400",
    "upload_backoff": "3600",
    "batch_size": "500",
    "endpoint": "http://localhost:4318/v1/logs",
    "exclude_fields": "",
}


def listing(res):
    return kvfile.parse_kv(res.out)


class TestConfigGrammar(CliCase):
    def test_missing_files_mean_defaults(self):
        res = self.sb.run("config")
        assert res.code == 0, res
        out = listing(res)
        assert {k: out[k] for k in DEFAULTS} == DEFAULTS
        assert (out["last_attempt_at"], out["consecutive_failures"]) == ("0", "0")

    def test_every_construct_of_the_grammar_in_one_file(self):
        # CRLF, comments, blank lines, padding, duplicate keys, inline "#", "=" inside a value
        # and a last line without newline.
        self.sb.write_config(
            "# batch_size=9\r\n"
            "\n"
            "   \n"
            "  # upload_backoff=9\n"
            "  upload_interval \t=\t 77  \r\n"
            "batch_size=5\n"
            "batch_size=7\r\n"
            "upload_backoff=9 # nine\n"
            "telemetry_consent=yes\n"
            "instance_id=i\n"
            "consented_endpoint=http://h/x?a=b&c=d\n"
            "endpoint=http://h/x?a=b&c=d"
        )
        out = listing(self.sb.run("config"))
        assert {k: out[k] for k in CONFIG_KEYS} == {
            "telemetry_consent": "yes",
            "instance_id": "i",
            "consented_endpoint": "http://h/x?a=b&c=d",
            "endpoint": "http://h/x?a=b&c=d",
            "upload_interval": "77",
            "upload_backoff": "3600",
            "batch_size": "7",
            "exclude_fields": "",
        }
        assert self.sb.run("consent").out.strip() == "yes"

    @pytest.mark.parametrize(
        "key, raw, expected",
        [
            ("batch_size", "1", "1"),
            ("batch_size", "5000", "5000"),
            ("batch_size", "5001", "500"),
            ("batch_size", "0", "500"),
            ("batch_size", "08", "500"),
            ("batch_size", "abc", "500"),
            ("batch_size", "", "500"),
            ("upload_interval", "1", "1"),
            ("upload_interval", "31536000", "31536000"),
            ("upload_interval", "31536001", "86400"),
            ("upload_backoff", "31536000", "31536000"),
            ("upload_backoff", "31536001", "3600"),
            ("upload_backoff", "007", "3600"),
        ],
    )
    def test_numeric_values_follow_the_integer_grammar_and_range(
        self, key, raw, expected
    ):
        self.sb.write_config(f"{key}={raw}\n")
        assert listing(self.sb.run("config"))[key] == expected

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("kw.event.date", "kw.event.date"),
            (" a.b ,, c.d , a.b ,", "a.b,c.d"),
            (",,,", ""),
            ("", ""),
            ("not.a.key,kw.event.date", "not.a.key,kw.event.date"),
        ],
        ids=[
            "single",
            "spaces-empties-duplicates",
            "only-empties",
            "empty",
            "unknown-key",
        ],
    )
    def test_exclude_fields_is_a_normalised_comma_list_printed_by_config(
        self, raw, expected
    ):
        self.sb.write_config(f"exclude_fields={raw}\n")
        out = listing(self.sb.run("config"))
        assert out["exclude_fields"] == expected

    @pytest.mark.parametrize(
        "raw, expected",
        [
            (
                "https://collector.example.org:4318/v1/logs",
                "https://collector.example.org:4318/v1/logs",
            ),
            ("ftp://host/x", DEFAULTS["endpoint"]),
            ("http:///v1/logs", DEFAULTS["endpoint"]),
            ("", DEFAULTS["endpoint"]),
        ],
    )
    def test_endpoint_must_be_an_http_or_https_url(self, raw, expected):
        self.sb.write_config(f"endpoint={raw}\n")
        assert listing(self.sb.run("config"))["endpoint"] == expected


class TestStateGrammar(CliCase):
    def test_integers_follow_the_config_grammar_and_unknown_status_is_empty(self):
        self.seed(1)
        self.sb.write_state()
        self.sb.state_path.write_bytes(
            b"last_attempt_at=-5\r\nlast_success_at=999999999999999999\n"
            b"last_status=exploded\r\nconsecutive_failures=+3"
        )
        res = self.sb.run("config")
        out = listing(res)
        assert (
            out["last_attempt_at"],
            out["last_success_at"],
            out["consecutive_failures"],
            out["last_status"],
        ) == ("0", "999999999999999999", "0", "")
        # an 18-digit time is valid state and must not crash the summary
        assert self.sb.run("info").code == 0

    def test_every_documented_status_is_accepted(self):
        statuses = [
            "ok",
            "partial",
            "http-503",
            "error:timeout",
            "error:connection",
            "error:schema",
            "error:nodb",
            "error:internal",
        ]
        got = []
        for status in statuses:
            self.sb.write_state(last_status=status)
            got.append(listing(self.sb.run("config"))["last_status"])
        assert got == statuses


class TestRewrite(CliCase):
    def test_rewrite_keeps_unknown_lines_comments_and_key_positions(self):
        self.sb.write_config(
            "# keep me\nfoo=bar\nbatch_size=5\n\nzzz = 1\nupload_backoff=60"
        )
        alias = self.sb.root / "alias.config"
        os.link(self.sb.config_path, alias)
        res = self.sb.run(
            "config", "--set-batch-size", "9", "--set-upload-interval", "100"
        )
        assert res.code == 0, res
        assert self.sb.read_config_text() == (
            "# keep me\nfoo=bar\nbatch_size=9\n\nzzz = 1\nupload_backoff=60\nupload_interval=100\n"
        )
        # A hard link to the old inode proves the file was replaced, not edited in place.
        assert alias.read_text().startswith("# keep me\nfoo=bar\nbatch_size=5\n")
        assert [p.name for p in self.sb.config_path.parent.iterdir()] == [
            "telemetry.config"
        ]

    def test_consent_rewrite_keeps_unknown_lines_and_exclude_fields(self):
        self.sb.write_config(
            "# note\nfoo=bar\nendpoint=http://h/x\nexclude_fields=a.b, c.d\n"
        )
        assert self.sb.run("consent", "--set", "yes").code == 0
        lines = self.sb.read_config_text().splitlines()
        assert lines[:4] == [
            "# note",
            "foo=bar",
            "endpoint=http://h/x",
            "exclude_fields=a.b, c.d",
        ]
        assert self.sb.run("config", "--set-batch-size", "9").code == 0
        assert "exclude_fields=a.b, c.d" in self.sb.read_config_text().splitlines()
        assert listing(self.sb.run("config"))["exclude_fields"] == "a.b,c.d"
        assert self.sb.read_config()["consented_endpoint"] == "http://h/x"
