"""§3 `config`."""

import pytest

from helpers.cli_case import CliCase
from helpers import constants, kvfile, kwdb
from helpers.constants import CONFIG_KEYS, COUNT_KEYS, STATE_KEYS


def keys_in_order(out):
    return [line.split("=", 1)[0] for line in out.splitlines()]


class TestListing(CliCase):
    def test_full_listing_in_documented_order_with_values(self):
        self.seed(4, endpoint="http://h.test/x", batch_size=7)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        self.sb.write_state(last_attempt_at=5, last_status="ok")
        res = self.sb.run("config")
        assert res.code == 0, res
        assert keys_in_order(res.out) == CONFIG_KEYS + STATE_KEYS + COUNT_KEYS
        assert kvfile.parse_kv(res.out) == {
            "telemetry_consent": "yes",
            "instance_id": constants.INSTANCE,
            "consented_endpoint": "http://h.test/x",
            "endpoint": "http://h.test/x",
            "upload_interval": "86400",
            "upload_backoff": "3600",
            "batch_size": "7",
            "exclude_fields": "",
            "last_attempt_at": "5",
            "last_success_at": "0",
            "consecutive_failures": "0",
            "last_status": "ok",
            "pending_events": "2",
            "sent_events": "1",
            "excluded_events": "1",
        }

    @pytest.mark.parametrize(
        "with_db", [False, True], ids=["no-db", "no-sent_at-column"]
    )
    def test_counts_are_unknown_and_consent_is_the_stored_value_not_the_effective_one(
        self, with_db
    ):
        if with_db:
            self.sb.make_db(with_sent_at=False)
        self.sb.write_config(
            "telemetry_consent=yes\nendpoint=http://a.test/x\n"
            "consented_endpoint=http://b.test/x\ninstance_id=i\n"
        )
        res = self.sb.run("config")
        assert res.code == 0, res
        assert keys_in_order(res.out) == CONFIG_KEYS + STATE_KEYS + COUNT_KEYS
        assert kvfile.parse_kv(res.out) == {
            "telemetry_consent": "yes",
            "instance_id": "i",
            "consented_endpoint": "http://b.test/x",
            "endpoint": "http://a.test/x",
            "upload_interval": "86400",
            "upload_backoff": "3600",
            "batch_size": "500",
            "exclude_fields": "",
            "last_attempt_at": "0",
            "last_success_at": "0",
            "consecutive_failures": "0",
            "last_status": "",
            "pending_events": "unknown",
            "sent_events": "unknown",
            "excluded_events": "unknown",
        }


class TestSet(CliCase):
    def test_valid_sets_are_persisted_echoed_and_leave_consent_alone(self):
        self.seed(1)
        res = self.sb.run(
            "config",
            "--set-upload-interval",
            "600",
            "--set-upload-backoff",
            "30",
            "--set-batch-size",
            "25",
        )
        assert res.code == 0, res
        expected = {
            "upload_interval": "600",
            "upload_backoff": "30",
            "batch_size": "25",
        }
        out = kvfile.parse_kv(res.out)
        cfg = self.sb.read_config()
        assert {k: out[k] for k in expected} == expected
        assert {k: cfg[k] for k in expected} == expected
        assert cfg["instance_id"] == constants.INSTANCE
        assert cfg["consented_endpoint"] == cfg["endpoint"]
        assert self.sb.run("consent").out.strip() == "yes"

    @pytest.mark.parametrize(
        "flag, key, value",
        [
            ("--set-upload-interval", "upload_interval", "1"),
            ("--set-upload-backoff", "upload_backoff", "31536000"),
            ("--set-batch-size", "batch_size", "5000"),
        ],
    )
    def test_each_flag_alone_accepts_its_upper_or_lower_boundary(
        self, flag, key, value
    ):
        res = self.sb.run("config", flag, value)
        assert res.code == 0, res
        assert kvfile.parse_kv(res.out)[key] == value
        assert self.sb.read_config()[key] == value

    @pytest.mark.parametrize(
        "args",
        [
            ["--set-batch-size", "0"],
            ["--set-batch-size", "5001"],
            ["--set-batch-size", "08"],
            ["--set-upload-interval", "31536001"],
            ["--set-upload-interval", "0"],
            ["--set-upload-backoff", "0"],
            ["--set-upload-interval", "600", "--set-batch-size", "0"],
            ["--set-endpoint", "http://evil.test/x"],
            ["--bogus"],
        ],
        ids=lambda a: " ".join(a) or "empty",
    )
    def test_invalid_values_and_unknown_flags_exit_2_and_write_nothing(self, args):
        self.seed(1, batch_size=11)
        before = self.snapshot()
        res = self.sb.run("config", *args)
        assert res.code == 2, res
        self.assert_no_side_effects(before)

    def test_invalid_set_on_missing_config_creates_nothing(self):
        assert self.sb.run("config", "--set-batch-size", "0").code == 2
        assert not self.sb.config_path.exists()
