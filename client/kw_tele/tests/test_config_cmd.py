"""§3 `config`."""

import pytest

from helpers.cli_case import CliCase
from helpers import constants, kvfile, kwdb
from helpers.constants import CONFIG_KEYS, COUNT_KEYS, STATE_KEYS


def keys_in_order(out):
    return [line.split("=", 1)[0] for line in out.splitlines()]


class TestListing(CliCase):
    def test_full_listing_in_documented_order(self):
        self.seed(3)
        self.sb.write_state(last_attempt_at=5, last_status="ok")
        res = self.sb.run("config")
        assert res.code == 0, res
        assert keys_in_order(res.out) == CONFIG_KEYS + STATE_KEYS + COUNT_KEYS
        for line in res.out.splitlines():
            assert "=" in line

    def test_listing_values(self):
        self.seed(4, endpoint="http://h.test/x", batch_size=7)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        out = kvfile.parse_kv(self.sb.run("config").out)
        assert out["telemetry_consent"] == "yes"
        assert out["instance_id"] == constants.INSTANCE
        assert out["consented_endpoint"] == "http://h.test/x"
        assert out["endpoint"] == "http://h.test/x"
        assert out["batch_size"] == "7"
        assert (out["pending_events"], out["sent_events"], out["excluded_events"]) == (
            "2",
            "1",
            "1",
        )

    def test_listing_shows_the_stored_consent_not_the_effective_one(self):
        self.sb.write_config(
            "telemetry_consent=yes\nendpoint=http://a.test/x\n"
            "consented_endpoint=http://b.test/x\ninstance_id=i\n"
        )
        out = kvfile.parse_kv(self.sb.run("config").out)
        assert out["telemetry_consent"] == "yes"

    @pytest.mark.parametrize("with_db", [False, True])
    def test_counts_are_unknown_without_db_or_column(self, with_db):
        if with_db:
            self.sb.make_db(with_sent_at=False)
        res = self.sb.run("config")
        assert res.code == 0, res
        out = kvfile.parse_kv(res.out)
        assert [out[k] for k in COUNT_KEYS] == ["unknown"] * 3
        assert keys_in_order(res.out) == CONFIG_KEYS + STATE_KEYS + COUNT_KEYS

    def test_listing_alone_writes_nothing(self):
        self.seed(1)
        before = self.snapshot()
        self.sb.run("config")
        self.assert_no_side_effects(before)


class TestSet(CliCase):
    def test_valid_sets_are_persisted_and_echoed(self):
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
        out = kvfile.parse_kv(res.out)
        assert (out["upload_interval"], out["upload_backoff"], out["batch_size"]) == (
            "600",
            "30",
            "25",
        )
        cfg = self.sb.read_config()
        assert (cfg["upload_interval"], cfg["upload_backoff"], cfg["batch_size"]) == (
            "600",
            "30",
            "25",
        )
        assert cfg["instance_id"] == constants.INSTANCE
        assert self.sb.run("consent").out.strip() == "yes"

    @pytest.mark.parametrize(
        "flag, key, value",
        [
            ("--set-upload-interval", "upload_interval", "1"),
            ("--set-upload-backoff", "upload_backoff", "31536000"),
            ("--set-batch-size", "batch_size", "5000"),
        ],
    )
    def test_each_flag_alone(self, flag, key, value):
        res = self.sb.run("config", flag, value)
        assert res.code == 0, res
        assert kvfile.parse_kv(res.out)[key] == value
        assert self.sb.read_config()[key] == value

    @pytest.mark.parametrize(
        "flag, value",
        [
            ("--set-batch-size", "0"),
            ("--set-batch-size", "5001"),
            ("--set-batch-size", "-1"),
            ("--set-batch-size", "08"),
            ("--set-batch-size", "abc"),
            ("--set-batch-size", ""),
            ("--set-batch-size", "1.5"),
            ("--set-batch-size", "1234567890123456789"),
            ("--set-upload-interval", "0"),
            ("--set-upload-interval", "31536001"),
            ("--set-upload-backoff", "0"),
            ("--set-upload-backoff", "007"),
            ("--set-upload-backoff", "+3"),
        ],
    )
    def test_invalid_values_exit_2_and_write_nothing(self, flag, value):
        self.seed(1, batch_size=11)
        before = self.snapshot()
        res = self.sb.run("config", flag, value)
        assert res.code == 2, res
        self.assert_no_side_effects(before)

    def test_one_invalid_flag_blocks_the_valid_ones(self):
        self.seed(1, batch_size=11)
        before = self.snapshot()
        res = self.sb.run("config", "--set-upload-interval", "600", "--set-batch-size", "0")
        assert res.code == 2, res
        self.assert_no_side_effects(before)

    def test_invalid_set_on_missing_config_creates_nothing(self):
        assert self.sb.run("config", "--set-batch-size", "0").code == 2
        assert not self.sb.config_path.exists()

    def test_there_is_no_set_endpoint(self):
        self.seed(1)
        before = self.snapshot()
        res = self.sb.run("config", "--set-endpoint", "http://evil.test/x")
        assert res.code == 2, res
        self.assert_no_side_effects(before)

    def test_unknown_flag_is_a_usage_error(self):
        assert self.sb.run("config", "--bogus").code == 2

    def test_set_does_not_change_consent_state(self):
        self.seed(2)
        self.sb.run("config", "--set-batch-size", "9")
        cfg = self.sb.read_config()
        assert cfg["telemetry_consent"] == "yes"
        assert cfg["consented_endpoint"] == cfg["endpoint"]
