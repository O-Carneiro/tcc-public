"""§1.2 effective consent and the `consent` command."""

import fcntl
import time
import uuid

import pytest

from helpers.cli_case import CliCase
from helpers import kwdb

EP = "http://collector.test:4318/v1/logs"
OTHER = "http://elsewhere.test/v1/logs"


def config(consent="yes", instance_id="i", consented=EP, endpoint=EP):
    lines = [f"telemetry_consent={consent}", f"instance_id={instance_id}"]
    if consented is not None:
        lines.append(f"consented_endpoint={consented}")
    lines.append(f"endpoint={endpoint}")
    return "\n".join(lines) + "\n"


class TestEffectiveConsent(CliCase):
    @pytest.mark.parametrize(
        "text, expected",
        [
            (None, "ask"),
            (config(), "yes"),
            (config(consent="no", instance_id=""), "no"),
            (config(consent="ask"), "ask"),
            (config(instance_id=""), "ask"),
            (config(consented=None), "ask"),
            (config(consented=OTHER), "ask"),
            (config(endpoint=EP + "/"), "ask"),
            *[(config(consent=raw), "ask") for raw in ("YES", "Yes", "true", "1", "")],
        ],
        ids=[
            "no-config",
            "yes",
            "no",
            "ask",
            "missing-instance-id",
            "missing-consented-endpoint",
            "endpoint-edited",
            "endpoint-differs-by-a-byte",
            *[
                f"case-sensitive-{raw or 'empty'}"
                for raw in ("YES", "Yes", "true", "1", "")
            ],
        ],
    )
    def test_consent_truth_table_and_the_command_writes_nothing(self, text, expected):
        if text is not None:
            self.sb.write_config(text)
        before = self.snapshot()
        res = self.sb.run("consent")
        assert res.code == 0, res
        assert res.out.strip() == expected
        self.assert_no_side_effects(before)
        assert not self.sb.state_dir.exists()


class TestSetYes(CliCase):
    def pending_db(self):
        self.sb.make_db()
        kwdb.add_executed_many(self.sb.db, 4)
        kwdb.set_sent_at(self.sb.db, 3, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 4, -1)
        return self.sb.db

    @pytest.mark.parametrize(
        "extra, expected",
        [
            ([], {1: -1, 2: -1, 3: 1_700_000_000, 4: -1}),
            (["--include-history"], {1: None, 2: None, 3: 1_700_000_000, 4: -1}),
        ],
        ids=["excludes-pending-history", "include-history-keeps-it-pending"],
    )
    def test_set_yes_decides_what_happens_to_rows_already_pending(
        self, extra, expected
    ):
        db = self.pending_db()
        res = self.sb.run("consent", "--set", "yes", *extra)
        assert res.code == 0, res
        assert kwdb.sent_map(db) == expected

    @pytest.mark.parametrize(
        "initial, final",
        [
            (None, ("http://localhost:4318/v1/logs", None)),
            (f"endpoint={EP}\n", (EP, None)),
            (
                config(consent="ask", instance_id="keep-me", consented=OTHER),
                (EP, "keep-me"),
            ),
            (config(consented=OTHER, instance_id="i"), (EP, "i")),
        ],
        ids=[
            "default-endpoint",
            "configured-endpoint",
            "keeps-instance-id",
            "reconsent-after-edit",
        ],
    )
    def test_set_yes_writes_consent_endpoint_and_instance_id(self, initial, final):
        if initial is not None:
            self.sb.write_config(initial)
        assert self.sb.run("consent", "--set", "yes").code == 0
        cfg = self.sb.read_config()
        endpoint, instance_id = final
        assert (cfg["telemetry_consent"], cfg["consented_endpoint"]) == (
            "yes",
            endpoint,
        )
        if instance_id:
            assert cfg["instance_id"] == instance_id
        else:
            assert uuid.UUID(cfg["instance_id"]).version == 4
        assert self.sb.run("consent").out.strip() == "yes"
        assert self.sb.state_dir.is_dir()

    def test_reconsent_after_an_endpoint_edit_excludes_rows_pending_for_the_old_endpoint(
        self,
    ):
        db = self.pending_db()
        self.sb.write_config(config(consented=OTHER, instance_id="i"))
        assert self.sb.run("consent", "--set", "yes").code == 0
        assert kwdb.sent_map(db)[1] == -1

    def test_second_set_yes_is_a_noop(self):
        db = self.pending_db()
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.config_path.read_bytes()
        kwdb.add_executed(db, date="2026-05-05")
        assert self.sb.run("consent", "--set", "yes").code == 0
        assert self.sb.config_path.read_bytes() == first
        assert kwdb.sent_map(db)[5] is None

    def test_include_history_once_consent_is_given_is_a_usage_error(self):
        db = self.pending_db()
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.config_path.read_bytes()
        res = self.sb.run("consent", "--set", "yes", "--include-history")
        assert res.code == 2, res
        assert "consent already given; excluded history is not re-released" in res.err
        assert self.sb.config_path.read_bytes() == first
        assert kwdb.sent_map(db)[1] == -1

    def test_set_yes_without_a_db_still_grants_consent(self):
        assert self.sb.run("consent", "--set", "yes").code == 0
        assert self.sb.read_config()["telemetry_consent"] == "yes"

    @pytest.mark.parametrize(
        "extra", [[], ["--include-history"]], ids=["default", "include-history"]
    )
    def test_set_yes_with_an_outdated_schema_exits_4_and_keeps_the_config(self, extra):
        self.sb.make_db(with_sent_at=False)
        kwdb.add_executed(self.sb.db)
        self.sb.write_config("telemetry_consent=ask\nfoo=bar\n")
        before = self.sb.config_path.read_bytes()
        assert self.sb.run("consent", "--set", "yes", *extra).code == 4
        assert self.sb.config_path.read_bytes() == before


class TestSetNo(CliCase):
    def test_set_no_empties_identity_and_keeps_rows(self):
        self.sb.make_db()
        kwdb.add_executed_many(self.sb.db, 3)
        kwdb.set_sent_at(self.sb.db, 1, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 2, -1)
        self.sb.configure(endpoint=EP)
        res = self.sb.run("consent", "--set", "no")
        assert res.code == 0, res
        cfg = self.sb.read_config()
        assert (
            cfg["telemetry_consent"],
            cfg["instance_id"],
            cfg["consented_endpoint"],
        ) == (
            "no",
            "",
            "",
        )
        assert kwdb.sent_map(self.sb.db) == {1: 1_700_000_000, 2: -1, 3: None}
        assert self.sb.run("consent").out.strip() == "no"

    def test_opt_in_after_opt_out_gets_a_new_instance_id(self):
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.read_config()["instance_id"]
        assert self.sb.run("consent", "--set", "no").code == 0
        assert self.sb.run("consent", "--set", "yes").code == 0
        second = self.sb.read_config()["instance_id"]
        assert second
        assert first != second


class TestConfigWriteLock(CliCase):
    @pytest.mark.parametrize(
        "args",
        [["consent", "--set", "yes"], ["config", "--set-batch-size", "9"]],
        ids=["consent-set-yes", "config-set-batch-size"],
    )
    def test_config_writes_are_serialized_and_time_out_with_exit_1(self, args):
        self.sb.write_config(config(consent="ask"))
        before = self.sb.config_path.read_bytes()
        self.sb.state_dir.mkdir(parents=True)
        with open(self.sb.state_dir / "telemetry.config.lock", "w") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            started = time.monotonic()
            res = self.sb.run(*args)
            waited = time.monotonic() - started
        assert res.code == 1, res
        assert waited >= 4.0
        assert self.sb.config_path.read_bytes() == before
