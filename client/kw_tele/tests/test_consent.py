"""§1.2 effective consent and the `consent` command."""

import time
import uuid

import pytest

from helpers.cli_case import CliCase
from helpers import kwdb

EP = "http://collector.test:4318/v1/logs"
OTHER = "http://elsewhere.test/v1/logs"


class TestEffectiveConsent(CliCase):
    def consent(self):
        res = self.sb.run("consent")
        assert res.code == 0, res
        return res.out.strip()

    def test_no_config_is_ask(self):
        assert self.consent() == "ask"

    def test_yes_needs_all_three_conditions(self):
        self.sb.configure(endpoint=EP)
        assert self.consent() == "yes"

    def test_missing_instance_id_makes_it_ask(self):
        self.sb.configure(endpoint=EP, instance_id="")
        assert self.consent() == "ask"

    def test_endpoint_mismatch_makes_it_ask(self):
        self.sb.write_config(
            "telemetry_consent=yes\ninstance_id=i\n"
            "consented_endpoint=%s\nendpoint=%s\n" % (EP, OTHER)
        )
        assert self.consent() == "ask"

    def test_endpoint_comparison_is_byte_for_byte(self):
        self.sb.write_config(
            "telemetry_consent=yes\ninstance_id=i\n"
            "consented_endpoint=%s\nendpoint=%s/\n" % (EP, EP)
        )
        assert self.consent() == "ask"

    def test_empty_consented_endpoint_makes_it_ask(self):
        self.sb.write_config("telemetry_consent=yes\ninstance_id=i\nendpoint=%s\n" % EP)
        assert self.consent() == "ask"

    def test_no_is_reported_as_no(self):
        self.sb.configure(endpoint=EP, consent="no")
        assert self.consent() == "no"

    def test_ask_is_reported_as_ask(self):
        self.sb.configure(endpoint=EP, consent="ask")
        assert self.consent() == "ask"

    def test_consent_without_flag_writes_nothing(self):
        self.sb.configure(endpoint=EP)
        before = self.sb.config_path.read_bytes()
        self.consent()
        assert self.sb.config_path.read_bytes() == before
        assert not self.sb.state_dir.exists()


class TestSetYes(CliCase):
    def pending_db(self):
        self.sb.make_db()
        kwdb.add_executed_many(self.sb.db, 4)
        kwdb.set_sent_at(self.sb.db, 3, 1_700_000_000)
        kwdb.set_sent_at(self.sb.db, 4, -1)
        return self.sb.db

    def test_set_yes_excludes_pending_rows(self):
        db = self.pending_db()
        res = self.sb.run("consent", "--set", "yes")
        assert res.code == 0, res
        assert kwdb.sent_map(db) == {1: -1, 2: -1, 3: 1_700_000_000, 4: -1}

    def test_set_yes_with_history_leaves_pending_rows(self):
        db = self.pending_db()
        res = self.sb.run("consent", "--set", "yes", "--include-history")
        assert res.code == 0, res
        assert kwdb.sent_map(db) == {1: None, 2: None, 3: 1_700_000_000, 4: -1}

    def test_set_yes_writes_consent_instance_id_and_endpoint(self):
        self.sb.write_config("endpoint=%s\n" % EP)
        res = self.sb.run("consent", "--set", "yes")
        assert res.code == 0, res
        cfg = self.sb.read_config()
        assert cfg["telemetry_consent"] == "yes"
        assert cfg["consented_endpoint"] == EP
        parsed = uuid.UUID(cfg["instance_id"])
        assert parsed.version == 4
        assert self.sb.run("consent").out.strip() == "yes"

    def test_set_yes_uses_default_endpoint_when_none_configured(self):
        assert self.sb.run("consent", "--set", "yes").code == 0
        cfg = self.sb.read_config()
        assert cfg["consented_endpoint"] == "http://localhost:4318/v1/logs"
        assert self.sb.run("consent").out.strip() == "yes"

    def test_set_yes_creates_state_dir_and_config_dir(self):
        assert self.sb.run("consent", "--set", "yes").code == 0
        assert self.sb.state_dir.is_dir()
        assert self.sb.config_path.is_file()

    def test_set_yes_keeps_an_existing_instance_id(self):
        self.sb.write_config(
            "telemetry_consent=ask\ninstance_id=keep-me\n"
            "consented_endpoint=%s\nendpoint=%s\n" % (OTHER, EP)
        )
        assert self.sb.run("consent", "--set", "yes").code == 0
        cfg = self.sb.read_config()
        assert cfg["instance_id"] == "keep-me"
        assert cfg["consented_endpoint"] == EP
        assert cfg["telemetry_consent"] == "yes"

    def test_reconsent_after_endpoint_edit_excludes_rows_pending_for_the_old_endpoint(self):
        db = self.pending_db()
        self.sb.write_config(
            "telemetry_consent=yes\ninstance_id=i\n"
            "consented_endpoint=%s\nendpoint=%s\n" % (OTHER, EP)
        )
        assert self.sb.run("consent").out.strip() == "ask"
        assert self.sb.run("consent", "--set", "yes").code == 0
        assert kwdb.sent_map(db)[1] == -1
        assert self.sb.read_config()["instance_id"] == "i"

    def test_second_set_yes_is_a_noop(self):
        db = self.pending_db()
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.config_path.read_bytes()
        kwdb.add_executed(db, date="2026-05-05")
        res = self.sb.run("consent", "--set", "yes")
        assert res.code == 0, res
        assert self.sb.config_path.read_bytes() == first
        assert kwdb.sent_map(db)[5] is None

    def test_set_yes_with_history_when_already_yes_is_a_usage_error(self):
        db = self.pending_db()
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.config_path.read_bytes()
        res = self.sb.run("consent", "--set", "yes", "--include-history")
        assert res.code == 2, res
        assert "consent already given; excluded history is not re-released" in res.err
        assert self.sb.config_path.read_bytes() == first
        assert kwdb.sent_map(db)[1] == -1

    def test_set_yes_without_db_still_grants_consent(self):
        res = self.sb.run("consent", "--set", "yes")
        assert res.code == 0, res
        assert self.sb.run("consent").out.strip() == "yes"

    def test_set_yes_with_missing_column_exits_4_and_writes_nothing(self):
        self.sb.make_db(with_sent_at=False)
        kwdb.add_executed(self.sb.db)
        res = self.sb.run("consent", "--set", "yes")
        assert res.code == 4, res
        assert not self.sb.config_path.exists()

    def test_set_yes_with_missing_column_keeps_existing_config(self):
        self.sb.make_db(with_sent_at=False)
        self.sb.write_config("telemetry_consent=ask\nfoo=bar\n")
        before = self.sb.config_path.read_bytes()
        assert self.sb.run("consent", "--set", "yes").code == 4
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
        assert cfg["telemetry_consent"] == "no"
        assert cfg["instance_id"] == ""
        assert cfg["consented_endpoint"] == ""
        assert kwdb.sent_map(self.sb.db) == {1: 1_700_000_000, 2: -1, 3: None}
        assert self.sb.run("consent").out.strip() == "no"

    def test_set_no_works_without_db_or_column(self):
        self.sb.configure(endpoint=EP)
        assert self.sb.run("consent", "--set", "no").code == 0
        self.sb.make_db(with_sent_at=False)
        assert self.sb.run("consent", "--set", "no").code == 0

    def test_opt_in_after_opt_out_gets_a_new_instance_id(self):
        assert self.sb.run("consent", "--set", "yes").code == 0
        first = self.sb.read_config()["instance_id"]
        assert self.sb.run("consent", "--set", "no").code == 0
        assert self.sb.run("consent", "--set", "yes").code == 0
        second = self.sb.read_config()["instance_id"]
        assert second
        assert first != second


class TestUsage(CliCase):
    @pytest.mark.parametrize("args", [["--include-history"], ["--set", "no", "--include-history"]])
    def test_include_history_without_set_yes_is_usage_error(self, args):
        self.sb.make_db()
        res = self.sb.run("consent", *args)
        assert res.code == 2, res
        assert not self.sb.config_path.exists()

    def test_set_rejects_other_values(self):
        res = self.sb.run("consent", "--set", "maybe")
        assert res.code == 2, res
        assert not self.sb.config_path.exists()

    def test_config_writes_are_serialized_and_time_out_with_exit_1(self):
        import fcntl

        self.sb.state_dir.mkdir(parents=True)
        with open(self.sb.state_dir / "telemetry.config.lock", "w") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            started = time.monotonic()
            res = self.sb.run("consent", "--set", "yes")
            waited = time.monotonic() - started
        assert res.code == 1, res
        assert waited >= 4.0
        assert not self.sb.config_path.exists()


class TestIncludeHistoryChecksSchema(CliCase):
    """`--include-history` must go through the same database check as plain `--set yes`."""

    @pytest.mark.parametrize("extra", [[], ["--include-history"]])
    def test_outdated_schema_is_exit_4_and_consent_unchanged(self, extra):
        self.sb.make_db(with_sent_at=False)
        res = self.sb.run("consent", "--set", "yes", *extra)
        assert res.code == 4, res
        assert not self.sb.config_path.exists()

    @pytest.mark.parametrize("extra", [[], ["--include-history"]])
    def test_missing_db_still_accepts_consent(self, extra):
        res = self.sb.run("consent", "--set", "yes", *extra)
        assert res.code == 0, res
        assert self.sb.read_config()["telemetry_consent"] == "yes"
