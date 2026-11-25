"""Self-tests of the harness. They must pass without kw_tele."""

import http.client
import json
import socket
import sqlite3
import urllib.error
import urllib.request

import pytest

from helpers import constants, kvfile, kwdb
from helpers.sandbox import Sandbox


class TestDbHelpers:
    def setup_method(self):
        self.sb = Sandbox()

    def teardown_method(self):
        self.sb.cleanup()

    def test_rows_inserted_through_views_appear_in_base_tables(self):
        db = self.sb.make_db()
        eid = kwdb.add_executed(db, label="build", status="failure", elapsed=7)
        tid = kwdb.add_timebox(db, tag="deep", duration=900, description="x")
        assert (eid, tid) == (1, 2)
        assert kwdb.sql(db, "SELECT count(*) FROM event")[0][0] == 2
        row = kwdb.sql(
            db,
            "SELECT c.name, s.name, x.elapsed_time_in_secs FROM executed x "
            "JOIN command_label c ON c.id=x.command_label_id "
            "JOIN status s ON s.id=x.status_id",
        )[0]
        assert row == ("build", "failure", 7)
        assert kwdb.sql(db, "SELECT tag_id, duration FROM timebox")[0][1] == 900
        assert kwdb.sent_map(db) == {1: None, 2: None}

    def test_database_is_built_from_the_real_kw_schema(self):
        db = self.sb.make_db()
        tables = {r[0] for r in kwdb.sql(db, "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "build_modules" in tables
        assert "sent_at" in [r[1] for r in kwdb.sql(db, "PRAGMA table_info(event)")]

    def test_missing_real_schema_skips_instead_of_erroring(self, monkeypatch):
        monkeypatch.setattr(kwdb, "KWDB_SQL", self.sb.root / "absent.sql")
        with pytest.raises(pytest.skip.Exception):
            self.sb.make_db()

    def test_sent_at_column_is_optional(self):
        db = self.sb.make_db(with_sent_at=False)
        cols = [r[1] for r in kwdb.sql(db, "PRAGMA table_info(event)")]
        assert "sent_at" not in cols
        db2 = self.sb.root / "other.db"
        kwdb.make_db(db2)
        assert "sent_at" in [r[1] for r in kwdb.sql(db2, "PRAGMA table_info(event)")]

    def test_deleting_last_event_frees_its_id_for_reuse(self):
        db = self.sb.make_db()
        kwdb.add_executed_many(db, 3)
        kwdb.sql(db, "DELETE FROM statistics_report WHERE id = 3")
        assert kwdb.add_executed(db, date="2030-01-01") == 3

    def test_invalid_utf8_text_is_storable(self):
        db = self.sb.make_db()
        kwdb.add_timebox(db)
        kwdb.sql(db, "UPDATE timebox SET description = CAST(X'66ff6f' AS TEXT)")
        conn = sqlite3.connect(str(db))
        conn.text_factory = bytes
        assert conn.execute("SELECT description FROM timebox").fetchone()[0] == b"f\xffo"
        conn.text_factory = str
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("SELECT description FROM timebox").fetchone()
        conn.close()

    def test_sandbox_env_is_isolated(self):
        env = self.sb.env()
        assert env["KW_TELE_TEST"] == "1"
        assert env["KW_TELE_NOW"] == str(constants.NOW)
        assert all(
            env[key].startswith(str(self.sb.root))
            for key in ("HOME", "KW_DATA_DIR", "KW_STATE_DIR", "KW_TELE_CONFIG")
        )
        assert "KW_TELE_NOW" not in self.sb.env(now=None)

    def test_config_helpers_round_trip(self):
        self.sb.configure(batch_size=7)
        cfg = self.sb.read_config()
        assert cfg["batch_size"] == "7"
        assert cfg["endpoint"] == cfg["consented_endpoint"]
        self.sb.write_state(last_status="ok")
        assert self.sb.read_state() == {"last_status": "ok"}

    def test_parse_kv_rules(self):
        text = "a=1\r\n# c\n\n b = x=y \na=2\nlast=z"
        assert kvfile.parse_kv(text) == {"a": "2", "b": "x=y", "last": "z"}


class TestStub:
    def post(self, url, body=b"{}", timeout=5):
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        return urllib.request.urlopen(req, timeout=timeout)

    @pytest.fixture(autouse=True)
    def _stubs(self, stub_factory):
        self.start = stub_factory

    def test_records_bodies_and_answers_200(self):
        stub = self.start()
        with self.post(stub.url, json.dumps({"a": 1}).encode()) as resp:
            assert resp.status == 200
        assert stub.requests[0].json() == {"a": 1}
        assert stub.requests[0].headers["Content-Type"] == "application/json"

    def test_status_sequence_repeats_last(self):
        stub = self.start(codes=[200, 503])
        self.post(stub.url).close()
        for _ in range(2):
            with pytest.raises(urllib.error.HTTPError) as ctx:
                self.post(stub.url)
            assert ctx.value.code == 503
            ctx.value.close()

    def test_redirect_is_302_with_location_and_target_answers_200(self):
        stub = self.start(codes=["redirect"])
        host, port = stub.server.server_address
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("POST", "/v1/logs", b"{}")
        resp = conn.getresponse()
        assert (resp.status, resp.getheader("Location")) == (302, "/ok")
        conn.close()
        conn = http.client.HTTPConnection(host, port, timeout=5)
        conn.request("GET", "/ok")
        assert conn.getresponse().status == 200
        conn.close()

    def test_hang_makes_client_time_out(self):
        stub = self.start(codes=["hang"])
        with pytest.raises((socket.timeout, TimeoutError, urllib.error.URLError)):
            self.post(stub.url, timeout=0.5)

    def test_before_hook_runs_before_response(self):
        seen = []
        stub = self.start(before=lambda req: seen.append(req.index))
        self.post(stub.url).close()
        assert seen == [0]
