import contextlib
import io
import json
import os
from unittest import mock

from helpers import kwdb
from helpers.constants import DEAD_URL
from helpers.http_stub import Stub
from helpers.sandbox import Sandbox


class CliCase:
    """Base for CLI tests. Importing kw_tele here makes every CLI test module fail at
    collection while the package does not exist."""

    def setup_method(self, method):
        from kw_tele import cli  # noqa: F401

        self.sb = Sandbox()
        self._cleanups = [self.sb.cleanup]

    def teardown_method(self, method):
        for fn in reversed(self._cleanups):
            fn()

    def addCleanup(self, fn):
        self._cleanups.append(fn)

    def serve(self, **kwargs):
        stub = Stub(**kwargs).start()
        self.addCleanup(stub.stop)
        return stub

    def seed(self, executed=3, endpoint=DEAD_URL, consent=True, **cfg):
        """DB with `executed` pending events plus a config; consent granted unless False."""
        self.sb.make_db()
        kwdb.add_executed_many(self.sb.db, executed)
        if consent:
            self.sb.configure(endpoint=endpoint, **cfg)
        return self.sb.db

    def call(self, argv, env=None):
        """Runs kw_tele.cli.main in this process; returns (code, stdout, stderr)."""
        from kw_tele import cli

        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, env or self.sb.env(), clear=True),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            try:
                code = cli.main(argv)
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue(), err.getvalue()

    def dry_run(self, **kwargs):
        res = self.sb.run("upload", "--dry-run", **kwargs)
        assert res.code == 0, res
        return json.loads(res.out)

    def snapshot(self):
        sb = self.sb
        return {
            "config": sb.config_path.read_bytes() if sb.config_path.exists() else None,
            "state": sb.state_path.read_bytes() if sb.state_path.exists() else None,
            "db": kwdb.sent_map(sb.db) if sb.db.exists() else None,
        }

    def assert_no_side_effects(self, before):
        assert self.snapshot() == before
