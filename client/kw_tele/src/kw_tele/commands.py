import contextlib
import datetime
import json
import sqlite3
import sys
import uuid

from kw_tele import db, kvfile, locks, payload, reflect, settings, transport
from kw_tele.deadline import DeadlineExceeded, limit

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USAGE = 2
EXIT_REFUSED = 3
EXIT_FAILED = 5

UPLOAD_DEADLINE_SECONDS = 60
MAX_SKIPPED_ROWS = 20  # bounds the POSTs a wrong-but-row-looking rejection can cost
CONFIG_LOCK_TIMEOUT_SECONDS = 5
PLACEHOLDER_INSTANCE_ID = "<assigned on opt-in>"


def _resource(env, config):
    instance_id = config.instance_id or PLACEHOLDER_INSTANCE_ID
    return payload.Resource(instance_id, env.kw_version, env.now)


@contextlib.contextmanager
def _db(path):
    conn = db.connect(path)
    try:
        yield conn
    finally:
        conn.close()


def complain(message):
    print(message, file=sys.stderr)


def _query(env, work):
    """Runs work(conn) on kw.db; returns (result, 0), or (None, exit code) after reporting."""
    try:
        with _db(env.db_path) as conn:
            return work(conn), EXIT_OK
    except db.DbError as exc:
        complain(str(exc))
        return None, exc.code
    except sqlite3.Error as exc:
        complain("kw database error: %s" % exc)
        return None, EXIT_INTERNAL


def upload_dry_run(env):
    config = settings.load_config(env.config_path)
    batch, code = _query(
        env,
        lambda conn: payload.pending(
            conn, reflect.load(conn), _resource(env, config), config.batch_size
        ),
    )
    if code == EXIT_OK:
        print(json.dumps(batch.document))
    return code


class _Upload:
    """One locked upload run. Background runs print nothing; --force reports to the user."""

    def __init__(self, env, force):
        self.env = env
        self.force = force
        self.sent_events = 0
        self.batches_committed = 0
        self.last_success_at = 0

    def _complain(self, message):
        if self.force:
            complain(message)

    def _record(self, status, failures):
        settings.save_state(
            self.env.state_path,
            settings.State(self.env.now, self.last_success_at, failures, status),
        )

    def run(self):
        env = self.env
        config = settings.load_config(env.config_path)
        if config.effective_consent != "yes":
            return EXIT_REFUSED
        with locks.try_lock(env.upload_lock_path) as acquired:
            if not acquired:
                self._complain("upload already in progress")
                return EXIT_OK
            return self._locked(config)

    def _locked(self, config):
        env = self.env
        before = settings.load_state(env.state_path, env.now)
        if not self.force and env.now < before.last_attempt_at + settings.retry_gap(config, before):
            return EXIT_OK

        self.last_success_at = before.last_success_at
        failures = before.consecutive_failures + 1
        self._record("error:internal", failures)
        try:
            with limit(UPLOAD_DEADLINE_SECONDS):
                return self._send_all(before, failures)
        except DeadlineExceeded:
            return self._on_deadline(failures)

    def _on_deadline(self, failures):
        if self.batches_committed:
            self._record("partial", 0)
        else:
            self._record("error:timeout", failures)
        self._complain("upload deadline exceeded")
        return EXIT_FAILED

    def _fail(self, status, failures, message, code):
        self._record(status, failures)
        self._complain(message)
        return code

    def _finish_refused(self, refused, skipped, failures, config):
        # Accepted rows alongside refused ones are partial progress, as when the deadline hits.
        self._report_sent(config)
        count = len(skipped)
        message = "upload failed: %s (%d event%s skipped)" % (
            refused.status,
            count,
            "" if count == 1 else "s",
        )
        return self._fail(refused.status, 0 if self.batches_committed else failures, message, EXIT_FAILED)

    def _report_sent(self, config):
        if self.force and self.sent_events:
            print("sent %d events to %s" % (self.sent_events, config.endpoint))

    def _send_all(self, before, failures):
        env = self.env
        refused = None  # the last row-content rejection
        skipped = []
        try:
            with _db(env.db_path) as conn:
                schema = reflect.load(conn)
                limit = None
                while True:
                    config = settings.load_config(env.config_path)
                    if config.effective_consent != "yes":
                        # A consent change is not a failure.
                        self._record(before.last_status, before.consecutive_failures)
                        return EXIT_REFUSED
                    # After a rejection the size stays small and doubles back on each success, so a
                    # bad row costs about log2(batch_size) POSTs instead of re-bisecting from the top.
                    limit = min(limit or config.batch_size, config.batch_size)
                    batch = payload.pending(conn, schema, _resource(env, config), limit, skipped)
                    if not batch.marks:
                        break
                    try:
                        transport.post(config.endpoint, payload.encode(batch.document))
                    except transport.UploadFailed as exc:
                        if not exc.permanent:
                            raise
                        refused = exc
                        if len(batch.marks) > 1:
                            # Halve from the same first row: the rows before the bad one get through.
                            limit = len(batch.marks) // 2
                            continue
                        skipped.append(batch.marks[0][0])
                        if len(skipped) >= MAX_SKIPPED_ROWS:
                            return self._finish_refused(refused, skipped, failures, config)
                        continue
                    self.sent_events += db.mark_sent(conn, batch.marks, env.now)
                    self.batches_committed += 1
                    if len(batch.marks) < limit:
                        # Drained; skipping the re-select keeps rows kw inserts mid-run for the next run.
                        break
                    limit *= 2
        except db.DbError as exc:
            return self._fail(exc.status, failures, str(exc), exc.code)
        except transport.UploadFailed as exc:
            message = "upload failed: %s" % exc.status
            return self._fail(exc.status, failures, message, EXIT_FAILED)

        if skipped:
            return self._finish_refused(refused, skipped, failures, config)

        self.last_success_at = env.now
        self._record("ok", 0)
        if self.force:
            if self.sent_events:
                self._report_sent(config)
            else:
                print("nothing to send")
        return EXIT_OK


def upload(env, force):
    try:
        return _Upload(env, force).run()
    except (OSError, sqlite3.Error) as exc:
        if force:
            complain("internal error: %s" % exc)
        return EXIT_INTERNAL


def _db_counts(env):
    try:
        with _db(env.db_path) as conn:
            return db.counts(conn)
    except (db.DbError, sqlite3.Error):
        return None


def consent_show(env):
    print(settings.load_config(env.config_path).effective_consent)
    return EXIT_OK


def consent_set(env, answer, include_history):
    if include_history and answer != "yes":
        complain("--include-history only applies to --set yes")
        return EXIT_USAGE
    try:
        with locks.wait_lock(env.config_lock_path, CONFIG_LOCK_TIMEOUT_SECONDS):
            if answer == "no":
                return _write_no(env)
            return _write_yes(env, include_history)
    except (OSError, sqlite3.Error) as exc:
        complain("internal error: %s" % exc)
        return EXIT_INTERNAL


def _write_no(env):
    kvfile.update(
        env.config_path,
        {"telemetry_consent": "no", "instance_id": "", "consented_endpoint": ""},
    )
    return EXIT_OK


def _write_yes(env, include_history):
    config = settings.load_config(env.config_path)
    if config.effective_consent == "yes":
        if include_history:
            complain("consent already given; excluded history is not re-released")
            return EXIT_USAGE
        return EXIT_OK
    # Before the config write: a crash in between leaves consent at ask and sends nothing.
    try:
        with _db(env.db_path) as conn:
            if not include_history:
                db.exclude_pending(conn)
    except db.NoDb:
        pass
    except db.DbError as exc:
        complain(str(exc))
        return exc.code
    kvfile.update(
        env.config_path,
        {
            "telemetry_consent": "yes",
            "instance_id": config.instance_id or str(uuid.uuid4()),
            "consented_endpoint": config.endpoint,
        },
    )
    return EXIT_OK


def config_command(env, changes):
    """`changes` maps config keys to already-validated integers."""
    if changes:
        try:
            with locks.wait_lock(env.config_lock_path, CONFIG_LOCK_TIMEOUT_SECONDS):
                kvfile.update(env.config_path, {k: str(v) for k, v in changes.items()})
        except OSError as exc:
            complain("internal error: %s" % exc)
            return EXIT_INTERNAL
    config = settings.load_config(env.config_path)
    state = settings.load_state(env.state_path)
    for source in (config, state):
        for key, value in vars(source).items():
            print("%s=%s" % (key, value))
    counts = _db_counts(env)
    for key in ("pending", "sent", "excluded"):
        print("%s_events=%s" % (key, counts[key] if counts else "unknown"))
    return EXIT_OK


def _iso(seconds):
    if seconds == 0:
        return "never"
    try:
        return datetime.datetime.fromtimestamp(seconds).astimezone().isoformat(timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return str(seconds)


def info_summary(env):
    config = settings.load_config(env.config_path)
    state = settings.load_state(env.state_path)
    print("consent: %s" % config.effective_consent)
    print("endpoint: %s" % config.endpoint)
    print("instance_id: %s" % (config.instance_id or "none"))
    print("last success: %s" % _iso(state.last_success_at))
    print("last attempt: %s" % _iso(state.last_attempt_at))
    print("last status: %s" % (state.last_status or "none"))
    found, code = _query(env, lambda conn: (db.counts(conn), reflect.load(conn).collected()))
    if code != EXIT_OK:
        return code
    counts, kinds = found
    for key in ("pending", "sent", "excluded"):
        print("%s events: %d" % (key, counts[key]))
    print("collected:")
    for kind, keys in kinds.items():
        print("  %s: %s" % (kind, ", ".join(keys)))
    return EXIT_OK


def info_pending(env):
    found, code = _query(env, lambda conn: (db.counts(conn)["pending"], db.pending_since(conn)))
    if code == EXIT_OK:
        print("pending_events=%d" % found[0])
        print("pending_since=%s" % (found[1] or ""))
    return code


def info_export(env):
    config = settings.load_config(env.config_path)
    batch, code = _query(
        env, lambda conn: payload.sent(conn, reflect.load(conn), _resource(env, config))
    )
    if code == EXIT_OK:
        print(json.dumps(batch.document))
    return code
