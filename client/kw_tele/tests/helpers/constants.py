from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent.parent
ROOT = TESTS_DIR.parent
SRC = ROOT / "src"
# The real schema, owned by kw (a git submodule); a missing copy must fail tests, not skip them.
KWDB_SQL = ROOT.parents[1] / "kworkflow" / "database" / "kwdb.sql"

NOW = 1_800_000_000
INSTANCE = "0b1f6b7e-5d0e-4c53-9d57-3f1a6d2b8c11"
# Port 9 (discard) has no listener on a test machine, so connections are refused.
DEAD_URL = "http://127.0.0.1:9/v1/logs"
KW_VERSION = "9.9.9-test"

CONFIG_KEYS = [
    "telemetry_consent",
    "instance_id",
    "consented_endpoint",
    "endpoint",
    "upload_interval",
    "upload_backoff",
    "batch_size",
    "exclude_fields",
]
STATE_KEYS = [
    "last_attempt_at",
    "last_success_at",
    "consecutive_failures",
    "last_status",
]
COUNT_KEYS = ["pending_events", "sent_events", "excluded_events"]
