import os
import subprocess
import sys
import tempfile
from pathlib import Path

from helpers import kvfile
from helpers.constants import DEAD_URL, INSTANCE, KW_VERSION, NOW, SRC
from helpers.kwdb import make_db


class Result:
    def __init__(self, code, out, err):
        self.code, self.out, self.err = code, out, err

    def __repr__(self):
        return f"Result(code={self.code!r}, out={self.out!r}, err={self.err!r})"


class Sandbox:
    """Temp KW_DATA_DIR / KW_STATE_DIR / KW_TELE_CONFIG; nothing touches the real home."""

    def __init__(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="kwtele-")
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.data_dir = self.root / "data"
        self.state_dir = self.root / "state"
        self.config_path = self.root / "cfg" / "telemetry.config"
        self.db = self.data_dir / "kw.db"
        self.state_path = self.state_dir / "telemetry.state"
        self.lock_path = self.state_dir / "telemetry.lock"

    def cleanup(self):
        self._tmp.cleanup()

    def env(self, now=NOW, overrides=None):
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(SRC),
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": str(self.home),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
            "XDG_STATE_HOME": str(self.home / ".local" / "state"),
            "XDG_DATA_HOME": str(self.home / ".local" / "share"),
            "KW_DATA_DIR": str(self.data_dir),
            "KW_STATE_DIR": str(self.state_dir),
            "KW_TELE_CONFIG": str(self.config_path),
            "KW_VERSION": KW_VERSION,
            "KW_TELE_TEST": "1",
            "TZ": "UTC",
        }
        if now is not None:
            env["KW_TELE_NOW"] = str(now)
        for key, value in (overrides or {}).items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env

    def run(self, *args, now=NOW, env=None, timeout=90):
        proc = subprocess.run(
            [sys.executable, "-m", "kw_tele", *args],
            env=self.env(now, env),
            cwd=str(self.root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout,
        )
        return Result(
            proc.returncode,
            proc.stdout.decode("utf-8", "replace"),
            proc.stderr.decode("utf-8", "replace"),
        )

    def write_config(self, data):
        kvfile.atomic_write(self.config_path, data)

    def configure(
        self, endpoint=DEAD_URL, consent="yes", instance_id=INSTANCE, **extra
    ):
        """Write a config that grants effective consent unless a field is overridden."""
        values = {
            "telemetry_consent": consent,
            "instance_id": instance_id,
            "consented_endpoint": endpoint,
            "endpoint": endpoint,
        }
        values.update({k: str(v) for k, v in extra.items()})
        self.write_config("".join("{}={}\n".format(*kv) for kv in values.items()))

    def read_config_text(self):
        return self.config_path.read_text()

    def read_config(self):
        return kvfile.parse_kv(self.read_config_text())

    def write_state(self, **values):
        kvfile.atomic_write(
            self.state_path, "".join("{}={}\n".format(*kv) for kv in values.items())
        )

    def read_state(self):
        return kvfile.parse_kv(self.state_path.read_text())

    def make_db(self, with_sent_at=True):
        make_db(self.db, with_sent_at)
        return self.db
