import os
import time
from dataclasses import dataclass

from kw_tele import kvfile


@dataclass(frozen=True)
class Env:
    config_path: str
    state_dir: str
    db_path: str
    kw_version: str
    now: int

    @property
    def state_path(self):
        return os.path.join(self.state_dir, "telemetry.state")

    @property
    def upload_lock_path(self):
        return os.path.join(self.state_dir, "telemetry.lock")

    @property
    def config_lock_path(self):
        return os.path.join(self.state_dir, "telemetry.config.lock")


def _xdg_dir(variable, home_subdir):
    base = os.environ.get(variable) or os.path.join(os.path.expanduser("~"), home_subdir)
    return os.path.join(base, "kw")


def _now():
    # Honoured only under KW_TELE_TEST so a stray variable cannot shift the timers.
    if os.environ.get("KW_TELE_TEST") == "1":
        override = kvfile.parse_int(os.environ.get("KW_TELE_NOW", ""), 0, 10**18 - 1)
        if override is not None:
            return override
    return int(time.time())


def from_environ():
    data_dir = os.environ.get("KW_DATA_DIR") or _xdg_dir("XDG_DATA_HOME", ".local/share")
    return Env(
        config_path=os.environ.get("KW_TELE_CONFIG")
        or os.path.join(_xdg_dir("XDG_CONFIG_HOME", ".config"), "telemetry.config"),
        state_dir=os.environ.get("KW_STATE_DIR") or _xdg_dir("XDG_STATE_HOME", ".local/state"),
        db_path=os.path.join(data_dir, "kw.db"),
        kw_version=os.environ.get("KW_VERSION") or "unknown",
        now=_now(),
    )
