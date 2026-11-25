import re
import urllib.parse
from dataclasses import dataclass

from kw_tele import kvfile

DEFAULT_ENDPOINT = "http://localhost:4318/v1/logs"
RANGES = {
    "upload_interval": (1, 31536000),
    "upload_backoff": (1, 31536000),
    "batch_size": (1, 5000),
}
_DEFAULTS = {"upload_interval": 86400, "upload_backoff": 3600, "batch_size": 500}
_COUNTER_MAX = 10**18 - 1
_STATUS = re.compile(r"ok|partial|http-[0-9]{3}|error:(timeout|connection|schema|nodb|internal)")


@dataclass(frozen=True)
class Config:
    # Field order is the order `kw_tele config` prints.
    telemetry_consent: str
    instance_id: str
    consented_endpoint: str
    endpoint: str
    upload_interval: int
    upload_backoff: int
    batch_size: int

    @property
    def effective_consent(self):
        if self.telemetry_consent == "no":
            return "no"
        granted = (
            self.telemetry_consent == "yes"
            and self.instance_id != ""
            and self.endpoint == self.consented_endpoint
        )
        return "yes" if granted else "ask"


@dataclass(frozen=True)
class State:
    last_attempt_at: int
    last_success_at: int
    consecutive_failures: int
    last_status: str


def _valid_endpoint(raw):
    try:
        parts = urllib.parse.urlsplit(raw)
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and bool(parts.hostname)


def load_config(path):
    values = kvfile.read(path)
    consent = values.get("telemetry_consent", "")
    endpoint = values.get("endpoint", "")
    numbers = {}
    for key, (low, high) in RANGES.items():
        parsed = kvfile.parse_int(values.get(key, ""), low, high)
        numbers[key] = _DEFAULTS[key] if parsed is None else parsed
    return Config(
        telemetry_consent=consent if consent in ("yes", "no") else "ask",
        instance_id=values.get("instance_id", ""),
        consented_endpoint=values.get("consented_endpoint", ""),
        endpoint=endpoint if _valid_endpoint(endpoint) else DEFAULT_ENDPOINT,
        **numbers,
    )


def _counter(values, key):
    parsed = kvfile.parse_int(values.get(key, ""), 0, _COUNTER_MAX)
    return 0 if parsed is None else parsed


def load_state(path, now=None):
    """With `now`, a time after it (the clock went back) reads as 0, so it cannot block the gate."""
    values = kvfile.read(path)
    status = values.get("last_status", "")

    def moment(key):
        value = _counter(values, key)
        return 0 if now is not None and value > now else value

    return State(
        last_attempt_at=moment("last_attempt_at"),
        last_success_at=moment("last_success_at"),
        consecutive_failures=_counter(values, "consecutive_failures"),
        last_status=status if _STATUS.fullmatch(status) else "",
    )


def save_state(path, state):
    kvfile.update(path, vars(state))


def retry_gap(config, state):
    """Minimum seconds between attempts: flat after a success, exponential after failures."""
    if state.consecutive_failures == 0:
        return config.upload_backoff
    ceiling = max(config.upload_interval, config.upload_backoff)
    return min(config.upload_backoff * 2 ** min(state.consecutive_failures - 1, 20), ceiling)
