import fcntl
import os
import time
from contextlib import contextmanager

_POLL_SECONDS = 0.05


def _open(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "a+")


@contextmanager
def try_lock(path):
    """Yields whether the exclusive lock was taken without blocking."""
    with _open(path) as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True


@contextmanager
def wait_lock(path, timeout):
    with _open(path) as handle:
        give_up = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= give_up:
                    raise TimeoutError(f"timed out waiting for {path}") from None
                time.sleep(_POLL_SECONDS)
        yield
