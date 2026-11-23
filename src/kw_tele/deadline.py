import signal
from contextlib import contextmanager


class DeadlineExceeded(BaseException):
    """BaseException so no `except Exception` inside urllib or sqlite3 can swallow it."""


def _raise(signum, frame):
    raise DeadlineExceeded()


@contextmanager
def limit(seconds):
    previous = signal.signal(signal.SIGALRM, _raise)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
