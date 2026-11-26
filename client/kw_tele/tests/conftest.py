import sys

import pytest

from helpers.constants import SRC
from helpers.http_stub import Stub
from helpers.sandbox import Sandbox

sys.path.insert(0, str(SRC))


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: waits for kw_tele's fixed 60 s deadline (deselect with -m 'not slow')",
    )


@pytest.fixture
def sandbox():
    sb = Sandbox()
    yield sb
    sb.cleanup()


@pytest.fixture
def stub_factory():
    made = []

    def make(**kwargs):
        stub = Stub(**kwargs).start()
        made.append(stub)
        return stub

    yield make
    for stub in made:
        stub.stop()
