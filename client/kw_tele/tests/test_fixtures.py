"""The one helper behaviour whose failure would otherwise be confusing."""

import pytest

from helpers import kwdb
from helpers.sandbox import Sandbox


def test_missing_real_schema_fails_with_the_fix_in_the_message(monkeypatch):
    sb = Sandbox()
    try:
        absent = sb.root / "absent.sql"
        monkeypatch.setattr(kwdb, "KWDB_SQL", absent)
        with pytest.raises(pytest.fail.Exception) as exc:
            sb.make_db()
        assert str(absent) in str(exc.value)
        assert "git submodule update --init" in str(exc.value)
    finally:
        sb.cleanup()
