import contextlib
import sqlite3
from pathlib import Path

from helpers.constants import KWDB_SQL


def connect(path):
    return sqlite3.connect(str(path), timeout=10)


def make_db(path, with_sent_at=True):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with contextlib.closing(connect(path)) as conn:
        conn.executescript(KWDB_SQL.read_text())
        if with_sent_at:
            conn.execute("ALTER TABLE event ADD COLUMN sent_at INTEGER")
        conn.commit()


def add_executed(
    db, label="build", status="success", date="2026-03-04", time="05:06:07", elapsed=42
):
    with contextlib.closing(connect(db)) as conn:
        conn.execute(
            "INSERT INTO statistics_report "
            "(label_name, status, date, time, elapsed_time_in_secs) VALUES (?,?,?,?,?)",
            (label, status, date, time, elapsed),
        )
        conn.commit()
        return conn.execute("SELECT max(id) FROM event").fetchone()[0]


def add_timebox(
    db, tag="work", date="2026-03-04", time="07:08:09", duration=1500, description="write tests"
):
    with contextlib.closing(connect(db)) as conn:
        conn.execute(
            "INSERT INTO pomodoro_report "
            "(tag_name, date, time, duration, description) VALUES (?,?,?,?,?)",
            (tag, date, time, duration, description),
        )
        conn.commit()
        return conn.execute("SELECT max(id) FROM event").fetchone()[0]


def add_executed_many(db, count, first_day=1):
    return [
        add_executed(db, date="2026-03-%02d" % (first_day + i), elapsed=i + 1) for i in range(count)
    ]


def sent_map(db):
    with contextlib.closing(connect(db)) as conn:
        return dict(conn.execute("SELECT id, sent_at FROM event ORDER BY id"))


def set_sent_at(db, event_id, value):
    with contextlib.closing(connect(db)) as conn:
        conn.execute("UPDATE event SET sent_at = ? WHERE id = ?", (value, event_id))
        conn.commit()


def sql(db, statement, params=()):
    with contextlib.closing(connect(db)) as conn:
        rows = conn.execute(statement, params).fetchall()
        conn.commit()
        return rows
