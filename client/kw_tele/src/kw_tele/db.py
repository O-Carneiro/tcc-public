import os
import sqlite3


class DbError(Exception):
    code: int
    status: str


class NoDb(DbError):
    code = 6
    status = "error:nodb"


class NoSchema(DbError):
    code = 4
    status = "error:schema"


def _decode(raw):
    # kw stores whatever bytes the shell gave it; one bad byte must not block the queue.
    return raw.decode("utf-8", errors="replace")


def connect(path):
    """Open kw.db, which kw owns: a missing file or missing event.sent_at is an error, never created."""
    if not os.path.exists(path):
        raise NoDb(f"kw database not found: {path}")
    conn = sqlite3.connect(path, timeout=10)
    conn.text_factory = _decode
    try:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(event)")]
    except sqlite3.OperationalError:
        conn.close()
        raise
    except sqlite3.DatabaseError:
        conn.close()
        raise NoDb(f"kw database is not a database: {path}") from None
    if "sent_at" not in columns:
        conn.close()
        raise NoSchema("kw database schema is outdated: event.sent_at is missing")
    return conn


def counts(conn):
    row = conn.execute(
        "SELECT COALESCE(SUM(sent_at IS NULL), 0), COALESCE(SUM(sent_at > 0), 0),"
        " COALESCE(SUM(sent_at = -1), 0) FROM event"
    ).fetchone()
    return {"pending": row[0], "sent": row[1], "excluded": row[2]}


def pending_since(conn):
    return conn.execute("SELECT MIN(date) FROM event WHERE sent_at IS NULL").fetchone()[0]


def exclude_pending(conn):
    with conn:
        conn.execute("UPDATE event SET sent_at = -1 WHERE sent_at IS NULL")


def mark_sent(conn, marks, now):
    """Returns how many rows were marked.

    Match on content as well as id, since event.id is a rowid and can be reused after a delete.
    The comparison is on raw bytes: text decoded with errors="replace" would never match.
    """
    with conn:
        cursor = conn.executemany(
            "UPDATE event SET sent_at = :now WHERE id = :id AND CAST(date AS BLOB) = :date"
            " AND CAST(time AS BLOB) IS :time AND (sent_at IS NULL OR sent_at = -1)",
            [{"now": now, "id": i, "date": d, "time": t} for i, d, t in marks],
        )
        return cursor.rowcount
