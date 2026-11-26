from dataclasses import dataclass

from kw_tele import coarsen


def include(table, column):
    """The single filter every table and column selection goes through."""
    return not (table == "event" and column == "sent_at")


def quote(name):
    return '"{}"'.format(name.replace('"', '""'))


@dataclass(frozen=True)
class Column:
    key: str
    expression: str


@dataclass(frozen=True)
class Extension:
    table: str
    columns: tuple

    @property
    def query(self):
        selected = [quote(self.table) + ".id"] + [c.expression for c in self.columns]
        return "SELECT {} FROM {} WHERE {}.id BETWEEN ? AND ?".format(
            ", ".join(selected),
            quote(self.table),
            quote(self.table),
        )


@dataclass(frozen=True)
class Schema:
    event_columns: tuple
    extensions: tuple

    def collected(self, exclude=frozenset()):
        """Attribute keys per event kind, as exported."""
        dropped = exclude - coarsen.PROTECTED_KEYS
        columns = (c for c in self.event_columns if c not in coarsen.OMITTED_COLUMNS)
        base = [f"kw.event.{c}" for c in columns] + ["kw.event.kind"]
        kinds = {"event": base}
        for extension in self.extensions:
            kinds[extension.table] = base + [c.key for c in extension.columns]
        return {kind: [k for k in keys if k not in dropped] for kind, keys in kinds.items()}


def _column_names(conn, table):
    return [row[1] for row in conn.execute(f"PRAGMA table_info({quote(table)})")]


def _foreign_keys(conn, table):
    """column -> (referenced table, referenced column or None)."""
    rows = conn.execute(f"PRAGMA foreign_key_list({quote(table)})")
    return {row[3]: (row[2], row[4]) for row in rows}


def _column(conn, table, name, foreign_keys):
    target = foreign_keys.get(name)
    if target and target[1] in ("id", None) and "name" in _column_names(conn, target[0]):
        lookup = "(SELECT {name} FROM {ref} WHERE {id} = {table}.{column})".format(
            name=quote("name"),
            ref=quote(target[0]),
            id=quote("id"),
            table=quote(table),
            column=quote(name),
        )
        return Column("kw.{}.{}".format(table, name.removesuffix("_id")), lookup)
    return Column(f"kw.{table}.{name}", f"{quote(table)}.{quote(name)}")


def _extension(conn, table):
    foreign_keys = _foreign_keys(conn, table)
    parent = foreign_keys.get("id")
    if parent is None or parent[0].lower() != "event":
        return None
    columns = tuple(
        _column(conn, table, name, foreign_keys)
        for name in _column_names(conn, table)
        if name != "id" and include(table, name)
    )
    return Extension(table, columns)


def load(conn):
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    extensions = (
        _extension(conn, table)
        for table in tables
        if table.lower() != "event" and include(table, None)
    )
    return Schema(
        event_columns=tuple(c for c in _column_names(conn, "event") if include("event", c)),
        extensions=tuple(e for e in extensions if e is not None),
    )
