from dataclasses import dataclass


def include(table, column):
    """The single filter every table and column selection goes through."""
    return not (table == "event" and column == "sent_at")


def quote(name):
    return '"%s"' % name.replace('"', '""')


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
        return "SELECT %s FROM %s WHERE %s.id BETWEEN ? AND ?" % (
            ", ".join(selected),
            quote(self.table),
            quote(self.table),
        )


@dataclass(frozen=True)
class Schema:
    event_columns: tuple
    extensions: tuple

    def collected(self):
        """Attribute keys per event kind."""
        base = ["kw.event.%s" % c for c in self.event_columns] + ["kw.event.kind"]
        kinds = {"event": base}
        for extension in self.extensions:
            kinds[extension.table] = base + [c.key for c in extension.columns]
        return kinds


def _column_names(conn, table):
    return [row[1] for row in conn.execute("PRAGMA table_info(%s)" % quote(table))]


def _foreign_keys(conn, table):
    """column -> (referenced table, referenced column or None)."""
    rows = conn.execute("PRAGMA foreign_key_list(%s)" % quote(table))
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
        return Column("kw.%s.%s" % (table, name.removesuffix("_id")), lookup)
    return Column("kw.%s.%s" % (table, name), "%s.%s" % (quote(table), quote(name)))


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
