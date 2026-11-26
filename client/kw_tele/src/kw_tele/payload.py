import base64
import json
import math
from collections import defaultdict
from dataclasses import dataclass

from kw_tele import __version__, coarsen, reflect

_SEVERITY_INFO = 9


@dataclass(frozen=True)
class Resource:
    instance_id: str
    kw_version: str
    now: int


@dataclass(frozen=True)
class Batch:
    document: dict
    marks: list  # (id, raw date, raw time) of each event, to mark it sent by content as well as id


def _value(value):
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value} if math.isfinite(value) else {"stringValue": str(value)}
    if isinstance(value, bytes):
        return {"bytesValue": base64.b64encode(value).decode("ascii")}
    return {"stringValue": value}


def _attributes(pairs):
    return [{"key": key, "value": _value(value)} for key, value in pairs if value is not None]


def _time_unix_nano(moment):
    return "0" if moment is None else str(coarsen.week_instant(moment) * 10**9)


def _string_attribute(key, value):
    return {"key": key, "value": {"stringValue": value}}


def _resource_attributes(resource):
    return [
        _string_attribute("service.name", "kworkflow"),
        _string_attribute("service.version", resource.kw_version),
        _string_attribute("service.instance.id", resource.instance_id),
        _string_attribute("telemetry.sdk.name", "kw_tele"),
        _string_attribute("telemetry.sdk.language", "python"),
        _string_attribute("telemetry.sdk.version", __version__),
    ]


def _extension_rows(conn, extensions, first_id, last_id):
    rows = {}
    for extension in extensions:
        by_event = defaultdict(list)
        for row in conn.execute(extension.query, (first_id, last_id)).fetchall():
            by_event[row[0]].append(row[1:])
        rows[extension.table] = by_event
    return rows


def _build(conn, schema, resource, condition, params, limit, export, exclude):
    names = schema.event_columns
    selected = ", ".join(reflect.quote(name) for name in names)
    events = conn.execute(
        f"SELECT {selected}, sent_at, CAST(date AS BLOB), CAST(time AS BLOB)"
        f" FROM event WHERE {condition} ORDER BY id LIMIT ?",
        (*params, limit),
    ).fetchall()
    if not events:
        return Batch({"resourceLogs": []}, [])

    first_id, last_id = events[0][names.index("id")], events[-1][names.index("id")]
    extension_rows = _extension_rows(conn, schema.extensions, first_id, last_id)
    observed = str(coarsen.week_instant(resource.now) * 10**9)
    dropped = exclude - coarsen.PROTECTED_KEYS

    records, marks = [], []
    for *values, sent_at, raw_date, raw_time in events:
        event = dict(zip(names, values))
        marks.append((event["id"], raw_date, raw_time))
        exported = ((name, coarsen.export_value(name, event)) for name in event)
        event_attributes = [
            (f"kw.event.{name}", value) for name, value in exported if value is not coarsen.OMIT
        ]
        moment = coarsen.event_moment(event)
        matches = [
            (extension.table, [(c.key, v) for c, v in zip(extension.columns, row)])
            for extension in schema.extensions
            for row in extension_rows[extension.table].get(event["id"], [])
        ] or [("event", [])]
        for kind, extra in matches:
            pairs = event_attributes + [("kw.event.kind", kind)] + extra
            if export:
                pairs.append(("kw.event.sent_at", sent_at))
            records.append(
                {
                    "timeUnixNano": _time_unix_nano(moment),
                    "observedTimeUnixNano": observed,
                    "severityNumber": _SEVERITY_INFO,
                    "severityText": "INFO",
                    "body": {"stringValue": kind},
                    "attributes": _attributes((k, v) for k, v in pairs if k not in dropped),
                }
            )

    document = {
        "resourceLogs": [
            {
                "resource": {"attributes": _resource_attributes(resource)},
                "scopeLogs": [
                    {
                        "scope": {"name": "kw_tele", "version": __version__},
                        "logRecords": records,
                    }
                ],
            }
        ]
    }
    return Batch(document, marks)


def pending(conn, schema, resource, limit, skip_ids=(), exclude=frozenset()):
    """`skip_ids` are rows already refused this run, left pending but not retried."""
    condition = "sent_at IS NULL"
    if skip_ids:
        condition += " AND id NOT IN ({})".format(", ".join("?" * len(skip_ids)))
    return _build(conn, schema, resource, condition, tuple(skip_ids), limit, export=False, exclude=exclude)


def sent(conn, schema, resource, exclude=frozenset()):
    return _build(conn, schema, resource, "sent_at > 0", (), -1, export=True, exclude=exclude)


def encode(document):
    return json.dumps(document).encode("utf-8")
