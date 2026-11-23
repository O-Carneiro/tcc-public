import calendar
import time


def records(doc):
    out = []
    for resource in doc.get("resourceLogs", []):
        for scope in resource.get("scopeLogs", []):
            out.extend(scope.get("logRecords", []))
    return out


def attrs(record):
    return {a["key"]: a["value"] for a in record["attributes"]}


def resource_attrs(doc):
    return doc["resourceLogs"][0]["resource"]["attributes"]


def event_ids(doc):
    return [int(attrs(r)["kw.event.id"]["intValue"]) for r in records(doc)]


def utc_ns(date, time_):
    """Epoch ns of a naive date/time read as UTC; the sandbox runs with TZ=UTC."""
    stamp = time.strptime("%s %s" % (date, time_), "%Y-%m-%d %H:%M:%S")
    return str(calendar.timegm(stamp) * 10**9)
