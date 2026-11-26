import calendar
import datetime
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
    stamp = time.strptime(f"{date} {time_}", "%Y-%m-%d %H:%M:%S")
    return str(calendar.timegm(stamp) * 10**9)


def week_instant(epoch):
    """Monday 00:13:00 UTC of the UTC week containing `epoch`; computed independently of kw_tele."""
    moment = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    monday = moment.date() - datetime.timedelta(days=moment.weekday())
    return calendar.timegm(monday.timetuple()) + 13 * 60


def week_date(epoch):
    moment = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    return (moment.date() - datetime.timedelta(days=moment.weekday())).isoformat()


def week_ns(date, time_):
    """timeUnixNano expected for a naive date/time read as UTC (the sandbox runs with TZ=UTC)."""
    return str(week_instant(int(utc_ns(date, time_)) // 10**9) * 10**9)
