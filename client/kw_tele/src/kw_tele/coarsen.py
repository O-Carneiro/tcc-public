import time

_DAY = 86400
_WEEK_OFFSET = 780  # a week instant is Monday 00:13:00 UTC

OMIT = object()
OMITTED_COLUMNS = frozenset({"time"})
# Identity of a record: listing these in exclude_fields has no effect.
PROTECTED_KEYS = frozenset({"kw.event.id", "kw.event.kind"})


def _monday(epoch_seconds):
    # 1970-01-01 was a Thursday; floor division keeps this right for negative values.
    return epoch_seconds - ((epoch_seconds // _DAY + 3) % 7) * _DAY - epoch_seconds % _DAY


def week_instant(epoch_seconds):
    return _monday(epoch_seconds) + _WEEK_OFFSET


def week_date(epoch_seconds):
    return time.strftime("%Y-%m-%d", time.gmtime(_monday(epoch_seconds)))


def event_moment(row):
    """The event's local epoch second, or None when unknown or not representable on the wire."""
    try:
        stamp = time.strptime(
            "{} {}".format(row["date"], row.get("time") or "00:00:00"), "%Y-%m-%d %H:%M:%S"
        )
        moment = int(time.mktime(stamp))
    except (ValueError, OverflowError, OSError, TypeError, KeyError):
        return None
    # OTLP carries the timestamp as fixed64; anything outside means unknown.
    return moment if 0 <= week_instant(moment) * 10**9 < 2**64 else None


def export_value(column, row):
    """The value of one event column as it leaves the machine, or OMIT."""
    if column == "time":
        return OMIT
    if column == "date":
        moment = event_moment(row)
        return OMIT if moment is None else week_date(moment)
    return row[column]
