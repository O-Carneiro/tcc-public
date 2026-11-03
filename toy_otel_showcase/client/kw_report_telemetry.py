#!/usr/bin/env python3
import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
SCHEMA_URL = "https://opentelemetry.io/docs/specs/otlp/"

PERIOD_SQL = {
    "day": "date = date('now','localtime')",
    "week": "date >= date('now','localtime','-6 days')",
    "month": "strftime('%Y-%m', date) = strftime('%Y-%m','now','localtime')",
    "year": "strftime('%Y', date) = strftime('%Y','now','localtime')",
    "all": "1 = 1",
}


def fetch_statistics(conn, period):
    """Aggregate statistics_report the way kw report -s does."""
    return conn.execute(
        f"""SELECT label_name, status, COUNT(*),
                   SUM(elapsed_time_in_secs), MIN(elapsed_time_in_secs),
                   MAX(elapsed_time_in_secs)
              FROM statistics_report
             WHERE {PERIOD_SQL[period]}
          GROUP BY label_name, status
          ORDER BY label_name, status"""
    ).fetchall()


def fetch_pomodoro(conn, period):
    """Aggregate pomodoro_report the way kw report -p does."""
    return conn.execute(
        f"""SELECT tag_name, COUNT(*), SUM(duration)
              FROM pomodoro_report
             WHERE {PERIOD_SQL[period]}
          GROUP BY tag_name
          ORDER BY tag_name"""
    ).fetchall()


def _attrs(**kwargs):
    out = []
    for k, v in kwargs.items():
        key = k.replace("__", ".")
        value = {"intValue": str(v)} if isinstance(v, int) else {"stringValue": str(v)}
        out.append({"key": key, "value": value})
    return out


def _sum(name, unit, points, description=""):
    return {
        "name": name,
        "unit": unit,
        "description": description,
        # Cumulative + monotonic: these are lifetime totals from kw's DB, which
        # is exactly what AGGREGATION_TEMPORALITY_CUMULATIVE means.
        "sum": {
            "aggregationTemporality": 2,
            "isMonotonic": True,
            "dataPoints": points,
        },
    }


def build_payload(stats, pomo, period, kw_version, now_ns):
    stat_points, dur_points, pomo_points, session_points = [], [], [], []

    for label, status, total, total_secs, min_secs, max_secs in stats:
        attrs = _attrs(kw__command=label, kw__status=status, kw__period=period)
        stat_points.append(
            {
                "asInt": str(total),
                "attributes": attrs,
                "timeUnixNano": str(now_ns),
                "startTimeUnixNano": str(now_ns),
            }
        )
        # A histogram with one bucket carries count/sum/min/max -- exactly the
        # Total/Max/Min/Average columns kw report prints, and cheap in bash.
        dur_points.append(
            {
                "count": str(total),
                "sum": float(total_secs),
                "min": float(min_secs),
                "max": float(max_secs),
                "bucketCounts": [str(total)],
                "explicitBounds": [],
                "attributes": attrs,
                "timeUnixNano": str(now_ns),
                "startTimeUnixNano": str(now_ns),
            }
        )

    for tag, sessions, focus_secs in pomo:
        attrs = _attrs(kw__tag=tag, kw__period=period)
        session_points.append(
            {
                "asInt": str(sessions),
                "attributes": attrs,
                "timeUnixNano": str(now_ns),
                "startTimeUnixNano": str(now_ns),
            }
        )
        pomo_points.append(
            {
                "asInt": str(focus_secs),
                "attributes": attrs,
                "timeUnixNano": str(now_ns),
                "startTimeUnixNano": str(now_ns),
            }
        )

    metrics = []
    if stat_points:
        metrics.append(
            _sum(
                "kw.commands.total", "1", stat_points, "Number of kw commands executed"
            )
        )
        metrics.append(
            {
                "name": "kw.command.duration",
                "unit": "s",
                "description": "Wall-clock time spent in kw commands",
                "histogram": {"aggregationTemporality": 2, "dataPoints": dur_points},
            }
        )
    if session_points:
        metrics.append(
            _sum(
                "kw.pomodoro.sessions",
                "1",
                session_points,
                "Completed pomodoro sessions",
            )
        )
        metrics.append(
            _sum("kw.pomodoro.focus_time", "s", pomo_points, "Focus time per tag")
        )

    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": _attrs(
                        service__name="kworkflow",
                        service__version=kw_version,
                        telemetry__sdk__name="kw-report",
                        telemetry__sdk__language="bash",
                    )
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "kw.report", "version": kw_version},
                        "metrics": metrics,
                    }
                ],
            }
        ]
    }


def export(endpoint, payload, timeout=10):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode()


def print_report(stats, pomo, period):
    print(f"# Statistics Report: {period}")
    if stats:
        print(f"{'':14s}{'Total':>6s}{'Max':>10s}{'Min':>10s}{'Average':>10s}")
        for label, status, total, total_secs, min_secs, max_secs in stats:
            name = label if status == "success" else f"{label} ({status})"
            print(
                f"{name:<14s}{total:>6d}{hms(max_secs):>10s}"
                f"{hms(min_secs):>10s}{hms(total_secs // total):>10s}"
            )
    else:
        print("  no statistics in this period")

    print(f"\n# Pomodoro Report: {period}")
    if pomo:
        total_secs = sum(p[2] for p in pomo)
        total_sessions = sum(p[1] for p in pomo)
        print(f"- Total focus time from all tags: {hms(total_secs)}")
        print(f"- Number of sessions from all tags: {total_sessions}")
        for tag, sessions, secs in pomo:
            print(f"  {tag:<14s} {hms(secs)}  ({sessions} sessions)")
    else:
        print("  no pomodoro sessions in this period")


def hms(secs):
    secs = int(secs)
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=HERE / "fake_kw_home" / "kw.db")
    ap.add_argument("--period", choices=PERIOD_SQL, default="all")
    ap.add_argument(
        "--endpoint",
        default="http://localhost:4318/v1/metrics",
        help="OTLP/HTTP metrics endpoint",
    )
    ap.add_argument("--kw-version", default="unstable-2026.08")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="print the OTLP payload instead of sending it",
    )
    args = ap.parse_args()

    if not args.db.exists():
        raise SystemExit(f"{args.db} not found -- run seed_kw_db.py first")

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    stats = fetch_statistics(conn, args.period)
    pomo = fetch_pomodoro(conn, args.period)
    conn.close()

    print_report(stats, pomo, args.period)

    payload = build_payload(stats, pomo, args.period, args.kw_version, time.time_ns())
    n = sum(
        len(m.get("sum", m.get("histogram", {})).get("dataPoints", []))
        for m in payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
    )

    if args.dry_run:
        print(f"\n# OTLP payload ({n} data points), see {SCHEMA_URL}")
        print(json.dumps(payload, indent=2))
        return

    if n == 0:
        print("\nnothing to export")
        return

    print(f"\n# Exporting {n} data points to {args.endpoint}")
    try:
        status, body = export(args.endpoint, payload)
    except urllib.error.URLError as e:
        raise SystemExit(f"export failed: {e}\nis the stack up? docker compose up -d")
    print(f"  HTTP {status} {body.strip() or '(empty)'}")


if __name__ == "__main__":
    main()
