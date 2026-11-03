#!/usr/bin/env python3
import argparse
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).parent

COMMANDS = [
    ("build", 5, (90, 900), 0.15),
    ("deploy", 4, (30, 300), 0.10),
    ("modules_deploy", 2, (10, 90), 0.05),
    ("list", 6, (1, 4), 0.0),
    ("uninstall", 1, (5, 30), 0.05),
    ("codestyle", 3, (2, 20), 0.0),
]

TAGS = ["amdgpu", "vkms", "patch-review", "docs"]


def seed(db_path: Path, days: int, seed_value: int):
    random.seed(seed_value)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.unlink(missing_ok=True)

    conn = sqlite3.connect(db_path)
    conn.executescript((HERE / "kwdb.sql").read_text())

    today = date.today()
    stats = pomos = 0

    for offset in range(days):
        day = today - timedelta(days=offset)
        if day.weekday() >= 5 and random.random() < 0.7:
            continue

        for label, weight, (lo, hi), fail_chance in COMMANDS:
            for _ in range(random.randint(0, weight)):
                status = "failure" if random.random() < fail_chance else "success"
                elapsed = random.randint(lo, hi)
                if status == "failure":
                    elapsed = max(1, elapsed // 3)
                conn.execute(
                    """INSERT INTO statistics_report
                       (label_name, status, date, time, elapsed_time_in_secs)
                       VALUES (?,?,?,?,?)""",
                    (label, status, day.isoformat(), random_time(), elapsed),
                )
                stats += 1

        for _ in range(random.randint(0, 4)):
            conn.execute(
                """INSERT INTO pomodoro_report
                   (tag_name, date, time, duration, description)
                   VALUES (?,?,?,?,?)""",
                (
                    random.choice(TAGS),
                    day.isoformat(),
                    random_time(),
                    random.choice([900, 1500, 1800, 2700]),
                    random.choice(
                        [
                            "review series",
                            "chase regression",
                            "rebase",
                            "write tests",
                            "",
                        ]
                    ),
                ),
            )
            pomos += 1

    conn.commit()
    conn.close()
    print(
        f"seeded {db_path}: {stats} command events, {pomos} pomodoro sessions "
        f"across {days} days"
    )


def random_time():
    return f"{random.randint(8, 22):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=HERE / "fake_kw_home" / "kw.db")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--seed", type=int, default=1337)
    args = ap.parse_args()
    seed(args.db, args.days, args.seed)
