#!/usr/bin/env python3
"""MOCK usage generator for one install. Prints SQL that is fed to the install's own kw.db through kw's
real insert views/triggers (build_modules_report, statistics_report, pomodoro_report). All values are random
(seeded) and say nothing about real kw users. The record shapes (build, executed,
timebox) are a demonstration of the pipeline, not a choice of which data kw collects.
usage: mockdata.py <idx> <N> [seed]   -> SQL on stdout; profile summary on stderr
"""
import datetime, random, sys

idx, n = int(sys.argv[1]), int(sys.argv[2])
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42
rng = random.Random(seed * 1000 + idx)
today = datetime.date.today()

grp = idx % 3                       # 0 heavy builder, 1 light config, 2 mixed
if idx in (1, 2):
    modules = lambda: 0              # installs without a .config
elif idx in (0, 3, 6):
    modules = lambda: rng.randint(520, 700)          # installs with many modules
elif grp == 0:
    modules = lambda: rng.randint(200, 480)
elif grp == 1:
    modules = lambda: rng.choice([rng.randint(10, 49), rng.randint(60, 190)])
else:
    modules = lambda: rng.randint(100, 330)

weeks_active = rng.randint(2, 8)    # installs differ in how long they have been around
join_day = rng.randint(0, 7 * (8 - weeks_active) + 6) if idx % 5 == 0 else 0   # some join late
other = []                          # (label, mean_secs, runs)
if grp in (0, 2):
    other.append(("deploy", 45, rng.randint(2, 10)))
if grp == 1:
    other.append(("list", 2, rng.randint(2, 8)))
if idx in (2, 5):
    other.append(("uninstall", 12, rng.randint(1, 3)))           
if idx % 7 == 3:
    other.append(("modules_deploy", 30, rng.randint(1, 4)))
pomo_tag = "rare-tag" if idx in (4, 5, 7) else ("kernel" if idx % 2 == 0 else None)  

def day():
    back = rng.randint(1, 7 * weeks_active)
    back = max(1, back - join_day) if join_day else back
    return (today - datetime.timedelta(days=back)).isoformat()

def hms():
    return "%02d:%02d:%02d" % (rng.randint(8, 22), rng.randint(0, 59), rng.randint(0, 59))

out = []
nb = rng.randint(4, 22)
for _ in range(nb):
    cnt = modules()
    secs = max(1, int(rng.lognormvariate(0, 0.35) * (60 + cnt * 1.1)))
    if rng.random() < 0.12:
        out.append(f"INSERT INTO statistics_report (label_name,status,date,time,elapsed_time_in_secs) VALUES ('build','failure','{day()}','{hms()}',{secs // 3});")
    else:
        names = ",".join(sorted("MOD_%d" % rng.randint(1, 900) for _ in range(min(cnt, 5)))) if cnt else ""
        out.append("INSERT INTO build_modules_report (label_name,status,date,time,elapsed_time_in_secs,module_count,module_names) "
                   f"VALUES ('build','success','{day()}','{hms()}',{secs},{cnt},'{names}');")
for label, mean, runs in other:
    for _ in range(runs):
        st = "failure" if rng.random() < 0.08 else "success"
        out.append(f"INSERT INTO statistics_report (label_name,status,date,time,elapsed_time_in_secs) VALUES ('{label}','{st}','{day()}','{hms()}',{max(0, int(rng.expovariate(1 / mean)))});")
if pomo_tag:
    for _ in range(rng.randint(2, 9)):
        out.append(f"INSERT INTO pomodoro_report (tag_name,date,time,duration,description) VALUES ('{pomo_tag}','{day()}','{hms()}',{rng.choice([900, 1500, 1800])},'mock');")
print("\n".join(out))
print(f"idx={idx} group={grp} builds={nb} other={[o[0] for o in other]} pomo={pomo_tag} weeks={weeks_active} join_day={join_day}", file=sys.stderr)
