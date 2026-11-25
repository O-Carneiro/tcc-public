#!/usr/bin/env bash
# End-to-end demo: real kw + real kw_tele -> real collector -> real sink -> retrieval API.
# The usage data is MOCK (seeded, back-dated); see demo/README.md.
# usage: demo/run.sh [-n INSTALLS] [--skip-retrieval] [--keep]
# env:   SINK_PORT, COLLECTOR_PORT (default: first free port at or above 18000 / 14318), SEED (42)
# shellcheck disable=SC2016  # bash -c bodies are meant to expand their own $1
set -uo pipefail

SHOW="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
N=30
SKIP_RETRIEVAL=0
KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    -n)
      N="$2"; shift
      ;;
    --skip-retrieval) SKIP_RETRIEVAL=1 ;;
    --keep) KEEP=1 ;;
    *)
      echo "usage: $0 [-n INSTALLS] [--skip-retrieval] [--keep]" >&2; exit 2
      ;;
  esac
  shift
done
[ "$N" -ge 8 ] || {
  echo "-n must be at least 8" >&2; exit 2
}

for t in docker sqlite3 python3 curl setsid timeout; do
  command -v "$t" > /dev/null || {
    echo "missing: $t" >&2; exit 2
  }
done
docker compose version > /dev/null 2>&1 || {
  echo "missing: docker compose" >&2; exit 2
}
[ -x "$SHOW/../kworkflow/kw" ] || {
  echo "kworkflow submodule missing: git submodule update --init" >&2; exit 2
}

free_port()
{
  python3 - "$1" << 'PY'
import socket, sys
p = int(sys.argv[1])
while True:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", p))
        print(p)
        break
    except OSError:
        p += 1
    finally:
        s.close()
PY
}
SINK_PORT="${SINK_PORT:-$(free_port 18000)}"
COLLECTOR_PORT="${COLLECTOR_PORT:-$(free_port 14318)}"
[ "$SINK_PORT" != "$COLLECTOR_PORT" ] || COLLECTOR_PORT="$(free_port $((COLLECTOR_PORT + 1)))"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/kwdemo.XXXXXX")"
DB="$WORK/data/telemetry.db"
SINK="http://127.0.0.1:$SINK_PORT"
export SHOW WORK COLLECTOR_PORT SEED="${SEED:-42}"

# The sink must write as the invoking user so the host can read and remove data/.
dc()
{
  env UID="$(id -u)" GID="$(id -g)" SINK_PORT="$SINK_PORT" COLLECTOR_PORT="$COLLECTOR_PORT" \
    SINK_DATA_DIR="$WORK/data" COLLECTOR_CONFIG="$SHOW/collector/config.no-filter.yaml" \
    docker compose -p kwdemo --project-directory "$SHOW" "$@"
}
cleanup()
{
  dc down -v --remove-orphans > /dev/null 2>&1
  if [ "$KEEP" = 1 ]; then echo "kept: $WORK (stack stopped)"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

FAILS=0
EMPTY="{}"
pass()
{
  echo "  PASS  $1"
}
fail()
{
  echo "  FAIL  $1${2:+   [$2]}"
  FAILS=$((FAILS + 1))
}
check()
{ # check <description> <command...>
  local d="$1"
  shift
  if "$@"; then pass "$d"; else fail "$d"; fi
}
stage()
{
  echo "== $1"
}
die()
{
  echo "FATAL: $1" >&2; exit 1
}

homes()
{
  ls -d "$WORK"/homes/h*
}
kw_in()
{ # kw_in <home> <args...>
  local h="$1"
  shift
  env -u XDG_CONFIG_HOME -u XDG_DATA_HOME -u XDG_STATE_HOME -u XDG_CACHE_HOME \
    HOME="$h" KW_TELE_CONFIG="$h/.config/kw/telemetry.config" \
    KW_DATA_DIR="$h/.local/share/kw" KW_STATE_DIR="$h/.local/state/kw" \
    PATH="$h/stub:$h/bin:$PATH" kw "$@"
}
sink_count()
{
  sqlite3 -readonly "$DB" 'SELECT COUNT(*) FROM log_record'
}
local_sent()
{ # sent-marked events plus their build_modules rows = records the sink must hold
  sqlite3 "$1/.local/share/kw/kw.db" \
    "SELECT (SELECT COUNT(*) FROM event WHERE sent_at>0)
          + (SELECT COUNT(*) FROM build_modules b JOIN event e ON e.id=b.id WHERE e.sent_at>0)"
}
local_pending()
{
  sqlite3 "$1/.local/share/kw/kw.db" 'SELECT COUNT(*) FROM event WHERE sent_at IS NULL'
}
total_local_sent()
{
  local t=0 h
  for h in $(homes); do t=$((t + $(local_sent "$h"))); done
  echo "$t"
}
wait_http()
{
  local _
  for _ in $(seq 60); do
    curl -s -o /dev/null "$1" && return 0
    sleep 0.5
  done
  return 1
}
role()
{
  cat "$1/role.txt"
}
by_role()
{
  local h
  for h in $(homes); do [ "$(role "$h")" = "$1" ] && echo "$h"; done
}

stage "1. start collector (module-name filter off) + sink: ports sink=$SINK_PORT collector=$COLLECTOR_PORT"
mkdir -p "$WORK/data" "$WORK/bin"
dc up -d --build --wait > "$WORK/compose.log" 2>&1 || {
  tail -n 20 "$WORK/compose.log"
  die "docker compose up failed"
}
ready_fail() {
  dc logs --tail 40 > "$WORK/ready.log" 2>&1
  cat "$WORK/ready.log" >&2
  die "$1"
}
wait_ready() { # sink /healthz up and the collector forwards an empty OTLP logs POST (2xx)
  local _
  for _ in $(seq 90); do
    if curl -fsS -o /dev/null "$SINK/healthz" 2> /dev/null \
      && curl -fsS -o /dev/null -H 'Content-Type: application/json' -d '{"resourceLogs":[]}' \
        "http://127.0.0.1:$COLLECTOR_PORT/v1/logs" 2> /dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}
wait_ready || ready_fail "stack not ready after 90 s (sink $SINK, collector port $COLLECTOR_PORT)"

stage "2. build kw_tele, create $N isolated installs (MOCK history + real kw build/pomodoro)"
bash "$SHOW/../client/kw_tele/build.sh" "$WORK/bin/kw_tele" || die "kw_tele build failed"
seq 0 $((N - 1)) | xargs -P 8 -I{} "$SHOW/demo/install.sh" {} "$N"
echo "  installs: $(homes | wc -l)"

stage "3. collection and storage"
for h in $(by_role never); do
  check "$(basename "$h"): never opted in, nothing marked sent" test "$(local_sent "$h")" = 0
done
expected="$(total_local_sent)"
got="$(sink_count)"
check "sink log_record rows ($got) == locally sent-marked records ($expected)" test "$got" = "$expected" -a "$expected" -gt 0
dups="$(sqlite3 -readonly "$DB" 'SELECT COUNT(*) FROM (SELECT 1 FROM log_record GROUP BY service_instance_id,event_id,body,attributes HAVING COUNT(*)>1)')"
check "no duplicate record in the sink" test "$dups" = 0
outs=""
for h in $(by_role in-history); do outs="$outs$(kw_in "$h" telemetry upload 2>&1 | tail -n 1)"$'\n'; done
check "second upload on every opted-in install: nothing to send, count unchanged" \
  bash -c '! grep -v "nothing to send" <<<"$1" | grep -q .' _ "$(sed '/^$/d' <<< "$outs")"
check "sink count unchanged after second upload" test "$(sink_count)" = "$got"

stage "4. sink down -> exit 5 -> recovery -> each row once (D23)"
h="$(by_role in-history | sed -n 3p)"
(cd "$h/linux" && echo CONFIG_DEMO=m > .config && kw_in "$h" build > /dev/null 2>&1 && kw_in "$h" build > /dev/null 2>&1)
pend="$(local_pending "$h")"
before="$(sink_count)"
dc stop sink > /dev/null 2>&1
kw_in "$h" telemetry upload > "$WORK/down.log" 2>&1
rc=$?
check "sink down: upload exits 5 (got $rc), pending stays $pend" test "$rc" = 5 -a "$(local_pending "$h")" = "$pend" -a "$pend" -ge 2
dc start sink > /dev/null 2>&1
wait_http "$SINK/healthz" || die "sink did not come back"
kw_in "$h" telemetry upload > "$WORK/up.log" 2>&1
rc=$?
check "after recovery: upload exits 0 (got $rc), pending 0" test "$rc" = 0 -a "$(local_pending "$h")" = 0
check "sink holds exactly the sent-marked records, no duplicates" \
  test "$(sink_count)" = "$(total_local_sent)" -a "$(sink_count)" -gt "$before" \
  -a "$(sqlite3 -readonly "$DB" 'SELECT COUNT(*) FROM (SELECT 1 FROM log_record GROUP BY service_instance_id,event_id,body,attributes HAVING COUNT(*)>1)')" = 0

if [ "$SKIP_RETRIEVAL" = 1 ]; then
  stage "5. retrieval: skipped (--skip-retrieval)"
else
  stage "5. retrieval through /analysis/schema and /analysis/group"
  get()
  {
    curl -fsS --get "$SINK$1" "${@:2}"
  }
  schema="$(get /analysis/schema)" || fail "GET /analysis/schema"
  kinds="$(python3 -c 'import json,sys; print(" ".join(k["kind"] for k in json.loads(sys.argv[1])["kinds"]))' "${schema:-$EMPTY}" 2> /dev/null)"
  check "schema lists executed, build_modules, timebox (got: $kinds)" \
    bash -c 'for k in executed build_modules timebox; do [[ " $1 " == *" $k "* ]] || exit 1; done' _ "$kinds"
  check "schema shows kw.build_modules.module_names (collector filter off)" \
    bash -c 'grep -q "kw.build_modules.module_names" <<<"$1"' _ "$schema"
  g1="$(get /analysis/group --data-urlencode kind=executed --data-urlencode by=kw.executed.command_label \
    --data-urlencode measure=kw.executed.elapsed_time_in_secs --data-urlencode bucket=week)" || fail "GET /analysis/group executed"
  g2="$(get /analysis/group --data-urlencode kind=build_modules --data-urlencode by=kw.build_modules.module_count)" || fail "GET /analysis/group build_modules"
  g3="$(get /analysis/group --data-urlencode kind=timebox --data-urlencode by=kw.timebox.tag --data-urlencode bucket=day)" || fail "GET /analysis/group timebox"
  sum_records()
  {
    python3 -c 'import json,sys; print(sum(g["records"] for g in json.loads(sys.argv[1])["groups"]))' "${1:-$EMPTY}" 2> /dev/null
  }
  for kind in executed build_modules timebox; do
    case "$kind" in executed) g="$g1" ;; build_modules) g="$g2" ;; *) g="$g3" ;; esac
    want="$(sqlite3 -readonly "$DB" "SELECT COUNT(*) FROM log_record WHERE body='$kind'")"
    check "group $kind: records sum ($(sum_records "$g")) == sink rows ($want)" test "$(sum_records "$g")" = "$want"
  done
  check "executed measure: every record numeric (non_numeric 0, measured == records)" \
    python3 -c 'import json,sys; g=json.loads(sys.argv[1])["groups"]; sys.exit(0 if g and all(x["non_numeric"]==0 and x["measured"]==x["records"] for x in g) else 1)' "$g1"
  check "responses carry no install ids" bash -c '! grep -q service_instance_id <<<"$1"' _ "$schema$g1$g2$g3"
  echo "  executed by command_label (per week), first groups:"
  python3 -c 'import json,sys
for g in json.loads(sys.argv[1])["groups"][:4]:
    print("   ", g["bucket"], g["kw.executed.command_label"], "records", g["records"], "installs", g["installs"], "measured", g["measured"], "mean_secs", round(g["mean"], 1))' "${g1:-$EMPTY}" 2> /dev/null
fi

stage "summary"
echo "  installs: $(homes | wc -l) (roles: $(for h in $(homes); do role "$h"; done | sort | uniq -c | tr '\n' ' '))"
echo "  records in sink: $(sink_count); log_record_raw: $(sqlite3 -readonly "$DB" 'SELECT COUNT(*) FROM log_record_raw')"
if [ "$FAILS" -eq 0 ]; then
  echo "RESULT: all checks passed (usage data is MOCK)"
else
  echo "RESULT: $FAILS check(s) FAILED"
  exit 1
fi
