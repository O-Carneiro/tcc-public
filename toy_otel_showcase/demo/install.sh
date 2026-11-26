#!/usr/bin/env bash
# One simulated kw user with an isolated HOME, real kw (repo mode) and real kw_tele on PATH.
# usage: install.sh <idx> <N>   env: SHOW, WORK, COLLECTOR_PORT, SEED
set -uo pipefail
i="$1"
n="$2"
kwrepo="$SHOW/../kworkflow"
H="$WORK/homes/h$(printf %02d "$i")"
rm -rf "$H"
mkdir -p "$H/bin" "$H/.config/kw" "$H/.local/share/kw" "$H/.local/state/kw" "$H/stub"
ln -s "$kwrepo/kw" "$H/bin/kw"
ln -s "$WORK/bin/kw_tele" "$H/bin/kw_tele"
sqlite3 "$H/.local/share/kw/kw.db" < "$kwrepo/database/kwdb.sql"
sed "s#^endpoint=.*#endpoint=http://127.0.0.1:${COLLECTOR_PORT}/v1/logs#" \
  "$kwrepo/etc/telemetry.config" > "$H/.config/kw/telemetry.config"

# Fake kernel tree plus a stub make, so the real build.sh path runs (including build_modules_record).
T="$H/linux"
mkdir -p "$T"/{Documentation,arch,include,drivers,fs,init,ipc,kernel,lib,scripts}
for f in COPYING CREDITS Kbuild MAINTAINERS Makefile README; do touch "$T/$f"; done
printf '#!/bin/sh\nsleep 1\nexit 0\n' > "$H/stub/make"
chmod +x "$H/stub/make"
: > "$H/mock.log"

kw_env()
{
  env -u XDG_CONFIG_HOME -u XDG_DATA_HOME -u XDG_STATE_HOME -u XDG_CACHE_HOME \
    HOME="$H" KW_TELE_CONFIG="$H/.config/kw/telemetry.config" \
    KW_DATA_DIR="$H/.local/share/kw" KW_STATE_DIR="$H/.local/state/kw" \
    PATH="$H/stub:$H/bin:$PATH" "$@"
}

# MOCK: back-dated history through kw's own insert views (real kw cannot record past events).
python3 "$SHOW/demo/mockdata.py" "$i" "$n" "${SEED:-42}" 2> "$H/profile.txt" |
  sqlite3 "$H/.local/share/kw/kw.db" >> "$H/mock.log" 2>&1

# REAL kw commands (stamped with today): one build with m modules in .config, one pomodoro.
if [ "$i" -ne 1 ] && [ "$i" -ne 2 ]; then
  m=$((5 + (i * 37) % 60))
  for k in $(seq "$m"); do echo "CONFIG_MOCK_$k=m"; done > "$T/.config"
  echo "CONFIG_BUILTIN=y" >> "$T/.config"
fi
(cd "$T" && kw_env kw build > "$H/real_build.log" 2>&1)
kw_env kw pomodoro -t 1s -g real-demo > "$H/real_pomo.log" 2>&1
sleep 1.5

role=in-history
if [ "$i" -ge $((n - 3)) ]; then
  role=never
elif [ "$i" -eq $((n - 4)) ]; then
  role=in-nohistory
fi
case "$role" in
  in-history)
    kw_env kw telemetry opt-in --include-history > "$H/optin.log" 2>&1
    # Example exclusion: the lowest-index opted-in install with build_modules rows excludes one field.
    if [ "$i" -eq 0 ] && [ "$(sqlite3 "$H/.local/share/kw/kw.db" 'SELECT COUNT(*) FROM build_modules')" -gt 0 ]; then
      echo "exclude_fields=kw.build_modules.module_names" >> "$H/.config/kw/telemetry.config"
      echo "kw.build_modules.module_names" > "$H/exclude_fields.txt"
    fi
    kw_env kw telemetry upload > "$H/upload1.log" 2>&1
    echo "upload exit $?" >> "$H/mock.log"
    ;;
  in-nohistory)
    kw_env kw telemetry opt-in > "$H/optin.log" 2>&1
    # Recorded after consent, so it must be sent.
    (cd "$T" && kw_env kw build > "$H/real_build2.log" 2>&1)
    kw_env kw telemetry upload > "$H/upload1.log" 2>&1
    echo "upload exit $?" >> "$H/mock.log"
    ;;
  never)
    kw_env kw telemetry opt-out > "$H/optin.log" 2>&1
    kw_env kw telemetry upload > "$H/upload1.log" 2>&1
    echo "upload exit $? (expected 3)" >> "$H/mock.log"
    ;;
esac
echo "$role" > "$H/role.txt"
