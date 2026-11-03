# kw + OpenTelemetry: a toy showcase

A minimal, working demonstration of shipping [kworkflow](https://kworkflow.org)
usage statistics as OpenTelemetry metrics, storing them in SQLite.

```
  client/  (host, python, stdlib only)          |  dockerized
  kw_report_telemetry.py                        |
      reads fake_kw_home/kw.db                  |
                ---- OTLP/JSON over HTTP  ----->|  [ collector ]   otel binary, config only
                                    :4318       |        |
                                                |        |  OTLP/JSON
                                                |        v
                                                |  [ sink ]  FastAPI -> telemetry.db
                                                |     :8000
```

There are two SQLite databases, and the distinction matters:

| database                    | role                                                          |
|-----------------------------|---------------------------------------------------------------|
| `client/fake_kw_home/kw.db` | **source.** Stands in for a developer's real `~/.local/share/kw/kw.db`. Raw kw events, seeded. Client-side. |
| `data/telemetry.db`         | **destination.** The metrics store, written only by the sink. Dockerized. |

The client reads SQLite because that is where `kw` already keeps its data: every
`kw build` / `kw deploy` writes a row, and `kw report` reads it back to print a
summary. So the client's job is inherently "read the existing DB, aggregate,
ship" — the same `GROUP BY` that `kw report -s` does today, POSTed instead of
printed.

The client stands in for a future `kw report --telemetry`. The collector and sink
are dockerized, so the "server" is isolated from the client exactly as it would
be in reality.

## Quick start

```sh
docker compose up -d --build     # start collector + sink
python3 client/seed_kw_db.py     # fake 30 days of kw activity -> client/fake_kw_home/kw.db
python3 client/kw_report_telemetry.py --period week
```

The client prints a `kw report`-style summary, then exports it. To confirm the
round trip, ask the server to rebuild that same summary from SQLite:

```sh
curl -s localhost:8000/stats | python3 -m json.tool
sqlite3 data/telemetry.db 'SELECT * FROM metric_point LIMIT 5;'
docker compose logs sink | grep points          # one line per batch stored
```

To watch the raw OTLP arrive at the collector, tail it in a second terminal
*before* exporting — `-f` blocks waiting for new output, so run it first, then
export from another shell, then Ctrl+C:

```sh
docker compose logs -f --tail=0 collector       # terminal 1, then export in terminal 2
docker compose logs --tail=40 collector         # or just read the last batch after the fact
```

The numbers from `/stats` match what the client printed. Useful flags:
`--dry-run` (print the OTLP payload without sending),
`--period day|week|month|year|all`, `--endpoint`.

## Design decisions

**Why OTLP/JSON over HTTP, not gRPC or protobuf.** The real client will be bash.
Bash cannot serialize protobuf or speak HTTP/2 framing, so OTLP/JSON over HTTP
is the only viable transport — and it's a good fit regardless: `kw` is a
short-lived CLI, so it gains nothing from gRPC's long-lived multiplexed
connection, and `grpcio` is a heavy compiled dependency to inflict on
contributors. Plain HTTP/1.1 on one port also survives proxies and
TLS-terminating load balancers, and debugs with `curl`.

**Why the Python client avoids the OTel SDK.** It uses only the standard library,
so every step has a direct bash equivalent. Using the SDK would mean relying on
abstractions with no bash counterpart, making the eventual port a rewrite rather
than a transcription:

| client step  | Python        | bash                                     |
|--------------|---------------|------------------------------------------|
| `fetch_*`    | `sqlite3`     | `sqlite3 -separator '\|' "$kw_db" 'SELECT …'` |
| `build_*`    | dict → `json` | `jq -n` constructing the payload         |
| `export`     | `urllib`      | `curl -X POST --data-binary @-`          |

**Metrics, not traces.** `kw report` is inherently aggregate (totals, min, max,
average per command), which maps onto cumulative sums and histograms. Each
`kw.command.duration` point is a single-bucket histogram carrying
`count`/`sum`/`min`/`max` — precisely the Total/Max/Min/Average columns
`kw report` prints, and cheap to build in bash.

Metrics emitted: `kw.commands.total`, `kw.command.duration`,
`kw.pomodoro.sessions`, `kw.pomodoro.focus_time`.

## Layout

| path                          | role                                            |
|-------------------------------|-------------------------------------------------|
| `client/kw_report_telemetry.py` | (1) the client — reads kw's db, exports OTLP    |
| `client/seed_kw_db.py`        | generates plausible kw activity                 |
| `client/fake_kw_home/kw.db`   | the seeded source db (stands in for the user's) |
| `client/kwdb.sql`             | vendored copy of kw's real schema               |
| `collector/config.yaml`       | (2) the server — the entire collector config    |
| `sink/app.py`                 | (3) OTLP receiver → SQLite                      |
| `data/telemetry.db`           | the resulting database (bind-mounted)           |

`client/kwdb.sql` is copied verbatim from kworkflow, so seeded rows pass through
the real `statistics_report` / `pomodoro_report` insert triggers.

## Notes for real integration

- The client reads the seeded `client/fake_kw_home/kw.db` by default. Nothing is
  tied to that fixture — point `--db` at the real `~/.local/share/kw/kw.db` to
  ship actual usage.
- `pipeline.hop` is stamped by the collector's `resource` processor, proving the
  data traversed the collector rather than going straight to the sink.
- The sink uses SQLite's rollback journal, not WAL. WAL's `-shm` index does not
  work across a macOS bind mount, so `sqlite3 data/telemetry.db` on the host
  would read a stale snapshot while the container saw newer rows.
- Telemetry must be opt-in. kw already has a `disable_statistics_data_track`
  config option; a real implementation should gate export behind explicit
  consent and let users see the payload first (`--dry-run`).
