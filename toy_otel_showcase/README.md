# kw telemetry: collector and sink

The server side of kw's opt-in usage telemetry: an OpenTelemetry collector in front of a small
FastAPI sink that stores records in SQLite. The client is `kw_tele` (`../client/kw_tele`), invoked by kw.

```
kw_tele --OTLP/JSON over HTTP--> collector :4318 --OTLP/JSON--> sink :8000 --> data/telemetry.db
```

## Run the whole thing

```sh
demo/run.sh      # see demo/README.md; MOCK usage data, real kw, kw_tele, collector and sink
```

To start only the servers:

```sh
mkdir -p data
UID=$(id -u) GID=$(id -g) docker compose up -d --build
```

`UID`/`GID` set the user the sink runs as (default 1000:1000; bash does not export `UID`, so pass it).
Without a matching user, `data/` and `telemetry.db` end up root-owned on Linux. Other switches:
`SINK_PORT` (8000), `COLLECTOR_PORT` (4318), `BIND_ADDR` (127.0.0.1), `SINK_DATA_DIR` (`./data`),
`COLLECTOR_CONFIG` (`./collector/config.yaml`).

## Security

There is no authentication on any endpoint, and no TLS. The container listens on 0.0.0.0 inside; compose
publishes the ports on `BIND_ADDR` (default 127.0.0.1). Setting `BIND_ADDR=0.0.0.0` exposes the sink and
the read endpoints to the network. `/analysis/*` returns aggregates of everything stored.

## Collector

`collector/config.yaml`: OTLP/HTTP receiver on 4318, one pipeline.

| pipeline | processors | exporter |
|---|---|---|
| logs | `memory_limiter`, `resource` | `otlphttp/sinklogs` (no queue, no retry) |

The logs exporter is synchronous on purpose: the collector answers 2xx only after the sink has committed,
so a client that gets 2xx may mark its rows sent (D23). With the sink down the client sees a failure.

The Collector filters nothing. Privacy is the user's per-field choice, made on the client: `exclude_fields`
in kw_tele's `telemetry.config` lists attribute keys that are not sent (empty by default, so everything is
sent). `kw.build_modules.module_names` is only an example of such a key; it is not a chosen field. A change
affects later uploads only; rows already stored stay. The sink coarsens stored times to the week (Monday
00:13:00 UTC), so `time_unix_nano`, `observed_time_unix_nano` and `received_at` are week instants.

## Sink endpoints

| endpoint | purpose |
|---|---|
| `POST /v1/logs` | OTLP/JSON log records into `log_record`; unparsable bodies are kept in `log_record_raw` |
| `GET /healthz` | liveness |
| `GET /analysis/schemas` | schemas (record bodies) with record and install counts, discovered from the stored data |
| `GET /analysis/schema/{schema}/attributes` | attributes of one schema with record counts (unknown schema: 404) |
| `GET /analysis/query` | `schema` (required); optional `join` (other schemas, same install and event id), `by` (any number of attributes), numeric `measure`, `bucket` (`week` and/or `install`), repeatable `where=attr:value`, `from`/`to` (`YYYY-MM-DD`) |

Example:

```sh
curl -s localhost:8000/analysis/schemas
curl -s --get localhost:8000/analysis/query \
  --data-urlencode schema=executed --data-urlencode by=kw.executed.command_label \
  --data-urlencode measure=kw.executed.elapsed_time_in_secs --data-urlencode bucket=week
```

Join example (modules per build outcome):
`schema=build_modules&join=executed&by=kw.executed.status&measure=kw.build_modules.module_count`.

Responses carry no real install ids; `bucket=install` labels installs `install N` (stable rank), which still
lets a reader follow one pseudonymous install across queries. A new schema or attribute shows up without a
sink change.

## Tests

```sh
cd sink
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

Client tests: `../client/kw_tele/README.md`. The folder `analysis/` holds a separate mock-data plot demo
(`analysis/README.md`).

## Layout

| path | role |
|---|---|
| `collector/` | collector config (no filter) |
| `sink/` | FastAPI app, Dockerfile, tests |
| `demo/` | end-to-end demo script and mock data |
| `analysis/` | mock-data plots |
| `docker-compose.yml` | collector + sink |
| `data/` | `telemetry.db` (bind mount, git-ignored) |

The real client is `../client/kw_tele`.
