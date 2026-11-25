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
the read endpoints to the network. `/stats/logs` and `/analysis/*` return aggregates of everything stored.

## Collector

`collector/config.yaml`: OTLP/HTTP receiver on 4318, two pipelines.

| pipeline | processors | exporter |
|---|---|---|
| metrics | `resource` (stamps `pipeline.hop`), `batch` | `debug`, `otlphttp/sink` (queued) |
| logs | `resource`, `attributes/privacy` | `otlphttp/sinklogs` (no queue, no retry) |

The logs exporter is synchronous on purpose: the collector answers 2xx only after the sink has committed,
so a client that gets 2xx may mark its rows sent (D23). With the sink down the client sees a failure.

`attributes/privacy` deletes `kw.build_modules.module_names` before export. It is a per-field filter
at the collector: extending or removing it needs no change in the client. Trade-off: with it on, module
names never reach storage; with it off, names that can identify a machine or project do.
`collector/config.no-filter.yaml` is the same config without the processor; the demo uses it, and
`COLLECTOR_CONFIG=./collector/config.no-filter.yaml` selects it for compose.

## Sink endpoints

| endpoint | purpose |
|---|---|
| `POST /v1/metrics` | OTLP/JSON metrics into `metric_point` |
| `POST /v1/logs` | OTLP/JSON log records into `log_record`; unparsable bodies are kept in `log_record_raw` |
| `GET /healthz` | liveness |
| `GET /stats` | metrics summary |
| `GET /stats/logs` | `kw report`-style aggregates over the stored log records |
| `GET /analysis/schema` | kinds, attributes and counts, discovered from the stored data |
| `GET /analysis/group` | group by up to 3 attributes, optional numeric `measure` and `bucket=day\|week` |

Example:

```sh
curl -s localhost:8000/analysis/schema
curl -s --get localhost:8000/analysis/group \
  --data-urlencode kind=executed --data-urlencode by=kw.executed.command_label \
  --data-urlencode measure=kw.executed.elapsed_time_in_secs --data-urlencode bucket=week
```

Responses carry no install ids. A new record kind or attribute shows up in the schema without a sink change.

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
| `collector/` | collector configs (default, and without the privacy filter) |
| `sink/` | FastAPI app, Dockerfile, tests |
| `demo/` | end-to-end demo script and mock data |
| `analysis/` | mock-data plots |
| `docker-compose.yml` | collector + sink |
| `data/` | `telemetry.db` (bind mount, git-ignored) |

`client/` in this folder is the older metrics-only prototype (`kw_report_telemetry.py`); the real client is
`../client/kw_tele`.
