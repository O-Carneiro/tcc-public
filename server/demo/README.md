# Demo: collect, store, retrieve

One command, from a fresh clone (with the `kworkflow` submodule checked out):

```sh
demo/run.sh                    # about 45 s, 30 installs
demo/run.sh -n 12              # fewer installs (minimum 8)
demo/run.sh --skip-retrieval   # stop after the storage checks
demo/run.sh --keep             # keep the work directory (the stack is still stopped)
```

Needs: docker with compose, sqlite3, python3 (3.10+), curl, setsid, timeout. The first run pulls
`python:3.12-slim` and the collector image.

What it does:

1. `docker compose` (project `kwdemo`) starts the collector and the sink on free host ports
   (`SINK_PORT` / `COLLECTOR_PORT` override). The collector uses `collector/config.yaml`, which filters
   nothing. Privacy is the user's per-field choice in kw_tele's config (`exclude_fields`, empty by default).
   The first opted-in install with `build_modules` rows sets `exclude_fields=kw.build_modules.module_names`
   as an example exclusion (`module_names` is an example, not a chosen field); the checks expect its records
   to lack that attribute and the other installs' records to have it. Stored times are coarsened to the week.
2. Builds `kw_tele` and creates N installs, each with its own `HOME`, `KW_TELE_CONFIG`, `KW_DATA_DIR`,
   `KW_STATE_DIR`, a `kw.db` from kw's real `kwdb.sql`, the real `kw`, and the real `kw_tele`.
   - **MOCK:** back-dated history over up to 8 weeks (build, executed, timebox shapes), inserted through
     kw's own insert views. Seeded random values; they say nothing about real users. The shapes are a
     demonstration, not a choice of which data kw collects.
   - Real: one `kw build` (fake kernel tree, stub `make`) and one `kw pomodoro` per install,
     `kw telemetry opt-in` / `opt-out` / `upload`. A few installs never opt in.
3. Checks: sink rows equal locally sent-marked records, no duplicates, second upload sends nothing; sink
   stopped gives `kw telemetry upload` exit 5, restarted gives exit 0 and each row stored once (D23). Also: the example exclusion, every stored time is 0 or a week instant
   (Monday 00:13:00 UTC), and no duplicate (install, event id, kind).
4. Retrieval through `GET /analysis/schemas`, `GET /analysis/schema/{schema}/attributes` and
   `GET /analysis/query` (schema, join, by, measure, bucket=week, one modules-vs-status join query;
   needs a sink that has these endpoints; skip with `--skip-retrieval`).
5. Prints a summary, tears the stack down, exits non-zero if any check failed.

Files: `run.sh` (driver), `install.sh` (one simulated user), `mockdata.py` (MOCK history).
