# kw_tele

Opt-in telemetry uploader for [kworkflow](https://github.com/kworkflow/kworkflow).
Installed by kw's `setup.sh` and invoked through a thin shim in kw, following
the same model as `patch-hub`.

Python 3.10+, standard library only.

## Build

    ./build.sh /path/to/kw_tele

This produces a single-file executable (a zipapp of `src/`) with entry point
`kw_tele.cli:main`.

## Tests

The tests need pytest, which is a development dependency only.

    python3 -m venv .venv
    .venv/bin/pip install -r requirements-dev.txt
    .venv/bin/python -m pytest -q -m "not slow"   # fast suite
    .venv/bin/python -m pytest -q -m slow         # waits for the 60 s deadline
