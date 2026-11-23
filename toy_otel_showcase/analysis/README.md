# Sink analysis demo (MOCKED data)

All data is generated randomly and says nothing about real kw users.

```sh
pip install -r requirements.txt -r ../sink/requirements.txt
cd ../sink && SINK_DB_PATH=/tmp/demo/t.db uvicorn app:app --port 8765 &
cd ../analysis
python generate.py --sink http://127.0.0.1:8765 --seed 42
python plot.py --sink http://127.0.0.1:8765 --out out
pytest test_generate.py
```

- The generator is not idempotent: use a fresh DB per run, or counts add up.
- The demo assumes the default K=5. Rare vendor names sit on 1-3 installs, so KANON_K below 5 would show them.
- Aggregates reveal distributions with the install count behind each group; groups under K are dropped or merged.
- Limitation: K counts client-supplied service_instance_id values. Someone posting fake ids can push a group over K, or difference a target group.
- `max_secs` in /analysis/commands is a single run's value.
