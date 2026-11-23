import os
from pathlib import Path


def parse_kv(text):
    out = {}
    for line in text.replace("\r\n", "\n").split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        data = data.encode()
    tmp = path.with_name(path.name + ".testtmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
