import os
import re
import tempfile

_INTEGER = re.compile(r"(0|[1-9][0-9]{0,17})")
_BLANKS = " \t\r"


def parse_int(raw, low, high):
    """Integer grammar shared with the bash gate; None when invalid or out of range."""
    if not _INTEGER.fullmatch(raw):
        return None
    value = int(raw)
    return value if low <= value <= high else None


def _read_text(path):
    try:
        with open(path, "rb") as handle:
            return handle.read().decode("utf-8", "replace")
    except FileNotFoundError:
        return ""


def _split_line(line):
    line = line.strip(_BLANKS)
    if line.startswith("#") or "=" not in line:
        return None
    key, value = line.split("=", 1)
    return key.strip(_BLANKS), value.strip(_BLANKS)


def read(path):
    values = {}
    for line in _read_text(path).split("\n"):
        pair = _split_line(line)
        if pair:
            values[pair[0]] = pair[1]
    return values


def update(path, changes):
    """Rewrite known keys in place, append missing ones, keep every other line."""
    lines = _read_text(path).split("\n")
    if lines[-1] == "":
        lines.pop()
    pending = dict(changes)
    for index, line in enumerate(lines):
        pair = _split_line(line)
        if pair and pair[0] in changes:
            lines[index] = f"{pair[0]}={changes[pair[0]]}"
            pending.pop(pair[0], None)
    lines.extend(f"{key}={value}" for key, value in pending.items())
    _atomic_write(path, "\n".join(lines) + "\n")


def _atomic_write(path, text):
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    descriptor, temp = tempfile.mkstemp(dir=directory, prefix=os.path.basename(path) + ".")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(temp, path)
    except BaseException:
        os.unlink(temp)
        raise
