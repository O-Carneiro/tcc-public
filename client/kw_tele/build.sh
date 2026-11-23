#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: build.sh <outfile>" >&2
    exit 2
fi

root=$(cd "$(dirname "$0")" && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
# Stage the sources so stray bytecode caches never end up in the archive.
cp -R "$root/src/kw_tele" "$stage/kw_tele"
find "$stage" -name __pycache__ -prune -exec rm -rf {} +
# zipapp's -m entry point calls main() without sys.exit, which would swallow the exit code.
printf 'import sys\n\nfrom kw_tele.cli import main\n\nsys.exit(main())\n' > "$stage/__main__.py"
python3 -m zipapp "$stage" -o "$1" -p "/usr/bin/env python3"
chmod 755 "$1"
