import argparse
import sys

from kw_tele import commands, kvfile, settings
from kw_tele.env import from_environ


def _bounded(key):
    low, high = settings.RANGES[key]

    def parse(raw):
        value = kvfile.parse_int(raw, low, high)
        if value is None:
            raise argparse.ArgumentTypeError(f"{raw!r} is not an integer in {low}..{high}")
        return value

    return parse


def _parser():
    parser = argparse.ArgumentParser(prog="kw_tele", description="kworkflow telemetry uploader")
    commands_ = parser.add_subparsers(dest="command", required=True)

    upload = commands_.add_parser("upload", help="send pending events")
    mode = upload.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="ignore the retry spacing")
    mode.add_argument("--dry-run", action="store_true", help="print the next batch, send nothing")

    consent = commands_.add_parser("consent", help="show or change consent")
    consent.add_argument("--set", choices=("yes", "no"))
    consent.add_argument("--include-history", action="store_true")

    info = commands_.add_parser("info", help="show what is collected and sent")
    shown = info.add_mutually_exclusive_group()
    shown.add_argument("--export", action="store_true")
    shown.add_argument("--pending", action="store_true")

    config = commands_.add_parser("config", help="show or change the upload settings")
    for key in settings.RANGES:
        config.add_argument("--set-" + key.replace("_", "-"), dest=key, type=_bounded(key))
    return parser


def _dispatch(args, env):
    if args.command == "upload":
        if args.dry_run:
            return commands.upload_dry_run(env)
        return commands.upload(env, args.force)
    if args.command == "consent":
        if args.set is None and not args.include_history:
            return commands.consent_show(env)
        return commands.consent_set(env, args.set, args.include_history)
    if args.command == "info":
        if args.export:
            return commands.info_export(env)
        if args.pending:
            return commands.info_pending(env)
        return commands.info_summary(env)
    changes = {key: getattr(args, key) for key in settings.RANGES if getattr(args, key) is not None}
    return commands.config_command(env, changes)


def main(argv=None):
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else commands.EXIT_USAGE
    return _dispatch(args, from_environ())


if __name__ == "__main__":
    sys.exit(main())
