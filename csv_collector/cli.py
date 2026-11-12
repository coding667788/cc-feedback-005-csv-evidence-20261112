"""Command line interface: collect / verify / show / batches / doctor."""

from __future__ import annotations

import argparse
import json
import shutil
import sys

from .collector import BatchCollector, list_staging
from .errors import CollectorError
from .manifest import load_config, load_manifest
from .query import list_batches, resolve_batch, show_record, verify_batch


def _print_json(payload) -> None:
    sys.stdout.buffer.write(
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
            "utf-8"
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m csv_collector",
        description="Streaming CSV collector with byte-level provenance.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="run a manifest-driven collection")
    collect.add_argument("--config", required=True)
    collect.add_argument("--manifest", required=True)

    verify = sub.add_parser("verify", help="verify a published batch offline")
    verify.add_argument("--config", required=True)
    verify.add_argument("--batch-id")
    verify.add_argument(
        "--source-root",
        action="append",
        default=[],
        help="directory containing original CSV files (repeatable)",
    )

    show = sub.add_parser("show", help="show one record and its raw evidence")
    show.add_argument("--config", required=True)
    show.add_argument("--batch-id")
    show.add_argument("--uid", required=True)

    batches = sub.add_parser("batches", help="list published batches")
    batches.add_argument("--config", required=True)

    doctor = sub.add_parser(
        "doctor", help="list unfinished staging directories and the lock"
    )
    doctor.add_argument("--config", required=True)
    doctor.add_argument(
        "--clean-staging",
        action="store_true",
        help="remove unfinished staging directories (lock is never removed)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "collect":
            config = load_config(args.config)
            manifest = load_manifest(args.manifest)
            result = BatchCollector(config, manifest).run()
            _print_json(
                {
                    "status": "published",
                    "batch_id": result.batch_id,
                    "batch_dir": str(result.batch_dir),
                    "files_collected": result.files_collected,
                    "total_rows": result.total_rows,
                }
            )
            return 0

        config = load_config(args.config)
        if args.command == "verify":
            batch_dir = resolve_batch(config.work_root, args.batch_id)
            report = verify_batch(
                batch_dir,
                source_roots=[p for p in args.source_root],
                quiet=config.quiet,
            )
            payload = {
                "batch_dir": str(report.batch_dir),
                "ok": report.ok,
                "files": report.files,
                "records_checked": report.records_checked,
                "bundles_checked": report.bundles_checked,
                "source_cross_checks": report.source_cross_checks,
                "errors": report.errors,
            }
            _print_json(payload)
            return 0 if report.ok else 8

        if args.command == "show":
            batch_dir = resolve_batch(config.work_root, args.batch_id)
            _print_json(show_record(batch_dir, args.uid))
            return 0

        if args.command == "batches":
            _print_json(list_batches(config.work_root))
            return 0

        if args.command == "doctor":
            staging = list_staging(config.work_root)
            lock = config.work_root / "collector.lock"
            payload = {
                "staging_directories": [str(p) for p in staging],
                "lock_present": lock.exists(),
                "lock_details": (
                    lock.read_text(encoding="utf-8") if lock.exists() else None
                ),
            }
            if args.clean_staging:
                for path in staging:
                    shutil.rmtree(path)
                    payload.setdefault("removed", []).append(str(path))
            _print_json(payload)
            return 0
    except CollectorError as exc:
        sys.stderr.write(f"ERROR [{type(exc).__name__}]: {exc}\n")
        return exc.exit_code
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"ERROR [{type(exc).__name__}]: {exc}\n")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

