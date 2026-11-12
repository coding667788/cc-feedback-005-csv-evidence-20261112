"""Offline known-answer verification for a published batch.

Reads the generator's gzip JSONL answer files and compares every stored
record field and byte-offset contiguity against the published SQLite batch.
This script is a verification aid; the collector itself never reads answers.

Usage:
    python scripts/answer_check.py --config config.perf.json --data data
"""

from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--data", required=True)
    args = parser.parse_args()

    root = Path.cwd()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    work_root = Path(config["work_root"])
    if not work_root.is_absolute():
        work_root = root / work_root
    pointer = json.loads((work_root / "current.json").read_text(encoding="utf-8"))
    db = Path(pointer["batch_dir"]) / "batch.sqlite3"
    data_dir = Path(args.data)
    manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))

    conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    total = 0
    try:
        for entry in manifest["files"]:
            file_id = entry["file_id"]
            answer_path = data_dir / "answers" / f"{file_id}.jsonl.gz"
            rows = conn.execute(
                "SELECT uid, fields_json, byte_start, byte_end FROM record "
                "WHERE file_id=? ORDER BY row_number",
                (file_id,),
            ).fetchall()
            with gzip.open(answer_path, "rt", encoding="utf-8") as handle:
                answers = [json.loads(line) for line in handle]
            if len(rows) != len(answers) or len(rows) != entry["expected_rows"]:
                print(f"FAIL {file_id}: row count mismatch")
                return 1
            previous_end = None
            for (uid, fields_json, start, end), expected in zip(rows, answers):
                if uid != expected["uid"]:
                    print(f"FAIL {file_id}: uid mismatch {uid}")
                    return 1
                if json.loads(fields_json) != expected["fields"]:
                    print(f"FAIL {file_id}: fields mismatch uid={uid}")
                    return 1
                if end <= start:
                    print(f"FAIL {file_id}: bad span uid={uid}")
                    return 1
                if previous_end is not None and start != previous_end:
                    print(f"FAIL {file_id}: non-contiguous offsets uid={uid}")
                    return 1
                previous_end = end
            total += len(rows)
            print(
                f"{file_id}: {len(rows)} rows match known answers, "
                f"offsets contiguous"
            )
    finally:
        conn.close()
    print(f"TOTAL {total} records verified field-by-field against answers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

