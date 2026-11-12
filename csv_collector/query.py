"""Offline, read-only query and verification entry points."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from .errors import BatchError
from .parser import StreamingCSVParser
from .storage import open_published, read_evidence


def resolve_batch(work_root: Path, batch_id: str | None = None) -> Path:
    if batch_id is not None:
        candidate = (work_root / "batches" / batch_id).resolve()
    else:
        pointer = work_root / "current.json"
        if not pointer.exists():
            raise BatchError(
                f"no current pointer at {pointer}; pass --batch-id"
            )
        payload = json.loads(pointer.read_text(encoding="utf-8"))
        candidate = Path(payload["batch_dir"]).resolve()
    published_root = (work_root / "batches").resolve()
    if published_root not in candidate.parents and candidate != published_root:
        raise BatchError(f"batch path escapes published root: {candidate}")
    if not candidate.is_dir():
        raise BatchError(f"batch directory not found: {candidate}")
    return candidate


def list_batches(work_root: Path) -> list[dict]:
    batches_dir = work_root / "batches"
    if not batches_dir.exists():
        return []
    result = []
    for item in sorted(batches_dir.iterdir()):
        if not item.is_dir():
            continue
        db = item / "batch.sqlite3"
        if not db.exists():
            continue
        row_count = None
        try:
            conn = open_published(db)
            row_count = conn.execute(
                "SELECT COUNT(*) FROM record"
            ).fetchone()[0]
            conn.close()
        except BatchError:
            pass
        result.append(
            {"batch_id": item.name, "path": str(item), "rows": row_count}
        )
    return result


@dataclass
class VerifyReport:
    batch_dir: Path
    files: int = 0
    records_checked: int = 0
    bundles_checked: int = 0
    errors: list[str] = field(default_factory=list)
    source_cross_checks: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def _bundle_path(batch_dir: Path, relative_path: str) -> Path:
    path = (batch_dir / relative_path).resolve()
    root = batch_dir.resolve()
    if root not in path.parents:
        raise BatchError(f"evidence path escapes batch directory: {path}")
    return path


def verify_batch(
    batch_dir: Path,
    source_roots: list[Path] | None = None,
    quiet: bool = False,
) -> VerifyReport:
    """Verify a published batch without modifying anything.

    Checks bundle existence/hash, per-record fragment hash, re-parses each
    stored fragment and compares it with the stored fields, validates offset
    ordering, and optionally cross-checks fragments against the original
    source files.
    """
    batch_dir = batch_dir.resolve()
    source_roots = [Path(root) for root in (source_roots or [])]
    report = VerifyReport(batch_dir=batch_dir)
    conn = open_published(batch_dir / "batch.sqlite3")
    try:
        sources = {
            row["file_id"]: dict(row)
            for row in conn.execute("SELECT * FROM source_file")
        }
        report.files = len(sources)
        bundles = {
            (row["file_id"], row["bundle_index"]): dict(row)
            for row in conn.execute("SELECT * FROM evidence_bundle")
        }
        source_handles: dict[str, object] = {}
        try:
            for (file_id, bundle_index), bundle in bundles.items():
                report.bundles_checked += 1
                path = _bundle_path(batch_dir, bundle["path"])
                if not path.exists():
                    report.errors.append(
                        f"missing evidence bundle: {path}"
                    )
                    continue
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                if digest != bundle["sha256"]:
                    report.errors.append(
                        f"bundle hash mismatch for {file_id}#{bundle_index}: "
                        f"stored {bundle['sha256']} actual {digest}"
                    )
                if path.stat().st_size != bundle["size_bytes"]:
                    report.errors.append(
                        f"bundle size mismatch for {file_id}#{bundle_index}"
                    )

            last_end: dict[str, int] = {}
            rows = conn.execute(
                "SELECT * FROM record ORDER BY file_id, row_number"
            )
            for row in rows:
                report.records_checked += 1
                file_id = row["file_id"]
                key = (file_id, row["evidence_bundle_index"])
                bundle = bundles.get(key)
                if bundle is None:
                    report.errors.append(
                        f"record {row['uid']} references unknown bundle {key}"
                    )
                    continue
                bundle_full_path = _bundle_path(batch_dir, bundle["path"])
                if not bundle_full_path.exists():
                    report.errors.append(
                        f"missing evidence bundle while checking uid="
                        f"{row['uid']}: {bundle_full_path}"
                    )
                    continue
                try:
                    raw = read_evidence(
                        bundle_full_path,
                        row["evidence_offset"],
                        row["evidence_length"],
                    )
                except (BatchError, OSError) as exc:
                    report.errors.append(str(exc))
                    continue
                digest = hashlib.sha256(raw).hexdigest()
                if digest != row["record_sha256"]:
                    report.errors.append(
                        f"record fragment tampered or corrupted: uid="
                        f"{row['uid']} stored {row['record_sha256']} "
                        f"actual {digest}"
                    )
                if row["byte_start"] >= row["byte_end"]:
                    report.errors.append(
                        f"invalid byte span for uid={row['uid']}"
                    )
                previous_end = last_end.get(file_id)
                if previous_end is not None and row["byte_start"] < previous_end:
                    report.errors.append(
                        f"overlapping/out-of-order span for uid={row['uid']}"
                    )
                last_end[file_id] = row["byte_end"]
                stored_fields = tuple(json.loads(row["fields_json"]))
                spec = sources[file_id]
                reparsed = _reparse_fragment(raw, spec["encoding"], len(stored_fields))
                if reparsed != stored_fields:
                    report.errors.append(
                        f"re-parsed fields differ from stored fields for "
                        f"uid={row['uid']}"
                    )
                if source_roots:
                    handle = _open_source(
                        source_handles, source_roots, spec["relative_path"]
                    )
                    if handle is not None:
                        # Stored offsets exclude the UTF-8 BOM; the physical
                        # source file still contains those three bytes.
                        bom_adjust = (
                            3 if spec["encoding"] == "utf-8-sig" else 0
                        )
                        handle.seek(row["byte_start"] + bom_adjust)
                        source_bytes = handle.read(
                            row["byte_end"] - row["byte_start"]
                        )
                        report.source_cross_checks += 1
                        if source_bytes != raw:
                            report.errors.append(
                                f"evidence does not match source bytes for "
                                f"uid={row['uid']} at "
                                f"[{row['byte_start']},{row['byte_end']})"
                            )
            for file_id, spec in sources.items():
                count = conn.execute(
                    "SELECT COUNT(*) FROM record WHERE file_id=?", (file_id,)
                ).fetchone()[0]
                if count != spec["actual_rows"]:
                    report.errors.append(
                        f"{file_id}: actual_rows meta {spec['actual_rows']} "
                        f"vs rows present {count}"
                    )
                if count != spec["expected_rows"]:
                    report.errors.append(
                        f"{file_id}: expected {spec['expected_rows']} rows, "
                        f"found {count}"
                    )
                if file_id in last_end and last_end[file_id] > spec["size_bytes"]:
                    report.errors.append(
                        f"{file_id}: evidence span exceeds source file size"
                    )
        finally:
            for handle in source_handles.values():
                handle.close()
    finally:
        conn.close()
    return report


def _reparse_fragment(
    raw: bytes, encoding: str, expected_columns: int
) -> tuple[str, ...]:
    # A stored record fragment never carries the file-level BOM, so use the
    # plain codec even when the source file was declared as utf-8-sig.
    fragment_encoding = "utf-8" if encoding == "utf-8-sig" else encoding
    parser = StreamingCSVParser(fragment_encoding, expected_columns)
    records = parser.feed(raw)
    records.extend(parser.finish())
    if len(records) != 1:
        raise BatchError(
            f"evidence fragment parsed into {len(records)} records, expected 1"
        )
    return records[0].fields


def _open_source(
    handles: dict, source_roots: list[Path], relative_path: str
):
    if relative_path in handles:
        return handles[relative_path]
    for root in source_roots:
        candidate = root / relative_path
        if candidate.is_file():
            handle = candidate.open("rb")
            handles[relative_path] = handle
            return handle
    return None


def show_record(batch_dir: Path, uid: str) -> dict:
    conn = open_published(batch_dir / "batch.sqlite3")
    try:
        row = conn.execute(
            "SELECT * FROM record WHERE uid=?", (uid,)
        ).fetchone()
        if row is None:
            raise BatchError(f"uid not found: {uid}")
        bundle = conn.execute(
            "SELECT * FROM evidence_bundle WHERE file_id=? AND bundle_index=?",
            (row["file_id"], row["evidence_bundle_index"]),
        ).fetchone()
        source = conn.execute(
            "SELECT * FROM source_file WHERE file_id=?", (row["file_id"],)
        ).fetchone()
    finally:
        conn.close()
    raw = read_evidence(
        _bundle_path(batch_dir, bundle["path"]),
        row["evidence_offset"],
        row["evidence_length"],
    )
    actual_hash = hashlib.sha256(raw).hexdigest()
    return {
        "uid": uid,
        "file_id": row["file_id"],
        "source_file": source["relative_path"],
        "encoding": source["encoding"],
        "version": source["version"],
        "row_number": row["row_number"],
        "byte_span": [row["byte_start"], row["byte_end"]],
        "span_rule": "half-open [start, end), 0-based, includes terminator",
        "record_sha256": row["record_sha256"],
        "evidence_sha256_actual": actual_hash,
        "evidence_intact": actual_hash == row["record_sha256"],
        "evidence_bundle": bundle["path"],
        "evidence_offset": row["evidence_offset"],
        "evidence_length": row["evidence_length"],
        "fields": json.loads(row["fields_json"]),
        "raw_text": raw.decode(
            "utf-8" if source["encoding"] == "utf-8-sig" else source["encoding"]
        ),
        "raw_hex_prefix": raw[:32].hex(),
    }
