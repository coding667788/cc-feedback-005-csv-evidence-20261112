"""Batch collection orchestration.

Guarantees
----------
* A batch is published only when every manifest file passes size/hash
  verification and the exact expected row count is stored.
* Publication is a directory move plus an atomic pointer replacement, so a
  previously published complete batch is never destroyed by a re-run.
* Source replacement/truncation during streaming aborts the attempt; after a
  finite retry budget the process exits non-zero instead of publishing.
* Evidence bytes are fsynced before each database transaction commits.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
import uuid
import gc
from dataclasses import dataclass
from pathlib import Path

from .errors import (
    AttemptsExhaustedError,
    BatchError,
    SourceError,
    TamperError,
)
from .manifest import Config, Manifest, SourceSpec
from .parser import Record, stream_records
from .storage import EvidenceStore, connect_staging, sha256_file

POINTER_NAME = "current.json"
LOCK_NAME = "collector.lock"


def _log(quiet: bool, message: str) -> None:
    if not quiet:
        print(message, flush=True)


class FaultHooks:
    """Test-only hooks driven by environment variables.

    CSV_COLLECTOR_CRASH_AFTER_RECORD: os._exit after the Nth global record
        (evidence on disk, DB transaction uncommitted).
    CSV_COLLECTOR_PAUSE_AFTER_RECORD: sleep at the Nth record so a test can
        replace or truncate the open source file.
    CSV_COLLECTOR_PAUSE_SECONDS: pause duration (default 20 seconds).
    """

    def __init__(self) -> None:
        crash = os.environ.get("CSV_COLLECTOR_CRASH_AFTER_RECORD")
        pause = os.environ.get("CSV_COLLECTOR_PAUSE_AFTER_RECORD")
        self.crash_after = int(crash) if crash else None
        self.pause_after = int(pause) if pause else None
        self.pause_seconds = float(
            os.environ.get("CSV_COLLECTOR_PAUSE_SECONDS", "20")
        )
        self.counter = 0

    def checkpoint(self) -> None:
        self.counter += 1
        if self.crash_after is not None and self.counter == self.crash_after:
            sys.stderr.write(
                f"[fault-hook] forced crash after record {self.counter}\n"
            )
            sys.stderr.flush()
            try:
                lock = Path(os.environ.get("CSV_COLLECTOR_LOCK_PATH", ""))
                if str(lock) and lock.exists():
                    lock.unlink()
            except OSError:
                pass
            os._exit(97)
        if self.pause_after is not None and self.counter == self.pause_after:
            watch_path = os.environ.get("CSV_COLLECTOR_WATCH_PATH")
            watch_size = os.environ.get("CSV_COLLECTOR_WATCH_SIZE")
            size_before = None
            if watch_path and watch_size:
                size_before = Path(watch_path).stat().st_size
            sys.stderr.write(
                f"[fault-hook] pausing {self.pause_seconds}s after record "
                f"{self.counter} (pid={os.getpid()})\n"
            )
            sys.stderr.flush()
            time.sleep(self.pause_seconds)
            if watch_path and watch_size:
                size_after = Path(watch_path).stat().st_size
                if size_after != int(watch_size) or size_after != size_before:
                    raise TamperError(
                        f"source changed during pause hook: size "
                        f"{watch_size} -> {size_after}"
                    )


@dataclass
class CollectionResult:
    batch_id: str
    batch_dir: Path
    files_collected: int
    total_rows: int


class BatchCollector:
    def __init__(self, config: Config, manifest: Manifest):
        self.config = config
        self.manifest = manifest
        self.work_root = config.work_root
        self.staging_root = self.work_root / "staging"
        self.published_root = self.work_root / "batches"
        self.pointer_path = self.work_root / POINTER_NAME
        self.lock_path = self.work_root / LOCK_NAME
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.published_root.mkdir(parents=True, exist_ok=True)

    def run(self) -> CollectionResult:
        self._acquire_lock()
        try:
            return self._run_locked()
        finally:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass

    def _acquire_lock(self) -> None:
        os.environ["CSV_COLLECTOR_LOCK_PATH"] = str(self.lock_path)
        try:
            fd = os.open(
                str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY
            )
        except FileExistsError as exc:
            stale = (
                self.lock_path.read_text(encoding="utf-8")
                if self.lock_path.exists()
                else ""
            )
            raise BatchError(
                f"another collection appears to be running "
                f"({self.lock_path}: {stale.strip() or 'no details'}). "
                "Remove the lock only after confirming no collector is alive."
            ) from exc
        try:
            os.write(
                fd,
                f"pid={os.getpid()} started={time.time():.0f}\n".encode(
                    "utf-8"
                ),
            )
        finally:
            os.close(fd)

    def _run_locked(self) -> CollectionResult:
        batch_id = (
            f"{self.manifest.manifest_id}-{time.strftime('%Y%m%dT%H%M%S')}"
            f"-pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
        )
        staging_dir = self.staging_root / batch_id
        staging_dir.mkdir(parents=True, exist_ok=False)
        db_path = staging_dir / "batch.sqlite3"
        evidence_dir = staging_dir / "evidence"
        evidence_dir.mkdir()
        _log(
            self.config.quiet,
            f"staging batch {batch_id} (block_size={self.config.block_size}, "
            f"max_attempts={self.config.max_attempts})",
        )
        conn = connect_staging(db_path)
        total_rows = 0
        try:
            for spec in self.manifest.files:
                total_rows += self._collect_file_with_retry(
                    spec, evidence_dir, conn
                )
            self._write_batch_meta(conn, batch_id, total_rows)
            conn.commit()
        except BaseException:
            conn.close()
            raise
        conn.close()
        self._publish(batch_id, staging_dir)
        return CollectionResult(
            batch_id=batch_id,
            batch_dir=self.published_root / batch_id,
            files_collected=len(self.manifest.files),
            total_rows=total_rows,
        )

    def _collect_file_with_retry(
        self,
        spec: SourceSpec,
        evidence_dir: Path,
        conn,
    ) -> int:
        attempt = 0
        last_error: Exception | None = None
        while attempt < self.config.max_attempts:
            attempt += 1
            try:
                _log(
                    self.config.quiet,
                    f"[{spec.file_id}] attempt {attempt}/"
                    f"{self.config.max_attempts}: {spec.path.name}",
                )
                return self._collect_file(spec, evidence_dir, conn)
            except (TamperError, SourceError) as exc:
                last_error = exc
                _log(
                    self.config.quiet,
                    f"[{spec.file_id}] attempt {attempt} failed with retryable "
                    f"error: {exc}",
                )
                self._rollback_file(conn, spec, evidence_dir)
        raise AttemptsExhaustedError(
            f"file {spec.file_id} failed after {self.config.max_attempts} "
            f"attempts; last error: {last_error}"
        )

    def _rollback_file(
        self, conn, spec: SourceSpec, evidence_dir: Path
    ) -> None:
        conn.rollback()
        gc.collect()  # release closed evidence handles before rmtree on Windows
        file_evidence_dir = evidence_dir / spec.file_id
        if file_evidence_dir.exists():
            shutil.rmtree(file_evidence_dir, ignore_errors=False)

    def _verify_source_metadata(self, spec: SourceSpec) -> None:
        if not spec.path.is_file():
            raise SourceError(f"source file missing: {spec.path}")
        stat = spec.path.stat()
        if stat.st_size != spec.size_bytes:
            raise TamperError(
                f"size mismatch for {spec.file_id}: manifest "
                f"{spec.size_bytes} vs disk {stat.st_size}"
            )
        digest = sha256_file(spec.path)
        if digest != spec.sha256:
            raise TamperError(
                f"sha256 mismatch for {spec.file_id}: manifest {spec.sha256} "
                f"vs disk {digest}"
            )

    def _collect_file(
        self, spec: SourceSpec, evidence_dir: Path, conn
    ) -> int:
        self._verify_source_metadata(spec)
        os.environ["CSV_COLLECTOR_WATCH_PATH"] = str(spec.path)
        os.environ["CSV_COLLECTOR_WATCH_SIZE"] = str(spec.size_bytes)
        file_evidence_dir = evidence_dir / spec.file_id
        store = EvidenceStore(
            file_evidence_dir,
            spec.file_id,
            self.config.evidence_rollover_bytes,
        )
        try:
            hooks = FaultHooks()
            rows = 0
            header: Record | None = None
            pending_rows: list[tuple] = []
            expected_size = spec.size_bytes
            stat_counter = 0

            def flush_pending() -> None:
                if not pending_rows:
                    return
                store.flush_durable()
                conn.executemany(
                    """
                    INSERT INTO record(
                        file_id, row_number, uid, byte_start, byte_end,
                        record_sha256, fields_json, evidence_bundle_index,
                        evidence_offset, evidence_length
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    pending_rows,
                )
                # Evidence fsynced in EvidenceStore before this commit.
                conn.commit()
                pending_rows.clear()

            handle = spec.path.open("rb")
            try:
                for record in stream_records(
                    handle,
                    encoding=spec.encoding,
                    expected_columns=spec.expected_columns,
                    block_size=self.config.block_size,
                ):
                    if header is None:
                        header = record
                        if record.start_offset != 0:
                            raise BatchError(
                                f"header must start at byte 0 for "
                                f"{spec.file_id}, got {record.start_offset}"
                            )
                        continue
                    if record.start_offset < header.end_offset:
                        raise BatchError("data record overlaps header region")
                    hooks.checkpoint()
                    record_digest = hashlib.sha256(record.raw).hexdigest()
                    location = store.append(record.raw)
                    rows += 1
                    uid = record.fields[0]
                    pending_rows.append(
                        (
                            spec.file_id,
                            rows,
                            uid,
                            record.start_offset,
                            record.end_offset,
                            record_digest,
                            json.dumps(
                                list(record.fields), ensure_ascii=False
                            ),
                            location.bundle_index,
                            location.offset,
                            location.length,
                        )
                    )
                    if len(pending_rows) >= self.config.insert_batch_rows:
                        flush_pending()
                    stat_counter += 1
                    if stat_counter % 4096 == 0:
                        current_size = spec.path.stat().st_size
                        if current_size != expected_size:
                            raise TamperError(
                                f"source {spec.file_id} size changed during "
                                f"streaming: {expected_size} -> "
                                f"{current_size}"
                            )
            finally:
                handle.close()
            flush_pending()
        finally:
            store.close()

        if header is None:
            raise SourceError(f"no header found in {spec.path}")
        if rows != spec.expected_rows:
            raise SourceError(
                f"row count mismatch for {spec.file_id}: manifest "
                f"{spec.expected_rows} vs parsed {rows}"
            )

        final_digest = sha256_file(spec.path)
        if final_digest != spec.sha256:
            raise TamperError(
                f"post-stream sha256 mismatch for {spec.file_id}: manifest "
                f"{spec.sha256} vs disk {final_digest}"
            )
        if spec.path.stat().st_size != expected_size:
            raise TamperError(
                f"post-stream size mismatch for {spec.file_id}"
            )

        header_digest = hashlib.sha256(header.raw).hexdigest()
        conn.execute(
            """
            INSERT INTO source_file(
                file_id, relative_path, encoding, version, source_sha256,
                size_bytes, expected_rows, actual_rows, header_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                spec.file_id,
                spec.path.name,
                spec.encoding,
                spec.version,
                spec.sha256,
                spec.size_bytes,
                spec.expected_rows,
                rows,
                header_digest,
            ),
        )
        for bundle in store.finished_bundles():
            conn.execute(
                """
                INSERT INTO evidence_bundle(
                    file_id, bundle_index, path, size_bytes, sha256
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    spec.file_id,
                    bundle["bundle_index"],
                    f"evidence/{spec.file_id}/{bundle['relative_path']}",
                    bundle["size_bytes"],
                    bundle["sha256"],
                ),
            )
        conn.commit()
        _log(self.config.quiet, f"[{spec.file_id}] collected {rows} rows")
        return rows

    def _write_batch_meta(
        self, conn, batch_id: str, total_rows: int
    ) -> None:
        meta = {
            "batch_id": batch_id,
            "manifest_id": self.manifest.manifest_id,
            "batch_label": self.manifest.batch_label,
            "schema_version": str(self.manifest.schema_version),
            "created_utc": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            ),
            "total_rows": str(total_rows),
            "file_count": str(len(self.manifest.files)),
            "block_size": str(self.config.block_size),
            "manifest_sha256": hashlib.sha256(
                json.dumps(
                    self.manifest.raw, sort_keys=True, ensure_ascii=False
                ).encode("utf-8")
            ).hexdigest(),
        }
        conn.executemany(
            "INSERT INTO batch_meta(key, value) VALUES (?, ?)",
            list(meta.items()),
        )

    def _publish(self, batch_id: str, staging_dir: Path) -> None:
        target = self.published_root / batch_id
        if target.exists():
            raise BatchError(f"published batch already exists: {target}")
        # os.replace is atomic on the same NTFS volume for directories.
        os.replace(staging_dir, target)
        pointer_tmp = self.work_root / f"{POINTER_NAME}.{os.getpid()}.tmp"
        pointer_payload = {
            "batch_id": batch_id,
            "batch_dir": str(target),
            "updated_utc": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            ),
            "pid": os.getpid(),
        }
        pointer_tmp.write_text(
            json.dumps(pointer_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(pointer_tmp, self.pointer_path)
        _log(self.config.quiet, f"published batch {batch_id} -> {target}")


def list_staging(work_root: Path) -> list[Path]:
    staging = work_root / "staging"
    if not staging.exists():
        return []
    return sorted(p for p in staging.iterdir() if p.is_dir())

