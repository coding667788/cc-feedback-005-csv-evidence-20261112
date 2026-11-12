"""SQLite schema, evidence bundles and read-only verification helpers."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .errors import BatchError

SCHEMA_SQL = """
PRAGMA journal_mode=DELETE;
PRAGMA synchronous=FULL;

CREATE TABLE batch_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE source_file (
    file_id TEXT PRIMARY KEY,
    relative_path TEXT NOT NULL,
    encoding TEXT NOT NULL,
    version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    expected_rows INTEGER NOT NULL,
    actual_rows INTEGER NOT NULL,
    header_sha256 TEXT NOT NULL
);

CREATE TABLE record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    uid TEXT NOT NULL,
    byte_start INTEGER NOT NULL,
    byte_end INTEGER NOT NULL,
    record_sha256 TEXT NOT NULL,
    fields_json TEXT NOT NULL,
    evidence_bundle_index INTEGER NOT NULL,
    evidence_offset INTEGER NOT NULL,
    evidence_length INTEGER NOT NULL,
    UNIQUE(file_id, row_number),
    UNIQUE(uid)
);

CREATE TABLE evidence_bundle (
    file_id TEXT NOT NULL,
    bundle_index INTEGER NOT NULL,
    path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    PRIMARY KEY (file_id, bundle_index)
);

CREATE INDEX idx_record_file_offset ON record(file_id, byte_start);
"""


def connect_staging(db_path: Path) -> sqlite3.Connection:
    if db_path.exists():
        raise BatchError(f"staging database already exists: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA_SQL)
    return conn


def open_published(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise BatchError(f"database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@dataclass
class EvidenceLocation:
    bundle_index: int
    path: Path
    offset: int
    length: int


class EvidenceStore:
    """Append-only evidence bundles, one rolling set per source file.

    Each append is flushed and fsynced before the corresponding database
    transaction is committed, so committed records always have durable bytes.
    """

    def __init__(self, evidence_dir: Path, file_id: str, rollover_bytes: int):
        self._dir = evidence_dir
        self._file_id = file_id
        self._rollover = rollover_bytes
        self._dir.mkdir(parents=True, exist_ok=True)
        self._bundle_index = -1
        self._handle = None
        self._path: Path | None = None
        self._offset = 0
        self._hasher = None
        self._size = 0
        self.rollover_events = 0
        self._closed_bundles: list[dict] = []

    def _open_next(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()
        self._bundle_index += 1
        name = f"{self._file_id}.bundle-{self._bundle_index:04d}.bin"
        self._path = self._dir / name
        self._handle = self._path.open("wb")
        self._offset = 0
        self._hasher = hashlib.sha256()
        self._size = 0

    def append(self, raw: bytes) -> EvidenceLocation:
        if self._handle is None or self._size >= self._rollover:
            if self._size >= self._rollover:
                self.rollover_events += 1
            self._open_next()
        location = EvidenceLocation(
            bundle_index=self._bundle_index,
            path=self._path,
            offset=self._offset,
            length=len(raw),
        )
        # Buffered write; durability is established by flush_pending(), which
        # fsyncs evidence before the matching database transaction commits.
        self._handle.write(raw)
        self._hasher.update(raw)
        self._offset += len(raw)
        self._size += len(raw)
        return location

    def flush_durable(self) -> None:
        """Flush and fsync all buffered evidence bytes."""
        if self._handle is not None:
            self._handle.flush()
            os.fsync(self._handle.fileno())

    def close(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()
            self._handle = None
            self._closed_bundles.append(
                {
                    "bundle_index": self._bundle_index,
                    "relative_path": self.relative_name(self._bundle_index),
                    "size_bytes": self._size,
                    "sha256": self._hasher.hexdigest(),
                }
            )

    def relative_name(self, bundle_index: int) -> str:
        return f"{self._file_id}.bundle-{bundle_index:04d}.bin"

    def finished_bundles(self) -> list[dict]:
        """Return metadata of every sealed bundle (call after close())."""
        return list(self._closed_bundles)

    def bundle_paths(self) -> list[Path]:
        return sorted(self._dir.glob(f"{self._file_id}.bundle-*.bin"))


def read_evidence(path: Path, offset: int, length: int) -> bytes:
    with path.open("rb") as handle:
        handle.seek(offset)
        data = handle.read(length)
    if len(data) != length:
        raise BatchError(
            f"evidence fragment missing or truncated in {path} "
            f"at offset {offset}, length {length}"
        )
    return data


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

