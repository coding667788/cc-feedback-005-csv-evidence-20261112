"""Manifest and configuration loading plus validation.

The collector only trusts the signed manifest and its JSON configuration.
It never imports generator or test code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .errors import ManifestError

SUPPORTED_ENCODINGS = ("utf-8-sig", "gb18030")


@dataclass(frozen=True)
class SourceSpec:
    file_id: str
    path: Path
    encoding: str
    version: str
    sha256: str
    size_bytes: int
    expected_rows: int
    expected_columns: int


@dataclass(frozen=True)
class Manifest:
    manifest_id: str
    batch_label: str
    schema_version: int
    files: tuple[SourceSpec, ...]
    raw: dict


@dataclass(frozen=True)
class Config:
    block_size: int
    max_attempts: int
    work_root: Path
    evidence_rollover_bytes: int
    insert_batch_rows: int
    quiet: bool
    config_dir: Path


def _load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise ManifestError(f"file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ManifestError(f"top-level JSON object expected in {path}")
    return data


def load_config(path: str | Path) -> Config:
    config_path = Path(path).resolve()
    data = _load_json(config_path)
    block_size = int(data.get("block_size", 65536))
    if block_size < 1:
        raise ManifestError("block_size must be >= 1")
    max_attempts = int(data.get("max_attempts", 3))
    if max_attempts < 1:
        raise ManifestError("max_attempts must be >= 1")
    work_root = Path(data.get("work_root", "var")).resolve()
    if not work_root.is_absolute():
        work_root = (config_path.parent / work_root).resolve()
    evidence_rollover = int(data.get("evidence_rollover_bytes", 64 * 1024 * 1024))
    if evidence_rollover < 1:
        raise ManifestError("evidence_rollover_bytes must be >= 1")
    insert_batch_rows = int(data.get("insert_batch_rows", 2000))
    if insert_batch_rows < 1:
        raise ManifestError("insert_batch_rows must be >= 1")
    return Config(
        block_size=block_size,
        max_attempts=max_attempts,
        work_root=work_root,
        evidence_rollover_bytes=evidence_rollover,
        insert_batch_rows=insert_batch_rows,
        quiet=bool(data.get("quiet", False)),
        config_dir=config_path.parent,
    )


def load_manifest(path: str | Path) -> Manifest:
    manifest_path = Path(path).resolve()
    data = _load_json(manifest_path)
    manifest_id = data.get("manifest_id")
    if not isinstance(manifest_id, str) or not manifest_id:
        raise ManifestError("manifest.manifest_id missing")
    batch_label = data.get("batch_label", manifest_id)
    schema_version = int(data.get("schema_version", 1))
    raw_files = data.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise ManifestError("manifest.files must be a non-empty list")
    base_dir = manifest_path.parent
    specs: list[SourceSpec] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict):
            raise ManifestError(f"files[{index}] must be an object")
        file_id = item.get("file_id")
        if not isinstance(file_id, str) or not file_id:
            raise ManifestError(f"files[{index}].file_id missing")
        if file_id in seen_ids:
            raise ManifestError(f"duplicate file_id: {file_id}")
        seen_ids.add(file_id)
        encoding = item.get("encoding")
        if encoding not in SUPPORTED_ENCODINGS:
            raise ManifestError(
                f"files[{index}].encoding must be one of {SUPPORTED_ENCODINGS}"
            )
        relative_path = item.get("path")
        if not isinstance(relative_path, str) or not relative_path:
            raise ManifestError(f"files[{index}].path missing")
        source_path = (base_dir / relative_path).resolve()
        sha256 = item.get("sha256")
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ManifestError(f"files[{index}].sha256 must be 64 hex chars")
        try:
            int(sha256, 16)
        except ValueError as exc:
            raise ManifestError(f"files[{index}].sha256 is not hex") from exc
        size_bytes = item.get("size_bytes")
        expected_rows = item.get("expected_rows")
        expected_columns = item.get("expected_columns")
        version = item.get("version", "1")
        if not isinstance(size_bytes, int) or size_bytes < 0:
            raise ManifestError(f"files[{index}].size_bytes invalid")
        if not isinstance(expected_rows, int) or expected_rows < 1:
            raise ManifestError(f"files[{index}].expected_rows invalid")
        if not isinstance(expected_columns, int) or expected_columns < 1:
            raise ManifestError(f"files[{index}].expected_columns invalid")
        specs.append(
            SourceSpec(
                file_id=file_id,
                path=source_path,
                encoding=encoding,
                version=str(version),
                sha256=sha256.lower(),
                size_bytes=size_bytes,
                expected_rows=expected_rows,
                expected_columns=expected_columns,
            )
        )
    return Manifest(
        manifest_id=manifest_id,
        batch_label=str(batch_label),
        schema_version=schema_version,
        files=tuple(specs),
        raw=data,
    )

