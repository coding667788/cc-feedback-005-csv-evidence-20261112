"""Shared helpers for the test suite.

Tests build their own small datasets and config files. The collector under
test never imports the generator; integration tests invoke the generator
through its public entry point and only hand the manifest to the collector.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_config(
    work_root: Path,
    block_size: int = 4096,
    max_attempts: int = 3,
) -> Path:
    config_path = work_root / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "block_size": block_size,
                "max_attempts": max_attempts,
                "work_root": str(work_root / "var"),
                "evidence_rollover_bytes": 1024 * 1024,
                "insert_batch_rows": 500,
                "quiet": True,
            }
        ),
        encoding="utf-8",
    )
    return config_path


def write_manifest(work_root: Path, entries: list[dict], manifest_id: str = "t") -> Path:
    manifest = {
        "manifest_id": manifest_id,
        "batch_label": "test",
        "schema_version": 1,
        "files": entries,
    }
    path = work_root / "manifest.json"
    path.write_text(
        json.dumps(manifest, ensure_ascii=False), encoding="utf-8"
    )
    return path


def run_collector(
    config_path: Path,
    manifest_path: Path,
    env_extra: dict | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if env_extra:
        env.update({k: str(v) for k, v in env_extra.items()})
    return subprocess.run(
        [
            sys.executable,
            "-B",
            "-X",
            "utf8",
            "-m",
            "csv_collector",
            "collect",
            "--config",
            str(config_path),
            "--manifest",
            str(manifest_path),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def run_cli(
    args: list[str], env_extra: dict | None = None, timeout: int = 120
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if env_extra:
        env.update({k: str(v) for k, v in env_extra.items()})
    return subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-m", "csv_collector", *args],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def generate_dataset(
    out_dir: Path, rows_a: int, rows_b: int, seed: int = 424242
) -> Path:
    from generator.generate_data import generate

    return generate(out_dir, rows_a=rows_a, rows_b=rows_b, seed=seed)


def load_answers(path: Path) -> list[dict]:
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            rows.append(json.loads(line))
    return rows


def wait_for_file(path: Path, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


class TempWorkdir:
    def __init__(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name)

    def cleanup(self):
        self._tmp.cleanup()
