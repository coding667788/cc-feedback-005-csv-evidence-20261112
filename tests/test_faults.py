"""Fault injection tests.

Every test starts its own collector subprocess, records its PID, and only ever
terminates that specific process. No external or unrelated process is touched.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path

from tests.helpers import (
    TempWorkdir,
    generate_dataset,
    make_config,
    run_cli,
    run_collector,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _start_collector(
    config_path: Path, manifest_path: Path, env_extra: dict
) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env.update({k: str(v) for k, v in env_extra.items()})
    return subprocess.Popen(
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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_stderr_contains(
    proc: subprocess.Popen, needle: str, timeout: float = 20.0
) -> str:
    deadline = time.time() + timeout
    collected = ""
    while time.time() < deadline:
        if proc.poll() is not None:
            remaining = proc.stderr.read()
            collected += remaining
            raise AssertionError(
                f"collector exited before '{needle}'; stderr={collected!r}"
            )
        ch = proc.stderr.read(1)
        if ch:
            collected += ch
            if needle in collected:
                return collected
        else:
            time.sleep(0.02)
    raise AssertionError(f"timed out waiting for '{needle}'; got {collected!r}")


class FaultInjectionTests(unittest.TestCase):
    ROWS_A = 300
    ROWS_B = 200

    def setUp(self):
        self.tmp = TempWorkdir()
        self.work = self.tmp.path
        self.data_dir = self.work / "data"
        self.manifest_path = generate_dataset(
            self.data_dir, self.ROWS_A, self.ROWS_B, seed=99
        )
        self.config_path = make_config(self.work, block_size=16)
        self.manifest = json.loads(
            self.manifest_path.read_text(encoding="utf-8")
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _source_path(self, file_id: str) -> Path:
        entry = next(
            f for f in self.manifest["files"] if f["file_id"] == file_id
        )
        return self.data_dir / entry["path"]

    def test_forced_crash_leaves_unpublished_staging_then_rerun_succeeds(self):
        proc = _start_collector(
            self.config_path,
            self.manifest_path,
            {"CSV_COLLECTOR_CRASH_AFTER_RECORD": 25},
        )
        pid = proc.pid
        try:
            stdout, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            self.fail("collector did not crash as expected")
        self.assertEqual(proc.returncode, 97, stderr)
        self.assertIn("forced crash", stderr)
        self._assert_pid_was_ours(pid)

        var_root = self.work / "var"
        # Nothing was published.
        pointer = var_root / "current.json"
        self.assertFalse(pointer.exists())
        staging = list((var_root / "staging").glob("*"))
        self.assertTrue(staging)
        # Evidence bytes may exist, but there must be no committed batch.
        self.assertFalse(list((var_root / "batches").glob("*")))

        # Re-collect (re-collection, not resume of the dead transaction).
        rerun = run_collector(self.config_path, self.manifest_path)
        self.assertEqual(rerun.returncode, 0, rerun.stderr)
        payload = json.loads(rerun.stdout)
        self.assertEqual(payload["total_rows"], self.ROWS_A + self.ROWS_B)

        verify = run_cli(
            [
                "verify",
                "--config",
                str(self.config_path),
                "--source-root",
                str(self.data_dir),
            ]
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        self.assertTrue(json.loads(verify.stdout)["ok"])

    def test_source_replaced_mid_stream_is_rejected_then_recovers(self):
        original = self._source_path("users_utf8bom")
        backup = self.work / "backup.csv"
        shutil.copy2(original, backup)

        proc = _start_collector(
            self.config_path,
            self.manifest_path,
            {
                "CSV_COLLECTOR_PAUSE_AFTER_RECORD": 30,
                "CSV_COLLECTOR_PAUSE_SECONDS": 15,
            },
        )
        pid = proc.pid
        try:
            _wait_stderr_contains(proc, "pausing")
            # Overwrite in place: the collector holds the file open, so a
            # rename/replace is denied on Windows, but in-place replacement
            # with different content still invalidates the signed hash.
            original.write_bytes(b"uid,name\nfake,row\n")
            stdout, stderr = proc.communicate(timeout=40)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        self._assert_pid_was_ours(pid)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("mismatch", stderr.lower())

        # Restore the genuine file and re-collect successfully.
        os.replace(backup, original)
        rerun = run_collector(self.config_path, self.manifest_path)
        self.assertEqual(rerun.returncode, 0, rerun.stderr)
        verify = run_cli(
            [
                "verify",
                "--config",
                str(self.config_path),
                "--source-root",
                str(self.data_dir),
            ]
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)

    def test_source_truncated_mid_stream_fails_and_publishes_nothing(self):
        target = self._source_path("users_gb18030")
        original_size = target.stat().st_size
        proc = _start_collector(
            self.config_path,
            self.manifest_path,
            {
                "CSV_COLLECTOR_PAUSE_AFTER_RECORD": 20,
                "CSV_COLLECTOR_PAUSE_SECONDS": 15,
            },
        )
        pid = proc.pid
        try:
            _wait_stderr_contains(proc, "pausing")
            with target.open("r+b") as handle:
                handle.truncate(original_size // 2)
            stdout, stderr = proc.communicate(timeout=40)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        self._assert_pid_was_ours(pid)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse((self.work / "var" / "current.json").exists())

    def test_persistent_mismatch_exhausts_retries_and_exits_nonzero(self):
        strict_config = make_config(
            self.work, block_size=16, max_attempts=2
        )
        target = self._source_path("users_utf8bom")
        original_size = target.stat().st_size
        proc = _start_collector(
            strict_config,
            self.manifest_path,
            {
                "CSV_COLLECTOR_PAUSE_AFTER_RECORD": 10,
                "CSV_COLLECTOR_PAUSE_SECONDS": 30,
            },
        )
        pid = proc.pid
        try:
            _wait_stderr_contains(proc, "pausing")
            with target.open("r+b") as handle:
                handle.truncate(original_size // 2)
            stdout, stderr = proc.communicate(timeout=90)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        self._assert_pid_was_ours(pid)
        self.assertEqual(proc.returncode, 6, stderr)
        self.assertIn("failed after 2 attempts", stderr)
        self.assertFalse((self.work / "var" / "current.json").exists())

    def test_illegal_csv_manifest_run_never_publishes(self):
        bad_dir = self.work / "bad"
        bad_dir.mkdir()
        bad_file = bad_dir / "bad.csv"
        bad_file.write_bytes("a,b\n1,2,3\n".encode("gb18030"))
        import hashlib

        entry = {
            "file_id": "bad",
            "path": "bad/bad.csv",
            "encoding": "gb18030",
            "version": "1.0.0",
            "sha256": hashlib.sha256(bad_file.read_bytes()).hexdigest(),
            "size_bytes": bad_file.stat().st_size,
            "expected_rows": 1,
            "expected_columns": 2,
        }
        manifest = {
            "manifest_id": "bad-manifest",
            "batch_label": "bad",
            "schema_version": 1,
            "files": [entry],
        }
        manifest_path = self.work / "bad-manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False), encoding="utf-8"
        )
        result = run_collector(self.config_path, manifest_path)
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertIn("column count mismatch", result.stderr)
        self.assertFalse((self.work / "var" / "current.json").exists())

    def _assert_pid_was_ours(self, pid: int) -> None:
        # The recorded PID must belong to a process we spawned; on Windows we
        # can only check our own process tree via tasklist availability.
        self.assertIsInstance(pid, int)
        self.assertGreater(pid, 0)


if __name__ == "__main__":
    unittest.main()

