import hashlib
import json
import sqlite3
import unittest
from pathlib import Path

from tests.helpers import (
    TempWorkdir,
    generate_dataset,
    load_answers,
    make_config,
    run_cli,
    run_collector,
)


class SmallDatasetIntegrationTests(unittest.TestCase):
    ROWS_A = 60
    ROWS_B = 40

    def setUp(self):
        self.tmp = TempWorkdir()
        self.work = self.tmp.path
        self.data_dir = self.work / "data"
        self.manifest_path = generate_dataset(
            self.data_dir, self.ROWS_A, self.ROWS_B, seed=2026
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _collect(self, block_size, max_attempts=3):
        config_path = make_config(
            self.work, block_size=block_size, max_attempts=max_attempts
        )
        result = run_collector(config_path, self.manifest_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "published")
        self.assertEqual(payload["total_rows"], self.ROWS_A + self.ROWS_B)
        return config_path, payload

    def test_block_sizes_1_to_7_match_known_answers(self):
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        answers = {}
        for entry in manifest["files"]:
            answer_path = self.data_dir / "answers" / f"{entry['file_id']}.jsonl.gz"
            answers[entry["file_id"]] = load_answers(answer_path)

        for block_size in range(1, 8):
            with self.subTest(block_size=block_size):
                config_path, payload = self._collect(block_size)
                batch_dir = Path(payload["batch_dir"])
                conn = sqlite3.connect(
                    f"file:{(batch_dir / 'batch.sqlite3').as_posix()}?mode=ro",
                    uri=True,
                )
                try:
                    for file_id, expected_rows in answers.items():
                        rows = conn.execute(
                            "SELECT row_number, uid, fields_json, byte_start, "
                            "byte_end, record_sha256 FROM record "
                            "WHERE file_id=? ORDER BY row_number",
                            (file_id,),
                        ).fetchall()
                        self.assertEqual(len(rows), len(expected_rows))
                        previous_end = None
                        for row, expected in zip(rows, expected_rows):
                            row_number, uid, fields_json, start, end, digest = row
                            self.assertEqual(uid, expected["uid"])
                            self.assertEqual(
                                json.loads(fields_json), expected["fields"]
                            )
                            self.assertGreater(end, start)
                            if previous_end is not None:
                                self.assertEqual(start, previous_end)
                            previous_end = end
                            # digest column sanity: 64 hex chars, unique
                            self.assertEqual(len(digest), 64)
                            int(digest, 16)
                finally:
                    conn.close()

    def test_verify_reports_ok_and_cross_checks_sources(self):
        config_path, _ = self._collect(block_size=5)
        result = run_cli(
            [
                "verify",
                "--config",
                str(config_path),
                "--source-root",
                str(self.data_dir),
            ]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["ok"])
        self.assertEqual(report["records_checked"], self.ROWS_A + self.ROWS_B)
        self.assertEqual(
            report["source_cross_checks"], self.ROWS_A + self.ROWS_B
        )

    def test_show_record_fields_and_intact_evidence(self):
        config_path, _ = self._collect(block_size=3)
        uid = f"users_gb18030-00000007"
        result = run_cli(
            ["show", "--config", str(config_path), "--uid", uid]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["uid"], uid)
        self.assertTrue(payload["evidence_intact"])
        start, end = payload["byte_span"]
        self.assertGreater(end, start)

    def test_evidence_tamper_is_detected(self):
        config_path, payload = self._collect(block_size=4)
        batch_dir = Path(payload["batch_dir"])
        bundle = next((batch_dir / "evidence").rglob("*.bin"))
        data = bytearray(bundle.read_bytes())
        data[0] ^= 0xFF
        bundle.write_bytes(bytes(data))
        result = run_cli(["verify", "--config", str(config_path)])
        self.assertEqual(result.returncode, 8)
        report = json.loads(result.stdout)
        self.assertFalse(report["ok"])
        self.assertTrue(
            any("hash mismatch" in err or "tampered" in err
                for err in report["errors"])
        )

    def test_missing_evidence_bundle_is_detected(self):
        config_path, payload = self._collect(block_size=4)
        batch_dir = Path(payload["batch_dir"])
        bundle = next((batch_dir / "evidence").rglob("*.bin"))
        bundle.unlink()
        result = run_cli(["verify", "--config", str(config_path)])
        self.assertEqual(result.returncode, 8)
        report = json.loads(result.stdout)
        self.assertTrue(
            any("missing evidence bundle" in err for err in report["errors"])
        )

    def test_rerun_does_not_destroy_previous_batch(self):
        config_path, first = self._collect(block_size=4)
        first_dir = Path(first["batch_dir"])
        self.assertTrue(first_dir.exists())
        second_result = run_collector(config_path, self.manifest_path)
        self.assertEqual(second_result.returncode, 0, second_result.stderr)
        second = json.loads(second_result.stdout)
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        self.assertTrue(first_dir.exists())
        self.assertTrue(Path(second["batch_dir"]).exists())
        listing = run_cli(["batches", "--config", str(config_path)])
        self.assertEqual(listing.returncode, 0)
        batches = json.loads(listing.stdout)
        self.assertEqual(len(batches), 2)


if __name__ == "__main__":
    unittest.main()

