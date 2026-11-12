"""Generate two known-answer CSV datasets plus a signed manifest.

Dataset A: UTF-8 with BOM, CRLF record terminators.
Dataset B: GB18030 without BOM, LF record terminators.

Every record has a unique uid. Field content covers Chinese text, empty
fields, embedded commas, escaped double quotes and newlines inside quoted
fields. Exact field values are written as gzip JSONL audit answers that the
test suite compares against parser output.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
from dataclasses import dataclass
from pathlib import Path

COLUMNS = (
    "uid",
    "name",
    "city",
    "note",
    "score",
    "tags",
    "description",
    "updated_at",
)
COLUMN_COUNT = len(COLUMNS)

CHINESE_NAMES = (
    "张伟",
    "王芳",
    "李娜",
    "刘洋",
    "陈静",
    "杨磊",
    "赵敏",
    "黄强",
    "周婷",
    "吴昊",
)
CITIES = ("北京", "上海", "广州", "深圳", "杭州", "成都", "西安", "武汉")
NOTES = (
    "",  # empty field
    "普通备注",
    "包含,逗号的备注",
    '他说:"你好"',
    "多行\n第二行",
    "引号\"和,逗号混合",
    "CRLF行\r\n第二行",
    "   前后空格   ",
    "数字123与符号#@%",
    "连续引号\"\"测试",
)
TAG_GROUPS = (
    "",
    "vip",
    "vip,new",
    '"quoted"',
    "a,b,c",
    "中文标签,标签2",
)
DESCRIPTIONS = (
    "标准描述文本",
    "字段内含双引号：\"重点\"",
    "第一行\n第二行\n第三行",
    "逗号，和英文 comma, 混用",
    "",
    "引号包裹\"内容\"结束",
    "CRLF\r\n换行描述",
)


def csv_quote(field: str) -> str:
    if any(ch in field for ch in (",", '"', "\r", "\n")):
        return '"' + field.replace('"', '""') + '"'
    return field


def build_record_fields(index: int, file_tag: str, rng: random.Random) -> list[str]:
    style = index % 10
    uid = f"{file_tag}-{index:08d}"
    name = rng.choice(CHINESE_NAMES)
    city = rng.choice(CITIES)
    note = NOTES[style]
    score = f"{rng.randint(0, 100)}.{rng.randint(0, 99):02d}"
    tags = rng.choice(TAG_GROUPS)
    description = rng.choice(DESCRIPTIONS)
    if style == 7:
        description = f'首"中"尾,逗号'
    updated_at = f"2026-{(index % 12) + 1:02d}-{(index % 28) + 1:02d}T08:30:00"
    return [uid, name, city, note, score, tags, description, updated_at]


@dataclass(frozen=True)
class DatasetSpec:
    file_id: str
    file_name: str
    encoding: str
    line_terminator: str
    rows: int
    version: str


def _write_dataset(
    spec: DatasetSpec, data_dir: Path, seed: int
) -> dict:
    rng = random.Random(seed)
    csv_path = data_dir / spec.file_name
    answer_dir = data_dir / "answers"
    answer_dir.mkdir(parents=True, exist_ok=True)
    answer_path = answer_dir / f"{spec.file_id}.jsonl.gz"
    encoding_for_write = spec.encoding.replace("-sig", "")
    digest = hashlib.sha256()
    with csv_path.open("wb") as csv_handle, gzip.open(
        answer_path, "wt", encoding="utf-8", newline="\n"
    ) as answer_handle:
        if spec.encoding == "utf-8-sig":
            csv_handle.write(b"\xef\xbb\xbf")
            digest.update(b"\xef\xbb\xbf")
        header = ",".join(COLUMNS) + spec.line_terminator
        header_bytes = header.encode(encoding_for_write)
        csv_handle.write(header_bytes)
        digest.update(header_bytes)
        for index in range(1, spec.rows + 1):
            fields = build_record_fields(index, spec.file_id, rng)
            line = (
                ",".join(csv_quote(field) for field in fields)
                + spec.line_terminator
            )
            raw = line.encode(encoding_for_write)
            csv_handle.write(raw)
            digest.update(raw)
            answer_handle.write(
                json.dumps(
                    {"uid": fields[0], "fields": fields},
                    ensure_ascii=False,
                )
                + "\n"
            )
    size = csv_path.stat().st_size
    return {
        "file_id": spec.file_id,
        "path": spec.file_name,
        "encoding": spec.encoding,
        "version": spec.version,
        "sha256": digest.hexdigest(),
        "size_bytes": size,
        "expected_rows": spec.rows,
        "expected_columns": COLUMN_COUNT,
        "answer_file": f"answers/{spec.file_id}.jsonl.gz",
    }


def generate(
    out_dir: str | Path,
    rows_a: int = 120_000,
    rows_b: int = 80_000,
    seed: int = 20261112,
) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    specs = (
        DatasetSpec(
            file_id="users_utf8bom",
            file_name="users_utf8bom_v1.csv",
            encoding="utf-8-sig",
            line_terminator="\r\n",
            rows=rows_a,
            version="1.0.0",
        ),
        DatasetSpec(
            file_id="users_gb18030",
            file_name="users_gb18030_v1.csv",
            encoding="gb18030",
            line_terminator="\n",
            rows=rows_b,
            version="1.0.0",
        ),
    )
    entries = []
    for offset, spec in enumerate(specs):
        entries.append(_write_dataset(spec, out, seed + offset))
    total_rows = rows_a + rows_b
    manifest = {
        "manifest_id": f"csv-batch-{seed}",
        "batch_label": "已知答案CSV批次",
        "schema_version": 1,
        "generator_version": "1.0.0",
        "total_rows": total_rows,
        "files": [
            {key: value for key, value in entry.items() if key != "answer_file"}
            for entry in entries
        ],
    }
    manifest_path = out / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    answers_index = {
        entry["file_id"]: entry["answer_file"] for entry in entries
    }
    (out / "answers" / "index.json").write_text(
        json.dumps(answers_index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate known-answer CSV data")
    parser.add_argument("--out", required=True)
    parser.add_argument("--rows-a", type=int, default=120_000)
    parser.add_argument("--rows-b", type=int, default=80_000)
    parser.add_argument("--seed", type=int, default=20261112)
    args = parser.parse_args(argv)
    manifest_path = generate(
        args.out, args.rows_a, args.rows_b, args.seed
    )
    print(f"manifest written: {manifest_path}")
    print(f"total data rows: {args.rows_a + args.rows_b}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

