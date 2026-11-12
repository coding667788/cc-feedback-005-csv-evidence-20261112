# Windows 运行说明（原生 Python 3.12）

## 环境

- 已实测：Windows + Python 3.12.8（命令 `python`），SQLite 3.45.3。
- 仅使用标准库：`sqlite3`、`csv`（仅测试交叉验证）、`hashlib`、`json`、
  `subprocess`、`unittest`、`tracemalloc`、`gzip`、`argparse` 等。
- 无需浏览器、Docker、数据库服务或任何 pip 安装。
- 建议始终用 `python -X utf8` 以保证控制台 UTF-8 输出；`.bat` 已通过
  `PYTHONIOENCODING`/环境处理编码。

## 命令一览

```bat
python -m generator.generate_data --out data
python -m csv_collector collect --config config.sample.json --manifest data\manifest.json
python -m csv_collector verify  --config config.sample.json --source-root data
python -m csv_collector show    --config config.sample.json --uid users_gb18030-00000001
python -m csv_collector batches --config config.sample.json
python -m csv_collector doctor  --config config.sample.json
python -m unittest discover -s tests -v
```

等价的批处理入口：`generate_data.bat`、`run_collector.bat`、`run_tests.bat`。

## 配置项（JSON）

| 键 | 含义 |
|----|------|
| `block_size` | 每次读取的字节数，最小 1；测试覆盖 1–7 |
| `max_attempts` | 单文件可重试次数，持续失败达到上限即退出（码 6） |
| `work_root` | 工作目录，含 `staging/`、`batches/`、`current.json`、锁 |
| `evidence_rollover_bytes` | 单个证据 bundle 滚动阈值 |
| `insert_batch_rows` | 每批插入并提交的记录数 |
| `quiet` | 是否静默进度日志 |

## 清单契约（manifest.json）

采集器只接受清单与配置，不读取生成器代码或测试答案。每个文件需声明：
`file_id`、`path`、`encoding`（仅 `utf-8-sig` / `gb18030`）、`version`、
`sha256`（64 位十六进制）、`size_bytes`、`expected_rows`、`expected_columns`。

## 工作区与原子发布

```
<work_root>\
  collector.lock                       运行锁（含 PID），崩溃后可用 doctor 诊断
  current.json                         指向当前完整批次（原子替换）
  staging\<batch_id>\                  未发布暂存（中断后残留，可 doctor 清理）
    batch.sqlite3
    evidence\<file_id>\*.bin
  batches\<batch_id>\                  已发布的不可变完整批次
```

- 发布 = 同 NTFS 卷 `os.replace` 目录 + 原子替换指针；旧批次保留不动。
- 中断/崩溃后直接重新 `collect` 即可（全新暂存目录重采，不依赖未提交事务）。
- `doctor` 只列出暂存与锁；`--clean-staging` 仅删除暂存，永不自动删锁。

## 故障注入测试约定

故障测试只终止**自己启动且记录了 PID** 的采集子进程：

- `CSV_COLLECTOR_CRASH_AFTER_RECORD=n`：处理到第 n 条后 `os._exit(97)`，
  模拟证据已落盘但数据库事务未提交的硬中断（钩子会自行释放锁文件）。
- `CSV_COLLECTOR_PAUSE_AFTER_RECORD=n` 与 `CSV_COLLECTOR_PAUSE_SECONDS=s`：
  在第 n 条暂停，供测试在暂停窗口内替换/截断源文件。

这些变量仅用于测试，正常采集不设置即完全不生效。

## Windows 文件占用注意

Windows 下对“已被采集器打开”的源文件执行 `os.replace` 会被拒绝（WinError 5）。
因此替换类故障测试采用**原地覆写为不同内容**；截断采用同句柄 `truncate`。
两种情况都会被尺寸/SHA256 复核捕获，效果等价于替换为不同文件。

