# 本地 CSV 流式采集与字节级原文追溯

纯 Python 3.12 标准库实现（无第三方依赖、无浏览器、无 Docker、无外部服务）。
将两份已知答案 CSV（带 BOM 的 UTF-8、GB18030）按可配置字节块流式解析入
SQLite，并为每条记录保存可独立校验的原始字节证据，支持原子批次发布、
离线查询、篡改/缺失检测与中断后重采。

## 目录结构

```
csv_collector/          采集器（采集器不导入 generator 或 tests）
  parser.py             字节级流式 CSV 状态机（跨块多字节/CRLF/引号/换行）
  manifest.py           清单与配置加载、强校验
  storage.py            SQLite 模式、证据 bundle、只读连接
  collector.py          批次编排、篡改检测、有限重试、故障钩子、原子发布
  query.py              离线 verify / show / 批次解析
  cli.py / __main__.py  命令行入口
generator/              独立数据生成器（确定性，产出 CSV、清单、gzip 答案）
tests/                  unittest 自动化测试（32 项）
scripts/answer_check.py 逐条逐字段对照已知答案
config.sample.json      默认配置样例
*.bat                   Windows 入口
data/                   生成的 20 万行数据、清单与答案（运行后产生）
var/                    采集工作区（staging/batches/current.json，运行后产生）
```

## 快速开始（Windows 原生 Python 3.12）

```bat
REM 1. 生成两份共 20 万行的已知答案数据与清单
generate_data.bat

REM 2. 流式采集并原子发布批次
run_collector.bat collect --config config.sample.json --manifest data\manifest.json

REM 3. 离线校验（含与源文件逐字节交叉核对）
run_collector.bat verify --config config.sample.json --source-root data

REM 4. 按唯一编号追溯一条记录的字段、原文与字节位置
run_collector.bat show --config config.sample.json --uid users_utf8bom-00000001

REM 5. 列出已发布批次 / 诊断未完成暂存
run_collector.bat batches --config config.sample.json
run_collector.bat doctor  --config config.sample.json

REM 6. 逐条逐字段对照生成器答案
python scripts\answer_check.py --config config.sample.json --data data

REM 7. 全量自动化测试
run_tests.bat
```

## 关键设计

- **字节级状态机**：逗号、引号、CR、LF 都是单 ASCII 字节，在 UTF-8 多字节
  序列与 GB18030 尾字节中均不会出现，因此可直接在原始字节上驱动 RFC 4180
  风格状态机，再按字段解码。块大小可配置到 1 字节，多字节字符、CRLF、
  `""` 转义、引号内换行跨块时边界仍正确。
- **只保留当前记录**：解析器维护一个连续缓冲区，仅保留尚未完成的记录后缀，
  完成即裁剪，永不读入整文件；内存测试用 tracemalloc 验证有界。
- **偏移语义**：从 0 开始、左闭右开 `[start, end)`，包含记录终止换行；
  首条数据不含 BOM 与表头；记录间偏移连续。
- **证据与事务**：每条记录原始字节追加进按文件滚动的证据 bundle，记录
  SHA256 与 bundle 内位置入库；证据在数据库事务提交前统一 flush+fsync，
  保证“已提交必有持久证据”。
- **原子发布**：先在 `var/.../staging/<batch_id>/` 构建，全部文件校验
  通过且行数齐备后，用同卷 `os.replace` 原子移动到 `batches/` 并原子替换
  `current.json` 指针；重采不会删除上一批完整结果。
- **篡改与截断**：采集前后比对大小与 SHA256，流中周期性 stat、暂停钩子
  复查尺寸；源文件被替换或截断即中止当前尝试并在有限重试后非零退出，
  绝不发布混合/残缺批次。
- **明确失败**：非法编码、未闭合引号、列数不符均抛 `ParseError` 并带字节
  偏移，退出码非 0，不跳过任何行。

## 退出码

| 码 | 含义 |
|----|------|
| 0  | 成功 |
| 2  | 清单/配置错误 |
| 3  | 源文件缺失或不可读、行数不符 |
| 4  | 解析错误（非法编码/未闭合引号/列数错误） |
| 5  | 采集中源被篡改或截断 |
| 6  | 重试次数耗尽 |
| 7  | 批次/锁/目录状态错误 |
| 8  | 离线校验发现证据被修改或缺失 |

详见 `WINDOWS.md` 与 `RESULTS.md`。

