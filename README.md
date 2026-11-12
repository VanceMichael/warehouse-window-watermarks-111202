# window-watermarks

按事件 UTC 日期计算计量日值，不用文件到达时间代替事件时间。

## 当前数据组织

- `readings`：`event TEXT PRIMARY KEY, device TEXT NOT NULL, instant TEXT NOT NULL, units INTEGER NOT NULL`

`examples/source.json` 为本地合成输入，`examples/expected.json` 是当前查询的公开样例结果。`schema.json` 描述结构，运行时定义在 `warehouse/definition.py`。业务计算位于 `warehouse/domain.py`，事务装载位于 `warehouse/catalog.py`，内容寻址 JSONL 对象位于 `warehouse/store.py`。

输入批次以内容 SHA-256 固定；相同批次重投不重复写入，相同批次内容不同则报错。装载以完整输入包为事务，主键冲突不留下半批记录。关系数据保存在 SQLite，精确数量用十进制文本；导出对象按内容哈希命名，读取会重新校验，来源不在运行时修改。

## 事件时间微批

设备断网恢复后会补发数小时观测。`warehouse/stream.py` 在仓库内新增事件时间微批层，输出接入同一日层集市（`daily_units`），离线回补后仍能解释数值为什么变；不接真实消息服务，输入为本地日志文件。

日志记录字段：`partition`（设备分区）、`position`（源位置）、`event`（事件标识）、`instant`（事件时间）、`units`（读数）。处理时钟（tick）由入口传入供重放，不依赖睡眠。

语义与不变量：

- 分区水位独立推进（分区最大事件时间减去乱序容忍），窗口封口取有效分区最小水位；静默分区按配置的处理时钟刻度转空闲，重新活跃时分区水位被抬到不低于已提交全局水位，水位不倒退。
- 允许迟到范围内（窗口结束加修订范围仍晚于水位）可修订已封口窗口；超范围进入回补队列（`stream_late`）而非丢弃。
- 重复位置不再贡献；同位置内容冲突停止整批并诊断（`ConflictError`）。
- 微批把输入进度（`stream_positions`）、窗口状态（`stream_windows`）、输出版本（`daily_units` 版本与 `daily_history`）在同一事务提交；重启后重放同一微批不丢窗口、不重复贡献。
- 离线回补（`backfill`）替换部分封口窗口并推进输出版本（epoch）；基于旧版本准备的微批提交被拒绝（`StaleEpochError`），重新准备后叠加在校正版之上。
- 最终日值对齐按事件时间的完整重算：`verify` 以全部已消费事件重算日值，挂起回补解释差额，回补清零后 `final` 为真。

流配置（窗口长度、乱序容忍、修订范围、空闲刻度）在首次提交时登记，后续不一致的配置被拒绝，保证重放确定。

## 环境与验证

Python 3.11 以上，仅使用标准库，无第三方安装。`runtime.txt` 记录验证解释器版本。时区相关开发使用系统 IANA 时区库。数据库和对象目录均由入口参数指定。源码和合成数据可以直接复制到 Linux 环境运行。

```bash
python3 -m compileall -q warehouse tests scripts
python3 -m unittest discover -s tests -v
```

回归全部使用入口传入的处理时钟，不靠等待碰运气。

## 运行

每次使用全新临时目录执行真实导入、查询、导出和内容校验：

```bash
python3 scripts/demo.py
```

事件时间微批的完整场景演示（乱序、多分区停发恢复、跨日窗口、故障重开、离线替换与实时竞争、最终对齐）：

```bash
python3 scripts/stream_demo.py
```

分步调用可用：

```bash
python3 -m warehouse import --db /tmp/window-watermarks.sqlite3 --input examples/source.json --batch first
python3 -m warehouse inspect --db /tmp/window-watermarks.sqlite3
python3 -m warehouse export --db /tmp/window-watermarks.sqlite3 --objects /tmp/window-watermarks-objects
```

微批与查询（`--tick` 为处理时钟，必填）：

```bash
python3 -m warehouse batch --db /tmp/window-watermarks.sqlite3 --input examples/stream/batch-01.json --tick 1
python3 -m warehouse prepare --db /tmp/window-watermarks.sqlite3 --input examples/stream/batch-05.json --tick 5 > /tmp/staged.json
python3 -m warehouse commit --db /tmp/window-watermarks.sqlite3 --staged /tmp/staged.json
python3 -m warehouse backfill --db /tmp/window-watermarks.sqlite3 --tick 5
python3 -m warehouse watermarks --db /tmp/window-watermarks.sqlite3   # 水位与空闲状态
python3 -m warehouse late --db /tmp/window-watermarks.sqlite3         # 迟到去向：修订/排队/已回补
python3 -m warehouse daily --db /tmp/window-watermarks.sqlite3        # 日层集市当前值
python3 -m warehouse explain --db /tmp/window-watermarks.sqlite3 --day 2026-10-01
python3 -m warehouse verify --db /tmp/window-watermarks.sqlite3       # 对齐事件时间完整重算
```

`prepare`/`commit` 分离用于复现离线回补与实时微批的竞争：准备后若回补推进了输出版本，`commit` 以退出码 2 拒绝并给出诊断。`batch` 等价于 `prepare` 后立即 `commit`。

`scripts/demo.py` 打印实际命令和完整输出，验证首次写入、重复导入、查询和导出往返。小规模样例在数秒内结束，失败返回非零退出码，不回放预制日志。手工重复使用数据库路径时应留意已存在的业务主键。
