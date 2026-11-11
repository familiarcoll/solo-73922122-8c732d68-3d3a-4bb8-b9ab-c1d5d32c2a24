# Avro 兼容检查与车载故障 OCF 归档导入服务

车载故障消息以 Avro 编码经 Kafka 入湖。本服务在原有**模式静态兼容检查**之上,
新增**Avro OCF(Object Container File)归档导入 API**:把 Kafka 导出的 OCF
归档转成可查询记录写入 SQLite,支持分块解码、断点续导、同 ID 一致性校验与
跨进程并发防护,导入中断后无需从头重导。

## 能力总览

1. **静态兼容检查(原接口,保持不变)**:校验 reader 模式能否读取 writer 写出的旧消息。
2. **归档导入**:提交归档 ID、预期 SHA-256、reader 模式与不超过 64 MiB 的 OCF;
   先核验摘要并沿用兼容检查,再逐块解码、按 reader 投影字段并补默认值,块级事务入库。
3. **断点续导**:按归档 ID 从下一个未导入 block 继续;重启服务后进度仍在。
4. **进度与分页查询**:查询归档进度,按 keyset 游标分页拉取已导入记录。
5. **递归故障演示归档**:内置可下载的多 block、deflate 压缩 OCF 与一键调用示例。

## 支持范围

- 模式子集:`null`、`boolean`、`int`、`long`、`string`、`array`、具名 `record`
  (允许递归引用)、`union`(JSON 数组);**不支持** namespace 与 aliases。
- OCF 压缩编码:仅 `null` 与 `deflate`,其余编码(如 snappy、bzip2)直接拒绝。
- 上传体积:OCF 本体不超过 **64 MiB**(超出立即拒绝,不继续缓冲)。
- 兼容规则:`int` 可提升为 `long`,其余基础类型必须相同;record 按字段名匹配,
  writer 独有字段被忽略,reader 新增字段必须带类型相符的默认值(union 默认值按首分支校验)。

## 导入与一致性语义

**处理顺序**(任一前置校验失败都不会写入任何进度):

1. 流式计算上传字节的 SHA-256,与 `expected_sha256` 不符 → `400` 拒绝;
2. 解析 OCF 头部与 block 目录:魔数、`avro.schema`、`avro.codec`、同步标记;
3. 用现有解析器校验 OCF 内嵌 writer 模式与提交的 reader 模式均属于支持的类型子集;
4. 沿用静态兼容检查确认 reader 可读全部旧消息,不兼容 → `422`;
5. OCF 原件原子落盘(`/data/files/<id>.ocf`),注册归档并开始逐块导入。

**块级提交(避免中断后从头重导)**:

- 每个 block 解码出的全部记录,与"下一个 block 的起始偏移"在**同一个
  `BEGIN IMMEDIATE` 事务**里提交;
- 坏块(解压失败、Avro 编码损坏、截断等)返回错误且**不推进偏移**,此前已提交的
  完整块全部保留;响应带 `failed_block` 与已导入计数,状态置为 `failed`;
- 进程在块中途退出:该块事务回滚,重启后续导从该块重新开始,已提交块不重复。

**同 ID 续导规则**:

- 同一 `archive_id` 再次导入/续导,SHA-256 与 reader 模式必须与首次**完全一致**
  (reader 以规范化 JSON 比较),否则 `409` 拒绝;
- 已完成的归档再次调用为幂等返回(`already_completed=true`),不重复入库;
- 续导接口无需重新上传,使用首次落盘的 OCF 原件,并先复核其 SHA-256。

**跨进程并发防护**:

- 每个归档有一把基于 SQLite 行的 advisory lock(`locked`/`locked_at`);
- 两个 API 进程同时续导同一归档:持锁者导入,另一者收到 `409`;锁残留超过
  300 秒视为进程已退出,新请求可接管;
- 即使锁失效,块提交仍以库内 `next_offset` 做乐观校验,且 `(archive_id, seq)`
  为主键,保证**不重复入库、不跳块**。

## 启动

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`,SQLite 库与 OCF 原件持久化在 compose 卷
`avro-data`(容器内 `/data`)。Swagger UI(可交互调用全部接口):
<http://localhost:8000/docs>(根路径 `/` 会重定向到这里)。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/compatibility/check` | [原接口] 静态兼容检查 |
| GET | `/api/v1/demo/recursive` | [原接口] 递归模式演示请求体 |
| POST | `/api/v1/archives/import` | 上传并导入(或同 ID 续导)OCF,`multipart/form-data` |
| POST | `/api/v1/archives/{archive_id}/resume` | 按 ID 从断点续导(无需重新上传) |
| GET | `/api/v1/archives` | 列出归档与进度(limit/offset) |
| GET | `/api/v1/archives/{archive_id}` | 查询单个归档进度 |
| GET | `/api/v1/archives/{archive_id}/records` | keyset 分页查询记录(`limit`、`cursor`) |
| GET | `/api/v1/demo/recursive-ocf` | 演示归档元信息、SHA-256 与 curl 示例 |
| GET | `/api/v1/demo/recursive-ocf/download` | 下载递归故障演示 OCF |

### 状态码约定

| 状态码 | 场景 |
| --- | --- |
| 200 | 导入/续导成功(含本次完成或此前已完成的幂等返回) |
| 400 | 摘要不匹配、不是合法 OCF、编码不支持、文件为空 |
| 404 | 续导/查询的归档不存在,或落盘原件丢失 |
| 409 | 同 ID 摘要或 reader 不一致、另一导入进程持锁、并发偏移冲突 |
| 413 | OCF 超过 64 MiB |
| 422 | 入参格式错误、模式非法、reader 与 writer 不兼容、**坏块**(进度已保留) |

## 快速上手:递归故障演示归档

演示归档 `examples/fault_events_demo.ocf`(deflate,3 个 block、4 条记录)中的
`Fault` 通过 `cause` 字段递归引用自身,覆盖深层级联故障、null cause 与多字节字符串。
reader 为演进模式:`severity` 由 int 提升为 long、丢弃 writer 的 `message` 字段、
新增 `occurredAt` 与 `fault.component` 默认值。

```bash
# 1. 取演示归档元信息(含 SHA-256、reader 模式、可复制命令)
curl -s http://localhost:8000/api/v1/demo/recursive-ocf

# 2. 下载 OCF 原件
curl -o fault_events_demo.ocf \
  http://localhost:8000/api/v1/demo/recursive-ocf/download

# 3. 导入(reader_schema 取上一步响应里的 reader_schema JSON 文本)
SHA=$(curl -s http://localhost:8000/api/v1/demo/recursive-ocf \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["sha256"])')
READER=$(curl -s http://localhost:8000/api/v1/demo/recursive-ocf \
  | python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin)["reader_schema"]))')
curl -X POST http://localhost:8000/api/v1/archives/import \
  -F archive_id=demo-fault-001 \
  -F expected_sha256="$SHA" \
  -F reader_schema="$READER" \
  -F file=@fault_events_demo.ocf

# 4. 查询进度
curl http://localhost:8000/api/v1/archives/demo-fault-001

# 5. 分页拉取记录(后续页把 next_cursor 原样回传)
curl 'http://localhost:8000/api/v1/archives/demo-fault-001/records?limit=2'

# 6. 续导(已完成时幂等返回 already_completed=true)
curl -X POST http://localhost:8000/api/v1/archives/demo-fault-001/resume
```

导入响应示例:

```json
{
  "archive_id": "demo-fault-001",
  "status": "completed",
  "sha256": "9b6ce38e...",
  "codec": "deflate",
  "total_blocks": 3,
  "total_records": 4,
  "imported_blocks": 3,
  "imported_records": 4,
  "next_offset": 1064,
  "blocks_this_call": 3,
  "records_this_call": 4,
  "error": null,
  "failed_block": null,
  "already_completed": false
}
```

投影后的记录(writer 独有 `message` 已移除,reader 新增字段补默认值):

```json
{
  "seq": 0,
  "block_index": 0,
  "record_index": 0,
  "record": {
    "eventId": "evt-0001",
    "vehicleId": "VIN-沪A-00001",
    "severity": 3,
    "occurredAt": 0,
    "fault": {
      "code": "BATT_OVERHEAT",
      "component": "unknown",
      "cause": { "code": "COOLANT_LOW", "cause": { "code": "PUMP_FAULT", "cause": null, "tags": ["powertrain", "cooling", "pump"] }, "tags": ["powertrain", "cooling"] },
      "tags": ["battery", "thermal"]
    }
  }
}
```

### 重新生成演示归档

```bash
python3 -m scripts.generate_demo_ocf examples/fault_events_demo.ocf deflate
# 第二个参数可选 null,生成不压缩的 OCF 用于对照
```

## 本地开发与测试(不依赖 Docker)

核心编解码、兼容逻辑与存储层只用标准库(FastAPI 仅用于 HTTP 层)。

```bash
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```

测试覆盖(73 例):基础类型/递归/union 编解码往返、reader 投影与默认值补全、
OCF 魔数/截断/同步标记/坏块检测、摘要与同 ID 冲突、坏块不推进与续导不重复、
模拟进程崩溃后的恢复、两个独立进程并发导入/续导的无重复无跳号、分页与原接口回归。

## 目录结构

```
app/avro_schema.py   模式解析与校验(支持递归引用的节点树)
app/compat.py        静态兼容检查与默认值校验
app/avro_codec.py    Avro 二进制编解码 + OCF 读写(仅 null/deflate,纯标准库)
app/storage.py       SQLite 持久层:块级事务、跨进程锁、断点偏移、keyset 分页
app/importer.py      导入编排:摘要核验 → OCF 校验 → 兼容检查 → 逐块提交
app/demo.py          递归模式演示请求体
app/main.py          FastAPI 入口:原接口 + 导入/续导/查询/演示归档接口
scripts/generate_demo_ocf.py  递归故障演示 OCF 生成器
examples/fault_events_demo.ocf  演示归档(deflate,3 block/4 记录)
examples/recursive_request.json 兼容检查演示请求体
tests/test_compat.py 兼容检查单元测试(原 39 例)
tests/test_codec.py  编解码与 OCF 容器测试
tests/test_api.py    导入 API 集成测试(含多进程并发)
tests/tests_helpers.py 测试辅助(构造坏块归档)
Dockerfile / docker-compose.yml  一键启动,数据持久化到命名卷
```
