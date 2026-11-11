# Avro 模式静态兼容检查服务

车载故障消息以 Avro 编码经 Kafka 入湖。升级入湖程序前,用本服务静态确认新
reader 模式能否读取全部旧消息(writer 模式写出的数据)。

## 支持范围与规则

- 类型子集:`null`、`boolean`、`int`、`long`、`string`、`array`、具名
  `record`、`union`(JSON 数组);允许通过具名 record 递归引用;
  **不支持** namespace 与 aliases(出现即报非法模式并定位)。
- `int` 可提升为 `long`,其余基础类型必须相同。
- record 名称必须相同;字段按名称匹配;writer 有而 reader 没有的字段被忽略;
  reader 新增字段必须带**类型相符**的默认值(union 类型的默认值按**首分支**校验)。
- union:writer 的每个分支都必须能被 reader 的至少一个分支读取
  (writer/reader 任一侧非 union 时按单分支处理)。
- 递归 record 不会仅凭名称相同判定兼容:首次遇到展开字段逐一比较,
  用"比较中的节点对"集合终止递归;不兼容时返回第一处字段/分支路径及原因。
- 非法模式(缺 `type`、名称非法、引用未定义类型、重复定义等)返回带路径的报错。

## 启动

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`:

- Swagger UI(可交互调用):<http://localhost:8000/docs>
- 检查接口:`POST /api/v1/compatibility/check`
- 演示请求体:`GET /api/v1/demo/recursive`

## 调用示例

递归模式演示(Fault 通过 cause 字段递归引用自身,兼容):

```bash
curl -s http://localhost:8000/api/v1/demo/recursive \
  | curl -s -X POST http://localhost:8000/api/v1/compatibility/check \
      -H 'Content-Type: application/json' -d @-
```

或直接:

```bash
curl -s -X POST http://localhost:8000/api/v1/compatibility/check \
  -H 'Content-Type: application/json' \
  -d @examples/recursive_request.json
```

响应(兼容):

```json
{"compatible": true, "writer_errors": [], "reader_errors": [], "issues": []}
```

不兼容示例(reader 把递归字段 `cause` 的 `Fault` 分支去掉,旧消息里级联故障不可读):

```bash
curl -s -X POST http://localhost:8000/api/v1/compatibility/check \
  -H 'Content-Type: application/json' -d '{
  "writer": {"type":"record","name":"Fault","fields":[
    {"name":"code","type":"string"},
    {"name":"cause","type":["null","Fault"],"default":null}]},
  "reader": {"type":"record","name":"Fault","fields":[
    {"name":"code","type":"string"},
    {"name":"cause","type":["null","string"],"default":null}]}
}'
```

响应(指出一处分支路径及原因):

```json
{
  "compatible": false,
  "writer_errors": [],
  "reader_errors": [],
  "issues": [{
    "path": "$.fields.cause[1]",
    "reason": "writer 分支 1(record Fault)无法被 reader 的任何分支读取;例如与 reader 首分支比较:..."
  }]
}
```

非法模式示例(定位报错):

```bash
curl -s -X POST http://localhost:8000/api/v1/compatibility/check \
  -H 'Content-Type: application/json' -d '{
  "writer": {"type":"record","name":"E","fields":[{"name":"a"}]},
  "reader": "null"
}'
```

```json
{
  "compatible": false,
  "writer_errors": [{"path": "$.fields[0]", "reason": "字段 'a' 缺少 'type'"}],
  "reader_errors": [],
  "issues": []
}
```

## 路径约定

- 兼容问题路径为逻辑路径:`.fields.<字段名>` 进入 record 字段,`[i]` 表示
  union 第 i 个分支,`.items` 表示数组元素,`.default` 表示默认值。
- 非法模式路径基于原始 JSON 结构,如 `$.fields[0].type[1]`。

## 本地开发(不依赖 Docker)

核心逻辑(`app/avro_schema.py`、`app/compat.py`)只用标准库,测试可直接运行:

```bash
python3 -m unittest discover -s tests -v
```

## 目录结构

```
app/avro_schema.py   模式解析与校验(生成带递归引用的节点树)
app/compat.py        兼容检查与默认值校验
app/demo.py          递归模式演示请求体
app/main.py          FastAPI 入口(/docs、检查接口、演示接口)
tests/test_compat.py 单元测试(39 例)
examples/recursive_request.json  演示请求体(同 demo 接口)
Dockerfile / docker-compose.yml  一键启动
```
