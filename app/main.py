"""FastAPI 入口:Avro 模式静态兼容检查服务。"""

from __future__ import annotations

from typing import Any, List, Optional

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .avro_schema import SchemaError, parse
from .compat import Issue, check_compatibility
from .demo import DEMO_RECURSIVE_REQUEST

app = FastAPI(
    title="Avro 模式兼容检查服务",
    description=(
        "静态检查 writer/reader 两份 Avro 模式是否兼容(reader 能否读取 writer "
        "写出的全部旧消息)。支持 null、boolean、int、long、string、array、"
        "具名 record、union,允许递归引用;不支持 namespace 与 aliases。\n\n"
        "规则:int 可提升为 long,其余基础类型须相同;record 名称须相同,字段按名称匹配,"
        "旧字段可忽略,新增字段须有类型相符的默认值(union 默认值按首分支校验);"
        "writer 每个 union 分支都须可读,reader 任一分支匹配即可。"
    ),
    version="1.0.0",
)


class CheckRequest(BaseModel):
    """兼容检查请求:writer 与 reader 两份 Avro 模式(JSON 形式)。"""

    writer: Any = Field(..., description="写入旧消息所用的 writer 模式(JSON)")
    reader: Any = Field(..., description="升级后入湖程序的 reader 模式(JSON)")

    model_config = {
        "json_schema_extra": {"examples": [DEMO_RECURSIVE_REQUEST]}
    }


class IssueOut(BaseModel):
    path: str = Field(..., description="出错位置(逻辑路径,如 $.fields.cause[1])")
    reason: str = Field(..., description="原因说明")


class CheckResponse(BaseModel):
    compatible: bool = Field(..., description="整体结论:reader 能否读取全部旧消息")
    writer_errors: List[IssueOut] = Field(
        default_factory=list, description="writer 模式非法时的定位报错"
    )
    reader_errors: List[IssueOut] = Field(
        default_factory=list, description="reader 模式非法时的定位报错"
    )
    issues: List[IssueOut] = Field(
        default_factory=list,
        description="不兼容时给出第一处字段/分支路径及原因",
    )


def _to_out(issue: Issue) -> IssueOut:
    return IssueOut(path=issue.path, reason=issue.reason)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.post(
    "/api/v1/compatibility/check",
    response_model=CheckResponse,
    summary="静态兼容检查",
    description="接收 writer、reader 两份模式,返回整体兼容结论;"
    "不兼容时指出一处字段或分支路径及原因;模式非法时定位报错。",
)
def check(req: CheckRequest) -> CheckResponse:
    writer_errors: List[IssueOut] = []
    reader_errors: List[IssueOut] = []

    writer_node: Optional[Any] = None
    reader_node: Optional[Any] = None
    try:
        writer_node = parse(req.writer)
    except SchemaError as e:
        writer_errors.append(IssueOut(path=e.path, reason=e.message))
    try:
        reader_node = parse(req.reader)
    except SchemaError as e:
        reader_errors.append(IssueOut(path=e.path, reason=e.message))

    if writer_errors or reader_errors:
        return CheckResponse(
            compatible=False,
            writer_errors=writer_errors,
            reader_errors=reader_errors,
            issues=[],
        )

    issue = check_compatibility(writer_node, reader_node)
    return CheckResponse(
        compatible=issue is None,
        issues=[_to_out(issue)] if issue is not None else [],
    )


@app.get(
    "/api/v1/demo/recursive",
    summary="递归模式演示请求体",
    description="返回一份可直接 POST 到 /api/v1/compatibility/check 的演示请求,"
    "其中 Fault record 通过 cause 字段递归引用自身。",
)
def demo_recursive() -> Any:
    return DEMO_RECURSIVE_REQUEST
