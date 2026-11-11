"""FastAPI 入口:Avro 兼容检查与 OCF 归档导入/续导/查询服务。

原接口(/api/v1/compatibility/check、/api/v1/demo/recursive)保持不变,
新增归档导入相关接口,详见各路由的 summary/description(会展示在 /docs)。
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, Field

from .avro_schema import SchemaError, parse
from .compat import check_compatibility
from .demo import DEMO_RECURSIVE_REQUEST
from .importer import (
    ImportRejected,
    ImportResult,
    import_archive,
    resume_archive,
)
from .storage import (
    ArchiveConflict,
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    Storage,
)

MAX_OCF_BYTES = 64 * 1024 * 1024  # 上传上限 64 MiB
ARCHIVE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

DATA_DIR = os.environ.get("AVRO_DATA_DIR", "/data")
DB_PATH = os.environ.get("AVRO_DB_PATH", os.path.join(DATA_DIR, "avro_imports.db"))
DEMO_OCF_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "fault_events_demo.ocf",
)

app = FastAPI(
    title="Avro 模式兼容检查与故障归档导入服务",
    description=(
        "1) 静态检查 writer/reader Avro 模式兼容性;\n"
        "2) 导入 Kafka 导出的 Avro OCF 归档:先核验 SHA-256 并做兼容检查,"
        "再逐块解码、按 reader 投影字段并补默认值,块级事务写入 SQLite,"
        "支持断点续导、同 ID 一致性校验与跨进程并发防护;\n"
        "3) 查询导入进度与分页记录。\n\n"
        "支持类型:null、boolean、int、long、string、array、具名 record(可递归)、"
        "union;OCF 压缩仅支持 null/deflate;上传上限 64 MiB。"
    ),
    version="2.0.0",
)

storage = Storage(DB_PATH)


# ================================================================ 响应模型

class IssueOut(BaseModel):
    path: str
    reason: str


class CheckRequest(BaseModel):
    writer: Any = Field(..., description="写入旧消息所用的 writer 模式(JSON)")
    reader: Any = Field(..., description="升级后入湖程序的 reader 模式(JSON)")

    model_config = {"json_schema_extra": {"examples": [DEMO_RECURSIVE_REQUEST]}}


class CheckResponse(BaseModel):
    compatible: bool
    writer_errors: List[IssueOut] = Field(default_factory=list)
    reader_errors: List[IssueOut] = Field(default_factory=list)
    issues: List[IssueOut] = Field(default_factory=list)


class ImportResponse(BaseModel):
    archive_id: str = Field(..., description="归档 ID")
    status: str = Field(..., description="processing(有坏块待续导)/ completed")
    sha256: str
    codec: str = Field(..., description="OCF 实际压缩编码:null 或 deflate")
    total_blocks: int = Field(..., description="OCF 内 block 总数")
    total_records: int = Field(..., description="OCF 内记录总数")
    imported_blocks: int = Field(..., description="已提交的完整 block 数")
    imported_records: int = Field(..., description="已入库记录数")
    next_offset: int = Field(..., description="下一个待导入 block 的文件偏移(断点)")
    blocks_this_call: int = Field(..., description="本次调用新导入的 block 数")
    records_this_call: int = Field(..., description="本次调用新入库的记录数")
    error: Optional[str] = Field(None, description="坏块等错误说明(失败块不推进)")
    failed_block: Optional[int] = Field(
        None, description="解码失败的 block 下标(仅坏块场景)"
    )
    already_completed: bool = Field(
        False, description="同 ID 归档此前已全部导入,本次为幂等返回"
    )


class ArchiveOut(BaseModel):
    archive_id: str
    status: str
    sha256: str
    codec: str
    total_blocks: int
    total_records: int
    imported_blocks: int
    imported_records: int
    next_offset: int
    error: Optional[str]
    locked: bool
    created_at: str
    updated_at: str


class RecordOut(BaseModel):
    seq: int = Field(..., description="归档内全局递增序号,可作分页游标")
    block_index: int
    record_index: int = Field(..., description="块内记录下标")
    record: Any = Field(..., description="按 reader 投影后的记录(JSON)")


class RecordsPage(BaseModel):
    archive_id: str
    limit: int
    next_cursor: Optional[int] = Field(
        None, description="下一页游标(传给 cursor);无更多数据时为 null"
    )
    records: List[RecordOut]


# ================================================================ 原兼容检查接口

@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.post(
    "/api/v1/compatibility/check",
    response_model=CheckResponse,
    summary="[原接口] 静态兼容检查",
)
def check(req: CheckRequest) -> CheckResponse:
    writer_errors: List[IssueOut] = []
    reader_errors: List[IssueOut] = []
    writer_node = reader_node = None
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
        issues=[IssueOut(path=issue.path, reason=issue.reason)] if issue else [],
    )


@app.get("/api/v1/demo/recursive", summary="[原接口] 递归模式演示请求体")
def demo_recursive() -> Any:
    return DEMO_RECURSIVE_REQUEST


# ================================================================ 归档导入

def _validate_archive_id(archive_id: str) -> None:
    if not ARCHIVE_ID_RE.match(archive_id):
        raise HTTPException(
            status_code=422,
            detail=(
                "archive_id 非法:长度 1-128,须以字母数字开头,仅允许字母数字、"
                "点、下划线与连字符"
            ),
        )


def _validate_sha(sha: str) -> str:
    norm = (sha or "").strip().lower()
    if not SHA256_RE.match(norm):
        raise HTTPException(
            status_code=422,
            detail="expected_sha256 必须是 64 位十六进制 SHA-256 摘要",
        )
    return norm


def _read_limited(upload: UploadFile) -> bytes:
    """流式读取上传文件,超过 64 MiB 立即拒绝(不继续缓冲)。"""
    chunks: List[bytes] = []
    total = 0
    while True:
        chunk = upload.file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_OCF_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"OCF 文件超过 64 MiB 上限(已读取 {total} 字节)",
            )
        chunks.append(chunk)
    if total == 0:
        raise HTTPException(status_code=422, detail="上传的 OCF 文件为空")
    return b"".join(chunks)


def _parse_reader(reader_schema_text: str) -> Any:
    try:
        doc = json.loads(reader_schema_text)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=422, detail=f"reader_schema 不是合法 JSON:{e}"
        ) from None
    return doc


def _rejected_to_http(exc: ImportRejected) -> HTTPException:
    status = {
        "digest_mismatch": 400,
        "invalid_ocf": 400,
        "invalid_schema": 422,
        "incompatible": 422,
        "not_found": 404,
    }.get(exc.reason, 400)
    return HTTPException(status_code=status, detail={"reason": exc.reason, "message": str(exc)})


def _import_response(result: ImportResult) -> ImportResponse:
    return ImportResponse(
        archive_id=result.archive_id,
        status=result.status,
        sha256=result.sha256,
        codec=result.codec,
        total_blocks=result.total_blocks,
        total_records=result.total_records,
        imported_blocks=result.imported_blocks,
        imported_records=result.imported_records,
        next_offset=result.next_offset,
        blocks_this_call=result.blocks_this_call,
        records_this_call=result.records_this_call,
        error=result.error,
        already_completed=result.already_completed,
        failed_block=result.detail.get("failed_block"),
    )


@app.post(
    "/api/v1/archives/import",
    response_model=ImportResponse,
    status_code=200,
    summary="导入(或同 ID 续导)Avro OCF 归档",
    description=(
        "multipart/form-data 提交归档 ID、预期 SHA-256、reader 模式与 OCF 文件"
        "(≤64 MiB,仅 null/deflate)。服务先核验摘要、校验 OCF 与模式子集并沿用"
        "静态兼容检查,再逐块解码、按 reader 保留字段并补默认值,以块级事务写入 "
        "SQLite:每块记录与下一块偏移一起提交,坏块不推进且此前完整块保留。\n\n"
        "同 ID 再次上传即为续导:SHA-256 与 reader 必须与首次完全一致,否则 409 拒绝;"
        "另一进程正在导入时同样 409。块解码失败返回 422,响应体仍是完整进度对象"
        "(含 failed_block),坏块不推进、此前完整块保留;修复后调用续导接口即可"
        "从断点继续。"
    ),
)
async def import_ocf(
    archive_id: str = Form(..., description="归档 ID,1-128 字符 [A-Za-z0-9._-]"),
    expected_sha256: str = Form(..., description="预期文件 SHA-256(64 位十六进制)"),
    reader_schema: str = Form(
        ..., description="reader 模式的 JSON 文本(同兼容检查接口的 reader)"
    ),
    file: UploadFile = File(..., description="Kafka 导出的 Avro OCF 文件,≤64 MiB"),
) -> ImportResponse:
    _validate_archive_id(archive_id)
    sha = _validate_sha(expected_sha256)
    reader_json = _parse_reader(reader_schema)
    data = _read_limited(file)
    try:
        result = import_archive(
            storage=storage,
            data_dir=DATA_DIR,
            archive_id=archive_id,
            expected_sha256=sha,
            reader_schema=reader_json,
            data=data,
        )
    except ImportRejected as e:
        raise _rejected_to_http(e) from None
    except ArchiveConflict as e:
        raise HTTPException(status_code=409, detail=str(e)) from None
    resp = _import_response(result)
    if result.status == "failed":
        # 坏块:进度已保留,用 422 明确提示调用方需要修复后续导
        raise HTTPException(status_code=422, detail=resp.model_dump())
    return resp


@app.post(
    "/api/v1/archives/{archive_id}/resume",
    response_model=ImportResponse,
    summary="按归档 ID 从断点续导",
    description=(
        "无需重新上传:使用首次导入落盘的 OCF,从 next_offset 指向的下一个 block "
        "继续。先复核落盘文件 SHA-256,再逐块提交;同 ID 的摘要/reader 一致性与"
        "跨进程锁规则与导入接口相同。已完成的归档幂等返回 already_completed=true。"
    ),
)
def resume_ocf(archive_id: str) -> ImportResponse:
    _validate_archive_id(archive_id)
    try:
        result = resume_archive(storage, archive_id)
    except ImportRejected as e:
        raise _rejected_to_http(e) from None
    except ArchiveConflict as e:
        raise HTTPException(status_code=409, detail=str(e)) from None
    resp = _import_response(result)
    if result.status == "failed":
        raise HTTPException(status_code=422, detail=resp.model_dump())
    return resp


# ================================================================ 进度与记录查询

def _archive_out(row) -> ArchiveOut:
    return ArchiveOut(
        archive_id=row["archive_id"],
        status=row["status"],
        sha256=row["sha256"],
        codec=row["codec"],
        total_blocks=row["total_blocks"],
        total_records=row["total_records"],
        imported_blocks=row["imported_blocks"],
        imported_records=row["imported_records"],
        next_offset=row["next_offset"],
        error=row["error"],
        locked=bool(row["locked"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.get(
    "/api/v1/archives",
    response_model=List[ArchiveOut],
    summary="列出全部归档及其导入进度",
)
def list_archives(
    limit: int = Query(100, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
) -> List[ArchiveOut]:
    return [_archive_out(r) for r in storage.list_archives(limit, offset)]


@app.get(
    "/api/v1/archives/{archive_id}",
    response_model=ArchiveOut,
    summary="查询单个归档的导入进度",
    description="返回块/记录计数、断点偏移、状态与锁信息;归档不存在返回 404。",
)
def get_archive(archive_id: str) -> ArchiveOut:
    _validate_archive_id(archive_id)
    row = storage.get_archive(archive_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"归档 '{archive_id}' 不存在")
    return _archive_out(row)


@app.get(
    "/api/v1/archives/{archive_id}/records",
    response_model=RecordsPage,
    summary="分页查询归档已导入的记录",
    description=(
        "keyset 分页:首次不传 cursor,后续把上一页响应的 next_cursor 原样传回;"
        "按归档内 seq 升序返回,重启服务后分页依然有效。"
    ),
)
def get_records(
    archive_id: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    cursor: Optional[int] = Query(None, ge=0, description="上一页返回的 next_cursor"),
) -> RecordsPage:
    _validate_archive_id(archive_id)
    if storage.get_archive(archive_id) is None:
        raise HTTPException(status_code=404, detail=f"归档 '{archive_id}' 不存在")
    rows, next_cursor = storage.list_records(archive_id, limit, cursor)
    return RecordsPage(
        archive_id=archive_id,
        limit=limit,
        next_cursor=next_cursor,
        records=[
            RecordOut(
                seq=r["seq"],
                block_index=r["block_index"],
                record_index=r["record_index"],
                record=json.loads(r["payload"]),
            )
            for r in rows
        ],
    )


# ================================================================ 演示归档

@app.get(
    "/api/v1/demo/recursive-ocf",
    summary="递归故障演示 OCF 的下载入口与调用说明",
    description=(
        "返回演示归档(examples/fault_events_demo.ocf)的元信息、SHA-256、"
        "reader 模式以及可直接复制的 curl 导入/续导/查询命令。"
        "原始 OCF 字节通过 /api/v1/demo/recursive-ocf/download 下载。"
    ),
)
def demo_ocf_info() -> Any:
    if not os.path.exists(DEMO_OCF_PATH):
        raise HTTPException(status_code=404, detail="演示归档未生成")
    import hashlib

    with open(DEMO_OCF_PATH, "rb") as f:
        data = f.read()
    sha = hashlib.sha256(data).hexdigest()
    return {
        "description": "递归故障演示归档:FaultEvent/Fault,cause 字段递归引用,"
        "deflate 压缩,共 3 个 block、4 条记录;reader 为 demo 演进模式"
        "(severity int->long、丢弃 message、新增 occurredAt/component 默认值)。",
        "file_name": "fault_events_demo.ocf",
        "size_bytes": len(data),
        "sha256": sha,
        "codec": "deflate",
        "blocks": 3,
        "records": 4,
        "reader_schema": DEMO_RECURSIVE_REQUEST["reader"],
        "download_url": "/api/v1/demo/recursive-ocf/download",
        "curl_examples": {
            "download": "curl -o fault_events_demo.ocf "
            "http://localhost:8000/api/v1/demo/recursive-ocf/download",
            "import": (
                "curl -X POST http://localhost:8000/api/v1/archives/import "
                "-F archive_id=demo-fault-001 "
                f"-F expected_sha256={sha} "
                "-F reader_schema=\"<见本响应 reader_schema 的 JSON 文本>\" "
                "-F file=@fault_events_demo.ocf"
            ),
            "resume": "curl -X POST "
            "http://localhost:8000/api/v1/archives/demo-fault-001/resume",
            "progress": "curl http://localhost:8000/api/v1/archives/demo-fault-001",
            "records": "curl "
            "'http://localhost:8000/api/v1/archives/demo-fault-001/records?limit=2'",
        },
    }


@app.get(
    "/api/v1/demo/recursive-ocf/download",
    summary="下载递归故障演示 OCF 字节",
)
def demo_ocf_download() -> Response:
    if not os.path.exists(DEMO_OCF_PATH):
        raise HTTPException(status_code=404, detail="演示归档未生成")
    with open(DEMO_OCF_PATH, "rb") as f:
        data = f.read()
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": 'attachment; filename="fault_events_demo.ocf"'
        },
    )
