"""归档导入编排:摘要核验、OCF 校验、兼容检查、逐块解码与块级提交。

导入流程(全部失败均不落任何该块之后的进度):
1. 对上传字节流式计算 SHA-256,与预期摘要不符直接拒绝;
2. 解析 OCF 头部:魔数、avro.schema、avro.codec(仅 null/deflate);
3. 用现有解析器校验 writer/reader 模式属于支持的类型子集,
   并沿用静态兼容检查确认 reader 可读全部旧消息;
4. 归档文件原子落盘,在 archives 表注册/接管(同 ID 校验摘要与 reader);
5. 从 next_offset 起逐块解码:按 reader 投影字段、补默认值,
   整块记录与下一块偏移在同一事务提交;坏块不推进、标记 failed。
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .avro_codec import AvroDataError, OcfReader
from .avro_schema import SchemaError, parse
from .compat import Issue, check_compatibility
from .storage import ArchiveConflict, BlockConflict, Storage


class ImportRejected(Exception):
    """导入在写入任何进度前被拒绝(摘要/格式/兼容问题)。"""

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason = reason  # digest_mismatch / invalid_ocf / invalid_schema / incompatible


@dataclass
class ImportResult:
    archive_id: str
    status: str                       # processing / completed / failed
    sha256: str
    codec: str
    total_blocks: int
    total_records: int
    imported_blocks: int
    imported_records: int
    next_offset: int
    blocks_this_call: int = 0
    records_this_call: int = 0
    error: Optional[str] = None
    already_completed: bool = False
    detail: Dict[str, Any] = field(default_factory=dict)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _persist_file(data_dir: str, archive_id: str, data: bytes) -> str:
    files_dir = os.path.join(data_dir, "files")
    os.makedirs(files_dir, exist_ok=True)
    final_path = os.path.join(files_dir, f"{archive_id}.ocf")
    tmp_path = os.path.join(files_dir, f".{archive_id}.ocf.tmp")
    with open(tmp_path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_path)
    return final_path


def validate_schemas_and_compat(
    writer_json: Any, reader_json: Any
) -> tuple:
    try:
        writer_node = parse(writer_json)
    except SchemaError as e:
        raise ImportRejected(
            f"OCF 内嵌 writer 模式非法({e.path}):{e.message}", "invalid_schema"
        ) from None
    try:
        reader_node = parse(reader_json)
    except SchemaError as e:
        raise ImportRejected(f"reader 模式非法({e.path}):{e.message}", "invalid_schema")
    issue: Optional[Issue] = check_compatibility(writer_node, reader_node)
    if issue is not None:
        raise ImportRejected(
            f"reader 与 OCF 内嵌 writer 模式不兼容({issue.path}):{issue.reason}",
            "incompatible",
        )
    return writer_node, reader_node


def import_archive(
    storage: Storage,
    data_dir: str,
    archive_id: str,
    expected_sha256: str,
    reader_schema: Any,
    data: bytes,
) -> ImportResult:
    """上传字节并完成(或续上)一次导入。"""
    actual_sha = sha256_hex(data)
    if actual_sha != expected_sha256.lower():
        raise ImportRejected(
            f"SHA-256 校验失败:期望 {expected_sha256.lower()},实际 {actual_sha}",
            "digest_mismatch",
        )

    try:
        reader = OcfReader(io.BytesIO(data))
    except AvroDataError as e:
        raise ImportRejected(str(e), "invalid_ocf") from None
    writer_node, reader_node = validate_schemas_and_compat(
        reader.writer_schema_json, reader_schema
    )

    file_path = _persist_file(data_dir, archive_id, data)

    try:
        row, created = storage.register_or_acquire(
            archive_id=archive_id,
            sha256=actual_sha,
            reader_schema=reader_schema,
            writer_schema=reader.writer_schema_json,
            codec=reader.codec,
            file_path=file_path,
            total_blocks=len(reader.blocks),
            total_records=reader.total_records,
            first_block_offset=reader.blocks[0].offset if reader.blocks else 0,
        )
    except ArchiveConflict:
        raise

    if row["status"] == "completed":
        return _result(row, already_completed=True)

    start_blocks = row["imported_blocks"]
    start_records = row["imported_records"]
    next_offset = row["next_offset"]

    block = None
    try:
        for pos, block in enumerate(reader.blocks):
            if block.offset < next_offset:
                continue
            records = reader.read_block(block, writer_node, reader_node)
            # 下一个待处理块的起始偏移(最后一块用 EOF 位置)
            if pos + 1 < len(reader.blocks):
                block_next = reader.blocks[pos + 1].offset
            else:
                block_next = reader.stream.tell()
            storage.commit_block(
                archive_id=archive_id,
                block_index=block.index,
                expected_offset=block.offset,
                next_offset=block_next,
                records=records,
            )
            next_offset = block_next
    except AvroDataError as e:
        idx = block.index if block is not None else -1
        msg = f"block {idx} 解码失败,该块未入库,此前完整块已保留:{e}"
        storage.mark_failed(archive_id, msg)
        row = storage.get_archive(archive_id)
        res = _result(row, start_blocks, start_records)
        res.error = msg
        res.detail["failed_block"] = idx
        return res
    except BlockConflict:
        storage.release_lock(archive_id, "processing", None)
        raise

    storage.mark_completed(archive_id)
    row = storage.get_archive(archive_id)
    return _result(row, start_blocks, start_records)


def resume_archive(storage: Storage, archive_id: str) -> ImportResult:
    """不重新上传,直接用已落盘的 OCF 文件从断点继续导入。"""
    row = storage.get_archive(archive_id)
    if row is None:
        raise ImportRejected(
            f"归档 '{archive_id}' 不存在,无法续导(请先调用导入接口上传 OCF)",
            "not_found",
        )
    if row["status"] == "completed":
        return _result(row, already_completed=True)
    file_path = row["file_path"]
    if not os.path.exists(file_path):
        raise ImportRejected(
            f"归档文件已丢失({file_path}),无法续导,请重新上传导入", "invalid_ocf"
        )
    with open(file_path, "rb") as f:
        data = f.read()
    actual_sha = sha256_hex(data)
    if actual_sha != row["sha256"]:
        raise ImportRejected(
            f"落盘文件 SHA-256 已变化(期望 {row['sha256']},实际 {actual_sha}),"
            "拒绝续导",
            "digest_mismatch",
        )

    try:
        reader = OcfReader(io.BytesIO(data))
    except AvroDataError as e:
        raise ImportRejected(str(e), "invalid_ocf") from None
    reader_schema = _loads(row["reader_schema"])
    writer_node, reader_node = validate_schemas_and_compat(
        reader.writer_schema_json, reader_schema
    )

    # 重新加锁(沿用同 ID 一致性规则;摘要已在上面对过)
    try:
        row2, _ = storage.register_or_acquire(
            archive_id=archive_id,
            sha256=actual_sha,
            reader_schema=reader_schema,
            writer_schema=reader.writer_schema_json,
            codec=reader.codec,
            file_path=file_path,
            total_blocks=len(reader.blocks),
            total_records=reader.total_records,
            first_block_offset=reader.blocks[0].offset if reader.blocks else 0,
        )
        row = row2
    except ArchiveConflict:
        raise

    start_blocks = row["imported_blocks"]
    start_records = row["imported_records"]
    next_offset = row["next_offset"]

    block = None
    try:
        for pos, block in enumerate(reader.blocks):
            if block.offset < next_offset:
                continue
            records = reader.read_block(block, writer_node, reader_node)
            if pos + 1 < len(reader.blocks):
                block_next = reader.blocks[pos + 1].offset
            else:
                block_next = reader.stream.tell()
            storage.commit_block(
                archive_id=archive_id,
                block_index=block.index,
                expected_offset=block.offset,
                next_offset=block_next,
                records=records,
            )
            next_offset = block_next
    except AvroDataError as e:
        idx = block.index if block is not None else -1
        msg = f"block {idx} 解码失败,该块未入库,此前完整块已保留:{e}"
        storage.mark_failed(archive_id, msg)
        row = storage.get_archive(archive_id)
        res = _result(row, start_blocks, start_records)
        res.error = msg
        res.detail["failed_block"] = idx
        return res
    except BlockConflict:
        storage.release_lock(archive_id, "processing", None)
        raise

    storage.mark_completed(archive_id)
    row = storage.get_archive(archive_id)
    return _result(row, start_blocks, start_records)


def _loads(canon: str) -> Any:
    return json.loads(canon)


def _result(
    row, start_blocks: Optional[int] = None, start_records: Optional[int] = None,
    already_completed: bool = False,
) -> ImportResult:
    if start_blocks is None:
        start_blocks = row["imported_blocks"]
    if start_records is None:
        start_records = row["imported_records"]
    return ImportResult(
        archive_id=row["archive_id"],
        status=row["status"],
        sha256=row["sha256"],
        codec=row["codec"],
        total_blocks=row["total_blocks"],
        total_records=row["total_records"],
        imported_blocks=row["imported_blocks"],
        imported_records=row["imported_records"],
        next_offset=row["next_offset"],
        blocks_this_call=row["imported_blocks"] - start_blocks,
        records_this_call=row["imported_records"] - start_records,
        error=row["error"],
        already_completed=already_completed,
        detail={"locked": bool(row["locked"])},
    )
