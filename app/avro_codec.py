"""Avro 二进制编解码与 OCF(Object Container File)读写,仅用标准库。

覆盖本服务支持的类型子集:null、boolean、int、long、string、array、
具名 record(允许递归)、union。容器压缩仅支持 null 与 deflate。

解码时同时按 writer/reader 两份模式做 schema 演进投影:
- writer 有而 reader 没有的字段被丢弃;
- reader 新增字段补类型相符的默认值;
- int 读出后可作为 long 呈现(本子集内 Python int 无需转换);
- union 按写入分支读取,再投影到 reader 同名分支;reader 无对应分支时,
  交由兼容检查提前拦截(导入前已做静态兼容检查,此处为防御性校验)。

OCF 布局(1.11.0 规范):
  魔数 b'Obj\\x01' | 元数据 map(键/值均为 bytes;avro.schema、avro.codec)
  | 16 字节同步标记 | 若干 block
每个 block:long 对象数 | long 压缩字节数 | (压缩的)对象字节流 | 同步标记;
以 0 对象数的 block 作为文件结束标记。
"""

from __future__ import annotations

import io
import json
import zlib
from typing import Any, BinaryIO, Dict, Iterator, List, Optional

from .avro_schema import Array, Field, Node, Primitive, Record, Union

OCF_MAGIC = b"Obj\x01"
SYNC_SIZE = 16
SUPPORTED_CODECS = ("null", "deflate")
MAX_BLOCK_OBJECTS = 1_000_000  # 防御性上限,防止损坏的长度字段导致失控


class AvroDataError(Exception):
    """Avro 数据/容器文件损坏或使用了不支持的特性。"""


# ================================================================ 底层读写字节

class BinaryDecoder:
    """从字节缓冲区按 Avro 二进制编码读取原始值。"""

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def read_byte(self) -> int:
        if self.pos >= len(self.buf):
            raise AvroDataError("数据已读完但仍尝试读取(块被截断)")
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def read_long(self) -> int:
        """读取 zig-zag 变长编码的 int/long。"""
        result = 0
        shift = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
            if shift > 63:
                raise AvroDataError("变长整数超过 64 位(数据损坏)")
        # zig-zag 解码
        return (result >> 1) ^ -(result & 1)

    def read_bytes_raw(self, n: int) -> bytes:
        if n < 0:
            raise AvroDataError(f"字节长度为负({n}),数据损坏")
        end = self.pos + n
        if end > len(self.buf):
            raise AvroDataError(
                f"需要读取 {n} 字节,但块内仅剩 {len(self.buf) - self.pos} 字节(块被截断)"
            )
        out = self.buf[self.pos:end]
        self.pos = end
        return out

    def read_string(self) -> str:
        n = self.read_long()
        raw = self.read_bytes_raw(n)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise AvroDataError(f"string 不是合法 UTF-8:{e}") from None

    def expect_end(self) -> None:
        if self.pos != len(self.buf):
            raise AvroDataError(
                f"块解码后仍有 {len(self.buf) - self.pos} 个多余字节"
            )


class BinaryEncoder:
    """把原始值按 Avro 二进制编码写入缓冲区(用于生成演示归档)。"""

    def __init__(self) -> None:
        self.parts: List[bytes] = []

    def write_long(self, value: int) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise AvroDataError(f"int/long 必须是整数,实际为 {type(value).__name__}")
        # zig-zag
        zz = (value << 1) ^ (value >> 63)
        if zz < 0:
            raise AvroDataError("整数超出 int64 范围")
        out = bytearray()
        while True:
            b = zz & 0x7F
            zz >>= 7
            if zz:
                out.append(b | 0x80)
            else:
                out.append(b)
                break
        self.parts.append(bytes(out))

    def write_string(self, value: str) -> None:
        if not isinstance(value, str):
            raise AvroDataError(f"string 必须是 str,实际为 {type(value).__name__}")
        raw = value.encode("utf-8")
        self.write_long(len(raw))
        self.parts.append(raw)

    def getvalue(self) -> bytes:
        return b"".join(self.parts)


# ================================================================ 值解码 + 投影

def decode_value(dec: BinaryDecoder, writer: Node, reader: Optional[Node]) -> Any:
    """按 writer 模式读一个值;reader 非空时同时做演进投影。"""
    # ---- 基础类型 ----
    if isinstance(writer, Primitive):
        return _decode_primitive(dec, writer.name)

    # ---- record ----
    if isinstance(writer, Record):
        return _decode_record(dec, writer, reader)

    # ---- array ----
    if isinstance(writer, Array):
        reader_items = reader.items if isinstance(reader, Array) else None
        if reader is not None and not isinstance(reader, Array):
            raise AvroDataError(
                "writer 写出 array,reader 模式为不兼容类型,无法投影"
            )
        items: List[Any] = []
        while True:
            block_count = dec.read_long()
            if block_count == 0:
                break
            if block_count < 0:
                dec.read_long()  # 块字节数,本实现不需要
                block_count = -block_count
            for _ in range(block_count):
                items.append(decode_value(dec, writer.items, reader_items))
        return items

    # ---- union ----
    if isinstance(writer, Union):
        branch_index = dec.read_long()
        if not (0 <= branch_index < len(writer.branches)):
            raise AvroDataError(
                "union 分支下标 {} 越界(共 {} 个分支)".format(
                    branch_index, len(writer.branches)
                )
            )
        wb = writer.branches[branch_index]
        rb = _reader_union_branch(reader, wb) if reader is not None else None
        return decode_value(dec, wb, rb)

    raise AvroDataError(f"内部错误:未知 writer 节点 {type(writer).__name__}")


def _decode_primitive(dec: BinaryDecoder, name: str) -> Any:
    if name == "null":
        return None
    if name == "boolean":
        return dec.read_byte() != 0
    if name in ("int", "long"):
        value = dec.read_long()
        if name == "int" and not (-(2**31) <= value <= 2**31 - 1):
            raise AvroDataError(f"int 值 {value} 超出 int32 范围")
        return value
    if name == "string":
        return dec.read_string()
    raise AvroDataError(f"内部错误:不支持的基础类型 '{name}'")


def _decode_record(dec: BinaryDecoder, writer: Record, reader: Optional[Node]) -> dict:
    if reader is not None and not isinstance(reader, Record):
        raise AvroDataError(
            f"writer 写出 record {writer.name},reader 模式为不兼容类型,无法投影"
        )
    reader_fields: Dict[str, Field] = (
        {f.name: f for f in reader.fields} if isinstance(reader, Record) else {}
    )
    result: Dict[str, Any] = {}
    for wf in writer.fields:
        value = decode_value(
            dec, wf.type, reader_fields[wf.name].type if wf.name in reader_fields else None
        )
        if wf.name in reader_fields:
            result[wf.name] = value
    if isinstance(reader, Record):
        # reader 新增字段:补默认值(writer 中不存在的字段)
        writer_names = {f.name for f in writer.fields}
        for rf in reader.fields:
            if rf.name not in writer_names:
                if not rf.has_default:
                    raise AvroDataError(
                        f"reader 新增字段 '{rf.name}' 缺少默认值,无法补全"
                    )
                result[rf.name] = default_value(rf.type, rf.default)
    return result


def _reader_union_branch(reader: Node, writer_branch: Node) -> Optional[Node]:
    """在 reader union 中找到与 writer 分支同名的 record 分支,
    基础/数组分支按类型匹配;reader 非 union 时直接返回自身。"""
    if not isinstance(reader, Union):
        return reader
    if isinstance(writer_branch, Record):
        for rb in reader.branches:
            if isinstance(rb, Record) and rb.name == writer_branch.name:
                return rb
        return None
    for rb in reader.branches:
        if type(rb) is type(writer_branch):
            if isinstance(writer_branch, Primitive) and isinstance(rb, Primitive):
                if rb.name == writer_branch.name or (
                    writer_branch.name == "int" and rb.name == "long"
                ):
                    return rb
            elif isinstance(writer_branch, Array) and isinstance(rb, Array):
                return rb
    return None


def default_value(node: Node, raw_default: Any) -> Any:
    """把模式中的 JSON 默认值转换为运行时值(本子集内即其本身,
    递归 record 的默认值按字段默认值补全)。"""
    if isinstance(node, Union):
        return default_value(node.branches[0], raw_default)
    if isinstance(node, Primitive):
        return raw_default
    if isinstance(node, Array):
        return list(raw_default) if isinstance(raw_default, list) else raw_default
    if isinstance(node, Record):
        out: Dict[str, Any] = {}
        given = raw_default if isinstance(raw_default, dict) else {}
        for f in node.fields:
            if f.name in given:
                out[f.name] = default_value(f.type, given[f.name])
            elif f.has_default:
                out[f.name] = default_value(f.type, f.default)
        return out
    raise AvroDataError(f"内部错误:未知节点类型 {type(node).__name__}")


# ================================================================ 值编码(演示用)

def encode_value(enc: BinaryEncoder, node: Node, value: Any) -> None:
    """按给定模式把 Python 值编码为 Avro 二进制(用于生成演示 OCF)。"""
    if isinstance(node, Primitive):
        _encode_primitive(enc, node.name, value)
        return
    if isinstance(node, Record):
        if not isinstance(value, dict):
            raise AvroDataError(f"record {node.name} 的值必须是对象")
        for f in node.fields:
            if f.name not in value:
                if f.has_default:
                    encode_value(enc, f.type, default_value(f.type, f.default))
                    continue
                raise AvroDataError(f"record {node.name} 缺少字段 '{f.name}' 的值")
            encode_value(enc, f.type, value[f.name])
        return
    if isinstance(node, Array):
        if not isinstance(value, list):
            raise AvroDataError("array 的值必须是数组")
        if value:
            # 非空数组:一个 block(计数=长度)+ 结束标记 0;空数组只有一个 0
            enc.write_long(len(value))
            for item in value:
                encode_value(enc, node.items, item)
        enc.write_long(0)
        return
    if isinstance(node, Union):
        idx = _match_union_branch(node, value)
        enc.write_long(idx)
        encode_value(enc, node.branches[idx], value)
        return
    raise AvroDataError(f"内部错误:未知节点类型 {type(node).__name__}")


def _encode_primitive(enc: BinaryEncoder, name: str, value: Any) -> None:
    if name == "null":
        if value is not None:
            raise AvroDataError("null 类型的值必须是 null")
        return
    if name == "boolean":
        if not isinstance(value, bool):
            raise AvroDataError("boolean 值必须是 true/false")
        enc.parts.append(b"\x01" if value else b"\x00")
        return
    if name in ("int", "long"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise AvroDataError(f"{name} 值必须是整数")
        if name == "int" and not (-(2**31) <= value <= 2**31 - 1):
            raise AvroDataError(f"int 值 {value} 超出 int32 范围")
        if not (-(2**63) <= value <= 2**63 - 1):
            raise AvroDataError(f"long 值 {value} 超出 int64 范围")
        enc.write_long(value)
        return
    if name == "string":
        enc.write_string(value)
        return
    raise AvroDataError(f"内部错误:不支持的基础类型 '{name}'")


def _match_union_branch(node: Union, value: Any) -> int:
    for i, b in enumerate(node.branches):
        if isinstance(b, Primitive):
            if b.name == "null" and value is None:
                return i
            if b.name == "boolean" and isinstance(value, bool):
                return i
            if b.name in ("int", "long") and isinstance(value, int) and not isinstance(value, bool):
                return i
            if b.name == "string" and isinstance(value, str):
                return i
        elif isinstance(b, Record) and isinstance(value, dict):
            return i
        elif isinstance(b, Array) and isinstance(value, list):
            return i
    raise AvroDataError(f"值 {value!r} 不匹配 union 的任何分支")


# ================================================================ OCF 读取

class OcfBlock:
    """一个 block 的定位与解码信息。"""

    def __init__(self, index: int, offset: int, object_count: int, codec: str):
        self.index = index            # 从 0 开始的块序号
        self.offset = offset          # block 头部(对象数字节)在文件中的绝对偏移
        self.object_count = object_count
        self.codec = codec


class OcfReader:
    """流式读取 OCF:先解析头部与 block 目录,再按需 seek 解码指定块。

    块目录只记录偏移与对象数,不缓存数据,因此大文件只占很少内存;
    续导时从指定偏移重新 seek 即可。
    """

    def __init__(self, stream: BinaryIO):
        self.stream = stream
        self.meta: Dict[str, bytes] = {}
        self.codec = "null"
        self.sync: bytes = b""
        self.writer_schema_json: Any = None
        self.blocks: List[OcfBlock] = []
        self._parse_header()
        self._build_block_index()

    # ---- 头部 ----
    def _parse_header(self) -> None:
        magic = self.stream.read(4)
        if magic != OCF_MAGIC:
            raise AvroDataError("不是 Avro OCF 文件:魔数不匹配(应为 b'Obj\\x01')")
        meta = _read_metadata_map(self.stream)
        if "avro.schema" not in meta:
            raise AvroDataError("OCF 元数据缺少 avro.schema")
        try:
            self.writer_schema_json = json.loads(meta["avro.schema"].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise AvroDataError(f"avro.schema 不是合法 UTF-8 JSON:{e}") from None
        codec_bytes = meta.get("avro.codec", b"null")
        try:
            self.codec = codec_bytes.decode("utf-8")
        except UnicodeDecodeError:
            raise AvroDataError("avro.codec 不是合法 UTF-8 字符串") from None
        if self.codec not in SUPPORTED_CODECS:
            raise AvroDataError(
                f"不支持的压缩编码 '{self.codec}',仅支持 {', '.join(SUPPORTED_CODECS)}"
            )
        self.meta = meta
        self.sync = self.stream.read(SYNC_SIZE)
        if len(self.sync) != SYNC_SIZE:
            raise AvroDataError("同步标记被截断(文件不完整)")

    # ---- block 目录 ----
    def _build_block_index(self) -> None:
        idx = 0
        while True:
            offset = self.stream.tell()
            header = self.stream.read(1)
            if not header:
                break  # 正常 EOF
            self.stream.seek(offset)
            count = _read_long_from_stream(self.stream)
            if count == 0:
                # 结束标记:后跟同步标记
                tail = self.stream.read(SYNC_SIZE)
                if tail != self.sync:
                    raise AvroDataError("文件结束块的同步标记不匹配(文件损坏)")
                trailing = self.stream.read(1)
                if trailing:
                    raise AvroDataError("OCF 结束标记之后仍有多余字节(文件损坏)")
                break
            if count < 0:
                raise AvroDataError("block 对象数为负,本实现要求未使用 block 计数扩展")
            if count > MAX_BLOCK_OBJECTS:
                raise AvroDataError(f"block 对象数 {count} 超过防御上限")
            size = _read_long_from_stream(self.stream)
            if size < 0:
                raise AvroDataError("block 字节数为负(文件损坏)")
            # 跳过数据与同步标记
            self.stream.seek(size, io.SEEK_CUR)
            tail = self.stream.read(SYNC_SIZE)
            if len(tail) != SYNC_SIZE:
                raise AvroDataError("block 同步标记被截断(文件不完整)")
            if tail != self.sync:
                raise AvroDataError(f"block {idx} 同步标记不匹配(文件损坏)")
            self.blocks.append(OcfBlock(idx, offset, count, self.codec))
            idx += 1

    # ---- 按块解码 ----
    def read_block(self, block: OcfBlock, writer_node: Node, reader_node: Optional[Node]) -> List[Any]:
        self.stream.seek(block.offset)
        count = _read_long_from_stream(self.stream)
        if count != block.object_count:
            raise AvroDataError(
                f"block {block.index} 对象数与目录不一致(文件被改动?)"
            )
        size = _read_long_from_stream(self.stream)
        payload = self.stream.read(size)
        if len(payload) != size:
            raise AvroDataError(f"block {block.index} 数据被截断")
        if self.stream.read(SYNC_SIZE) != self.sync:
            raise AvroDataError(f"block {block.index} 同步标记不匹配")
        if block.codec == "deflate":
            try:
                raw = zlib.decompress(payload, -15)
            except zlib.error as e:
                raise AvroDataError(f"block {block.index} deflate 解压失败:{e}") from None
        else:
            raw = payload
        dec = BinaryDecoder(raw)
        records: List[Any] = []
        for i in range(block.object_count):
            try:
                records.append(decode_value(dec, writer_node, reader_node))
            except AvroDataError:
                raise
            except Exception as e:  # 防御:任何解码异常都归类为坏块
                raise AvroDataError(
                    f"block {block.index} 第 {i} 条记录解码失败:{e}"
                ) from None
        dec.expect_end()
        return records

    def blocks_from_offset(self, next_offset: int) -> Iterator[OcfBlock]:
        for block in self.blocks:
            if block.offset >= next_offset:
                yield block

    @property
    def total_records(self) -> int:
        return sum(b.object_count for b in self.blocks)


def _read_long_from_stream(stream: BinaryIO) -> int:
    result = 0
    shift = 0
    while True:
        chunk = stream.read(1)
        if not chunk:
            raise AvroDataError("读取变长整数时遇到 EOF(文件被截断)")
        b = chunk[0]
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
        if shift > 63:
            raise AvroDataError("变长整数超过 64 位(数据损坏)")
    return (result >> 1) ^ -(result & 1)


def _read_metadata_map(stream: BinaryIO) -> Dict[str, bytes]:
    """读取 OCF 头部的元数据 map:long(对数,负数带块大小) + 键值对。"""
    count = _read_long_from_stream(stream)
    block_size: Optional[int] = None
    if count < 0:
        block_size = _read_long_from_stream(stream)
        count = -count
    if block_size is not None and block_size <= 0:
        raise AvroDataError("元数据 map 块大小非法")
    meta: Dict[str, bytes] = {}
    for _ in range(count):
        key_len = _read_long_from_stream(stream)
        key_raw = stream.read(key_len)
        if len(key_raw) != key_len:
            raise AvroDataError("元数据键被截断")
        val_len = _read_long_from_stream(stream)
        val = stream.read(val_len)
        if len(val) != val_len:
            raise AvroDataError("元数据值被截断")
        try:
            key = key_raw.decode("utf-8")
        except UnicodeDecodeError:
            raise AvroDataError("元数据键不是合法 UTF-8") from None
        meta[key] = val
    if count != 0:
        # 头部 map 规范上只有一个 block;若有后续 block(0 结束标记)消费掉
        end = _read_long_from_stream(stream)
        if end != 0:
            raise AvroDataError("本实现只支持单 block 的 OCF 元数据 map")
    return meta


# ================================================================ OCF 写入(演示归档)

def write_ocf(
    stream: BinaryIO,
    schema_json: Any,
    blocks: List[List[Any]],
    schema_node: Node,
    codec: str = "deflate",
    sync: Optional[bytes] = None,
) -> None:
    """把若干 block 的记录写成 OCF。blocks 为记录列表的列表(每个元素一个块)。"""
    if codec not in SUPPORTED_CODECS:
        raise AvroDataError(f"不支持的编码 '{codec}'")
    if sync is None:
        sync = b"fault-demo-sync!"  # 固定 16 字节,演示用
    if len(sync) != SYNC_SIZE:
        raise AvroDataError("同步标记必须是 16 字节")

    # 头部
    stream.write(OCF_MAGIC)
    schema_bytes = json.dumps(schema_json, separators=(",", ":")).encode("utf-8")
    meta = {"avro.schema": schema_bytes, "avro.codec": codec.encode("utf-8")}
    enc = BinaryEncoder()
    enc.write_long(len(meta))
    for key, val in meta.items():
        enc.write_string(key)
        enc.write_long(len(val))
        enc.parts.append(val)
    enc.write_long(0)  # map 结束
    stream.write(enc.getvalue())
    stream.write(sync)

    # 数据块
    for records in blocks:
        body = BinaryEncoder()
        for record in records:
            encode_value(body, schema_node, record)
        raw = body.getvalue()
        if codec == "deflate":
            compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
            payload = compressor.compress(raw) + compressor.flush()
        else:
            payload = raw
        hdr = BinaryEncoder()
        hdr.write_long(len(records))
        hdr.write_long(len(payload))
        stream.write(hdr.getvalue())
        stream.write(payload)
        stream.write(sync)

    # 结束块:0 对象数 + 同步标记
    zero = BinaryEncoder()
    zero.write_long(0)
    stream.write(zero.getvalue())
    stream.write(sync)
    stream.flush()
