"""测试辅助:构造指定块 Avro 负载损坏但 OCF 结构仍可解析的归档。"""

import io
import zlib

from app.avro_codec import (
    BinaryEncoder,
    OcfReader,
    _read_long_from_stream,
)


class _Stream:
    def __init__(self, data, pos):
        self.data, self.pos = data, pos

    def read(self, n):
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk


def corrupt_block(data: bytes, block_index: int) -> bytes:
    """在指定 block 的解压后 Avro 字节流前插入非法字节,重新 deflate 拼回。"""
    reader = OcfReader(io.BytesIO(data))
    target = reader.blocks[block_index]
    stream = _Stream(data, target.offset)
    _read_long_from_stream(stream)
    size = _read_long_from_stream(stream)
    payload_start = stream.pos
    raw = zlib.decompress(data[payload_start:payload_start + size], -15)
    garbage = b"\xff\xff\xff\xff" + raw
    compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
    new_payload = compressor.compress(garbage) + compressor.flush()
    header = BinaryEncoder()
    header.write_long(target.object_count)
    header.write_long(len(new_payload))
    return (
        data[:target.offset]
        + header.getvalue()
        + new_payload
        + data[payload_start + size:]
    )
