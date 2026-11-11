"""Avro 二进制编解码与 OCF 容器的单元测试(仅依赖标准库)。"""

import io
import json
import unittest
import zlib

from app.avro_codec import (
    BinaryEncoder,
    OcfReader,
    AvroDataError,
    decode_value,
    BinaryDecoder,
    default_value,
    write_ocf,
)
from app.avro_schema import parse
from app.demo import DEMO_RECURSIVE_REQUEST


WRITER = DEMO_RECURSIVE_REQUEST["writer"]
READER = DEMO_RECURSIVE_REQUEST["reader"]


def build_ocf(records, codec="deflate", schema=WRITER):
    buf = io.BytesIO()
    write_ocf(buf, schema, [records], parse(schema), codec=codec)
    return buf.getvalue()


class TestPrimitivesEncoding(unittest.TestCase):
    def roundtrip(self, schema, value):
        node = parse(schema)
        enc = BinaryEncoder()
        from app.avro_codec import encode_value
        encode_value(enc, node, value)
        return decode_value(BinaryDecoder(enc.getvalue()), node, node)

    def test_zigzag_long(self):
        for v in [0, 1, -1, 2, -2, 63, 64, -64, 2**40, -(2**40), 2**63 - 1]:
            self.assertEqual(self.roundtrip("long", v), v)

    def test_int_bounds(self):
        self.assertEqual(self.roundtrip("int", 2**31 - 1), 2**31 - 1)
        self.assertEqual(self.roundtrip("int", -(2**31)), -(2**31))
        with self.assertRaises(AvroDataError):
            self.roundtrip("int", 2**31)

    def test_boolean_null_string(self):
        self.assertEqual(self.roundtrip("boolean", True), True)
        self.assertEqual(self.roundtrip("boolean", False), False)
        self.assertIsNone(self.roundtrip("null", None))
        self.assertEqual(self.roundtrip("string", "故障码 ✅"), "故障码 ✅")

    def test_truncated_varlong(self):
        enc = BinaryEncoder()
        enc.write_long(100000)
        raw = enc.getvalue()
        with self.assertRaises(AvroDataError):
            BinaryDecoder(raw[:-1]).read_long()


class TestRecordProjection(unittest.TestCase):
    def setUp(self):
        self.records = [
            {
                "eventId": "e1", "vehicleId": "v1", "severity": 2,
                "fault": {
                    "code": "C1", "message": "m",
                    "cause": {
                        "code": "C2", "message": None, "cause": None,
                        "tags": ["a"],
                    },
                    "tags": ["t1", "t2"],
                },
            },
        ]
        self.wn = parse(WRITER)
        self.rn = parse(READER)

    def test_project_drops_fields_and_adds_defaults(self):
        data = build_ocf(self.records, "deflate")
        r = OcfReader(io.BytesIO(data))
        out = r.read_block(r.blocks[0], self.wn, self.rn)
        rec = out[0]
        self.assertEqual(rec["eventId"], "e1")
        self.assertEqual(rec["occurredAt"], 0)           # reader 新增默认值
        self.assertEqual(rec["fault"]["component"], "unknown")
        self.assertNotIn("message", rec["fault"])         # writer 字段被丢弃
        self.assertNotIn("message", rec["fault"]["cause"])
        self.assertEqual(rec["fault"]["cause"]["code"], "C2")
        self.assertEqual(rec["fault"]["tags"], ["t1", "t2"])

    def test_null_codec_roundtrip(self):
        data = build_ocf(self.records, "null")
        r = OcfReader(io.BytesIO(data))
        self.assertEqual(r.codec, "null")
        self.assertEqual(len(r.read_block(r.blocks[0], self.wn, self.rn)), 1)

    def test_default_value_recursive_record(self):
        node = parse(
            {
                "type": "record", "name": "N",
                "fields": [
                    {"name": "v", "type": "int", "default": 5},
                    {"name": "next", "type": ["null", "N"], "default": None},
                ],
            }
        )
        val = default_value(node, {"v": 1})
        self.assertEqual(val, {"v": 1, "next": None})


class TestOcfIndexAndCorruption(unittest.TestCase):
    def setUp(self):
        self.wn = parse(WRITER)
        self.rn = parse(READER)
        buf = io.BytesIO()
        recs = [
            {"eventId": f"e{i}", "vehicleId": "v", "severity": i,
             "fault": {"code": "C", "message": None, "cause": None, "tags": []}}
            for i in range(5)
        ]
        write_ocf(buf, WRITER, [recs[:2], recs[2:4], recs[4:]], self.wn)
        self.data = buf.getvalue()

    def test_block_offsets_and_counts(self):
        r = OcfReader(io.BytesIO(self.data))
        self.assertEqual([b.object_count for b in r.blocks], [2, 2, 1])
        self.assertEqual(r.total_records, 5)
        offsets = [b.offset for b in r.blocks]
        self.assertEqual(offsets, sorted(set(offsets)))

    def test_bad_magic(self):
        with self.assertRaises(AvroDataError):
            OcfReader(io.BytesIO(b"XXXX" + self.data[4:]))

    def test_truncation_detected(self):
        with self.assertRaises(AvroDataError):
            OcfReader(io.BytesIO(self.data[:-30]))

    def test_unsupported_codec_rejected(self):
        enc = BinaryEncoder()
        meta = {
            "avro.schema": json.dumps(WRITER, separators=(",", ":")).encode(),
            "avro.codec": b"snappy",
        }
        enc.write_long(len(meta))
        for k, v in meta.items():
            enc.write_string(k)
            enc.write_long(len(v))
            enc.parts.append(v)
        enc.write_long(0)
        head = b"Obj\x01" + enc.getvalue() + b"0123456789abcdef"
        with self.assertRaises(AvroDataError):
            OcfReader(io.BytesIO(head))

    def test_corrupt_block_does_not_touch_prior_blocks(self):
        r = OcfReader(io.BytesIO(self.data))
        b1 = r.blocks[1]

        class S:
            def __init__(self, d, p):
                self.d, self.p = d, p

            def read(self, n):
                c = self.d[self.p:self.p + n]
                self.p += n
                return c

        from app.avro_codec import _read_long_from_stream
        st = S(self.data, b1.offset)
        _read_long_from_stream(st)
        size = _read_long_from_stream(st)
        pstart = st.p
        raw = zlib.decompress(self.data[pstart:pstart + size], -15)
        garbage = raw[:8] + b"\xff\xff\xff" + raw[8:]
        co = zlib.compressobj(6, zlib.DEFLATED, -15)
        new_payload = co.compress(garbage) + co.flush()
        hdr = BinaryEncoder()
        hdr.write_long(b1.object_count)
        hdr.write_long(len(new_payload))
        rebuilt = (
            self.data[:b1.offset] + hdr.getvalue() + new_payload
            + self.data[pstart + size:]
        )
        r2 = OcfReader(io.BytesIO(rebuilt))
        with self.assertRaises(AvroDataError):
            r2.read_block(r2.blocks[1], self.wn, self.rn)
        # 块 0 与块 2 仍可解码
        self.assertEqual(len(r2.read_block(r2.blocks[0], self.wn, self.rn)), 2)
        self.assertEqual(len(r2.read_block(r2.blocks[2], self.wn, self.rn)), 1)

    def test_bad_sync_marker_rejected(self):
        bad = bytearray(self.data)
        # 破坏最后一个 block 的同步标记(文件尾部 16 字节)
        bad[-1] ^= 0x01
        with self.assertRaises(AvroDataError):
            OcfReader(io.BytesIO(bytes(bad)))


class TestUnionDecoding(unittest.TestCase):
    def test_union_branches(self):
        schema = {
            "type": "record", "name": "U",
            "fields": [{"name": "x", "type": ["null", "string", "int"], "default": None}],
        }
        node = parse(schema)
        from app.avro_codec import encode_value
        for val in [None, "s", 7]:
            enc = BinaryEncoder()
            encode_value(enc, node, {"x": val})
            out = decode_value(BinaryDecoder(enc.getvalue()), node, node)
            self.assertEqual(out, {"x": val})

    def test_bad_union_index(self):
        enc = BinaryEncoder()
        enc.write_long(9)  # 越界分支下标
        node = parse(["null", "string"])
        with self.assertRaises(AvroDataError):
            decode_value(BinaryDecoder(enc.getvalue()), node, node)


if __name__ == "__main__":
    unittest.main(verbosity=2)
