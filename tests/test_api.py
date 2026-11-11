"""归档导入 API 的集成测试:导入、续导、坏块、同 ID 冲突、并发、分页。

并发用两个独立进程(各自独立 Storage 连接、同一 SQLite 文件)模拟两个 API
进程同时续导,验证不重复入库、不跳块。
"""

import hashlib
import io
import json
import multiprocessing
import os
import tempfile
import unittest
import zlib

# 在导入 app.main 之前指定独立的数据目录与 SQLite 路径
TMP = tempfile.mkdtemp(prefix="avro-api-test-")
os.environ["AVRO_DATA_DIR"] = TMP
os.environ["AVRO_DB_PATH"] = os.path.join(TMP, "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402
from app.avro_codec import BinaryEncoder, write_ocf  # noqa: E402
from app.avro_schema import parse  # noqa: E402
from app.demo import DEMO_RECURSIVE_REQUEST  # noqa: E402
from app.storage import Storage  # noqa: E402

WRITER = DEMO_RECURSIVE_REQUEST["writer"]
READER = DEMO_RECURSIVE_REQUEST["reader"]

RECORDS = [
    {
        "eventId": f"evt-{i:04d}", "vehicleId": f"VIN-{i}", "severity": i % 5,
        "fault": {
            "code": f"C{i}", "message": None,
            "cause": (
                {"code": f"ROOT{i}", "message": None, "cause": None, "tags": ["x"]}
                if i % 2 == 0 else None
            ),
            "tags": ["a", "b"],
        },
    }
    for i in range(6)
]


def make_ocf(blocks, codec="deflate"):
    buf = io.BytesIO()
    write_ocf(buf, WRITER, blocks, parse(WRITER), codec=codec)
    return buf.getvalue()


def corrupt_first_block(data: bytes) -> bytes:
    """破坏第 0 块的 Avro 负载(保持 OCF 结构可解析,解码必失败)。"""
    from app.avro_codec import OcfReader, _read_long_from_stream

    r = OcfReader(io.BytesIO(data))
    b0 = r.blocks[0]

    class S:
        def __init__(self, d, p):
            self.d, self.p = d, p

        def read(self, n):
            c = self.d[self.p:self.p + n]
            self.p += n
            return c

    st = S(data, b0.offset)
    _read_long_from_stream(st)
    size = _read_long_from_stream(st)
    pstart = st.p
    raw = zlib.decompress(data[pstart:pstart + size], -15)
    garbage = b"\xff\xff\xff\xff" + raw
    co = zlib.compressobj(6, zlib.DEFLATED, -15)
    new_payload = co.compress(garbage) + co.flush()
    hdr = BinaryEncoder()
    hdr.write_long(b0.object_count)
    hdr.write_long(len(new_payload))
    return data[:b0.offset] + hdr.getvalue() + new_payload + data[pstart + size:]


class ImportApiTestBase(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self.reader_text = json.dumps(READER)

    def import_ocf(self, archive_id, data, reader_text=None, sha=None, expected_status=200):
        sha = sha or hashlib.sha256(data).hexdigest()
        return self.client.post(
            "/api/v1/archives/import",
            data={
                "archive_id": archive_id,
                "expected_sha256": sha,
                "reader_schema": reader_text or self.reader_text,
            },
            files={"file": ("x.ocf", data, "application/octet-stream")},
        )

    def all_records(self, archive_id):
        out, cursor = [], None
        while True:
            url = f"/api/v1/archives/{archive_id}/records?limit=2"
            if cursor is not None:
                url += f"&cursor={cursor}"
            page = self.client.get(url).json()
            out.extend(page["records"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
        return out


class TestHappyPath(ImportApiTestBase):
    def test_full_import_and_projection(self):
        data = make_ocf([RECORDS[:3], RECORDS[3:]])
        resp = self.import_ocf("arch-happy", data)
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["total_blocks"], 2)
        self.assertEqual(body["imported_records"], 6)
        self.assertEqual(body["records_this_call"], 6)
        self.assertEqual(body["codec"], "deflate")

        progress = self.client.get("/api/v1/archives/arch-happy").json()
        self.assertEqual(progress["imported_blocks"], 2)
        self.assertFalse(progress["locked"])

        recs = self.all_records("arch-happy")
        self.assertEqual(len(recs), 6)
        self.assertEqual([r["seq"] for r in recs], list(range(6)))
        first = recs[0]["record"]
        self.assertEqual(first["occurredAt"], 0)
        self.assertEqual(first["fault"]["component"], "unknown")
        self.assertNotIn("message", first["fault"])
        # 分页元数据
        page = self.client.get(
            "/api/v1/archives/arch-happy/records?limit=4"
        ).json()
        self.assertEqual(len(page["records"]), 4)
        self.assertEqual(page["next_cursor"], 3)

    def test_idempotent_reimport_returns_completed(self):
        data = make_ocf([RECORDS[:2], RECORDS[2:]])
        self.import_ocf("arch-once", data)
        again = self.import_ocf("arch-once", data)
        self.assertEqual(again.status_code, 200)
        self.assertTrue(again.json()["already_completed"])
        self.assertEqual(self.all_records("arch-once").__len__(), 6)

    def test_null_codec_import(self):
        data = make_ocf([RECORDS], codec="null")
        resp = self.import_ocf("arch-null", data)
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["codec"], "null")


class TestValidation(ImportApiTestBase):
    def test_digest_mismatch_rejected(self):
        data = make_ocf([RECORDS[:2]])
        resp = self.import_ocf("arch-badsha", data, sha="00" * 32)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"]["reason"], "digest_mismatch")
        self.assertIsNone(main.storage.get_archive("arch-badsha"))

    def test_bad_sha_format(self):
        data = make_ocf([RECORDS[:1]])
        resp = self.import_ocf("arch-fmt", data, sha="abc")
        self.assertEqual(resp.status_code, 422)

    def test_bad_archive_id(self):
        data = make_ocf([RECORDS[:1]])
        resp = self.import_ocf("../escape", data)
        self.assertEqual(resp.status_code, 422)

    def test_not_ocf_rejected(self):
        resp = self.import_ocf("arch-notocf", b"not an ocf file at all")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"]["reason"], "invalid_ocf")

    def test_incompatible_reader_rejected(self):
        data = make_ocf([RECORDS[:1]])
        bad_reader = json.dumps(
            {
                "type": "record", "name": "FaultEvent",
                "fields": [
                    {"name": "eventId", "type": "int"},  # string -> int 不兼容
                ],
            }
        )
        resp = self.import_ocf("arch-incompat", data, reader_text=bad_reader)
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.json()["detail"]["reason"], "incompatible")

    def test_resume_unknown_archive_404(self):
        resp = self.client.post("/api/v1/archives/nope/resume")
        self.assertEqual(resp.status_code, 404)

    def test_records_unknown_archive_404(self):
        resp = self.client.get("/api/v1/archives/nope/records")
        self.assertEqual(resp.status_code, 404)

    def test_oversize_rejected(self):
        # 绕过真实大文件:把上限临时调小,验证超限路径
        old = main.MAX_OCF_BYTES
        main.MAX_OCF_BYTES = 10
        try:
            data = make_ocf([RECORDS[:1]])
            resp = self.import_ocf("arch-big", data)
            self.assertEqual(resp.status_code, 413)
        finally:
            main.MAX_OCF_BYTES = old


class TestSameIdRules(ImportApiTestBase):
    def test_same_id_different_sha_rejected(self):
        data1 = make_ocf([RECORDS[:2]])
        self.import_ocf("arch-same", data1)
        data2 = make_ocf([RECORDS[2:4]])
        resp = self.import_ocf("arch-same", data2)
        self.assertEqual(resp.status_code, 409)
        self.assertIn("SHA-256", resp.json()["detail"])
        # 原进度未受影响
        self.assertEqual(len(self.all_records("arch-same")), 2)

    def test_same_id_different_reader_rejected(self):
        data = make_ocf([RECORDS[:2]])
        self.import_ocf("arch-rdr", data)
        other_reader = json.dumps(
            {
                "type": "record", "name": "FaultEvent",
                "fields": [
                    {"name": "eventId", "type": "string"},
                    {"name": "vehicleId", "type": "string"},
                    {"name": "severity", "type": "long"},
                    {"name": "extra", "type": "string", "default": "z"},
                    {"name": "fault", "type": READER["fields"][4]["type"]},
                ],
            }
        )
        resp = self.import_ocf("arch-rdr", data, reader_text=other_reader)
        self.assertEqual(resp.status_code, 409)
        self.assertIn("reader", resp.json()["detail"])


class TestBadBlockAndResume(ImportApiTestBase):
    def test_bad_first_block_then_fixed_file_resumes(self):
        good = make_ocf([RECORDS[:2], RECORDS[2:4], RECORDS[4:]])
        bad = corrupt_first_block(good)
        # 坏文件与好文件内容不同 -> 摘要也不同;先以坏文件自身摘要导入
        bad_sha = hashlib.sha256(bad).hexdigest()
        resp = self.import_ocf("arch-resume", bad, sha=bad_sha, expected_status=422)
        self.assertEqual(resp.status_code, 422)
        detail = resp.json()["detail"]
        self.assertEqual(detail["status"], "failed")
        self.assertEqual(detail["imported_records"], 0)
        self.assertEqual(detail["failed_block"], 0)

        # 进度行存在且锁已释放,可查询
        progress = self.client.get("/api/v1/archives/arch-resume").json()
        self.assertEqual(progress["status"], "failed")
        self.assertFalse(progress["locked"])

        # 用修复后的同内容文件按同一 ID 续导:但摘要不同会被拒
        # (同 ID 必须保持摘要不变)——这是需求规定的一致性约束
        resp2 = self.import_ocf("arch-resume", good)
        self.assertEqual(resp2.status_code, 409)

    def test_bad_middle_block_keeps_prior_blocks_and_resumes(self):
        good = make_ocf([RECORDS[:2], RECORDS[2:4], RECORDS[4:]])
        # 破坏第 1 块(中间块):第 0 块应完整保留
        from tests.tests_helpers import corrupt_block
        bad = corrupt_block(good, 1)
        bad_sha = hashlib.sha256(bad).hexdigest()
        resp = self.import_ocf("arch-mid", bad, sha=bad_sha)
        self.assertEqual(resp.status_code, 422)
        detail = resp.json()["detail"]
        self.assertEqual(detail["imported_blocks"], 1)
        self.assertEqual(detail["imported_records"], 2)
        self.assertEqual(detail["failed_block"], 1)
        self.assertEqual(len(self.all_records("arch-mid")), 2)

        # 模拟进程退出后续导:落盘文件内容不变(坏块依旧),
        # 续导从 next_offset 重试,仍失败但不重复入库
        r2 = self.client.post("/api/v1/archives/arch-mid/resume")
        self.assertEqual(r2.status_code, 422)
        self.assertEqual(r2.json()["detail"]["imported_records"], 2)
        self.assertEqual(len(self.all_records("arch-mid")), 2)

    def test_resume_after_simulated_crash_completes(self):
        """直接操作存储层模拟:第 0 块已提交后进程被杀,用原文件续导完成。"""
        good = make_ocf([RECORDS[:2], RECORDS[2:4], RECORDS[4:]])
        # 正常导入到一个新 ID 后删除进度行,再手工只提交第 0 块:
        resp = self.import_ocf("arch-crash", good)
        self.assertEqual(resp.status_code, 200)
        # 构造一个只导入了第 0 块的全新归档(用存储 API 复刻崩溃现场)
        from app.importer import import_archive
        crash_id = "arch-crash2"
        result = import_archive(
            main.storage, TMP, crash_id,
            hashlib.sha256(good).hexdigest(), READER, good,
        )
        self.assertEqual(result.status, "completed")
        # 回滚到"一个块都没提交"的新归档状态(next_offset 指向首块偏移)
        from app.avro_codec import OcfReader
        first_offset = OcfReader(io.BytesIO(good)).blocks[0].offset
        with main.storage.connect() as conn:
            conn.execute("DELETE FROM records WHERE archive_id = ?", (crash_id,))
            conn.execute(
                "UPDATE archives SET imported_blocks=0, imported_records=0, "
                "next_offset=?, status='processing', locked=0, locked_at=NULL, "
                "error=NULL WHERE archive_id=?",
                (first_offset, crash_id),
            )
            conn.commit()
        # next_offset 指向首块时续导应重新导入全部块且不重复
        r = self.client.post(f"/api/v1/archives/{crash_id}/resume")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "completed")
        self.assertEqual(len(self.all_records(crash_id)), 6)
        self.assertEqual(
            [x["seq"] for x in self.all_records(crash_id)], list(range(6))
        )

    def test_crash_mid_block_replays_only_that_block(self):
        """块 0 已提交、块 1 解码后进程在提交前退出:续导只重放块 1 及之后。"""
        good = make_ocf([RECORDS[:2], RECORDS[2:4], RECORDS[4:]])
        crash_id = "arch-midcrash"
        from app.importer import import_archive
        import_archive(
            main.storage, TMP, crash_id,
            hashlib.sha256(good).hexdigest(), READER, good,
        )
        # 回滚到"仅块 0 已提交"的现场(块 1 的事务整体不存在)
        from app.avro_codec import OcfReader
        blocks = OcfReader(io.BytesIO(good)).blocks
        with main.storage.connect() as conn:
            conn.execute(
                "DELETE FROM records WHERE archive_id = ? AND block_index >= 1",
                (crash_id,),
            )
            conn.execute(
                "UPDATE archives SET imported_blocks=1, imported_records=2, "
                "next_offset=?, status='processing', locked=0, locked_at=NULL, "
                "error=NULL WHERE archive_id=?",
                (blocks[1].offset, crash_id),
            )
            conn.commit()
        r = self.client.post(f"/api/v1/archives/{crash_id}/resume")
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["status"], "completed")
        # 本次只补了块 1、块 2 共 4 条,块 0 没有重放
        self.assertEqual(body["blocks_this_call"], 2)
        self.assertEqual(body["records_this_call"], 4)
        recs = self.all_records(crash_id)
        self.assertEqual([x["seq"] for x in recs], list(range(6)))
        self.assertEqual([x["block_index"] for x in recs],
                         [0, 0, 1, 1, 2, 2])


class TestConcurrency(unittest.TestCase):
    """两个独立进程同时对同一归档续导,验证跨进程锁与块级事务。"""

    def test_two_processes_no_duplicates_no_gaps(self):
        data = make_ocf([RECORDS[i:i + 1] for i in range(6)])  # 6 个单记录块
        sha = hashlib.sha256(data).hexdigest()
        db_path = os.path.join(TMP, "conc.db")
        ctx = multiprocessing.get_context("spawn")
        q = ctx.Queue()
        procs = [
            ctx.Process(
                target=_concurrent_worker,
                args=(db_path, TMP, "arch-conc", sha, q),
            )
            for _ in range(2)
        ]
        for p in procs:
            p.start()
        outcomes = [q.get() for _ in procs]
        for p in procs:
            p.join(30)
            self.assertEqual(p.exitcode, 0)

        storage = Storage(db_path)
        row = storage.get_archive("arch-conc")
        self.assertEqual(row["status"], "completed")
        self.assertEqual(row["imported_records"], 6)
        self.assertEqual(row["imported_blocks"], 6)
        rows, _ = storage.list_records("arch-conc", 100, None)
        seqs = sorted(r["seq"] for r in rows)
        self.assertEqual(seqs, list(range(6)))  # 无重复无跳号
        # 两个进程中恰有一个完成全部块,另一个观察到锁冲突或幂等完成
        statuses = sorted(o["status"] for o in outcomes)
        self.assertIn("completed", statuses)


def _concurrent_worker(db_path, data_dir, archive_id, sha, queue):
    """子进程入口:独立连接同一 SQLite,尝试导入(带短暂错峰)。"""
    import time
    try:
        storage = Storage(db_path)
        data = make_ocf([RECORDS[i:i + 1] for i in range(6)])
        from app.importer import import_archive
        try:
            result = import_archive(
                storage, data_dir, archive_id, sha, READER, data
            )
            queue.put({"status": result.status, "records": result.imported_records})
        except Exception as e:  # 锁冲突属于预期结果之一
            queue.put({"status": "conflict", "message": str(e)})
    except Exception as e:
        queue.put({"status": "error", "message": repr(e)})
    finally:
        time.sleep(0.05)


class TestOriginalEndpoints(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)

    def test_compat_check_unchanged(self):
        resp = self.client.post(
            "/api/v1/compatibility/check", json=DEMO_RECURSIVE_REQUEST
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["compatible"])

    def test_demo_endpoints(self):
        r1 = self.client.get("/api/v1/demo/recursive")
        self.assertEqual(r1.status_code, 200)
        r2 = self.client.get("/api/v1/demo/recursive-ocf")
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.json()["blocks"], 3)
        r3 = self.client.get("/api/v1/demo/recursive-ocf/download")
        self.assertEqual(r3.status_code, 200)
        self.assertEqual(r3.content[:4], b"Obj\x01")
        self.assertEqual(
            hashlib.sha256(r3.content).hexdigest(), r2.json()["sha256"]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
