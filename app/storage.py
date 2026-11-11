"""SQLite 持久层:归档进度、块级提交、跨进程导入锁与分页查询。

关键设计:
- 每条归档在 archives 表占一行,next_offset 指向下一个待导入 block 的文件偏移;
  每解码完一个 block,在同一个 BEGIN IMMEDIATE 事务里插入该块全部记录并
  推进 next_offset——坏块不推进,进程崩溃后此前完整块已落库。
- records 以 (archive_id, seq) 为主键,seq 为归档内全局递增序号,
  唯一约束保证即使锁失效也不会重复入库。
- locked/locked_at 构成跨进程 advisory lock:锁残留超过 STALE_LOCK_SECONDS
  视为进程已退出,续导请求可接管。
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

STALE_LOCK_SECONDS = 300
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 500

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS archives (
    archive_id       TEXT PRIMARY KEY,
    sha256           TEXT NOT NULL,
    reader_schema    TEXT NOT NULL,
    writer_schema    TEXT NOT NULL,
    codec            TEXT NOT NULL,
    file_path        TEXT NOT NULL,
    status           TEXT NOT NULL,            -- processing / completed / failed
    total_blocks     INTEGER NOT NULL,
    total_records    INTEGER NOT NULL,
    next_offset      INTEGER NOT NULL,
    imported_blocks  INTEGER NOT NULL DEFAULT 0,
    imported_records INTEGER NOT NULL DEFAULT 0,
    error            TEXT,
    locked           INTEGER NOT NULL DEFAULT 0,
    locked_at        TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    archive_id   TEXT NOT NULL,
    block_index  INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    PRIMARY KEY (archive_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_records_archive_seq
    ON records (archive_id, seq);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def canonical_json(doc: Any) -> str:
    """模式的规范化 JSON 串:同 ID 续导时据此判定 reader 是否变化。"""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            # WAL 允许多进程/多连接并发读,写事务串行化
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def immediate(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        """开启写事务并立即获取 RESERVED 锁,跨进程串行化块提交。"""
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------ 归档注册/锁

    def register_or_acquire(
        self,
        archive_id: str,
        sha256: str,
        reader_schema: Any,
        writer_schema: Any,
        codec: str,
        file_path: str,
        total_blocks: int,
        total_records: int,
        first_block_offset: int,
    ) -> Tuple[sqlite3.Row, bool]:
        """新归档:插入并上锁;同 ID 续导:校验一致性后接管锁。

        返回 (归档行, 是否新建)。冲突抛 ArchiveConflict。
        """
        reader_canon = canonical_json(reader_schema)
        writer_canon = canonical_json(writer_schema)
        now = utc_now()
        with self.connect() as conn, self.immediate(conn):
            row = conn.execute(
                "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO archives (
                        archive_id, sha256, reader_schema, writer_schema, codec,
                        file_path, status, total_blocks, total_records, next_offset,
                        imported_blocks, imported_records, error, locked, locked_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'processing', ?, ?, ?, 0, 0, NULL,
                              1, ?, ?, ?)
                    """,
                    (
                        archive_id, sha256, reader_canon, writer_canon, codec,
                        file_path, total_blocks, total_records, first_block_offset,
                        now, now, now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
                ).fetchone()
                return row, True

            # 同 ID 续导:摘要与 reader 必须保持不变
            if row["sha256"] != sha256:
                raise ArchiveConflict(
                    f"归档 '{archive_id}' 已存在:期望 SHA-256 {row['sha256']},"
                    f"本次提交 {sha256},摘要不一致,拒绝续导"
                )
            if row["reader_schema"] != reader_canon:
                raise ArchiveConflict(
                    f"归档 '{archive_id}' 已存在:reader 模式与首次导入不同,"
                    "同 ID 续导必须保持 reader 不变"
                )
            if row["status"] == "completed":
                return row, False  # 幂等:已完成,无需处理
            if row["locked"] and not self._is_stale(row["locked_at"]):
                raise ArchiveConflict(
                    f"归档 '{archive_id}' 的另一个导入进程正在运行"
                    f"(locked_at={row['locked_at']}),并发续导被拒绝"
                )
            conn.execute(
                "UPDATE archives SET locked = 1, locked_at = ?, updated_at = ? "
                "WHERE archive_id = ?",
                (now, now, archive_id),
            )
            row = conn.execute(
                "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()
            return row, False

    def release_lock(self, archive_id: str, status: str, error: Optional[str]) -> None:
        with self.connect() as conn, self.immediate(conn):
            conn.execute(
                "UPDATE archives SET locked = 0, locked_at = NULL, status = ?, "
                "error = ?, updated_at = ? WHERE archive_id = ?",
                (status, error, utc_now(), archive_id),
            )

    @staticmethod
    def _is_stale(locked_at: Optional[str]) -> bool:
        if not locked_at:
            return True
        try:
            ts = datetime.strptime(locked_at, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return True
        return (datetime.now(timezone.utc) - ts).total_seconds() > STALE_LOCK_SECONDS

    # ------------------------------------------------------------ 块级提交

    def commit_block(
        self,
        archive_id: str,
        block_index: int,
        expected_offset: int,
        next_offset: int,
        records: List[Dict[str, Any]],
    ) -> int:
        """在一个事务里写入整块记录并推进偏移。返回写入后的累计记录数。

        expected_offset 必须等于库中 next_offset,否则说明有其他进程已经推进
        (双重防护),抛 BlockConflict 且不写入。
        """
        with self.connect() as conn, self.immediate(conn):
            row = conn.execute(
                "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()
            if row is None:
                raise ArchiveConflict(f"归档 '{archive_id}' 不存在")
            if row["next_offset"] != expected_offset:
                raise BlockConflict(
                    f"归档 '{archive_id}' 的进度偏移为 {row['next_offset']},"
                    f"与待提交块偏移 {expected_offset} 不一致,拒绝重复入库"
                )
            base_seq = row["imported_records"]
            payloads: List[Tuple[str, int, int, int, str]] = []
            for i, record in enumerate(records):
                payloads.append(
                    (
                        archive_id,
                        block_index,
                        i,
                        base_seq + i,
                        json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                    )
                )
            conn.executemany(
                "INSERT INTO records (archive_id, block_index, record_index, seq, "
                "payload) VALUES (?, ?, ?, ?, ?)",
                payloads,
            )
            conn.execute(
                """
                UPDATE archives
                   SET imported_blocks = imported_blocks + 1,
                       imported_records = imported_records + ?,
                       next_offset = ?,
                       updated_at = ?
                 WHERE archive_id = ?
                """,
                (len(records), next_offset, utc_now(), archive_id),
            )
            return base_seq + len(records)

    def mark_failed(self, archive_id: str, error: str) -> None:
        self.release_lock(archive_id, "failed", error)

    def mark_completed(self, archive_id: str) -> None:
        self.release_lock(archive_id, "completed", None)

    # ------------------------------------------------------------ 查询

    def get_archive(self, archive_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()

    def list_archives(self, limit: int, offset: int) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM archives ORDER BY created_at DESC, archive_id "
                "LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

    def list_records(
        self, archive_id: str, limit: int, cursor: Optional[int]
    ) -> Tuple[List[sqlite3.Row], Optional[int]]:
        """keyset 分页:cursor 为上一页最后一条 seq(不含)。"""
        with self.connect() as conn:
            if cursor is None:
                rows = conn.execute(
                    "SELECT seq, block_index, record_index, payload FROM records "
                    "WHERE archive_id = ? ORDER BY seq ASC LIMIT ?",
                    (archive_id, limit + 1),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT seq, block_index, record_index, payload FROM records "
                    "WHERE archive_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
                    (archive_id, cursor, limit + 1),
                ).fetchall()
        next_cursor: Optional[int] = None
        if len(rows) > limit:
            next_cursor = rows[limit - 1]["seq"]
            rows = rows[:limit]
        return rows, next_cursor


class ArchiveConflict(Exception):
    """同 ID 归档的摘要/reader 不一致,或有并发导入在运行。"""


class BlockConflict(Exception):
    """块偏移与库内进度不一致(并发重复入库防护)。"""
