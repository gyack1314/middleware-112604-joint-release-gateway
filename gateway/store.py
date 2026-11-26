"""SQLite 持久化层。

所有网关状态都落盘，进程 / “重启”后用同一个 db 路径新建 Gateway 即可继续。
预算扣减的正确性由 ``BEGIN IMMEDIATE`` 串行写事务保证：

    cap - committed - held >= unit 与插入 hold 行在同一个立即写事务内，
    因而两个请求不可能同时读到同一份余量并争走“最后一个额度”。

同一进程内再用一把锁避免单连接上的语句交错；跨进程 / 跨连接由 SQLite
文件锁 + busy_timeout 兜底。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS catalog (
    name           TEXT NOT NULL,
    rev            INTEGER NOT NULL,
    groups_known   INTEGER NOT NULL,      -- 0 表示 groups=null，范围未完成
    groups_json    TEXT,                  -- known=1 时为群体数组
    audiences_json TEXT NOT NULL,
    updated_at     INTEGER,
    PRIMARY KEY (name, rev)
);

CREATE TABLE IF NOT EXISTS budget (
    grp        TEXT PRIMARY KEY,
    cap        INTEGER NOT NULL,
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS query (
    id                   TEXT PRIMARY KEY,
    audience             TEXT NOT NULL,
    sources_json         TEXT NOT NULL,
    unit                 INTEGER NOT NULL,
    reuse                TEXT,
    derived_from         TEXT,
    state                TEXT NOT NULL,
    reason_code          TEXT,
    reason_detail_json   TEXT,
    result               TEXT,
    groups_json          TEXT NOT NULL,
    scope_complete       INTEGER NOT NULL,
    access_mode          TEXT NOT NULL DEFAULT 'active', -- active/revoked/tightened/expired
    access_allowlist_json TEXT,
    public_fact          INTEGER NOT NULL DEFAULT 0,
    fully_settled        INTEGER NOT NULL DEFAULT 0,
    settlement           TEXT,                          -- full/mixed
    withdraw_requested   INTEGER NOT NULL DEFAULT 0,
    first_exposed_at     INTEGER,
    created_at           INTEGER,
    updated_at           INTEGER,
    record_json          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hold (
    query_id   TEXT NOT NULL,
    grp        TEXT NOT NULL,
    amount     INTEGER NOT NULL,
    status     TEXT NOT NULL,             -- held/committed/released
    note       TEXT,
    updated_at INTEGER,
    PRIMARY KEY (query_id, grp)
);

CREATE TABLE IF NOT EXISTS receipt (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    at         INTEGER,
    query_id   TEXT NOT NULL,
    state      TEXT NOT NULL,
    specified  INTEGER NOT NULL,
    scope_json TEXT,
    raw_json   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS read_log (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    at       INTEGER,
    query_id TEXT NOT NULL,
    audience TEXT NOT NULL,
    via      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    at           INTEGER,
    kind         TEXT NOT NULL,
    query_id     TEXT,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    at          INTEGER,
    kind        TEXT NOT NULL,
    record_json TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False：允许线程池共享一个 Gateway；真正的串行化
        # 由 Gateway 锁 + 立即写事务保证。
        self.conn = sqlite3.connect(
            db_path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        if db_path != ":memory:":
            # WAL 让多连接竞争更平顺；内存库不支持该 pragma 的某些形态。
            try:
                self.conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._txn_lock = threading.RLock()
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version','1')"
        )

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    @contextmanager
    def transaction(self) -> Iterable[sqlite3.Connection]:
        """串行化的 BEGIN IMMEDIATE 事务。"""
        with self._txn_lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    # ---- 轻量辅助 ---------------------------------------------------------

    def query_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchone()

    def query_all(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchall()

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    # ---- 序列化 -----------------------------------------------------------

    @staticmethod
    def dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def loads(text: Optional[str], default: Any = None) -> Any:
        if text is None:
            return default
        return json.loads(text)

    # ---- 预算台账 ---------------------------------------------------------

    def ledger(self, group: str) -> Dict[str, Any]:
        """返回某预算域 cap/committed/reserved（无 cap 时 cap=None）。"""
        budget = self.query_one("SELECT cap FROM budget WHERE grp=?", (group,))
        cap = None if budget is None else int(budget["cap"])
        row = self.query_one(
            """
            SELECT
              COALESCE(SUM(CASE WHEN status='committed' THEN amount END), 0) AS committed,
              COALESCE(SUM(CASE WHEN status='held'      THEN amount END), 0) AS reserved
            FROM hold WHERE grp=?
            """,
            (group,),
        )
        committed = int(row["committed"])
        reserved = int(row["reserved"])
        available = None if cap is None else cap - committed - reserved
        return {
            "group": group,
            "cap": cap,
            "committed": committed,
            "reserved": reserved,
            "available": available,
        }

    def all_groups(self) -> List[str]:
        rows = self.query_all(
            """
            SELECT grp FROM budget
            UNION
            SELECT DISTINCT grp FROM hold
            ORDER BY grp
            """
        )
        return [r["grp"] for r in rows]
