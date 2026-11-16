"""SQLite 连接与建表。

并发模型：
- 所有写操作（导入、生成数据集、创建划分）都在 ``BEGIN IMMEDIATE`` 事务中执行，
  事务开始即持有 RESERVED 锁，其他进程/连接的写事务只能等待或超时失败，
  因此一个事务内读到的永远是同一个已提交快照，提交/回滚都是原子的。
- 开启 WAL，读请求（GET）不阻塞写，也不被写阻塞。
- busy_timeout 让并发写短暂等待而不是立刻报错；超时后由 API 层映射为 503。
"""
from __future__ import annotations

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS forecasts (
    station     TEXT NOT NULL,
    target_hour TEXT NOT NULL,   -- 目标 UTC 小时，规范形 YYYY-MM-DDTHH:00:00Z
    issued_at   TEXT NOT NULL,   -- 发布 UTC 时刻，规范形 YYYY-MM-DDTHH:MM:SSZ
    irradiance  REAL NOT NULL,   -- 预测辐照
    PRIMARY KEY (station, target_hour, issued_at)
);
CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
    ON forecasts (station, target_hour, issued_at);

CREATE TABLE IF NOT EXISTS actuals (
    station     TEXT NOT NULL,
    target_hour TEXT NOT NULL,
    energy      REAL NOT NULL,   -- 实测电量
    PRIMARY KEY (station, target_hour)
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id  TEXT PRIMARY KEY,
    params_json TEXT NOT NULL,   -- 规范化后的生成参数（用于同号重试/冲突判定）
    result_json TEXT NOT NULL,   -- 完整结果快照（重试时原样读回）
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_rows (
    dataset_id       TEXT NOT NULL REFERENCES datasets (dataset_id),
    station          TEXT NOT NULL,
    target_hour      TEXT NOT NULL,
    cutoff_issued_at TEXT NOT NULL,  -- 发布截止时刻 = target_hour - lead_hours
    issued_at        TEXT,           -- 实际采用的预报来源时刻（缺配为 NULL）
    irradiance       REAL,
    energy           REAL,
    missing_reason   TEXT,           -- no_forecast / no_actual / no_forecast+no_actual / NULL
    PRIMARY KEY (dataset_id, target_hour)
);

CREATE TABLE IF NOT EXISTS splits (
    split_id    TEXT PRIMARY KEY,
    params_json TEXT NOT NULL,   -- 规范化后的划分参数（用于同号重试/冲突判定）
    result_json TEXT NOT NULL,   -- 完整划分结果快照（重试时原样读回）
    created_at  TEXT NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开一个连接。isolation_level=None：显式控制事务边界。"""
    if db_path != ":memory:":
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
