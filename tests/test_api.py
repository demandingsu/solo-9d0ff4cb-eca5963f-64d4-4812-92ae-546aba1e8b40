"""端到端测试：导入、配对、幂等、冲突、快照隔离、失败回滚。"""
import json
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, logic  # noqa: E402
from app.main import ACTUAL_CSV_EXAMPLE, FORECAST_CSV_EXAMPLE, GENERATE_EXAMPLE, app  # noqa: E402

CSV_HEADERS = {"Content-Type": "text/csv"}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test.db")
    monkeypatch.setenv("APP_DB_PATH", db_file)
    logic.SNAPSHOT_TEST_HOOK = None
    with TestClient(app) as client:
        yield client, db_file
    logic.SNAPSHOT_TEST_HOOK = None


def import_demo(client):
    r = client.post("/imports/forecasts", content=FORECAST_CSV_EXAMPLE, headers=CSV_HEADERS)
    assert r.status_code == 200
    r = client.post("/imports/actuals", content=ACTUAL_CSV_EXAMPLE, headers=CSV_HEADERS)
    assert r.status_code == 200


def table_counts(db_file):
    conn = db.connect(db_file)
    try:
        return {
            t: conn.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
            for t in ("forecasts", "actuals", "datasets", "dataset_rows")
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 导入

def test_import_and_idempotent_reimport(env):
    client, _ = env
    r = client.post("/imports/forecasts", content=FORECAST_CSV_EXAMPLE, headers=CSV_HEADERS)
    assert r.status_code == 200
    assert r.json() == {"inserted": 4, "skipped_duplicates": 0, "accepted_rows": 4}
    # 同键同值重导：幂等跳过
    r = client.post("/imports/forecasts", content=FORECAST_CSV_EXAMPLE, headers=CSV_HEADERS)
    assert r.status_code == 200
    assert r.json() == {"inserted": 0, "skipped_duplicates": 4, "accepted_rows": 4}


def test_import_conflict_against_db_rejects_whole_batch(env):
    client, db_file = env
    import_demo(client)
    batch = (
        "station,target_hour,issued_at,irradiance\n"
        "ST01,2026-03-01T00:00:00Z,2026-02-27T08:00:00Z,999.0\n"  # 同键异值
        "ST01,2026-03-01T05:00:00Z,2026-02-27T08:00:00Z,123.0\n"  # 正常行
    )
    r = client.post("/imports/forecasts", content=batch, headers=CSV_HEADERS)
    assert r.status_code == 409
    assert r.json()["detail"]["conflicts"][0]["db_value"] == 500.0
    # 整批拒收：正常行也没有写入
    assert table_counts(db_file)["forecasts"] == 4


def test_import_conflict_within_batch_rejected(env):
    client, db_file = env
    batch = (
        "station,target_hour,energy\n"
        "ST01,2026-03-01T00:00:00Z,48.2\n"
        "ST01,2026-03-01T00:00:00Z,48.9\n"
    )
    r = client.post("/imports/actuals", content=batch, headers=CSV_HEADERS)
    assert r.status_code == 409
    assert table_counts(db_file)["actuals"] == 0


def test_import_invalid_rows_rejected_with_details(env):
    client, db_file = env
    batch = (
        "station,target_hour,issued_at,irradiance\n"
        "ST01,2026-03-01T00:30:00Z,2026-02-27T08:00:00Z,500.0\n"  # 目标小时未整点
        "ST01,2026-03-01T01:00:00Z,not-a-time,510.0\n"            # 发布时间非法
        "ST01,2026-03-01T02:00:00Z,2026-02-27T08:00:00Z,abc\n"    # 辐照非数值
    )
    r = client.post("/imports/forecasts", content=batch, headers=CSV_HEADERS)
    assert r.status_code == 422
    assert len(r.json()["detail"]["errors"]) == 3
    assert table_counts(db_file)["forecasts"] == 0


def test_import_chinese_headers(env):
    client, db_file = env
    batch = "场站,目标UTC小时,发布UTC时刻,预测辐照\nST09,2026-03-01T00:00:00Z,2026-02-27T08:00:00Z,321.5\n"
    r = client.post("/imports/forecasts", content=batch, headers=CSV_HEADERS)
    assert r.status_code == 200
    assert r.json()["inserted"] == 1


# ---------------------------------------------------------------- 配对

def test_pairing_latest_before_cutoff_and_missing_reasons(env):
    client, _ = env
    import_demo(client)
    r = client.post("/datasets/demo/generate", json=GENERATE_EXAMPLE)
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] is False
    assert body["summary"] == {
        "total_hours": 4, "paired": 2,
        "missing_forecast": 1, "missing_actual": 1, "missing_both": 0,
    }
    rows = {row["target_hour"]: row for row in body["rows"]}

    # 00 时：550.0 发布于截止时刻之后（迟到），采用 500.0
    row0 = rows["2026-03-01T00:00:00Z"]
    assert row0["cutoff_issued_at"] == "2026-02-28T00:00:00Z"
    assert row0["issued_at"] == "2026-02-27T08:00:00Z"
    assert row0["irradiance"] == 500.0
    assert row0["energy"] == 48.2
    assert row0["missing_reason"] is None

    # 01 时：00:30 发布，恰在截止 01:00 之前
    assert rows["2026-03-01T01:00:00Z"]["irradiance"] == 510.0

    # 02 时：无预报
    row2 = rows["2026-03-01T02:00:00Z"]
    assert row2["missing_reason"] == "no_forecast"
    assert row2["issued_at"] is None and row2["energy"] == 47.5

    # 03 时：无实测
    row3 = rows["2026-03-01T03:00:00Z"]
    assert row3["missing_reason"] == "no_actual"
    assert row3["irradiance"] == 520.0 and row3["energy"] is None


def test_pairing_both_missing(env):
    client, _ = env
    import_demo(client)
    params = dict(GENERATE_EXAMPLE, start_hour="2026-03-01T04:00:00Z", end_hour="2026-03-01T04:00:00Z")
    r = client.post("/datasets/both-missing/generate", json=params)
    assert r.status_code == 200
    row = r.json()["rows"][0]
    assert row["missing_reason"] == "no_forecast+no_actual"


# ---------------------------------------------------------------- 数据集号语义

def test_retry_same_params_returns_original_result(env):
    client, _ = env
    import_demo(client)
    first = client.post("/datasets/ds1/generate", json=GENERATE_EXAMPLE).json()
    assert first["replayed"] is False

    # 生成后导入会改变配对结果的新数据
    extra = "station,target_hour,issued_at,irradiance\nST01,2026-03-01T02:00:00Z,2026-02-27T10:00:00Z,999.0\n"
    assert client.post("/imports/forecasts", content=extra, headers=CSV_HEADERS).status_code == 200

    # 同号同参数重试：读回原结果
    second = client.post("/datasets/ds1/generate", json=GENERATE_EXAMPLE).json()
    assert second["replayed"] is True
    assert second["rows"] == first["rows"]
    assert second["created_at"] == first["created_at"]
    row2 = [r for r in second["rows"] if r["target_hour"] == "2026-03-01T02:00:00Z"][0]
    assert row2["missing_reason"] == "no_forecast"  # 新导入的 999.0 不影响已保存结果


def test_same_id_different_params_conflict(env):
    client, _ = env
    import_demo(client)
    assert client.post("/datasets/ds1/generate", json=GENERATE_EXAMPLE).status_code == 200
    r = client.post("/datasets/ds1/generate", json=dict(GENERATE_EXAMPLE, lead_hours=12))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["existing_params"]["lead_hours"] == 24
    assert detail["requested_params"]["lead_hours"] == 12


def test_get_dataset_and_list(env):
    client, _ = env
    import_demo(client)
    assert client.get("/datasets/nope").status_code == 404
    client.post("/datasets/ds1/generate", json=GENERATE_EXAMPLE)
    r = client.get("/datasets/ds1")
    assert r.status_code == 200
    assert r.json()["dataset_id"] == "ds1"
    listing = client.get("/datasets").json()
    assert [d["dataset_id"] for d in listing] == ["ds1"]


def test_generate_param_validation(env):
    client, _ = env
    bad = dict(GENERATE_EXAMPLE, start_hour="2026-03-02T00:00:00Z")  # 区间倒置
    assert client.post("/datasets/d1/generate", json=bad).status_code == 422
    bad = dict(GENERATE_EXAMPLE, start_hour="2026-03-01T00:30:00Z")  # 未整点
    assert client.post("/datasets/d2/generate", json=bad).status_code == 422
    bad = dict(GENERATE_EXAMPLE, lead_hours=-1)
    assert client.post("/datasets/d3/generate", json=bad).status_code == 422


# ---------------------------------------------------------------- 快照隔离与原子性

def test_generate_reads_single_committed_snapshot(env):
    """生成事务期间，另一连接的导入无法提交；结果不含交错数据。"""
    client, db_file = env
    import_demo(client)
    state = {}

    def hook():
        # 模拟另一进程：在生成事务持有写锁期间尝试导入
        other = db.connect(db_file)
        other.execute("PRAGMA busy_timeout=300")
        try:
            other.execute("BEGIN IMMEDIATE")
            other.execute(
                "INSERT INTO forecasts VALUES ('ST01', '2026-03-01T02:00:00Z', "
                "'2026-02-27T10:00:00Z', 999.0)"
            )
            other.execute("COMMIT")
            state["committed"] = True
        except sqlite3.OperationalError as exc:
            state["error"] = str(exc)
        finally:
            other.close()

    logic.SNAPSHOT_TEST_HOOK = hook
    r = client.post("/datasets/ds1/generate", json=GENERATE_EXAMPLE)
    assert r.status_code == 200
    body = r.json()

    # 交错导入被锁挡住，未提交；生成结果完全来自导入前的已提交快照
    assert state.get("committed") is not True
    assert "locked" in state.get("error", "").lower()
    row2 = [x for x in body["rows"] if x["target_hour"] == "2026-03-01T02:00:00Z"][0]
    assert row2["missing_reason"] == "no_forecast"

    # 生成结束后，该导入可以正常提交
    logic.SNAPSHOT_TEST_HOOK = None
    extra = "station,target_hour,issued_at,irradiance\nST01,2026-03-01T02:00:00Z,2026-02-27T10:00:00Z,999.0\n"
    assert client.post("/imports/forecasts", content=extra, headers=CSV_HEADERS).status_code == 200

    # 新数据集号反映新数据；旧数据集保持原样
    newer = client.post("/datasets/ds2/generate", json=GENERATE_EXAMPLE).json()
    row2_new = [x for x in newer["rows"] if x["target_hour"] == "2026-03-01T02:00:00Z"][0]
    assert row2_new["irradiance"] == 999.0
    old = client.get("/datasets/ds1").json()
    row2_old = [x for x in old["rows"] if x["target_hour"] == "2026-03-01T02:00:00Z"][0]
    assert row2_old["missing_reason"] == "no_forecast"


def test_failed_generate_leaves_no_partial_result(env, monkeypatch):
    """提交失败时整体回滚：数据集与行都不可见，已导入数据不受影响。"""
    client, db_file = env
    import_demo(client)

    import app.main as main_module

    real_commit = main_module._commit
    calls = {"n": 0}

    def flaky_commit(conn):
        if calls["n"] == 0:
            calls["n"] += 1
            raise sqlite3.OperationalError("simulated commit failure")
        return real_commit(conn)

    monkeypatch.setattr(main_module, "_commit", flaky_commit)
    # TestClient 默认会把服务端未处理异常重新抛出；服务端此时已整体回滚
    with pytest.raises(sqlite3.OperationalError, match="simulated commit failure"):
        client.post("/datasets/dsX/generate", json=GENERATE_EXAMPLE)
    monkeypatch.undo()

    assert client.get("/datasets/dsX").status_code == 404
    counts = table_counts(db_file)
    assert counts["datasets"] == 0
    assert counts["dataset_rows"] == 0
    assert counts["forecasts"] == 4 and counts["actuals"] == 3

    # 数据库仍可用：重试成功
    r = client.post("/datasets/dsX/generate", json=GENERATE_EXAMPLE)
    assert r.status_code == 200
    assert r.json()["replayed"] is False
