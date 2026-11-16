"""端到端测试：训练/验证划分（合并去重、冲突、隔离带、幂等、快照隔离、失败回滚）。"""
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, logic  # noqa: E402
from app.main import app  # noqa: E402

CSV_HEADERS = {"Content-Type": "text/csv"}

# 小时 00..07：06 缺实测（no_actual），07 缺预报（no_forecast），其余配对成功
PAIRED_HOURS = [0, 1, 2, 3, 4, 5]
FORECAST_HOURS = [0, 1, 2, 3, 4, 5, 6]
ACTUAL_HOURS = [0, 1, 2, 3, 4, 5, 7]
ISSUED = "2026-02-26T08:00:00Z"  # 对 lead 24/48 都在截止时刻之前

SPLIT_PARAMS = {
    "dataset_ids": ["dsA", "dsB", "dsC"],
    "validation_start_hour": "2026-03-01T02:00:00Z",
    "validation_end_hour": "2026-03-01T03:00:00Z",
    "gap_hours": 1,
}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test.db")
    monkeypatch.setenv("APP_DB_PATH", db_file)
    logic.SNAPSHOT_TEST_HOOK = None
    with TestClient(app) as client:
        yield client, db_file
    logic.SNAPSHOT_TEST_HOOK = None


def hour(h):
    return f"2026-03-01T{h:02d}:00:00Z"


def gen_params(h0, h1, lead, station="ST01"):
    return {"station": station, "start_hour": hour(h0), "end_hour": hour(h1), "lead_hours": lead}


def import_base(client, station="ST01"):
    fc = ["station,target_hour,issued_at,irradiance"]
    fc += [f"{station},{hour(h)},{ISSUED},{500.0 + h}" for h in FORECAST_HOURS]
    r = client.post("/imports/forecasts", content="\n".join(fc) + "\n", headers=CSV_HEADERS)
    assert r.status_code == 200
    ac = ["station,target_hour,energy"]
    ac += [f"{station},{hour(h)},{40.0 + h}" for h in ACTUAL_HOURS]
    r = client.post("/imports/actuals", content="\n".join(ac) + "\n", headers=CSV_HEADERS)
    assert r.status_code == 200


def setup_datasets(client):
    """dsA: lead24 00..07；dsB: lead24 04..07（与 dsA 重叠且同值）；dsC: lead48 00..07。"""
    import_base(client)
    assert client.post("/datasets/dsA/generate", json=gen_params(0, 7, 24)).status_code == 200
    assert client.post("/datasets/dsB/generate", json=gen_params(4, 7, 24)).status_code == 200
    assert client.post("/datasets/dsC/generate", json=gen_params(0, 7, 48)).status_code == 200


def split_counts(db_file):
    conn = db.connect(db_file)
    try:
        return conn.execute("SELECT COUNT(*) AS n FROM splits").fetchone()["n"]
    finally:
        conn.close()


# ---------------------------------------------------------------- 合并、去重与分侧

def test_split_merges_dedupes_and_assigns_sides(env):
    client, _ = env
    setup_datasets(client)
    r = client.post("/splits/sp1", json=SPLIT_PARAMS)
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] is False
    assert body["params"]["dataset_ids"] == ["dsA", "dsB", "dsC"]

    # 计数：14 行配对成功（6+2+6），去重合并 2 条（04/05 时 lead24 来自 dsA+dsB），
    # 唯一样本 12 条 = 验证 4（02/03 时 × 2 个提前量）+ 隔离剔除 4（01/04 时 × 2）+ 训练 4（00/05 时 × 2）
    assert body["summary"] == {
        "station": "ST01",
        "input_datasets": 3,
        "paired_rows": 14,
        "unique_samples": 12,
        "merged_duplicates": 2,
        "train_count": 4,
        "validation_count": 4,
        "dropped_gap": 4,
        "dropped_unpaired": 6,  # 每份数据集各 2 行缺配（06 no_actual、07 no_forecast）
    }

    train, validation = body["train"], body["validation"]
    assert {(s["target_hour"], s["lead_hours"]) for s in train} == {
        (hour(0), 24), (hour(5), 24), (hour(0), 48), (hour(5), 48),
    }
    assert {(s["target_hour"], s["lead_hours"]) for s in validation} == {
        (hour(2), 24), (hour(3), 24), (hour(2), 48), (hour(3), 48),
    }
    # 缺配小时（06/07）与隔离带小时（01/04）不出现在任何一侧
    assert {s["target_hour"] for s in train + validation} == {hour(0), hour(2), hour(3), hour(5)}

    # 同值去重：05 时 lead24 同时出现在 dsA 与 dsB，合并为一条、来源号并集，取值不变
    sample05 = [s for s in train if s["target_hour"] == hour(5) and s["lead_hours"] == 24][0]
    assert sample05["source_dataset_ids"] == ["dsA", "dsB"]
    assert sample05["irradiance"] == 505.0 and sample05["energy"] == 45.0
    assert sample05["issued_at"] == ISSUED
    assert sample05["cutoff_issued_at"] == "2026-02-28T05:00:00Z"

    # 仅 dsA 覆盖的小时来源只有 dsA；lead48 的来源只有 dsC
    sample00 = [s for s in train if s["target_hour"] == hour(0) and s["lead_hours"] == 24][0]
    assert sample00["source_dataset_ids"] == ["dsA"]
    assert all(s["source_dataset_ids"] == ["dsC"] for s in train + validation if s["lead_hours"] == 48)

    # 同一小时的不同提前量必然同侧
    sides = {}
    for side_name, samples in (("train", train), ("validation", validation)):
        for s in samples:
            assert sides.setdefault(s["target_hour"], side_name) == side_name


def test_split_gap_zero_keeps_all_paired_hours(env):
    client, _ = env
    setup_datasets(client)
    params = dict(SPLIT_PARAMS, dataset_ids=["dsA"], gap_hours=0)
    body = client.post("/splits/sp0", json=params).json()
    assert body["summary"]["dropped_gap"] == 0
    assert body["summary"]["train_count"] == 4  # 00、01、04、05 时
    assert body["summary"]["validation_count"] == 2  # 02、03 时


# ---------------------------------------------------------------- 跨数据集冲突

def test_split_value_mismatch_reports_conflict_and_saves_nothing(env):
    client, db_file = env
    # 只用 00..03 时，全部配对成功
    fc = ["station,target_hour,issued_at,irradiance"]
    fc += [f"ST01,{hour(h)},{ISSUED},{500.0 + h}" for h in range(4)]
    assert client.post("/imports/forecasts", content="\n".join(fc) + "\n", headers=CSV_HEADERS).status_code == 200
    ac = ["station,target_hour,energy"]
    ac += [f"ST01,{hour(h)},{40.0 + h}" for h in range(4)]
    assert client.post("/imports/actuals", content="\n".join(ac) + "\n", headers=CSV_HEADERS).status_code == 200
    assert client.post("/datasets/ds1/generate", json=gen_params(0, 3, 24)).status_code == 200

    # 生成 ds1 后又导入一条更晚发布但仍合格的预报：ds2 的 01 时将采用新值
    extra = f"station,target_hour,issued_at,irradiance\nST01,{hour(1)},2026-02-27T08:00:00Z,999.0\n"
    assert client.post("/imports/forecasts", content=extra, headers=CSV_HEADERS).status_code == 200
    assert client.post("/datasets/ds2/generate", json=gen_params(0, 3, 24)).status_code == 200

    params = dict(SPLIT_PARAMS, dataset_ids=["ds1", "ds2"])
    r = client.post("/splits/spc", json=params)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert "不一致" in detail["message"]
    assert len(detail["conflicts"]) == 1
    conflict = detail["conflicts"][0]
    assert conflict["target_hour"] == hour(1) and conflict["lead_hours"] == 24
    by_ds = {v["dataset_id"]: v for v in conflict["values"]}
    assert by_ds["ds1"]["irradiance"] == 501.0 and by_ds["ds1"]["issued_at"] == ISSUED
    assert by_ds["ds2"]["irradiance"] == 999.0 and by_ds["ds2"]["issued_at"] == "2026-02-27T08:00:00Z"
    # 整次报冲突：不留半份划分
    assert split_counts(db_file) == 0
    assert client.get("/splits/spc").status_code == 404


# ---------------------------------------------------------------- 划分号语义

def test_split_retry_same_params_returns_original_even_after_new_data(env):
    client, _ = env
    setup_datasets(client)
    params = dict(SPLIT_PARAMS, dataset_ids=["dsA"])
    first = client.post("/splits/sp1", json=params).json()
    assert first["replayed"] is False

    # 划分后又有新数据导入、新数据集生成
    extra = f"station,target_hour,issued_at,irradiance\nST01,{hour(7)},2026-02-26T09:00:00Z,888.0\n"
    assert client.post("/imports/forecasts", content=extra, headers=CSV_HEADERS).status_code == 200
    assert client.post("/datasets/dsD/generate", json=gen_params(0, 7, 24)).status_code == 200

    second = client.post("/splits/sp1", json=params).json()
    assert second["replayed"] is True
    assert second["train"] == first["train"]
    assert second["validation"] == first["validation"]
    assert second["summary"] == first["summary"]
    assert second["created_at"] == first["created_at"]


def test_split_dataset_ids_order_and_duplicates_normalized(env):
    client, _ = env
    setup_datasets(client)
    first = client.post("/splits/spN", json=dict(SPLIT_PARAMS, dataset_ids=["dsC", "dsA", "dsA"])).json()
    assert first["replayed"] is False
    assert first["params"]["dataset_ids"] == ["dsA", "dsC"]
    second = client.post("/splits/spN", json=dict(SPLIT_PARAMS, dataset_ids=["dsA", "dsC"])).json()
    assert second["replayed"] is True
    assert second["summary"] == first["summary"]


def test_split_same_id_different_params_conflict(env):
    client, _ = env
    setup_datasets(client)
    assert client.post("/splits/sp1", json=SPLIT_PARAMS).status_code == 200
    r = client.post("/splits/sp1", json=dict(SPLIT_PARAMS, gap_hours=2))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["existing_params"]["gap_hours"] == 1
    assert detail["requested_params"]["gap_hours"] == 2
    r = client.post("/splits/sp1", json=dict(SPLIT_PARAMS, dataset_ids=["dsA"]))
    assert r.status_code == 409


def test_split_missing_dataset_rejected(env):
    client, _ = env
    setup_datasets(client)
    r = client.post("/splits/sp1", json=dict(SPLIT_PARAMS, dataset_ids=["dsA", "nope"]))
    assert r.status_code == 422
    assert "nope" in r.json()["detail"]["message"]


def test_split_mixed_stations_rejected(env):
    client, _ = env
    setup_datasets(client)
    import_base(client, station="ST02")
    assert client.post(
        "/datasets/dsOther/generate", json=gen_params(0, 7, 24, station="ST02")
    ).status_code == 200
    r = client.post("/splits/sp1", json=dict(SPLIT_PARAMS, dataset_ids=["dsA", "dsOther"]))
    assert r.status_code == 422
    assert "场站" in r.json()["detail"]["message"]


def test_split_param_validation(env):
    client, _ = env
    setup_datasets(client)
    bad = dict(SPLIT_PARAMS, validation_start_hour="2026-03-01T00:30:00Z")  # 未整点
    assert client.post("/splits/s1", json=bad).status_code == 422
    bad = dict(SPLIT_PARAMS, validation_start_hour="2026-03-01T05:00:00Z")  # 区间倒置
    assert client.post("/splits/s2", json=bad).status_code == 422
    bad = dict(SPLIT_PARAMS, gap_hours=-1)
    assert client.post("/splits/s3", json=bad).status_code == 422
    bad = dict(SPLIT_PARAMS, dataset_ids=[])
    assert client.post("/splits/s4", json=bad).status_code == 422


def test_get_split_and_list(env):
    client, _ = env
    setup_datasets(client)
    assert client.get("/splits/nope").status_code == 404
    created = client.post("/splits/sp1", json=SPLIT_PARAMS).json()
    r = client.get("/splits/sp1")
    assert r.status_code == 200
    assert r.json()["split_id"] == "sp1"
    assert r.json()["train"] == created["train"]
    listing = client.get("/splits").json()
    assert [s["split_id"] for s in listing] == ["sp1"]
    assert listing[0]["params"]["dataset_ids"] == ["dsA", "dsB", "dsC"]


# ---------------------------------------------------------------- 快照隔离与原子性

def test_split_reads_single_committed_snapshot(env):
    """划分事务期间，另一连接的写入无法提交；结果不含交错数据。"""
    client, db_file = env
    setup_datasets(client)
    state = {}

    def hook():
        other = db.connect(db_file)
        other.execute("PRAGMA busy_timeout=300")
        try:
            other.execute("BEGIN IMMEDIATE")
            other.execute("UPDATE dataset_rows SET irradiance = 999.0 WHERE dataset_id = 'dsA'")
            other.execute("COMMIT")
            state["committed"] = True
        except sqlite3.OperationalError as exc:
            state["error"] = str(exc)
        finally:
            other.close()

    logic.SNAPSHOT_TEST_HOOK = hook
    r = client.post("/splits/sp1", json=SPLIT_PARAMS)
    assert r.status_code == 200
    assert state.get("committed") is not True
    assert "locked" in state.get("error", "").lower()
    values = [s["irradiance"] for s in r.json()["train"] + r.json()["validation"]]
    assert 999.0 not in values  # 交错写入未进入划分结果

    # 划分结束后其他写入可正常进行；已保存的划分保持原样
    logic.SNAPSHOT_TEST_HOOK = None
    extra = f"station,target_hour,issued_at,irradiance\nST01,{hour(7)},2026-02-26T09:00:00Z,888.0\n"
    assert client.post("/imports/forecasts", content=extra, headers=CSV_HEADERS).status_code == 200
    again = client.get("/splits/sp1").json()
    assert 999.0 not in [s["irradiance"] for s in again["train"] + again["validation"]]


def test_failed_split_leaves_no_partial_result(env, monkeypatch):
    """提交失败时整体回滚：划分不可见，已导入数据与数据集不受影响。"""
    client, db_file = env
    setup_datasets(client)

    import app.main as main_module

    real_commit = main_module._commit
    calls = {"n": 0}

    def flaky_commit(conn):
        if calls["n"] == 0:
            calls["n"] += 1
            raise sqlite3.OperationalError("simulated commit failure")
        return real_commit(conn)

    monkeypatch.setattr(main_module, "_commit", flaky_commit)
    with pytest.raises(sqlite3.OperationalError, match="simulated commit failure"):
        client.post("/splits/spX", json=SPLIT_PARAMS)
    monkeypatch.undo()

    assert client.get("/splits/spX").status_code == 404
    assert split_counts(db_file) == 0

    # 数据库仍可用：重试成功
    r = client.post("/splits/spX", json=SPLIT_PARAMS)
    assert r.status_code == 200
    assert r.json()["replayed"] is False
