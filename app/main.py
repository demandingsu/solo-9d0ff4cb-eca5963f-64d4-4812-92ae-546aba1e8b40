"""FastAPI 入口：预报/实测 CSV 导入、回测样本数据集生成与查询、训练/验证划分。"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Body, FastAPI, HTTPException, Path
from pydantic import BaseModel, Field

from . import db, logic

# ---------------------------------------------------------------- 示例（同时用于 /docs 与 README）

FORECAST_CSV_EXAMPLE = """station,target_hour,issued_at,irradiance
ST01,2026-03-01T00:00:00Z,2026-02-27T08:00:00Z,500.0
ST01,2026-03-01T00:00:00Z,2026-02-28T06:00:00Z,550.0
ST01,2026-03-01T01:00:00Z,2026-02-28T00:30:00Z,510.0
ST01,2026-03-01T03:00:00Z,2026-02-27T09:00:00Z,520.0
"""

ACTUAL_CSV_EXAMPLE = """station,target_hour,energy
ST01,2026-03-01T00:00:00Z,48.2
ST01,2026-03-01T01:00:00Z,49.1
ST01,2026-03-01T02:00:00Z,47.5
"""

GENERATE_EXAMPLE = {
    "station": "ST01",
    "start_hour": "2026-03-01T00:00:00Z",
    "end_hour": "2026-03-01T03:00:00Z",
    "lead_hours": 24,
}

SPLIT_EXAMPLE = {
    "dataset_ids": ["demo-20260301"],
    "validation_start_hour": "2026-03-01T01:00:00Z",
    "validation_end_hour": "2026-03-01T02:00:00Z",
    "gap_hours": 1,
}

API_DESCRIPTION = """
光伏回测样本服务：把历史辐照预报与实测发电量配成回测样本，**保证事后发布的预报不会进入样本**；
并可把多份已保存数据集合并成**训练/验证划分**，保证同一小时的预测不会泄漏到验证侧。

## 使用流程

1. `POST /imports/forecasts` 导入预报 CSV（场站、目标 UTC 小时、发布 UTC 时刻、预测辐照）。
2. `POST /imports/actuals` 导入实测 CSV（场站、目标小时、电量）。
3. `POST /datasets/{数据集号}/generate` 指定场站、目标小时区间、提前小时数生成配对结果并保存。
4. `GET /datasets/{数据集号}` 随时读回已保存的结果。
5. `POST /splits/{划分号}` 把同一场站的多份数据集合并，按验证区间+隔离小时数划分训练/验证索引。
6. `GET /splits/{划分号}` 随时读回已保存的划分。

## 配对与迟到预报规则

对每个目标小时，发布截止时刻 = `目标小时 - lead_hours`。
只采用 `issued_at <= 截止时刻` 的预报，取其中**最新**一条；晚于截止时刻发布的预报视为**迟到预报**，不参与配对。
每行结果都带回 `cutoff_issued_at`（截止时刻）与实际采用的 `issued_at`（来源时刻），迟到预报可在导入后通过对比发现。

## 缺配原因（missing_reason）

| 代码 | 含义 |
|---|---|
| `no_forecast` | 截止时刻前无已发布预报（或全部迟到） |
| `no_actual` | 无实测电量 |
| `no_forecast+no_actual` | 两者均缺 |
| `null` | 配对成功 |

## 训练/验证划分规则

- 只取各数据集中**配对成功**的行；缺配行计入 `dropped_unpaired`。
- 同一目标小时、同一提前量出现在多份数据集：来源时刻、辐照、电量一致则**去重**，
  来源数据集号并入 `source_dataset_ids`；任一不一致则**整次报 409 冲突**，划分不落库。
- 目标小时落在验证区间（含两端）→ 验证侧；落在区间两端各 `gap_hours` 的隔离带内 →
  剔除（不进训练也不进验证，计入 `dropped_gap`）；其余 → 训练侧。
- 侧别只由目标小时决定，因此**同一小时的不同提前量必然同侧**，不会泄漏。

## 导入幂等与整批拒收

- 同键同值重复导入：幂等跳过（`skipped_duplicates`）。
- 同键异值（批内或与本库已有数据冲突）：**整批拒收**（409），一行都不会写入。
- 行格式错误：整批拒收（422），并逐行列出原因。

## 数据集号 / 划分号语义

- 同一编号 + 相同参数重试：直接读回首次保存的结果（`replayed=true`），即使期间又有新数据导入。
- 同一编号 + 不同参数：409 冲突，返回已存参数与请求参数。

## 快照隔离

生成与划分都在一个 `BEGIN IMMEDIATE` 写事务内完成：事务期间其他进程的写入无法提交，
整份结果必然来自**同一个已提交快照**；任一环节失败即整体回滚，不会留下可见的半份结果。
"""

TAGS = [
    {"name": "imports", "description": "CSV 导入（整批原子，同键异值拒收）"},
    {"name": "datasets", "description": "回测样本数据集：生成、幂等重试、查询"},
    {"name": "splits", "description": "训练/验证划分：合并多份数据集、隔离带防泄漏、幂等重试"},
    {"name": "meta", "description": "服务信息"},
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_path = os.environ.get("APP_DB_PATH", os.path.join(os.getcwd(), "data", "app.db"))
    db.init_db(app.state.db_path)
    yield


app = FastAPI(
    title="光伏回测样本服务",
    version="1.0.0",
    description=API_DESCRIPTION,
    openapi_tags=TAGS,
    lifespan=lifespan,
)


# ---------------------------------------------------------------- 模型

class GenerateParams(BaseModel):
    station: str = Field(min_length=1, max_length=128, description="场站编号", examples=["ST01"])
    start_hour: str = Field(description="起始目标 UTC 小时（整点）", examples=["2026-03-01T00:00:00Z"])
    end_hour: str = Field(description="结束目标 UTC 小时（整点，含）", examples=["2026-03-01T03:00:00Z"])
    lead_hours: int = Field(ge=0, le=24 * 90, description="提前小时数：只采用 目标小时-lead_hours 之前发布的预报", examples=[24])


class SplitParams(BaseModel):
    dataset_ids: list[str] = Field(
        min_length=1, max_length=500,
        description="参与合并的数据集号（须属于同一场站；去重排序后保存，顺序/重复不影响参数判定）",
        examples=[["demo-20260301"]],
    )
    validation_start_hour: str = Field(description="验证区间起始 UTC 小时（整点，含）", examples=["2026-03-01T01:00:00Z"])
    validation_end_hour: str = Field(description="验证区间结束 UTC 小时（整点，含）", examples=["2026-03-01T02:00:00Z"])
    gap_hours: int = Field(
        ge=0, le=24 * 90,
        description="隔离小时数：验证区间两端各这么宽的小时带不进训练（也不进验证），防止相邻小时泄漏",
        examples=[1],
    )


DATASET_ID_PATH = Path(
    pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
    description="数据集号（字母数字开头，可含 . _ -）",
    examples=["demo-20260301"],
)

SPLIT_ID_PATH = Path(
    pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
    description="划分号（字母数字开头，可含 . _ -）",
    examples=["split-20260301"],
)


# ---------------------------------------------------------------- 工具

def _commit(conn: sqlite3.Connection) -> None:
    """提交当前事务。独立成函数以便测试注入提交失败。"""
    conn.execute("COMMIT")


def _locked_to_503(exc: sqlite3.OperationalError) -> None:
    if "locked" in str(exc).lower():
        raise HTTPException(status_code=503, detail={
            "message": "数据库正被其他进程写入，请稍后重试",
            "retryable": True,
        })


def _import_batch(table: str, fields: list[str], key_fields: list[str], value_field: str, rows: list[dict]):
    """单事务导入：同键同值跳过，同键异值整批回滚并返回 409。"""
    conn = db.connect(app.state.db_path)
    inserted = skipped = 0
    conflicts = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        where = " AND ".join(f"{f} = ?" for f in key_fields)
        columns = ", ".join(fields)
        placeholders = ", ".join("?" for _ in fields)
        for row in rows:
            key = tuple(row[f] for f in key_fields)
            existing = conn.execute(f"SELECT {value_field} AS v FROM {table} WHERE {where}", key).fetchone()
            if existing is None:
                conn.execute(
                    f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                    tuple(row[f] for f in fields),
                )
                inserted += 1
            elif existing["v"] != row[value_field]:
                conflicts.append({
                    "key": dict(zip(key_fields, key)),
                    "db_value": existing["v"],
                    "csv_value": row[value_field],
                })
            else:
                skipped += 1
        if conflicts:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail={
                "message": "同键异值，整批拒收（未写入任何行）",
                "conflicts": conflicts,
            })
        _commit(conn)
        return inserted, skipped
    except HTTPException:
        raise
    except sqlite3.OperationalError as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        _locked_to_503(exc)
        raise
    finally:
        conn.close()


def _parse_or_reject(kind: str, parse_fn, csv_text: str):
    try:
        rows, errors, conflicts = parse_fn(csv_text)
    except logic.CsvFormatError as exc:
        raise HTTPException(status_code=422, detail={"message": str(exc)})
    if errors:
        raise HTTPException(status_code=422, detail={
            "message": f"{kind} CSV 存在错误行，整批未导入",
            "errors": errors,
        })
    if conflicts:
        raise HTTPException(status_code=409, detail={
            "message": f"{kind} CSV 批内同键异值，整批拒收（未写入任何行）",
            "conflicts": conflicts,
        })
    if not rows:
        raise HTTPException(status_code=422, detail={"message": f"{kind} CSV 无有效数据行"})
    return rows


# ---------------------------------------------------------------- 路由：meta

@app.get("/", tags=["meta"], summary="服务信息")
def root():
    return {
        "service": "光伏回测样本服务",
        "docs": "/docs",
        "endpoints": [
            "POST /imports/forecasts",
            "POST /imports/actuals",
            "POST /datasets/{dataset_id}/generate",
            "GET /datasets",
            "GET /datasets/{dataset_id}",
            "POST /splits/{split_id}",
            "GET /splits",
            "GET /splits/{split_id}",
        ],
    }


@app.get("/health", tags=["meta"], summary="健康检查")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------- 路由：导入

@app.post(
    "/imports/forecasts",
    tags=["imports"],
    summary="导入预报 CSV",
    description="整批原子导入。同键（场站+目标小时+发布时刻）同值幂等跳过；同键异值整批拒收（409）。",
)
def import_forecasts(
    csv_text: str = Body(
        media_type="text/csv",
        openapi_examples={
            "含迟到预报的预报批次": {
                "summary": "预报导入示例（含一条迟到预报）",
                "description": (
                    "配合生成示例（lead_hours=24）：目标小时 2026-03-01T00:00:00Z 的发布截止时刻为 "
                    "2026-02-28T00:00:00Z。第二行 550.0 发布于 2026-02-28T06:00:00Z，**晚于截止时刻，"
                    "属于迟到预报，不会被采用**，配对结果将使用 500.0（2026-02-27T08:00:00Z 发布）。"
                ),
                "value": FORECAST_CSV_EXAMPLE,
            },
        },
    ),
):
    rows = _parse_or_reject("预报", logic.parse_forecast_csv, csv_text)
    inserted, skipped = _import_batch(
        "forecasts",
        ["station", "target_hour", "issued_at", "irradiance"],
        ["station", "target_hour", "issued_at"],
        "irradiance",
        rows,
    )
    return {"inserted": inserted, "skipped_duplicates": skipped, "accepted_rows": len(rows)}


@app.post(
    "/imports/actuals",
    tags=["imports"],
    summary="导入实测 CSV",
    description="整批原子导入。同键（场站+目标小时）同值幂等跳过；同键异值整批拒收（409）。",
)
def import_actuals(
    csv_text: str = Body(
        media_type="text/csv",
        openapi_examples={
            "实测批次": {
                "summary": "实测导入示例",
                "description": "目标小时 2026-03-01T03:00:00Z 故意缺实测，用于演示缺配原因 no_actual。",
                "value": ACTUAL_CSV_EXAMPLE,
            },
        },
    ),
):
    rows = _parse_or_reject("实测", logic.parse_actual_csv, csv_text)
    inserted, skipped = _import_batch(
        "actuals",
        ["station", "target_hour", "energy"],
        ["station", "target_hour"],
        "energy",
        rows,
    )
    return {"inserted": inserted, "skipped_duplicates": skipped, "accepted_rows": len(rows)}


# ---------------------------------------------------------------- 路由：数据集

@app.post(
    "/datasets/{dataset_id}/generate",
    tags=["datasets"],
    summary="生成回测样本数据集",
    description=(
        "在单个写事务内读取同一已提交快照完成配对并保存。"
        "同号同参数重试读回原结果（replayed=true）；同号异参数返回 409。"
        "失败整体回滚，不留半份结果。"
    ),
    responses={
        409: {"description": "数据集号已存在且参数不同"},
        422: {"description": "参数非法（未整点、区间倒置、区间过长等）"},
        503: {"description": "数据库被其他写入占用，可重试"},
    },
)
def generate_dataset(
    params: GenerateParams = Body(
        openapi_examples={
            "首次生成": {
                "summary": "首次生成（配合 /imports 的示例数据）",
                "description": (
                    "预期结果：00 时配对 500.0（550.0 为迟到预报被排除）；01 时配对 510.0；"
                    "02 时缺合格预报 no_forecast；03 时缺实测 no_actual。"
                ),
                "value": GENERATE_EXAMPLE,
            },
            "同号同参数重试": {
                "summary": "同号同参数重试 → 读回原结果",
                "description": (
                    "用同一 dataset_id 再次提交完全相同的参数：即使期间又导入了新数据，"
                    "也直接读回首次保存的结果，响应中 replayed=true。"
                ),
                "value": GENERATE_EXAMPLE,
            },
            "同号异参数冲突": {
                "summary": "同号异参数 → 409 冲突",
                "description": "同一 dataset_id 换用 lead_hours=12 等任何不同参数，返回 409 及已存参数。",
                "value": {**GENERATE_EXAMPLE, "lead_hours": 12},
            },
        },
    ),
    dataset_id: str = DATASET_ID_PATH,
):
    try:
        canon_params, start_dt, end_dt = logic.normalize_generate_params(
            params.station, params.start_hour, params.end_hour, params.lead_hours
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={"message": str(exc)})

    conn = db.connect(app.state.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            "SELECT params_json, result_json FROM datasets WHERE dataset_id = ?",
            (dataset_id,),
        ).fetchone()
        if existing is not None:
            stored_params = json.loads(existing["params_json"])
            if stored_params == canon_params:
                result = json.loads(existing["result_json"])
                _commit(conn)
                result["replayed"] = True
                return result
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail={
                "message": "数据集号已存在且参数不同，未覆盖",
                "dataset_id": dataset_id,
                "existing_params": stored_params,
                "requested_params": canon_params,
            })

        rows, summary = logic.build_pairs(
            conn, canon_params["station"], start_dt, end_dt, canon_params["lead_hours"]
        )

        if logic.SNAPSHOT_TEST_HOOK is not None:  # 测试钩子：验证快照隔离，生产为空
            logic.SNAPSHOT_TEST_HOOK()

        created_at = logic.canon(datetime.now(timezone.utc))
        result = {
            "dataset_id": dataset_id,
            "created_at": created_at,
            "params": canon_params,
            "summary": summary,
            "rows": rows,
        }
        conn.execute(
            "INSERT INTO datasets (dataset_id, params_json, result_json, created_at) VALUES (?, ?, ?, ?)",
            (dataset_id, json.dumps(canon_params, sort_keys=True), json.dumps(result), created_at),
        )
        conn.executemany(
            "INSERT INTO dataset_rows (dataset_id, station, target_hour, cutoff_issued_at, "
            "issued_at, irradiance, energy, missing_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    dataset_id, r["station"], r["target_hour"], r["cutoff_issued_at"],
                    r["issued_at"], r["irradiance"], r["energy"], r["missing_reason"],
                )
                for r in rows
            ],
        )
        _commit(conn)
        result["replayed"] = False
        return result
    except HTTPException:
        raise
    except sqlite3.OperationalError as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        _locked_to_503(exc)
        raise
    finally:
        conn.close()


@app.get("/datasets", tags=["datasets"], summary="列出已保存的数据集")
def list_datasets():
    conn = db.connect(app.state.db_path)
    try:
        rows = conn.execute(
            "SELECT dataset_id, params_json, created_at FROM datasets ORDER BY created_at, dataset_id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {"dataset_id": r["dataset_id"], "params": json.loads(r["params_json"]), "created_at": r["created_at"]}
        for r in rows
    ]


@app.get(
    "/datasets/{dataset_id}",
    tags=["datasets"],
    summary="读回数据集结果",
    responses={404: {"description": "数据集不存在"}},
)
def get_dataset(dataset_id: str):
    conn = db.connect(app.state.db_path)
    try:
        row = conn.execute(
            "SELECT result_json FROM datasets WHERE dataset_id = ?", (dataset_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail={"message": f"数据集 {dataset_id!r} 不存在"})
    return json.loads(row["result_json"])


# ---------------------------------------------------------------- 路由：训练/验证划分

@app.post(
    "/splits/{split_id}",
    tags=["splits"],
    summary="合并多份数据集并创建训练/验证划分",
    description=(
        "在单个写事务内读取同一已提交快照：只取配对成功的行；同一目标小时+提前量跨数据集"
        "同值去重（来源号并入 source_dataset_ids），异值整次报 409；验证区间内的小时进验证侧，"
        "区间两端各 gap_hours 隔离带内的小时不进训练；同一小时的不同提前量始终同侧。"
        "同号同参数重试读回原结果（replayed=true）；同号异参数返回 409。"
        "失败整体回滚，不留半份划分。"
    ),
    responses={
        409: {"description": "划分号已存在且参数不同，或跨数据集同键样本取值不一致"},
        422: {"description": "参数非法（数据集不存在、场站不一致、未整点、区间倒置等）"},
        503: {"description": "数据库被其他写入占用，可重试"},
    },
)
def create_split(
    params: SplitParams = Body(
        openapi_examples={
            "首次划分": {
                "summary": "首次划分（配合 /datasets 的示例数据集）",
                "description": (
                    "把 demo-20260301 数据集中配对成功的行按小时分侧：01、02 时进验证侧；"
                    "隔离 1 小时使 00、03 时不进训练（dropped_gap）；缺配行计入 dropped_unpaired。"
                ),
                "value": SPLIT_EXAMPLE,
            },
            "同号同参数重试": {
                "summary": "同号同参数重试 → 读回原结果",
                "description": (
                    "用同一 split_id 再次提交完全相同的参数（dataset_ids 顺序/重复不影响判定）："
                    "即使期间又导入了新数据、生成了新数据集，也直接读回首次保存的划分，replayed=true。"
                ),
                "value": SPLIT_EXAMPLE,
            },
            "同号异参数冲突": {
                "summary": "同号异参数 → 409 冲突",
                "description": "同一 split_id 换用 gap_hours=2 等任何不同参数，返回 409 及已存参数。",
                "value": {**SPLIT_EXAMPLE, "gap_hours": 2},
            },
        },
    ),
    split_id: str = SPLIT_ID_PATH,
):
    try:
        canon_params, val_start_dt, val_end_dt = logic.normalize_split_params(
            params.dataset_ids, params.validation_start_hour,
            params.validation_end_hour, params.gap_hours,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={"message": str(exc)})

    conn = db.connect(app.state.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            "SELECT params_json, result_json FROM splits WHERE split_id = ?",
            (split_id,),
        ).fetchone()
        if existing is not None:
            stored_params = json.loads(existing["params_json"])
            if stored_params == canon_params:
                result = json.loads(existing["result_json"])
                _commit(conn)
                result["replayed"] = True
                return result
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail={
                "message": "划分号已存在且参数不同，未覆盖",
                "split_id": split_id,
                "existing_params": stored_params,
                "requested_params": canon_params,
            })

        try:
            train, validation, summary = logic.build_split(
                conn, canon_params["dataset_ids"], val_start_dt, val_end_dt,
                canon_params["gap_hours"],
            )
        except ValueError as exc:  # 数据集不存在 / 场站不一致
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=422, detail={"message": str(exc)})
        except logic.SplitConflictError as exc:
            conn.execute("ROLLBACK")
            raise HTTPException(status_code=409, detail={
                "message": "同一目标小时+提前量在不同数据集中的来源时刻、辐照或电量不一致，"
                           "整次划分未保存",
                "conflicts": exc.conflicts,
            })

        if logic.SNAPSHOT_TEST_HOOK is not None:  # 测试钩子：验证快照隔离，生产为空
            logic.SNAPSHOT_TEST_HOOK()

        created_at = logic.canon(datetime.now(timezone.utc))
        result = {
            "split_id": split_id,
            "created_at": created_at,
            "params": canon_params,
            "summary": summary,
            "train": train,
            "validation": validation,
        }
        conn.execute(
            "INSERT INTO splits (split_id, params_json, result_json, created_at) VALUES (?, ?, ?, ?)",
            (split_id, json.dumps(canon_params, sort_keys=True), json.dumps(result), created_at),
        )
        _commit(conn)
        result["replayed"] = False
        return result
    except HTTPException:
        raise
    except sqlite3.OperationalError as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        _locked_to_503(exc)
        raise
    finally:
        conn.close()


@app.get("/splits", tags=["splits"], summary="列出已保存的划分")
def list_splits():
    conn = db.connect(app.state.db_path)
    try:
        rows = conn.execute(
            "SELECT split_id, params_json, created_at FROM splits ORDER BY created_at, split_id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {"split_id": r["split_id"], "params": json.loads(r["params_json"]), "created_at": r["created_at"]}
        for r in rows
    ]


@app.get(
    "/splits/{split_id}",
    tags=["splits"],
    summary="读回划分结果",
    responses={404: {"description": "划分不存在"}},
)
def get_split(split_id: str):
    conn = db.connect(app.state.db_path)
    try:
        row = conn.execute(
            "SELECT result_json FROM splits WHERE split_id = ?", (split_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail={"message": f"划分 {split_id!r} 不存在"})
    return json.loads(row["result_json"])
