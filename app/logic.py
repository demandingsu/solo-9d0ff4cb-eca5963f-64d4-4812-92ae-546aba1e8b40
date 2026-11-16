"""业务逻辑：时间规范化、CSV 解析、预报-实测配对。"""
from __future__ import annotations

import csv
import io
import math
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

CANON_FMT = "%Y-%m-%dT%H:%M:%SZ"

# 测试专用钩子：在生成事务持有写锁期间被调用，用于验证快照隔离。生产环境保持 None。
SNAPSHOT_TEST_HOOK: Optional[Callable[[], None]] = None

# 生成区间上限（约 5 年的小时数），防止误传超大区间。
MAX_SPAN_HOURS = 24 * 367 * 5

MISSING_NO_FORECAST = "no_forecast"
MISSING_NO_ACTUAL = "no_actual"
MISSING_BOTH = "no_forecast+no_actual"

MISSING_REASON_CN = {
    MISSING_NO_FORECAST: "缺合格预报（截止发布时刻前无已发布预报，或已有预报全部迟到）",
    MISSING_NO_ACTUAL: "缺实测电量",
    MISSING_BOTH: "合格预报与实测电量均缺",
}


class CsvFormatError(ValueError):
    """CSV 结构错误（空文件、缺列、重复列），整批拒绝。"""


# ---------------------------------------------------------------- 时间处理

def parse_utc(value: str) -> datetime:
    """解析 UTC 时间，容忍 Z 后缀、+00:00、空格分隔与裸时间（视为 UTC）。"""
    s = (value or "").strip()
    if not s:
        raise ValueError("时间为空")
    iso = s[:-1] + "+00:00" if s[-1:] in ("Z", "z") else s
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        raise ValueError(f"无法解析时间 {value!r}，需为 ISO 8601（UTC）")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def parse_hour(value: str) -> datetime:
    """解析目标小时，必须整点对齐。"""
    dt = parse_utc(value)
    if dt.minute or dt.second:
        raise ValueError(f"目标小时 {value!r} 未整点对齐")
    return dt


def canon(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(CANON_FMT)


# ---------------------------------------------------------------- CSV 解析

def _norm_header(name: str) -> str:
    return name.strip().lower().replace(" ", "").replace("_", "")


def _alias_set(*names: str) -> frozenset:
    return frozenset(_norm_header(n) for n in names)


FORECAST_COLUMNS = {
    "station": _alias_set("station", "site", "plant", "场站", "电站", "场站编号"),
    "target_hour": _alias_set("target_hour", "target", "target_time", "目标小时", "目标UTC小时", "目标时刻", "目标时间"),
    "issued_at": _alias_set("issued_at", "issue_time", "issued", "published_at", "publish_time",
                            "发布时间", "发布UTC时刻", "发布时刻"),
    "irradiance": _alias_set("irradiance", "forecast_irradiance", "ghi", "预测辐照", "辐照", "辐照度"),
}

ACTUAL_COLUMNS = {
    "station": _alias_set("station", "site", "plant", "场站", "电站", "场站编号"),
    "target_hour": _alias_set("target_hour", "target", "target_time", "目标小时", "目标UTC小时", "目标时刻", "目标时间"),
    "energy": _alias_set("energy", "actual_energy", "generation", "电量", "实测电量", "发电量"),
}


def _header_map(header, aliases, kind):
    mapping = {}
    for idx, name in enumerate(header):
        key = _norm_header(name)
        if not key:
            continue
        for field, names in aliases.items():
            if key in names:
                if field in mapping:
                    raise CsvFormatError(f"{kind} CSV 表头重复: {name!r}")
                mapping[field] = idx
    missing = [f for f in aliases if f not in mapping]
    if missing:
        raise CsvFormatError(f"{kind} CSV 缺少必需列: {', '.join(missing)}")
    return mapping


def _parse_number(value: str, field_cn: str) -> float:
    s = (value or "").strip()
    try:
        x = float(s)
    except ValueError:
        raise ValueError(f"{field_cn}不是数值: {value!r}")
    if not math.isfinite(x):
        raise ValueError(f"{field_cn}非法数值: {value!r}")
    return x


def _parse_forecast_record(mapping, rec):
    station = rec[mapping["station"]].strip()
    if not station:
        raise ValueError("场站为空")
    target = canon(parse_hour(rec[mapping["target_hour"]]))
    issued = canon(parse_utc(rec[mapping["issued_at"]]))
    irradiance = _parse_number(rec[mapping["irradiance"]], "预测辐照")
    key = (station, target, issued)
    return key, irradiance, {
        "station": station, "target_hour": target,
        "issued_at": issued, "irradiance": irradiance,
    }


def _parse_actual_record(mapping, rec):
    station = rec[mapping["station"]].strip()
    if not station:
        raise ValueError("场站为空")
    target = canon(parse_hour(rec[mapping["target_hour"]]))
    energy = _parse_number(rec[mapping["energy"]], "电量")
    key = (station, target)
    return key, energy, {"station": station, "target_hour": target, "energy": energy}


def _iter_records(text: str, aliases, kind: str, parse_record):
    """解析整份 CSV。

    返回 (rows, errors, conflicts)：
    - rows      已按业务键去重的有效行；
    - errors    行级解析错误（调用方应整批拒收，返回 422）；
    - conflicts 批内同键异值（调用方应整批拒收，返回 409）。
    """
    text = text.lstrip("﻿")
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if header is None:
        raise CsvFormatError(f"{kind} CSV 为空")
    mapping = _header_map(header, aliases, kind)
    width = max(mapping.values()) + 1

    rows, errors, conflicts = [], [], []
    seen = {}
    for lineno, record in enumerate(reader, start=2):
        if not record or all(not cell.strip() for cell in record):
            continue
        try:
            key, value, row = parse_record(mapping, record)
        except IndexError:
            errors.append({"line": lineno, "message": f"列数不足（至少需要 {width} 列）"})
            continue
        except ValueError as exc:
            errors.append({"line": lineno, "message": str(exc)})
            continue
        if key in seen:
            if seen[key] != value:
                conflicts.append({
                    "line": lineno, "key": list(key),
                    "value_first": seen[key], "value_second": value,
                })
            continue
        seen[key] = value
        rows.append(row)
    return rows, errors, conflicts


def parse_forecast_csv(text: str):
    return _iter_records(text, FORECAST_COLUMNS, "预报", _parse_forecast_record)


def parse_actual_csv(text: str):
    return _iter_records(text, ACTUAL_COLUMNS, "实测", _parse_actual_record)


# ---------------------------------------------------------------- 生成参数

def normalize_generate_params(station: str, start_hour: str, end_hour: str, lead_hours: int):
    """校验并规范化生成参数，返回 (规范参数 dict, start_dt, end_dt)。"""
    station = (station or "").strip()
    if not station:
        raise ValueError("场站不能为空")
    start = parse_hour(start_hour)
    end = parse_hour(end_hour)
    if end < start:
        raise ValueError("end_hour 早于 start_hour")
    span = int((end - start).total_seconds() // 3600) + 1
    if span > MAX_SPAN_HOURS:
        raise ValueError(f"目标小时区间过长（{span} 小时，上限 {MAX_SPAN_HOURS}）")
    if lead_hours < 0:
        raise ValueError("lead_hours 不能为负")
    params = {
        "station": station,
        "start_hour": canon(start),
        "end_hour": canon(end),
        "lead_hours": lead_hours,
    }
    return params, start, end


# ---------------------------------------------------------------- 配对

def build_pairs(conn, station: str, start_dt: datetime, end_dt: datetime, lead_hours: int):
    """逐目标小时配对：取发布时间不晚于 目标小时-lead_hours 的最新预报，配上实测。

    调用方必须已处于写事务中（BEGIN IMMEDIATE），保证读到同一已提交快照。
    """
    start_s, end_s = canon(start_dt), canon(end_dt)
    forecast_rows = conn.execute(
        "SELECT target_hour, issued_at, irradiance FROM forecasts "
        "WHERE station = ? AND target_hour BETWEEN ? AND ? "
        "ORDER BY target_hour, issued_at",
        (station, start_s, end_s),
    ).fetchall()
    by_target: dict[str, list[tuple[str, float]]] = {}
    for r in forecast_rows:
        by_target.setdefault(r["target_hour"], []).append((r["issued_at"], r["irradiance"]))

    actual_rows = conn.execute(
        "SELECT target_hour, energy FROM actuals "
        "WHERE station = ? AND target_hour BETWEEN ? AND ?",
        (station, start_s, end_s),
    ).fetchall()
    actuals = {r["target_hour"]: r["energy"] for r in actual_rows}

    lead = timedelta(hours=lead_hours)
    one_hour = timedelta(hours=1)
    rows = []
    t = start_dt
    while t <= end_dt:
        target_s = canon(t)
        cutoff_s = canon(t - lead)
        candidates = by_target.get(target_s, [])
        issued_list = [c[0] for c in candidates]
        idx = bisect_right(issued_list, cutoff_s) - 1  # 不晚于截止时刻的最新一条
        chosen = candidates[idx] if idx >= 0 else None
        energy = actuals.get(target_s)

        missing = None
        if chosen is None and energy is None:
            missing = MISSING_BOTH
        elif chosen is None:
            missing = MISSING_NO_FORECAST
        elif energy is None:
            missing = MISSING_NO_ACTUAL

        rows.append({
            "station": station,
            "target_hour": target_s,
            "cutoff_issued_at": cutoff_s,
            "issued_at": chosen[0] if chosen else None,
            "irradiance": chosen[1] if chosen else None,
            "energy": energy,
            "missing_reason": missing,
            "missing_reason_cn": MISSING_REASON_CN.get(missing) if missing else None,
        })
        t += one_hour

    summary = {
        "total_hours": len(rows),
        "paired": sum(1 for r in rows if r["missing_reason"] is None),
        "missing_forecast": sum(1 for r in rows if r["missing_reason"] == MISSING_NO_FORECAST),
        "missing_actual": sum(1 for r in rows if r["missing_reason"] == MISSING_NO_ACTUAL),
        "missing_both": sum(1 for r in rows if r["missing_reason"] == MISSING_BOTH),
    }
    return rows, summary
