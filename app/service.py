"""核账服务：读数补传、手工核对与读视图的编排。

事务边界（对应"补传与手工核对交错提交"的需求）：
1. 所有输入校验与 solver 重计算都在写事务之外完成，求解再久也不阻塞
   网页查看、CSV 下载与清空账期。
2. 提交只走短的 BEGIN IMMEDIATE 事务，并在其中复核 epoch 与数据/参数指纹：
   读数冲突、参数无效、求解失败或写入失败时，本次改动整体不生效；
   两批补传、或补传与改参数交错时，各自依据仍在、合并后仍有效，则都生效。
3. 补传成功后按"现行参数"自动重算：读数与当前结论同一事务生效；
   账单核不过（target_unreachable / others_breach）也保存新结论；
   initial 初核只写一次，重算不覆盖。
4. 每次查看/下载都在同一只读快照里取数，绝不混用两次提交的数据。
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from . import solver, storage
from .solver import WINDOW_MINUTES


class AuditInputError(ValueError):
    """参数不完整或非法（不产生结论，页面提示即可）。"""


def _run_solver(readings, cap, peak, target):
    """solver 调用的单点封装，便于测试注入慢求解，产品逻辑无额外分支。"""
    return solver.audit(readings, cap, peak, target)


def parse_window_ts(text: str) -> datetime:
    for fmt in storage.TS_FORMATS:
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    raise AuditInputError("所报时段格式示例：2026-11-01 08:15")


def _positive_int(text: str, label: str) -> int:
    try:
        v = int(str(text).strip())
    except (ValueError, AttributeError):
        raise AuditInputError(f"{label}必须是整数")
    if v <= 0:
        raise AuditInputError(f"{label}必须为正整数")
    return v


# --------------------------------------------------------------------------- #
# 求解编排（无写操作，可在写事务之外长时间运行）
# --------------------------------------------------------------------------- #
def _solve(rows: List[Tuple[datetime, int]], cap: int, peak: int,
           win_start: datetime) -> solver.AuditResult:
    """把库内读数与参数组装为 solver 输入并执行核对。"""
    if len(rows) < 2:
        raise AuditInputError("请先上传至少两个读数")
    t0 = rows[0][0]
    t_end = rows[-1][0]
    if not (t0 <= win_start < t_end):
        raise AuditInputError("所报时段起点不在账期内")
    if (t_end - win_start).total_seconds() < WINDOW_MINUTES * 60:
        raise AuditInputError("所报时段必须完整落在账期内")

    offsets = [(int((ts - t0).total_seconds()) // 60, v) for ts, v in rows]
    target = int((win_start - t0).total_seconds()) // 60 // WINDOW_MINUTES
    try:
        return _run_solver(offsets, cap, peak, target)
    except solver.SolverError as exc:
        raise AuditInputError(f"当前实读无解：{exc}") from exc


def _current_params(cfg: sqlite3.Row
                    ) -> Optional[Tuple[int, int, datetime]]:
    """现行参数齐备才返回 (cap, peak, window)，否则 None（尚无结论可重算）。"""
    if cfg["per_minute_cap"] is None or cfg["peak_wh"] is None \
            or not cfg["reported_window"]:
        return None
    win_start = parse_window_ts(cfg["reported_window"])
    return int(cfg["per_minute_cap"]), int(cfg["peak_wh"]), win_start


# --------------------------------------------------------------------------- #
# 手工核对
# --------------------------------------------------------------------------- #
MAX_COMMIT_ATTEMPTS = 5


def run_audit(conn: sqlite3.Connection, cap_text: str, peak_text: str,
              window_text: str) -> solver.AuditResult:
    """手工核对：成功才写 current；initial 只写一次；失败不动任何数据。

    若求解期间读数/参数被其它提交改变，按最新状态重新校验重算后再提交
    （BasisConflict 乐观重试）；账期被清空则 EpochConflict 直接拒绝。
    """
    cap = _positive_int(cap_text, "每分钟用电上限")
    peak_raw = int(str(peak_text).strip())
    if peak_raw < 0:
        raise AuditInputError("账单峰值必须是非负整数")
    win_start = parse_window_ts(window_text)
    if win_start.minute % WINDOW_MINUTES or win_start.second:
        raise AuditInputError("所报时段起点必须对齐 15 分钟边界")

    for _ in range(MAX_COMMIT_ATTEMPTS):
        # 全部依据先在事务外读快照取齐，重计算不持锁
        with storage.snapshot(conn):
            epoch = storage.get_epoch(conn)
            rows = storage.load_readings(conn)
            readings_fp = storage._readings_fingerprint(conn)
            config_fp = storage._config_fingerprint(conn)
            is_first = not storage.has_initial(conn)
            result = _solve(rows, cap, peak_raw, win_start)

        plan = storage.AuditPlan(
            cap=cap, peak=peak_raw,
            win_text=win_start.strftime("%Y-%m-%d %H:%M"),
            result=result, epoch=epoch,
            readings_fingerprint=readings_fp,
            config_fingerprint=config_fp, is_first=is_first)
        try:
            storage.commit_audit(conn, plan)
            return result
        except storage.BasisConflict:
            continue  # 依据已变：带最新数据重算，合并后仍有效即可生效
    raise storage.ConflictError("核对参数竞争过于激烈，多次重试仍冲突，本次不生效")


# --------------------------------------------------------------------------- #
# 补传：合并读数 + 按现行参数重算，同一事务生效
# --------------------------------------------------------------------------- #
@dataclass
class UploadOutcome:
    total: int
    overwritten: int
    added: int
    recomputed: bool                 # 是否触发了现行参数重算
    result: Optional[solver.AuditResult]


def submit_readings(conn: sqlite3.Connection,
                    incoming: Dict[datetime, int]) -> UploadOutcome:
    """补传一批读数。

    流程（重活全部在写事务外）：
      规划合并方案（含回卷/结构/现行上限校验，失败整批拒绝）
      → 按现行参数重算（参数齐备时）
      → 单个短事务内复核 epoch/指纹后写读数与 current 结论。
    读数冲突、参数无效、求解失败或写入失败：读数、参数、结论全部维持原状。
    求解期间若有其它有效提交（BasisConflict），带最新状态重新规划重算，
    合并后仍有效则本次补传照样生效；账期被清空（EpochConflict）则直接拒绝。
    """
    for _ in range(MAX_COMMIT_ATTEMPTS):
        # 纯计算：任何校验失败都在写库之前抛出，库内状态不变
        plan = storage.plan_reading_merge(conn, incoming)

        # 重算依据：合并后的读数 + 库内现行参数（参数本身不被补传修改）
        with storage.snapshot(conn):
            cfg = storage.get_config(conn)
            params = _current_params(cfg)
            config_fp = storage._config_fingerprint(conn)
        result = None
        if params is not None:
            cap, peak, win_start = params
            result = _solve(plan.ordered, cap, peak, win_start)

        # 读数与结论在同一短事务落库：任一失败整体回滚，无半成品
        try:
            with storage.write_tx(conn):
                storage._guard(conn, plan.epoch,
                               readings_fp=plan.basis_fingerprint,
                               config_fp=config_fp)
                conn.execute("DELETE FROM readings")
                conn.executemany(
                    "INSERT INTO readings(minute_ts, reading_wh) "
                    "VALUES(?, ?)",
                    [(ts.strftime(storage.TS_FMT), v)
                     for ts, v in plan.ordered])
                if result is not None:
                    detail = json.dumps(asdict(result), ensure_ascii=False)
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute(
                        "INSERT INTO audits(kind, status, message, "
                        "detail_json, created_ts) "
                        "VALUES('current', ?, ?, ?, ?)",
                        (result.status, result.message, detail, now))
        except storage.BasisConflict:
            continue  # 依据已被其它有效提交更新：基于最新状态重来
        return UploadOutcome(
            total=plan.total, overwritten=plan.overwritten,
            added=plan.incoming_count - plan.overwritten,
            recomputed=result is not None, result=result)
    raise storage.ConflictError(
        "补传竞争过于激烈，多次重试仍冲突，本次改动不生效")


# --------------------------------------------------------------------------- #
# 读视图：同一次请求只用同一次提交的数据
# --------------------------------------------------------------------------- #
def dashboard(conn: sqlite3.Connection) -> Dict[str, Any]:
    """单快照取齐页面所需全部数据。"""
    with storage.snapshot(conn):
        rows = storage.load_readings(conn)
        cfg = storage.get_config(conn)
        initial = storage.latest_audit(conn, "initial")
        current = storage.latest_audit(conn, "current")
        current_result = load_result(current)
        windows = window_rows(rows, current_result)
        return {"readings": rows, "cfg": cfg, "initial": initial,
                "current": current, "current_result": current_result,
                "windows": windows, "epoch": storage.get_epoch(conn)}


def windows_csv_rows(conn: sqlite3.Connection
                     ) -> Optional[List[Tuple[str, int, int, bool]]]:
    """单快照导出各窗电量；没有可导出结论时返回 None。"""
    with storage.snapshot(conn):
        rows = storage.load_readings(conn)
        current = storage.latest_audit(conn, "current")
        result = load_result(current)
        return window_rows(rows, result) or None


def window_rows(rows: List[Tuple[datetime, int]],
                result: Optional[solver.AuditResult]
                ) -> List[Tuple[str, int, int, bool]]:
    """把结论里的各窗电量还原为 (起点时间, 电量Wh, 窗索引, 是否所报窗)。"""
    if not rows or result is None or not result.window_totals:
        return []
    t0 = rows[0][0]
    out = []
    for i, wh in enumerate(result.window_totals):
        ts = t0 + timedelta(minutes=i * WINDOW_MINUTES)
        out.append((ts.strftime("%Y-%m-%d %H:%M"), wh, i,
                    i == result.target_window))
    return out


def load_result(row) -> Optional[solver.AuditResult]:
    if row is None:
        return None
    data = json.loads(row["detail_json"])
    return solver.AuditResult(**data)
