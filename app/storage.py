"""SQLite 存储层：一块表的一期数据。

表：
- config    单行：核账参数（每分钟上限、峰值、所报窗起点）
- readings  每分钟累计读数（同刻覆盖，按分钟时间戳唯一）
- audits    核账结论：kind=initial 只写一次，kind=current 每次更新
- meta      单行：账期代号 epoch，每次清空调期 +1。
             长事务（求解）提交时核对 epoch，保证"清空前已受理的改动
             不得回写新账期，即使新账期录入了相同读数"。

并发约定（与 service 层配合）：
- 所有重计算在开启写事务之前完成，写事务只做短的校验与写入，
  因此求解进行中页面查看、CSV 下载、清空都不会被阻塞。
- 每次查看/下载走单一只读快照事务，保证同一次请求读到同一份提交。
- 写事务用 BEGIN IMMEDIATE 串行化，并在其中复核 epoch 与数据指纹，
  发现依据已被其它提交改变即整体放弃（ConflictError）。
"""
from __future__ import annotations

import csv
import hashlib
import io
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Tuple

from . import solver

SCHEMA = """
CREATE TABLE IF NOT EXISTS config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    per_minute_cap INTEGER,
    peak_wh INTEGER,
    reported_window TEXT
);
CREATE TABLE IF NOT EXISTS readings (
    minute_ts TEXT PRIMARY KEY,
    reading_wh INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL CHECK (kind IN ('initial', 'current')),
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audits_kind ON audits(kind);
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    epoch INTEGER NOT NULL
);
"""

TS_FORMATS = ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M",
              "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")
TS_FMT = "%Y-%m-%d %H:%M"


class UploadError(ValueError):
    """CSV 整批拒绝。"""


class ConflictError(RuntimeError):
    """提交依据已被其它请求改变，本次提交未生效。"""


class EpochConflict(ConflictError):
    """账期已清空重建：在途改动永远不得回写新账期（不重试）。"""


class BasisConflict(ConflictError):
    """读数/参数在求解期间被其它提交改变：可基于最新状态重算后重试。"""


@dataclass(frozen=True)
class ReadingPlan:
    """一次补传在写库前算好的完整方案：校验通过后只剩落库。"""
    ordered: List[Tuple[datetime, int]]   # 合并并排序后的全量读数
    total: int                             # 合并后读数条数
    overwritten: int                       # 本次同刻覆盖条数
    incoming_count: int
    epoch: int
    basis_fingerprint: str                 # 提交前库内读数指纹


@dataclass(frozen=True)
class AuditPlan:
    """一次核对/重算在写库前算好的方案。"""
    cap: int
    peak: int
    win_text: str
    result: "object"                       # solver.AuditResult
    epoch: int
    readings_fingerprint: str              # 求解所依据的读数指纹
    config_fingerprint: str                # 求解所依据的参数指纹
    is_first: bool                         # 本次是否需要落 initial


# --------------------------------------------------------------------------- #
# 连接与事务
# --------------------------------------------------------------------------- #
def get_db(path: str) -> sqlite3.Connection:
    """打开连接：WAL 允许求解（写前计算）期间继续读/清空；
    busy_timeout 让写锁竞争等待而不是立即报错。"""
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(SCHEMA)
    if conn.execute("SELECT COUNT(*) FROM config").fetchone()[0] == 0:
        conn.execute("INSERT INTO config(id) VALUES(1)")
    if conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0] == 0:
        conn.execute("INSERT INTO meta(id, epoch) VALUES(1, 0)")
    return conn


@contextmanager
def snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """单一只读快照：整个查看/下载请求只看到同一次提交的数据。"""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.rollback()


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """短写事务：重活必须在进入前做完。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# --------------------------------------------------------------------------- #
# 账期代号与数据指纹
# --------------------------------------------------------------------------- #
def get_epoch(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT epoch FROM meta WHERE id=1").fetchone()[0]


def _readings_fingerprint(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT minute_ts, reading_wh FROM readings ORDER BY minute_ts"
    ).fetchall()
    h = hashlib.sha256()
    for r in rows:
        h.update(r["minute_ts"].encode("utf-8"))
        h.update(b"=")
        h.update(str(r["reading_wh"]).encode("ascii"))
        h.update(b";")
    return h.hexdigest()


def _config_fingerprint(conn: sqlite3.Connection) -> str:
    cfg = get_config(conn)
    raw = (str(cfg["per_minute_cap"]), str(cfg["peak_wh"]),
           str(cfg["reported_window"]))
    return hashlib.sha256("|".join(raw).encode("utf-8")).hexdigest()


def _guard(conn: sqlite3.Connection, epoch: int,
           readings_fp: Optional[str] = None,
           config_fp: Optional[str] = None) -> None:
    """写事务内复核：epoch 与求解依据仍一致，否则整笔放弃。"""
    if get_epoch(conn) != epoch:
        raise EpochConflict("账期已被清空重建，本次改动不生效")
    if readings_fp is not None and _readings_fingerprint(conn) != readings_fp:
        raise BasisConflict("读数已被其它提交改变，按最新状态重算")
    if config_fp is not None and _config_fingerprint(conn) != config_fp:
        raise BasisConflict("核对参数已被其它提交改变，按最新状态重算")


# --------------------------------------------------------------------------- #
# CSV 解析
# --------------------------------------------------------------------------- #
def parse_csv(content: bytes) -> Dict[datetime, int]:
    """解析整分钟累计读数 CSV。同文件内同刻取最后一条。"""
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadError(f"CSV 必须为 UTF-8 编码：{exc}") from exc
    reader = csv.reader(io.StringIO(text))
    rows: List[List[str]] = [r for r in reader if r and any(c.strip() for c in r)]
    if not rows:
        raise UploadError("CSV 为空")

    header = [c.strip().lower() for c in rows[0]]
    data_rows = rows[1:]
    if header[:2] not in (["timestamp", "reading"],
                          ["time", "reading"],
                          ["minute_ts", "reading_wh"]):
        # 无表头容错：首行也当数据
        if _parse_int(rows[0][1]) is None:
            raise UploadError("CSV 表头应为 timestamp,reading")
        data_rows = rows

    parsed: Dict[datetime, int] = {}
    for lineno, row in enumerate(data_rows, start=2):
        if len(row) < 2:
            raise UploadError(f"第 {lineno} 行列数不足，应为 timestamp,reading")
        ts = _parse_ts(row[0].strip(), lineno)
        val = _parse_int(row[1].strip())
        if val is None or val < 0:
            raise UploadError(f"第 {lineno} 行读数必须是非负整数 Wh")
        parsed[ts] = val  # 同刻覆盖
    if not parsed:
        raise UploadError("CSV 没有有效数据行")
    return parsed


def _parse_int(s: str) -> Optional[int]:
    try:
        if s.strip() != str(int(s.strip())):
            return None
        return int(s.strip())
    except (ValueError, TypeError):
        return None


def _parse_ts(s: str, lineno: int) -> datetime:
    for fmt in TS_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    else:
        raise UploadError(
            f"第 {lineno} 行时间 {s!r} 无法解析，格式示例 2026-11-01 08:00")
    if dt.second != 0 or dt.microsecond != 0:
        raise UploadError(f"第 {lineno} 行时间必须是整分钟")
    return dt


# --------------------------------------------------------------------------- #
# 读数合并：先纯计算出方案，提交在短写事务内完成
# --------------------------------------------------------------------------- #
def plan_reading_merge(conn: sqlite3.Connection,
                       incoming: Dict[datetime, int]) -> ReadingPlan:
    """纯计算合并方案并做全部校验，不写库。

    合并后若回卷、结构非法或超过现行每分钟上限导致实读无解，
    抛 UploadError，库内数据与结论不变。
    """
    # 全部依据在同一只读快照内取齐，保证指纹与被校验数据严格对应
    with snapshot(conn):
        epoch = get_epoch(conn)
        basis = _readings_fingerprint(conn)
        existing = {datetime.strptime(r["minute_ts"], TS_FMT):
                    r["reading_wh"]
                    for r in conn.execute(
                        "SELECT minute_ts, reading_wh FROM readings")}
        cap = conn.execute(
            "SELECT per_minute_cap FROM config").fetchone()["per_minute_cap"]
    overwritten = sum(1 for ts in incoming if ts in existing)
    merged = dict(existing)
    merged.update(incoming)
    ordered = sorted(merged.items())

    # 单调性冲突：指出冲突的相邻读数（用绝对时间戳，先于偏移量校验）
    for (t1, v1), (t2, v2) in zip(ordered, ordered[1:]):
        if v2 < v1:
            raise UploadError(
                "电表不回卷，发现冲突的相邻读数："
                f"{t1:%Y-%m-%d %H:%M} = {v1} Wh -> "
                f"{t2:%Y-%m-%d %H:%M} = {v2} Wh，整批拒绝")

    # 结构校验（与参数无关）
    first_ts, last_ts = ordered[0][0], ordered[-1][0]
    if (first_ts.minute % solver.WINDOW_MINUTES
            or last_ts.minute % solver.WINDOW_MINUTES):
        raise UploadError(
            "账期两端必须对齐 15 分钟边界："
            f"首读数 {first_ts:%Y-%m-%d %H:%M}、末读数 "
            f"{last_ts:%Y-%m-%d %H:%M}，整批拒绝")
    offsets = _to_offsets(ordered)
    try:
        solver.validate_period(offsets)
    except solver.SolverError as exc:
        raise UploadError(str(exc)) from exc

    # 若已配置每分钟上限，合并后还要满足段可行（现行参数）
    if cap:
        try:
            solver.build_pieces(offsets, cap,
                                (offsets[-1][0] - offsets[0][0])
                                // solver.WINDOW_MINUTES)
        except solver.SolverError as exc:
            raise UploadError(f"合并后实读无解：{exc}，整批拒绝") from exc

    return ReadingPlan(ordered=ordered, total=len(ordered),
                       overwritten=overwritten,
                       incoming_count=len(incoming),
                       epoch=epoch, basis_fingerprint=basis)


def commit_reading_merge(conn: sqlite3.Connection,
                         plan: ReadingPlan) -> None:
    """短事务落库读数；epoch/依据变化则整笔放弃。"""
    with write_tx(conn):
        _guard(conn, plan.epoch, readings_fp=plan.basis_fingerprint)
        conn.execute("DELETE FROM readings")
        conn.executemany(
            "INSERT INTO readings(minute_ts, reading_wh) VALUES(?, ?)",
            [(ts.strftime(TS_FMT), v) for ts, v in plan.ordered])


def merge_upload(conn: sqlite3.Connection,
                 incoming: Dict[datetime, int]) -> Tuple[int, int]:
    """向后兼容的同步封装：规划 + 提交，返回 (合并后条数, 覆盖条数)。"""
    plan = plan_reading_merge(conn, incoming)
    commit_reading_merge(conn, plan)
    return plan.total, plan.overwritten


def _to_offsets(ordered: List[Tuple[datetime, int]]
                ) -> List[Tuple[int, int]]:
    t0 = ordered[0][0]
    return [(int((ts - t0).total_seconds()) // 60, v)
            for ts, v in ordered]


def load_readings(conn: sqlite3.Connection
                  ) -> List[Tuple[datetime, int]]:
    rows = conn.execute(
        "SELECT minute_ts, reading_wh FROM readings ORDER BY minute_ts"
    ).fetchall()
    return [(datetime.strptime(r["minute_ts"], TS_FMT),
             r["reading_wh"]) for r in rows]


# --------------------------------------------------------------------------- #
# 配置与结论
# --------------------------------------------------------------------------- #
def save_config(conn: sqlite3.Connection, cap: Optional[int],
                peak: Optional[int], window_ts: Optional[str]) -> None:
    with conn:
        conn.execute(
            "UPDATE config SET per_minute_cap=?, peak_wh=?, "
            "reported_window=? WHERE id=1", (cap, peak, window_ts))


def get_config(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM config WHERE id=1").fetchone()


def latest_audit(conn: sqlite3.Connection, kind: str
                 ) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM audits WHERE kind=? ORDER BY id DESC LIMIT 1",
        (kind,)).fetchone()


def has_initial(conn: sqlite3.Connection) -> bool:
    return latest_audit(conn, "initial") is not None


def add_audit(conn: sqlite3.Connection, kind: str, status: str,
              message: str, detail_json: str) -> None:
    with conn:
        conn.execute(
            "INSERT INTO audits(kind, status, message, detail_json, "
            "created_ts) VALUES(?, ?, ?, ?, ?)",
            (kind, status, message, detail_json,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def commit_audit(conn: sqlite3.Connection, plan: AuditPlan) -> None:
    """短事务：参数与结论同生共死；epoch/依据变化整笔回滚。

    账单核不过（target_unreachable / others_breach）同样落新结论。
    initial 只在本账期首次成功核对时写一次，补传重算只动 current。
    """
    import json
    from dataclasses import asdict

    detail = json.dumps(asdict(plan.result), ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with write_tx(conn):
        _guard(conn, plan.epoch,
               readings_fp=plan.readings_fingerprint,
               config_fp=plan.config_fingerprint)
        conn.execute(
            "UPDATE config SET per_minute_cap=?, peak_wh=?, "
            "reported_window=? WHERE id=1",
            (plan.cap, plan.peak, plan.win_text))
        conn.execute(
            "INSERT INTO audits(kind, status, message, detail_json, "
            "created_ts) VALUES('current', ?, ?, ?, ?)",
            (plan.result.status, plan.result.message, detail, now))
        if plan.is_first:
            conn.execute(
                "INSERT INTO audits(kind, status, message, detail_json, "
                "created_ts) VALUES('initial', ?, ?, ?, ?)",
                (plan.result.status, plan.result.message, detail, now))


def reset_all(conn: sqlite3.Connection) -> None:
    """清空账期并 bump epoch：所有在途改动的旧 epoch 即刻失效。"""
    with write_tx(conn):
        conn.execute("DELETE FROM readings")
        conn.execute("DELETE FROM audits")
        conn.execute("UPDATE config SET per_minute_cap=NULL, "
                     "peak_wh=NULL, reported_window=NULL WHERE id=1")
        conn.execute("UPDATE meta SET epoch = epoch + 1 WHERE id=1")
