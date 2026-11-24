"""补传 × 手工核对交错提交的专项自测。

覆盖：
- 用户给出的 00:00/00:15/00:30 场景：补传后自动按现行参数重算，网页/CSV 同步；
- 补传与核对的原子性：读数冲突、参数无效、求解失败、写入失败均整体不生效；
- 交错提交：两批补传、补传与改参数各自有效且合并有效时都生效；依据失效则拒绝；
- 并发：求解进行中可查看/下载/清空；查看与下载只用同一次提交的快照；
- 清空调期（epoch）后，在途补传/核对不得回写，即使读数完全相同。
"""
from __future__ import annotations

import io
import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime
from unittest import mock

from app import service, solver, storage


def csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


class RecomputeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "r.db")
        self.conn = storage.get_db(self.db)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _upload(self, text):
        return service.submit_readings(
            self.conn, storage.parse_csv(csv_bytes(text)))

    def test_report_scenario_supplement_recomputes_current_conclusion(self):
        # 00:00=0、00:15=30、00:30=60，上限 10Wh/min，核对 00:00 申报 30Wh
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        r = service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        self.assertEqual(r.status, "feasible")
        initial_id = storage.latest_audit(self.conn, "initial")["id"]
        first_current = storage.latest_audit(self.conn, "current")
        self.assertEqual(
            service.load_result(first_current).window_totals, [30, 30])

        # 补传 00:15=40：两段变为 40 / 20，窗总量 [40, 20]，报窗 0 无法达到 30
        out = self._upload(
            "timestamp,reading\n2026-11-01 00:15,40\n")
        self.assertTrue(out.recomputed)
        self.assertEqual(out.result.status, "target_unreachable")
        # 读数、当前结论一并生效，初核保留
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 40)
        current = storage.latest_audit(self.conn, "current")
        self.assertNotEqual(current["id"], first_current["id"])
        self.assertEqual(
            storage.latest_audit(self.conn, "initial")["id"], initial_id)
        self.assertEqual(
            service.load_result(current).status, "target_unreachable")

        # 网页视图与 CSV 都基于新结论（target_unreachable 无窗电量可导出）
        view = service.dashboard(self.conn)
        self.assertEqual(view["current"]["id"], current["id"])
        self.assertIsNone(service.windows_csv_rows(self.conn))

        # 再补传回 30，当前结论应恢复 feasible 且 CSV 同步
        self._upload("timestamp,reading\n2026-11-01 00:15,30\n")
        view = service.dashboard(self.conn)
        self.assertEqual(view["current_result"].status, "feasible")
        csv_rows = service.windows_csv_rows(self.conn)
        self.assertEqual([r[1] for r in csv_rows], [30, 30])

    def test_failed_bill_check_still_persists_new_current(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,90\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        # 补传后总量结构使申报 30 在窗 0 不可达 → 核不过也保存新结论
        out = self._upload(
            "timestamp,reading\n2026-11-01 00:15,40\n")  # [40, 50]
        self.assertEqual(out.result.status, "target_unreachable")
        self.assertEqual(
            storage.latest_audit(self.conn, "current")["status"],
            "target_unreachable")

    def test_supplement_without_params_skips_recompute(self):
        out = self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n")
        self.assertFalse(out.recomputed)
        self.assertIsNone(storage.latest_audit(self.conn, "current"))
        # 之后手工核对可正常产生 initial/current
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        self.assertIsNotNone(storage.latest_audit(self.conn, "initial"))

    def test_reading_conflict_makes_whole_change_ineffective(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,100\n"
            "2026-11-01 00:15,130\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        before_rows = storage.load_readings(self.conn)
        before_current = storage.latest_audit(self.conn, "current")["id"]
        with self.assertRaises(storage.UploadError):
            self._upload(  # 00:30=120 相对 00:15 的 130 回卷
                "timestamp,reading\n2026-11-01 00:30,120\n")
        self.assertEqual(storage.load_readings(self.conn), before_rows)
        self.assertEqual(
            storage.latest_audit(self.conn, "current")["id"], before_current)

    def test_cap_infeasible_supplement_rejected_and_conclusion_kept(self):
        storage.save_config(self.conn, 5, None, None)
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n")
        storage.save_config(self.conn, 10, 30, "2026-11-01 00:00")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        before = storage.latest_audit(self.conn, "current")["id"]
        # 00:30 段 60 分钟内要增 200Wh，超上限 10 → 无解，读数与结论都不动
        with self.assertRaises(storage.UploadError):
            self._upload(
                "timestamp,reading\n2026-11-01 00:30,230\n")
        self.assertEqual(
            storage.latest_audit(self.conn, "current")["id"], before)
        self.assertEqual(len(storage.load_readings(self.conn)), 2)

    def test_invalid_audit_params_change_nothing(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n")
        with self.assertRaises(service.AuditInputError):
            service.run_audit(self.conn, "0", "30", "2026-11-01 00:00")
        with self.assertRaises(service.AuditInputError):
            service.run_audit(self.conn, "10", "-1", "2026-11-01 00:00")
        with self.assertRaises(service.AuditInputError):
            service.run_audit(self.conn, "10", "30", "not-a-time")
        cfg = storage.get_config(self.conn)
        self.assertIsNone(cfg["per_minute_cap"])
        self.assertIsNone(storage.latest_audit(self.conn, "current"))

    def test_write_failure_rolls_back_readings_and_conclusion(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        before_rows = storage.load_readings(self.conn)
        before_current = storage.latest_audit(self.conn, "current")["id"]

        # 模拟写入失败：给写事务内会碰到的表加触发器，INSERT audits 即报错
        self.conn.execute(
            "CREATE TRIGGER boom BEFORE INSERT ON audits "
            "BEGIN SELECT RAISE(ABORT, 'injected write failure'); END")
        with self.assertRaises(sqlite3.Error) as ctx:
            self._upload(
                "timestamp,reading\n2026-11-01 00:15,40\n")
        self.assertIn("injected write failure", str(ctx.exception))
        # 读数与结论都回滚到提交前
        self.assertEqual(storage.load_readings(self.conn), before_rows)
        self.assertEqual(
            storage.latest_audit(self.conn, "current")["id"], before_current)


class InterleaveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "i.db")
        self.conn = storage.get_db(self.db)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _upload(self, text, conn=None):
        conn = conn or self.conn
        return service.submit_readings(
            conn, storage.parse_csv(csv_bytes(text)))

    def test_two_valid_supplement_batches_both_take_effect(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:30,60\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        # 第一批补 00:15=30：窗总量 [30,30]，申报仍可行
        self._upload("timestamp,reading\n2026-11-01 00:15,30\n")
        # 第二批再补 00:45（延长账期），各自有效、合并后仍满足上限
        self._upload(
            "timestamp,reading\n2026-11-01 00:45,90\n")
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 30)
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 45)], 90)
        # 每次补传都按现行参数重算，当前结论对应最新读数
        current = service.load_result(
            storage.latest_audit(self.conn, "current"))
        self.assertEqual(current.status, "feasible")
        self.assertEqual(current.window_totals, [30, 30, 30])

    def test_supplement_and_param_change_interleave_both_effective(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")
        # 改参数核对（同一线程顺序请求，模拟交错到达）
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:15")
        cfg = storage.get_config(self.conn)
        self.assertEqual(cfg["reported_window"], "2026-11-01 00:15")
        # 随后补传：必须按新参数（报窗 1）重算
        out = self._upload(
            "timestamp,reading\n2026-11-01 00:15,40\n")  # [40,20]
        self.assertTrue(out.recomputed)
        self.assertEqual(out.result.target_window, 1)
        self.assertEqual(out.result.status, "target_unreachable")
        cfg = storage.get_config(self.conn)
        self.assertEqual(cfg["reported_window"], "2026-11-01 00:15")
        self.assertEqual(cfg["peak_wh"], 30)

    def _seed(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")

    def test_stale_plan_rejected_when_basis_changed_underneath(self):
        self._seed()
        # 底层短事务接口：依据变化时给出 BasisConflict，由 service 层决定重试
        plan_a = storage.plan_reading_merge(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:15,33\n")))
        conn_b = storage.get_db(self.db)
        try:
            storage.merge_upload(
                conn_b, storage.parse_csv(csv_bytes(
                    "timestamp,reading\n2026-11-01 00:15,35\n")))
        finally:
            conn_b.close()
        with self.assertRaises(storage.BasisConflict):
            storage.commit_reading_merge(self.conn, plan_a)
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 35)

    def test_high_level_supplement_retries_and_both_changes_effective(self):
        self._seed()
        # 高层接口在依据变化时带最新数据重算：交错的两笔有效补传都生效
        plan_a = storage.plan_reading_merge(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:15,33\n")))
        conn_b = storage.get_db(self.db)
        try:
            storage.merge_upload(
                conn_b, storage.parse_csv(csv_bytes(
                    "timestamp,reading\n2026-11-01 00:45,90\n")))
        finally:
            conn_b.close()
        # A 原方案已过期，直接提交会冲突；高层 submit 重新规划后成功
        with self.assertRaises(storage.BasisConflict):
            storage.commit_reading_merge(self.conn, plan_a)
        out = self._upload(
            "timestamp,reading\n2026-11-01 00:15,33\n")
        self.assertTrue(out.recomputed)
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 33)
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 45)], 90)

    def test_retry_still_rejects_when_merged_result_becomes_invalid(self):
        self._seed()
        # B 的提交单独有效；A(00:45=80) 与 B 合并后相对 00:30=120 回卷 → 拒绝
        plan_a = storage.plan_reading_merge(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:45,80\n")))
        conn_b = storage.get_db(self.db)
        try:
            storage.merge_upload(
                conn_b, storage.parse_csv(csv_bytes(
                    "timestamp,reading\n2026-11-01 00:30,120\n")))
        finally:
            conn_b.close()
        with self.assertRaises(storage.BasisConflict):
            storage.commit_reading_merge(self.conn, plan_a)
        # 高层重试：合并后 00:30=120 -> 00:45=80 回卷，A 整笔拒绝
        with self.assertRaises(storage.UploadError):
            self._upload(
                "timestamp,reading\n2026-11-01 00:45,80\n")
        rows = dict(storage.load_readings(self.conn))
        self.assertNotIn(datetime(2026, 11, 1, 0, 45), rows)
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 30)], 120)

    def test_stale_audit_rejected_after_interleaved_supplement(self):
        self._upload(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")

        # 手工构造一份基于旧读数的 AuditPlan
        with storage.snapshot(self.conn):
            epoch = storage.get_epoch(self.conn)
            rfp = storage._readings_fingerprint(self.conn)
            cfp = storage._config_fingerprint(self.conn)
        stale = storage.AuditPlan(
            cap=10, peak=30, win_text="2026-11-01 00:00",
            result=solver.AuditResult(
                status="feasible", target_window=0, target_min=30,
                target_max=30, window_totals=[30, 30],
                breach_windows=[], min_overflow=0, message="stale"),
            epoch=epoch, readings_fingerprint=rfp,
            config_fingerprint=cfp, is_first=False)
        # 交错补传先落库
        self._upload(
            "timestamp,reading\n2026-11-01 00:15,40\n")
        with self.assertRaises(storage.BasisConflict):
            storage.commit_audit(self.conn, stale)

    def test_high_level_audit_retries_after_interleaved_supplement(self):
        self._seed()
        # 构造交错：先拿到旧依据的方案，再让一笔补传落库
        with storage.snapshot(self.conn):
            epoch = storage.get_epoch(self.conn)
            rfp = storage._readings_fingerprint(self.conn)
            cfp = storage._config_fingerprint(self.conn)
        stale = storage.AuditPlan(
            cap=10, peak=30, win_text="2026-11-01 00:00",
            result=solver.AuditResult(
                status="feasible", target_window=0, target_min=30,
                target_max=30, window_totals=[30, 30],
                breach_windows=[], min_overflow=0, message="stale"),
            epoch=epoch, readings_fingerprint=rfp,
            config_fingerprint=cfp, is_first=False)
        self._upload(
            "timestamp,reading\n2026-11-01 00:15,40\n")
        with self.assertRaises(storage.BasisConflict):
            storage.commit_audit(self.conn, stale)
        # 高层 run_audit 基于新读数重试：同参数下结论变为不可达，改动生效
        result = service.run_audit(
            self.conn, "10", "30", "2026-11-01 00:00")
        self.assertEqual(result.status, "target_unreachable")


class ConcurrencyTests(unittest.TestCase):
    """用真线程 + 两个连接模拟 gunicorn 多 worker 下的交错请求。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "c.db")
        self.conn = storage.get_db(self.db)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _seed(self):
        service.submit_readings(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:00,0\n"
                "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")))
        service.run_audit(self.conn, "10", "30", "2026-11-01 00:00")

    def _patch_slow_solver(self, entered, release):
        """让求解在计算前阻塞，模拟长求解（不持任何写锁）。"""
        original = service._run_solver

        def slow(readings, upper, peak, target_window):
            entered.set()
            release.wait(10)
            return original(readings, upper, peak, target_window)

        return mock.patch.object(service, "_run_solver", slow), original

    def test_reads_and_reset_work_while_solve_in_progress(self):
        self._seed()
        entered = threading.Event()
        release = threading.Event()
        patcher, _ = self._patch_slow_solver(entered, release)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(release.set)

        errors = []

        def long_audit():
            c = storage.get_db(self.db)
            try:
                service.run_audit(c, "10", "30", "2026-11-01 00:15")
            except Exception as exc:  # 预期可能因 reset 产生 ConflictError
                errors.append(exc)
            finally:
                c.close()

        t = threading.Thread(target=long_audit)
        t.start()
        self.assertTrue(entered.wait(5), "求解未按预期进入")

        # 求解中：网页与 CSV 立即可读，且数据自洽
        view = service.dashboard(storage.get_db(self.db))
        self.assertEqual(view["current_result"].window_totals, [30, 30])
        csv_rows = service.windows_csv_rows(storage.get_db(self.db))
        self.assertEqual([r[1] for r in csv_rows], [30, 30])
        # 求解中：清空账期立即完成
        rc = storage.get_db(self.db)
        storage.reset_all(rc)
        rc.close()
        self.assertEqual(storage.load_readings(
            storage.get_db(self.db)), [])

        release.set()
        t.join(10)
        self.assertFalse(t.is_alive())
        # 在途核对的旧 epoch 失效，不允许回写新账期
        self.assertIsNone(storage.latest_audit(
            storage.get_db(self.db), "current"))
        self.assertTrue(
            all(isinstance(e, storage.EpochConflict) for e in errors))

    def test_in_flight_audit_retries_after_interleaved_supplement(self):
        self._seed()
        entered = threading.Event()
        release = threading.Event()
        calls = {"n": 0}
        original = service._run_solver

        def slow(readings, upper, peak, target_window):
            calls["n"] += 1
            if calls["n"] == 1:
                entered.set()
                release.wait(10)
            return original(readings, upper, peak, target_window)

        patcher = mock.patch.object(service, "_run_solver", slow)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(release.set)

        result_box = []

        def long_audit():
            c = storage.get_db(self.db)
            try:
                result_box.append(service.run_audit(
                    c, "10", "30", "2026-11-01 00:00"))
            except Exception as exc:
                result_box.append(exc)
            finally:
                c.close()

        t = threading.Thread(target=long_audit)
        t.start()
        self.assertTrue(entered.wait(5))
        # 求解进行中，另一请求补传 00:15=40（依据改变，但写事务不被阻塞）
        sc = storage.get_db(self.db)
        service.submit_readings(
            sc, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:15,40\n")))
        sc.close()
        release.set()
        t.join(15)
        self.assertFalse(t.is_alive())

        # 在途核对带新读数重试成功，结论与补传后的数据一致，两次改动都生效
        self.assertEqual(len(result_box), 1)
        result = result_box[0]
        self.assertIsInstance(result, solver.AuditResult)
        self.assertEqual(result.status, "target_unreachable")
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 40)
        self.assertGreaterEqual(calls["n"], 2)  # 至少重算过一次

    def test_snapshot_reads_never_mix_two_commits(self):
        self._seed()
        # 单快照视图里读数条数与结论窗数始终配套
        view = service.dashboard(self.conn)
        self.assertEqual(len(view["readings"]), 3)
        self.assertEqual(len(view["current_result"].window_totals), 2)
        # 补传提交后，新视图整体来自新提交
        service.submit_readings(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:45,90\n")))
        view2 = service.dashboard(self.conn)
        self.assertEqual(len(view2["readings"]), 4)
        self.assertEqual(len(view2["current_result"].window_totals), 3)
        # 旧视图对象不随新提交变化（同一次请求的数据定格）
        self.assertEqual(len(view["readings"]), 3)

    def test_in_flight_supplement_after_reset_does_not_write_back(self):
        self._seed()
        # 规划一笔与新账期读数完全相同的补传
        plan = storage.plan_reading_merge(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:15,40\n")))
        storage.reset_all(self.conn)
        # 新账期录入"相同的读数"
        service.submit_readings(
            self.conn, storage.parse_csv(csv_bytes(
                "timestamp,reading\n2026-11-01 00:00,0\n"
                "2026-11-01 00:15,40\n2026-11-01 00:30,60\n")))
        before = storage.load_readings(self.conn)
        with self.assertRaises(storage.ConflictError):
            storage.commit_reading_merge(self.conn, plan)
        # 新账期数据原样保留，旧在途提交没有回写
        self.assertEqual(storage.load_readings(self.conn), before)
        self.assertEqual(storage.get_epoch(self.conn), 1)

    def test_parallel_supplements_both_effective_via_retry(self):
        self._seed()
        barrier = threading.Barrier(2, timeout=10)
        outcomes = []

        def worker(payload):
            c = storage.get_db(self.db)
            try:
                barrier.wait()
                outcomes.append(service.submit_readings(
                    c, storage.parse_csv(csv_bytes(payload))))
            except storage.ConflictError as exc:
                outcomes.append(exc)
            finally:
                c.close()

        # 两笔不同刻的有效补传：依据互撞时乐观重试，合并后仍有效 → 都生效
        t1 = threading.Thread(target=worker, args=(
            "timestamp,reading\n2026-11-01 00:15,28\n",))
        t2 = threading.Thread(target=worker, args=(
            "timestamp,reading\n2026-11-01 00:45,90\n",))
        t1.start(); t2.start()
        t1.join(15); t2.join(15)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        self.assertEqual(len(outcomes), 2)
        self.assertTrue(
            all(isinstance(o, service.UploadOutcome) for o in outcomes),
            f"两笔有效补传都应生效，实际：{outcomes}")
        rows = dict(storage.load_readings(self.conn))
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 15)], 28)
        self.assertEqual(rows[datetime(2026, 11, 1, 0, 45)], 90)
        # 两次重算结论都已落库（current 至少推进两次）
        self.assertGreaterEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM audits WHERE kind='current'"
            ).fetchone()[0], 2)

    def test_parallel_same_minute_writes_serialize_without_dirty_state(self):
        self._seed()
        barrier = threading.Barrier(2, timeout=10)

        def worker(payload):
            c = storage.get_db(self.db)
            try:
                barrier.wait()
                service.submit_readings(
                    c, storage.parse_csv(csv_bytes(payload)))
            finally:
                c.close()

        t1 = threading.Thread(target=worker, args=(
            "timestamp,reading\n2026-11-01 00:15,28\n",))
        t2 = threading.Thread(target=worker, args=(
            "timestamp,reading\n2026-11-01 00:15,29\n",))
        t1.start(); t2.start()
        t1.join(15); t2.join(15)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        # 同刻覆盖最终恰为其中一个值，读数仍单调、无脏写/半写
        rows = dict(storage.load_readings(self.conn))
        self.assertIn(rows[datetime(2026, 11, 1, 0, 15)], (28, 29))
        vals = [v for _, v in storage.load_readings(self.conn)]
        self.assertEqual(vals, sorted(vals))


class HttpConcurrencyTests(unittest.TestCase):
    """Flask 端到端：补传后页面与 CSV 立即反映重算结论。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "w.db")
        from app import create_app
        self.app = create_app(self.db)
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def _post_csv(self, text):
        return self.client.post(
            "/upload",
            data={"csv": (io.BytesIO(csv_bytes(text)), "r.csv")},
            content_type="multipart/form-data")

    def test_http_recompute_and_snapshot_views(self):
        self._post_csv(
            "timestamp,reading\n2026-11-01 00:00,0\n"
            "2026-11-01 00:15,30\n2026-11-01 00:30,60\n")
        self.client.post("/audit", data={
            "per_minute_cap": "10", "peak_wh": "30",
            "reported_window": "2026-11-01 00:00"})
        page = self.client.get("/").data.decode()
        self.assertIn("核对通过", page)

        # 补传 40 → 自动重算为 target_unreachable，页面同步换结论
        self._post_csv(
            "timestamp,reading\n2026-11-01 00:15,40\n")
        page = self.client.get("/").data.decode()
        self.assertIn("所报时段达不到峰值", page)
        self.assertNotIn("导出 windows.csv", page)  # 无窗电量，不提供旧 CSV

        # 补传回 30 → 恢复 feasible，CSV 与页面同属新提交
        self._post_csv(
            "timestamp,reading\n2026-11-01 00:15,30\n")
        page = self.client.get("/").data.decode()
        self.assertIn("核对通过", page)
        csv_out = self.client.get("/windows.csv").data.decode()
        self.assertIn("2026-11-01 00:00,0,30,1", csv_out)
        self.assertIn("2026-11-01 00:15,1,30,0", csv_out)

        # reset 后 epoch 变化，在途方案无法回写
        self.client.post("/reset")
        self.assertEqual(storage.load_readings(
            storage.get_db(self.db)), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
