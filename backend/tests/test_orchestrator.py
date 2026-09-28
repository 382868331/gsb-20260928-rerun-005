import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import DB
from app.engine import Engine
from app.default_flow import DEFAULT_FLOW


def make_engine(path):
    db = DB(path)
    engine = Engine(db)
    db.upsert_workflow(DEFAULT_FLOW["id"], DEFAULT_FLOW["name"],
                       DEFAULT_FLOW["definition"])
    return engine, db


class OrchestratorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine, self.db = make_engine(
            os.path.join(self.tmp.name, "t.db"))
        self.wf = self.db.get_workflow(DEFAULT_FLOW["id"])

    def tearDown(self):
        for rid, execution in list(self.engine.runs.items()):
            execution.cancel_flag = True
        time.sleep(1.0)
        self.tmp.cleanup()

    def _wait_terminal(self, run_id, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            run = self.db.get_run(run_id)
            if run["status"] in ("SUCCEEDED", "FAILED", "CANCELLED",
                                  "ERROR"):
                return run
            time.sleep(0.05)
        self.fail("run did not finish")

    def test_obstacle_wait_then_cancel_keeps_record_consistent(self):
        # 障碍点 (2,0)：MOVE 到 (4,0) 途中在此失败
        self.engine.robot.add_obstacle(2, 0)
        run_id = self.engine.start_run(self.wf)

        # 等待走到 wait_clear（MOVE 失败 -> failure 分支）
        deadline = time.time() + 15
        while time.time() < deadline:
            nodes = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            wn = nodes.get("wait_clear")
            if wn and wn["status"] == "WAITING":
                break
            time.sleep(0.05)
        else:
            self.fail("never reached WAITING state")

        # 记录显示机器人实际停在障碍点，且没有越过后继
        self.assertEqual(self.engine.robot.pos, [2, 0])
        move_record = next(n for n in self.db.get_nodes(run_id)
                           if n["node_id"] == "move_rack")
        self.assertEqual(move_record["status"], "FAILED")
        self.assertIn("obstacle", move_record["detail"])

        # 撤销：等待节点尚未执行任何动作，直接取消，后续不启动
        self.assertTrue(self.engine.cancel_run(run_id))
        run = self._wait_terminal(run_id)
        self.assertEqual(run["status"], "CANCELLED")
        node_ids = {n["node_id"] for n in self.db.get_nodes(run_id)}
        self.assertNotIn("retry_move", node_ids)
        self.assertNotIn("pick", node_ids)
        # 实际位置与执行记录一致
        self.assertEqual(self.engine.robot.pos, [2, 0])
        events = self.db.events_after(run_id, 0)
        self.assertTrue(any(e["kind"] == "branch" and
                            e["payload"].get("branch") == "failure"
                            for e in events))

    def test_cancel_during_move_finishes_tick_but_advances_nowhere(self):
        run_id = self.engine.start_run(self.wf)
        # 在长距离移动途中立即取消
        deadline = time.time() + 5
        while time.time() < deadline:
            nodes = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            mr = nodes.get("move_rack")
            if mr and mr["status"] == "RUNNING":
                break
            time.sleep(0.02)
        self.engine.cancel_run(run_id)
        run = self._wait_terminal(run_id)
        self.assertEqual(run["status"], "CANCELLED")
        node_ids = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
        self.assertNotIn("pick", node_ids)
        self.assertNotIn("wait_clear", node_ids)
        self.assertIn(self.engine.robot.pos[0], (0, 1, 2, 3, 4))
        self.assertEqual(self.engine.robot.pos[1], 0)

    def test_pause_and_resume_continues_same_state(self):
        run_id = self.engine.start_run(self.wf)
        time.sleep(0.6)
        self.assertTrue(self.engine.pause_run(run_id))
        self.assertEqual(self.db.get_run(run_id)["status"], "PAUSED")
        pos_when_paused = list(self.engine.robot.pos)
        time.sleep(0.8)
        self.assertEqual(self.engine.robot.pos, pos_when_paused)
        self.assertTrue(self.engine.resume_run(run_id))
        run = self._wait_terminal(run_id, timeout=30)
        self.assertEqual(run["status"], "SUCCEEDED")
        self.assertEqual(self.engine.robot.pos, [0, 0])
        self.assertFalse(self.engine.robot.carrying)

    def test_condition_branches_to_low_battery_path(self):
        run_id = self.engine.start_run(self.wf)
        # 在条件判断前把电量压低，应走 false 分支去补电
        deadline = time.time() + 10
        while time.time() < deadline:
            nodes = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            if nodes.get("pick", {}).get("status") in (
                    "RUNNING", "SUCCEEDED"):
                self.engine.robot.battery = 10
                break
            time.sleep(0.02)
        run = self._wait_terminal(run_id, timeout=30)
        self.assertEqual(run["status"], "SUCCEEDED")
        nodes = {n["node_id"]: n["status"]
                 for n in self.db.get_nodes(run_id)}
        self.assertEqual(nodes["cond_battery"], "SUCCEEDED")
        self.assertEqual(nodes["move_charge"], "SUCCEEDED")
        cond_detail = next(n["detail"] for n in self.db.get_nodes(run_id)
                           if n["node_id"] == "cond_battery")
        self.assertIn("false", cond_detail)

    def test_signal_releases_wait_node(self):
        self.engine.robot.add_obstacle(2, 0)
        run_id = self.engine.start_run(self.wf)
        deadline = time.time() + 15
        while time.time() < deadline:
            nodes = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            if nodes.get("wait_clear", {}).get("status") == "WAITING":
                break
            time.sleep(0.05)
        else:
            self.fail("did not enter wait")
        self.engine.robot.clear_obstacle(2, 0)
        self.assertTrue(self.engine.send_signal(
            run_id, "obstacle_cleared", True))
        run = self._wait_terminal(run_id, timeout=30)
        self.assertEqual(run["status"], "SUCCEEDED")
        self.assertEqual(self.engine.robot.pos, [0, 0])

    def test_persistence_survives_db_reopen(self):
        self.engine.robot.add_obstacle(2, 0)
        run_id = self.engine.start_run(self.wf)
        deadline = time.time() + 15
        while time.time() < deadline:
            nodes = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            if nodes.get("wait_clear", {}).get("status") == "WAITING":
                break
            time.sleep(0.05)
        # 用新连接重新打开同一个数据库文件，状态必须完整保留
        dbfile = self.db.query("PRAGMA database_list")[0]["file"]
        fresh = DB(dbfile)
        run = fresh.get_run(run_id)
        self.assertIn(run["status"], ("RUNNING", "PAUSED"))
        records = fresh.get_nodes(run_id)
        self.assertTrue(any(r["status"] == "FAILED" and
                            r["node_id"] == "move_rack" for r in records))
        self.assertTrue(any(r["status"] == "WAITING" and
                            r["node_id"] == "wait_clear" for r in records))
        self.engine.cancel_run(run_id)
        self._wait_terminal(run_id)

    def test_acceptance_scenario_record_matches_physical_state(self):
        """验收脚本：取物途中遇障碍 -> 走向等待 -> 撤销任务，
        执行记录中的最后状态必须与机器人物理状态完全一致。"""
        # 1. 在必经之路 (2,0) 放障碍并启动
        self.engine.robot.add_obstacle(2, 0)
        run_id = self.engine.start_run(self.wf)

        # 2. 等到流程停在“等待障碍清除”
        deadline = time.time() + 15
        while time.time() < deadline:
            recs = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
            if recs.get("wait_clear", {}).get("status") == "WAITING":
                break
            time.sleep(0.05)
        else:
            self.fail("流程未进入等待")

        # 3. 用户撤销任务（等待节点没有在执行动作，立即取消）
        self.assertTrue(self.engine.cancel_run(run_id))
        run = self._wait_terminal(run_id)

        # 4. 一致性断言
        self.assertEqual(run["status"], "CANCELLED")
        recs = {n["node_id"]: n for n in self.db.get_nodes(run_id)}
        self.assertEqual(recs["move_rack"]["status"], "FAILED")
        self.assertEqual(recs["wait_clear"]["status"], "CANCELLED")
        touched = set(recs)
        # 失败后的后继（重试/取货等）绝不能偷偷执行
        for forbidden in ("retry_move", "pick", "cond_battery", "move_home"):
            self.assertNotIn(forbidden, touched)
        # 物理状态：机器人停在障碍点，没穿过去
        self.assertEqual(self.engine.robot.pos, [2, 0])
        # 事件日志最后一条必须是 run=CANCELLED
        events = self.db.events_after(run_id, 0)
        run_events = [e for e in events if e["kind"] == "run"]
        self.assertEqual(run_events[-1]["payload"]["status"], "CANCELLED")
        # 失败走向记录存在
        self.assertTrue(any(
            e["kind"] == "branch" and e["node_id"] == "move_rack"
            and e["payload"].get("branch") == "failure"
            for e in events))


if __name__ == "__main__":
    unittest.main()
