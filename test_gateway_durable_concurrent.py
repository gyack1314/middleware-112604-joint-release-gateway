"""重启恢复、示例场景轨迹、并发抢额度与 CLI 的测试。"""

import io
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from gateway import (
    ACCESS_CLOSED,
    AWAITING_PUBLICATION,
    PREPARING,
    SCOPE_PENDING,
    VERIFYING,
    Gateway,
    run_scenario,
)
from gateway.cli import main

ROOT = Path(__file__).resolve().parent
EXAMPLES = ROOT / "examples"


class RestartRecoveryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self._tmp.name) / "gateway.db")

    def tearDown(self):
        self._tmp.cleanup()

    def test_recover_preparing_and_awaiting_and_verifying(self):
        gw = Gateway(self.db)
        gw.announce_catalog("a", 1, ["shared", "left"], ["lab"], at=0)
        gw.announce_catalog("b", 1, None, ["lab"], at=0)
        gw.set_budget("shared", 4, at=0)
        gw.set_budget("left", 10, at=0)
        # q1：preparing；q2：scope_pending；q3：awaiting_publication；
        # q4：withdraw 后 verifying。
        gw.submit("q1", "lab", ["a@1"], 2, at=1)
        gw.submit("q2", "lab", ["b@1"], 1, at=2)  # b 范围 null
        gw.submit("q3", "lab", ["a@1"], 1, at=3)
        gw.confirm_computation("q3", "r3", at=4)
        gw.submit("q4", "lab", ["a@1"], 1, at=5)
        gw.confirm_computation("q4", "r4", at=6)
        gw.withdraw("q4", at=7)
        gw.close()

        recovered = Gateway(self.db)
        try:
            self.assertEqual(recovered.inspect_query("q1")["state"], PREPARING)
            self.assertEqual(recovered.inspect_query("q2")["state"], SCOPE_PENDING)
            self.assertEqual(recovered.inspect_query("q3")["state"],
                             AWAITING_PUBLICATION)
            self.assertEqual(recovered.inspect_query("q4")["state"], VERIFYING)
            # 预留全部还在：shared 4 - 2 - 1(q3) - 1(q4) = 0；
            # 注意 q2 只引用 b@1(null)，没有已知群体，不占 shared。
            self.assertEqual(recovered.inspect_group("shared")["reserved"], 4)
            self.assertEqual(recovered.inspect_group("shared")["available"], 0)

            # 重启后仍可继续推进并正确记账。
            recovered.confirm_publication("q3", "exposed", at=8)
            self.assertEqual(recovered.inspect_query("q3")["state"], "published")
            snap = recovered.inspect_group("shared")
            self.assertEqual(snap["committed"], 1)
            self.assertEqual(snap["reserved"], 3)

            recovered.confirm_publication(
                "q4", "never", scope=frozenset({"shared", "left"}), at=9)
            self.assertEqual(recovered.inspect_query("q4")["state"],
                             "never_published")
            snap = recovered.inspect_group("shared")
            self.assertEqual(snap["committed"], 1)
            self.assertEqual(snap["reserved"], 2)  # q1 仍 held
        finally:
            recovered.close()

    def test_authorization_adjustment_survives_restart(self):
        gw = Gateway(self.db)
        gw.announce_catalog("s", 1, ["g"], ["lab", "partner"], at=0)
        gw.set_budget("g", 5, at=0)
        gw.submit("q", "lab", ["s@1"], 1, at=1)
        gw.confirm_computation("q", "r", at=2)
        gw.confirm_publication("q", "exposed", at=3)
        # 访问条件收紧：catalog 受众只剩 partner。
        gw.announce_catalog("s", 1, ["g"], ["partner"], at=4)
        gw.close()

        recovered = Gateway(self.db)
        try:
            self.assertFalse(
                recovered.audience_disposition("q", "lab")["result_available"])
            events = recovered.inspect_query("q")["events"]
            self.assertTrue(any(e["kind"] == "authorization_adjusted" for e in events))
        finally:
            recovered.close()

    def test_reopen_in_place_matches_scenario_restart(self):
        gw = Gateway(self.db)
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 2, at=0)
        gw.submit("q", "lab", ["s@1"], 1, at=1)
        gw.handle("restart", {}, at=2)  # 内部 close + 重开同一文件
        self.assertEqual(gw.inspect_query("q")["state"], PREPARING)
        self.assertEqual(gw.inspect_group("g")["available"], 1)
        gw.close()


class ExampleScenarioTests(unittest.TestCase):
    def test_overlap_and_late_publication_trace(self):
        trace = run_scenario(EXAMPLES / "overlap-and-late-publication.jsonl")
        self.assertEqual(len(trace), 13)
        by_kind = {}
        for item in trace:
            by_kind.setdefault(item["kind"], []).append(item["outcome"])

        submits = {o["id"]: o for o in by_kind["submit"]}
        self.assertEqual(submits["one"]["status"], "accepted")
        self.assertEqual(submits["two"]["status"], "denied")
        self.assertEqual(submits["two"]["reason"], "insufficient_budget")

        inspect_shared = [o for o in by_kind["inspect"] if o.get("group") == "shared"][0]
        self.assertEqual(inspect_shared["cap"], 4)
        self.assertEqual(inspect_shared["committed"], 3)
        self.assertEqual(inspect_shared["reserved"], 0)
        self.assertEqual(inspect_shared["available"], 1)

        self.assertTrue(any(o["status"] == "restarted" for o in by_kind["restart"]))

        # at=7 的 exposed 已把 one 置为 access_closed（withdraw 在先）。
        published = by_kind["published"]
        self.assertEqual(published[0]["state_after"], VERIFYING)  # unknown
        self.assertEqual(published[1]["state_after"], ACCESS_CLOSED)

    def test_overlap_example_audience_body_hidden_after_revoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "x.db")
            run_scenario(EXAMPLES / "overlap-and-late-publication.jsonl", db_path=db)
            gw = Gateway(db)
            try:
                view = gw.audience_disposition("one", "lab")
                self.assertFalse(view["result_available"])
                self.assertIsNone(view["result"])
                self.assertEqual(view["state"], ACCESS_CLOSED)
                # 真实可用量仍是 1，没有因为撤回 / unknown 回执而虚增。
                self.assertEqual(gw.inspect_group("shared")["available"], 1)
            finally:
                gw.close()

    def test_unknown_overlap_and_scope_change_trace(self):
        trace = run_scenario(EXAMPLES / "unknown-overlap-and-scope-change.jsonl")
        self.assertEqual(len(trace), 9)
        submits = {item["outcome"]["id"]: item["outcome"]
                   for item in trace if item["kind"] == "submit"}
        self.assertEqual(submits["a"]["status"], "accepted")
        self.assertEqual(submits["b"]["state"], SCOPE_PENDING)
        self.assertEqual(submits["c"]["status"], "denied")
        self.assertEqual(submits["c"]["reason"], "insufficient_budget")
        self.assertTrue(submits["c"]["detail"]["scope_change"]["new_audience"])

        inspect_base = [item["outcome"] for item in trace
                         if item["kind"] == "inspect"][0]
        # a committed 3；b 在到达时锁走最后 1 个额度。
        self.assertEqual(inspect_base["committed"], 3)
        self.assertEqual(inspect_base["reserved"], 1)
        self.assertEqual(inspect_base["available"], 0)


class ConcurrencyTests(unittest.TestCase):
    def test_many_threads_compete_for_five_quota(self):
        gw = Gateway(":memory:")
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        barrier = threading.Barrier(20)

        def worker(i):
            barrier.wait()
            return gw.submit(f"q{i}", "lab", ["s@1"], 1, at=i + 1)["status"]

        with ThreadPoolExecutor(max_workers=20) as pool:
            statuses = list(pool.map(worker, range(20)))
        self.assertEqual(statuses.count("accepted"), 5)
        self.assertEqual(statuses.count("denied"), 15)
        snap = gw.inspect_group("g")
        self.assertEqual(snap["reserved"], 5)
        self.assertEqual(snap["available"], 0)

    def test_joint_domains_have_exactly_two_full_winners(self):
        gw = Gateway(":memory:")
        for g in ("g1", "g2"):
            gw.announce_catalog(f"s-{g}", 1, [g], ["lab"], at=0)
            gw.set_budget(g, 2, at=0)
        barrier = threading.Barrier(12)

        def worker(i):
            barrier.wait()
            out = gw.submit(f"j{i}", "lab", ["s-g1@1", "s-g2@1"], 1, at=i + 1)
            return out["status"]

        with ThreadPoolExecutor(max_workers=12) as pool:
            statuses = list(pool.map(worker, range(12)))
        self.assertEqual(statuses.count("accepted"), 2)
        self.assertEqual(statuses.count("denied"), 10)
        self.assertEqual(gw.inspect_group("g1")["reserved"], 2)
        self.assertEqual(gw.inspect_group("g2")["reserved"], 2)
        # 每个赢家都同时持有两个域的预留，不存在半单。
        winners = [f"j{i}" for i, s in enumerate(statuses) if s == "accepted"]
        for qid in winners:
            groups = {h["group"] for h in gw.inspect_query(qid)["holds"]}
            self.assertEqual(groups, {"g1", "g2"})

    def test_separate_connections_same_file_serialize_on_last_quota(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "race.db")
            init = Gateway(db)
            init.announce_catalog("s", 1, ["g"], ["lab"], at=0)
            init.set_budget("g", 3, at=0)
            init.close()

            barrier = threading.Barrier(10)

            def worker(i):
                barrier.wait()
                own = Gateway(db)
                try:
                    return own.submit(f"p{i}", "lab", ["s@1"], 1, at=i + 1)["status"]
                finally:
                    own.close()

            with ThreadPoolExecutor(max_workers=10) as pool:
                statuses = list(pool.map(worker, range(10)))
            self.assertEqual(statuses.count("accepted"), 3, statuses)
            gw = Gateway(db)
            try:
                self.assertEqual(gw.inspect_group("g")["available"], 0)
            finally:
                gw.close()


class CliTests(unittest.TestCase):
    def test_one_shot_commands_and_inspect(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cli.db")
            buf = io.StringIO()
            with __import__("contextlib").redirect_stdout(buf):
                code = main(["--db", db, "budget", "--group", "g", "--cap", "5"])
            self.assertEqual(code, 0)
            out = json.loads(buf.getvalue())
            self.assertEqual(out["available"], 5)

            buf = io.StringIO()
            with __import__("contextlib").redirect_stdout(buf):
                main(["--db", db, "catalog", "--name", "s", "--rev", "1",
                      "--groups", "g", "--audiences", "lab"])
                main(["--db", db, "submit", "--id", "q", "--audience", "lab",
                      "--sources", "s@1", "--unit", "2"])
                main(["--db", db, "inspect", "--group", "g"])
            text = buf.getvalue()
            decoder = json.JSONDecoder()
            docs = []
            pos = 0
            while pos < len(text):
                while pos < len(text) and text[pos].isspace():
                    pos += 1
                if pos >= len(text):
                    break
                doc, end = decoder.raw_decode(text, pos)
                docs.append(doc)
                pos = end
            inspect = docs[-1]
            self.assertEqual(inspect["reserved"], 2)
            self.assertEqual(inspect["available"], 3)

    def test_serve_reads_jsonl_and_replies_per_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "serve.db")
            messages = "\n".join([
                json.dumps({"kind": "budget", "record": {"group": "g", "cap": 2}}),
                json.dumps({"kind": "catalog",
                            "record": {"name": "s", "rev": 1, "groups": ["g"],
                                       "audiences": ["lab"]}}),
                json.dumps({"at": 1, "kind": "submit",
                            "record": {"id": "q", "audience": "lab",
                                       "sources": ["s@1"], "unit": 1}}),
                json.dumps({"kind": "inspect", "record": {"group": "g"}}),
            ])
            out = io.StringIO()
            with patch("sys.stdin", io.StringIO(messages)), \
                    __import__("contextlib").redirect_stdout(out):
                code = main(["--db", db, "serve"])
            self.assertEqual(code, 0)
            replies = [json.loads(line) for line in out.getvalue().strip().splitlines()]
            self.assertEqual(replies[0]["status"], "ok")
            self.assertEqual(replies[2]["status"], "accepted")
            self.assertEqual(replies[3]["available"], 1)


if __name__ == "__main__":
    unittest.main()
