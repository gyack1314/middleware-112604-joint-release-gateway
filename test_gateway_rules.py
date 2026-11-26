"""网关核心规则测试：预算域、去重、原子预留、回执范围、复用与授权。"""

import unittest

from gateway import (
    ACCESS_CLOSED,
    AWAITING_PUBLICATION,
    DENIED,
    NEVER_PUBLISHED,
    PREPARING,
    PUBLISHED,
    SCOPE_PENDING,
    VERIFYING,
    Gateway,
)
from gateway.model import ConflictError, ValidationError


def make_gateway():
    """:memory: 网关；纯规则测试不需要真重启。"""
    return Gateway(":memory:")


def seed_shared(gw, cap_shared=4):
    gw.announce_catalog("a", 1, ["shared", "left"], ["lab"], at=0)
    gw.announce_catalog("b", 1, ["shared", "right"], ["lab"], at=0)
    gw.set_budget("shared", cap_shared, at=0)
    gw.set_budget("left", 10, at=0)
    gw.set_budget("right", 10, at=0)
    return gw


class ReservationTests(unittest.TestCase):
    def test_same_group_in_multiple_sources_counted_once(self):
        gw = seed_shared(make_gateway())
        out = gw.submit("one", "lab", ["a@1", "b@1"], 3, at=1)
        self.assertEqual(out["status"], "accepted")
        one = gw.inspect_query("one")
        shared_holds = [h for h in one["holds"] if h["group"] == "shared"]
        self.assertEqual(len(shared_holds), 1)
        self.assertEqual(shared_holds[0]["amount"], 3)
        # shared 4-3=1；left/right 10-3=7
        self.assertEqual(gw.inspect_group("shared")["available"], 1)
        self.assertEqual(gw.inspect_group("left")["available"], 7)
        self.assertEqual(gw.inspect_group("right")["available"], 7)

    def test_repeated_group_refs_and_repeated_sources_dedup(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g", "g", "g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        out = gw.submit("q", "lab", ["s@1", "s@1"], 2, at=1)
        self.assertEqual(out["status"], "accepted")
        q = gw.inspect_query("q")
        self.assertEqual(len(q["holds"]), 1)
        self.assertEqual(q["holds"][0]["group"], "g")
        self.assertEqual(gw.inspect_group("g")["reserved"], 2)

    def test_last_remaining_quota_is_protected(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 4, at=0)
        self.assertEqual(gw.submit("a", "lab", ["s@1"], 3, at=1)["status"], "accepted")
        # 只剩 1：unit=2 被拒，unit=1 成功并把真实可用量打到 0。
        self.assertEqual(gw.submit("b", "lab", ["s@1"], 2, at=2)["status"], "denied")
        self.assertEqual(gw.inspect_group("g")["available"], 1)
        self.assertEqual(gw.submit("c", "lab", ["s@1"], 1, at=3)["status"], "accepted")
        snap = gw.inspect_group("g")
        self.assertEqual(snap["available"], 0)
        self.assertEqual(snap["reserved"], 4)
        self.assertEqual(gw.submit("d", "lab", ["s@1"], 1, at=4)["status"], "denied")

    def test_joint_reservation_is_all_or_nothing(self):
        gw = make_gateway()
        for g, cap in (("g1", 10), ("g2", 1)):
            gw.announce_catalog(f"s-{g}", 1, [g], ["lab"], at=0)
            gw.set_budget(g, cap, at=0)
        # 先让 g2 的最后额度被别的单占走。
        self.assertEqual(gw.submit("holder", "lab", ["s-g2@1"], 1, at=1)["status"],
                         "accepted")
        denied = gw.submit("joint", "lab", ["s-g1@1", "s-g2@1"], 1, at=2)
        self.assertEqual(denied["status"], "denied")
        self.assertEqual(denied["reason"], "insufficient_budget")
        joint = gw.inspect_query("joint")
        self.assertEqual(joint["state"], DENIED)
        self.assertEqual(joint["holds"], [])
        # g1 没有被“先扣一半”。
        self.assertEqual(gw.inspect_group("g1")["reserved"], 0)
        self.assertEqual(gw.inspect_group("g1")["available"], 10)

    def test_missing_budget_domain_means_zero_quota(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["unfunded"], ["lab"], at=0)
        out = gw.submit("q", "lab", ["s@1"], 1, at=1)
        self.assertEqual(out["status"], "denied")
        self.assertEqual(out["reason"], "no_budget_declared")

    def test_cap_below_usage_reports_negative_available_honestly(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        gw.submit("q", "lab", ["s@1"], 4, at=1)
        gw.confirm_computation("q", "r", at=2)
        gw.confirm_publication("q", "exposed", at=3)
        gw.set_budget("g", 3, at=4)
        snap = gw.inspect_group("g")
        self.assertEqual(snap["committed"], 4)
        self.assertEqual(snap["available"], -1)
        self.assertEqual(gw.submit("q2", "lab", ["s@1"], 1, at=5)["status"], "denied")


class UnknownScopeTests(unittest.TestCase):
    def _seed(self):
        gw = make_gateway()
        gw.announce_catalog("a", 1, ["base"], ["lab", "partner"], at=0)
        gw.announce_catalog("b", 1, None, ["lab"], at=0)
        gw.set_budget("base", 4, at=0)
        return gw

    def test_unknown_scope_holds_known_domain_but_blocks_publish(self):
        gw = self._seed()
        gw.submit("a", "lab", ["a@1"], 3, at=1)
        gw.confirm_computation("a", "q", at=2)
        gw.confirm_publication("a", "exposed", at=3)
        b = gw.submit("b", "lab", ["a@1", "b@1"], 1, at=4)
        self.assertEqual(b["state"], SCOPE_PENDING)
        # 已确定涉及的 base 最后 1 额度必须在到达时被保护。
        self.assertEqual(gw.inspect_group("base")["available"], 0)
        qb = gw.inspect_query("b")
        self.assertFalse(qb["scope_complete"])
        # 范围未补齐，即使计算回执到了也不能发布 / 读取。
        gw.confirm_computation("b", "rb", at=5)
        self.assertEqual(gw.inspect_query("b")["state"], SCOPE_PENDING)
        view = gw.audience_disposition("b", "lab")
        self.assertFalse(view["result_available"])

    def test_scope_completion_adds_domains_and_reaches_preparing(self):
        gw = self._seed()
        gw.set_budget("extra", 10, at=0)
        gw.submit("b", "lab", ["a@1", "b@1"], 1, at=1)
        self.assertEqual(gw.inspect_query("b")["state"], SCOPE_PENDING)
        out = gw.announce_catalog("b", 1, ["base", "extra"], ["lab"], at=2)
        self.assertEqual(out["action"], "completed_scope")
        qb = gw.inspect_query("b")
        self.assertEqual(qb["state"], PREPARING)
        groups = {h["group"] for h in qb["holds"]}
        self.assertEqual(groups, {"base", "extra"})

    def test_computed_before_completion_goes_straight_to_awaiting(self):
        gw = self._seed()
        gw.set_budget("extra", 10, at=0)
        gw.submit("b", "lab", ["a@1", "b@1"], 1, at=1)
        gw.confirm_computation("b", "rb", at=2)
        gw.announce_catalog("b", 1, ["extra"], ["lab"], at=3)
        qb = gw.inspect_query("b")
        self.assertEqual(qb["state"], AWAITING_PUBLICATION)
        self.assertEqual(qb["result"], "rb")

    def test_scope_completion_unfunded_domain_denies_and_releases_all(self):
        gw = self._seed()
        # base cap4；先让 a 公开花掉3，b 预留最后1。
        gw.submit("a", "lab", ["a@1"], 3, at=1)
        gw.confirm_computation("a", "q", at=2)
        gw.confirm_publication("a", "exposed", at=3)
        gw.submit("b", "lab", ["a@1", "b@1"], 1, at=4)
        self.assertEqual(gw.inspect_group("base")["reserved"], 1)
        # 补齐出一个没有预算的 rare 域 => 联合单失败，base 预留原子释放。
        gw.announce_catalog("b", 1, ["rare"], ["lab"], at=5)
        qb = gw.inspect_query("b")
        self.assertEqual(qb["state"], DENIED)
        self.assertEqual(qb["reason"], "insufficient_budget")
        base = gw.inspect_group("base")
        self.assertEqual(base["reserved"], 0)
        self.assertEqual(base["available"], 1)


class ReceiptAndReleaseTests(unittest.TestCase):
    def _prepared(self, groups=("g1", "g2"), cap=5, unit=2):
        gw = make_gateway()
        for g in groups:
            gw.announce_catalog(f"s-{g}", 1, [g], ["lab"], at=0)
            gw.set_budget(g, cap, at=0)
        sources = [f"s-{g}@1" for g in groups]
        gw.submit("q", "lab", sources, unit, at=1)
        gw.confirm_computation("q", "r", at=2)
        return gw

    def test_unknown_receipt_never_releases(self):
        gw = self._prepared(groups=("g",))
        out = gw.confirm_publication("q", "unknown", at=3)
        self.assertEqual(out["state_after"], VERIFYING)
        self.assertEqual(gw.inspect_group("g")["reserved"], 2)

    def test_never_without_scope_keeps_hold(self):
        gw = self._prepared(groups=("g",))
        gw.withdraw("q", at=3)
        out = gw.confirm_publication("q", "never", at=4)  # 无 scope
        self.assertEqual(out["state_after"], VERIFYING)
        snap = gw.inspect_group("g")
        self.assertEqual(snap["reserved"], 2)
        self.assertEqual(snap["available"], 3)
        q = gw.inspect_query("q")
        self.assertTrue(any(e["kind"] == "never_without_scope_kept_hold"
                            for e in q["events"]))

    def test_never_with_full_scope_releases(self):
        gw = self._prepared(groups=("g1", "g2"))
        gw.confirm_publication("q", "never", scope=frozenset({"g1", "g2"}), at=3)
        q = gw.inspect_query("q")
        self.assertEqual(q["state"], NEVER_PUBLISHED)
        self.assertEqual(gw.inspect_group("g1")["available"], 5)
        self.assertEqual(gw.inspect_group("g2")["available"], 5)

    def test_never_with_wildcard_scope_releases(self):
        from gateway.model import extract_scope
        specified, scope = extract_scope({"state": "never", "scope": "全部"})
        self.assertTrue(specified)
        gw = self._prepared(groups=("g1", "g2"))
        gw.handle("published",
                  {"id": "q", "state": "never", "scope": "all"}, at=3)
        self.assertEqual(gw.inspect_query("q")["state"], NEVER_PUBLISHED)
        self.assertEqual(gw.inspect_group("g1")["reserved"], 0)

    def test_never_with_partial_scope_releases_only_covered(self):
        gw = self._prepared(groups=("g1", "g2"))
        gw.confirm_publication("q", "never", scope=frozenset({"g1"}), at=3)
        q = gw.inspect_query("q")
        self.assertEqual(q["state"], VERIFYING)
        holds = {h["group"]: h["status"] for h in q["holds"]}
        self.assertEqual(holds, {"g1": "released", "g2": "held"})
        self.assertEqual(gw.inspect_group("g1")["available"], 5)
        self.assertEqual(gw.inspect_group("g2")["available"], 3)
        # 补齐剩余范围的确证后全部释放。
        gw.confirm_publication("q", "never", scope=frozenset({"g2"}), at=4)
        self.assertEqual(gw.inspect_query("q")["state"], NEVER_PUBLISHED)

    def test_exposed_without_scope_commits_all_domains(self):
        gw = self._prepared(groups=("g1", "g2"))
        out = gw.confirm_publication("q", "exposed", at=3)
        self.assertEqual(out["state_after"], PUBLISHED)
        for g in ("g1", "g2"):
            snap = gw.inspect_group(g)
            self.assertEqual(snap["committed"], 2)
            self.assertEqual(snap["reserved"], 0)
            self.assertEqual(snap["available"], 3)

    def test_scoped_exposed_then_full_exposed(self):
        gw = self._prepared(groups=("g1", "g2"))
        gw.confirm_publication("q", "exposed", scope=frozenset({"g1"}), at=3)
        q = gw.inspect_query("q")
        self.assertEqual(q["state"], VERIFYING)
        self.assertEqual(gw.inspect_group("g1")["committed"], 2)
        self.assertEqual(gw.inspect_group("g2")["reserved"], 2)
        gw.confirm_publication("q", "exposed", at=4)  # 无 scope => 覆盖全部
        self.assertEqual(gw.inspect_query("q")["state"], PUBLISHED)
        self.assertEqual(gw.inspect_group("g2")["committed"], 2)

    def test_exposed_after_scoped_never_recharges_public_fact(self):
        gw = self._prepared(groups=("g",))
        gw.confirm_publication("q", "never", scope=frozenset({"g"}), at=3)
        self.assertEqual(gw.inspect_group("g")["available"], 5)
        # 延期回执推翻早先 never：公开事实重新记账，哪怕把可用量打成负数。
        gw.confirm_publication("q", "exposed", scope=frozenset({"g"}), at=4)
        snap = gw.inspect_group("g")
        self.assertEqual(snap["committed"], 2)
        self.assertEqual(snap["available"], 3)
        q = gw.inspect_query("q")
        self.assertTrue(q["public_fact"])
        self.assertTrue(any(e["kind"] == "public_fact_recharged" for e in q["events"]))


class WithdrawAndAccessTests(unittest.TestCase):
    def _published_one(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 4, at=0)
        gw.submit("one", "lab", ["s@1"], 3, at=1)
        gw.confirm_computation("one", "digest", at=2)
        gw.confirm_publication("one", "exposed", at=3)
        return gw

    def test_withdraw_after_exposure_keeps_budget_closes_body(self):
        gw = self._published_one()
        self.assertTrue(gw.audience_disposition("one", "lab")["result_available"])
        gw.withdraw("one", at=4)
        q = gw.inspect_query("one")
        self.assertEqual(q["state"], ACCESS_CLOSED)
        snap = gw.inspect_group("g")
        self.assertEqual(snap["committed"], 3)
        self.assertEqual(snap["available"], 1)
        view = gw.audience_disposition("one", "lab")
        self.assertEqual(view["state"], ACCESS_CLOSED)
        self.assertFalse(view["result_available"])
        self.assertIsNone(view["result"])
        self.assertTrue(view["disposition"])  # 处置说明仍可见

    def test_withdraw_before_exposure_needs_never_confirmation(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 4, at=0)
        gw.submit("one", "lab", ["s@1"], 3, at=1)
        gw.confirm_computation("one", "digest", at=2)
        gw.withdraw("one", at=3)
        gw.confirm_publication("one", "unknown", at=4)
        self.assertEqual(gw.inspect_group("g")["available"], 1)
        # 后来确认 exposed：公开事实成立，预算不释放，访问按撤回关闭。
        gw.confirm_publication("one", "exposed", at=7)
        q = gw.inspect_query("one")
        self.assertEqual(q["state"], ACCESS_CLOSED)
        self.assertEqual(gw.inspect_group("g")["committed"], 3)

    def test_withdraw_then_confirmed_never_releases(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 4, at=0)
        gw.submit("one", "lab", ["s@1"], 3, at=1)
        gw.withdraw("one", at=2)
        gw.confirm_publication("one", "never", scope=frozenset({"g"}), at=3)
        self.assertEqual(gw.inspect_query("one")["state"], NEVER_PUBLISHED)
        self.assertEqual(gw.inspect_group("g")["available"], 4)

    def test_tighten_allowlist_hides_result_from_removed_audience(self):
        gw = self._published_one()
        gw.withdraw("one", reason="tighten", audiences=["partner"], at=4)
        self.assertFalse(gw.audience_disposition("one", "lab")["result_available"])
        # 明确保留 lab 时仍可读。
        gw2 = self._published_one()
        gw2.withdraw("one", reason="tighten", audiences=["lab"], at=4)
        self.assertTrue(gw2.audience_disposition("one", "lab")["result_available"])

    def test_expiry_closes_access_but_keeps_commit(self):
        gw = self._published_one()
        gw.withdraw("one", reason="expire", at=4)
        view = gw.audience_disposition("one", "lab")
        self.assertFalse(view["result_available"])
        self.assertEqual(view["reason"], "access_closed")
        self.assertEqual(gw.inspect_group("g")["committed"], 3)

    def test_explicit_null_scope_is_still_unspecified(self):
        from gateway.model import extract_scope
        self.assertEqual(extract_scope({"state": "never", "scope": None})[0], False)
        self.assertEqual(
            extract_scope({"state": "never", "participants": None})[0], False)

    def test_catalog_audience_tightening_takes_effect_dynamically(self):
        gw = self._published_one()
        # 同一 rev 收紧 audiences（群体不变）。
        gw.announce_catalog("s", 1, ["g"], ["partner"], at=4)
        view = gw.audience_disposition("one", "lab")
        self.assertFalse(view["result_available"])
        self.assertEqual(view["reason"], "not_authorized")

    def test_non_owner_audience_cannot_see_disposition(self):
        gw = self._published_one()
        self.assertEqual(gw.audience_disposition("one", "spy"), {"found": False, "id": "one"})


class ReuseTests(unittest.TestCase):
    def _base(self):
        gw = make_gateway()
        gw.announce_catalog("a", 1, ["base"], ["lab", "partner"], at=0)
        gw.set_budget("base", 4, at=0)
        gw.submit("a", "lab", ["a@1"], 3, at=1)
        gw.confirm_computation("a", "q", at=2)
        gw.confirm_publication("a", "exposed", at=3)
        return gw

    def test_exact_repeat_by_original_audience_costs_nothing(self):
        gw = self._base()
        before = gw.inspect_group("base")
        out = gw.submit("again", "lab", ["a@1"], 99, reuse="q", at=4)
        self.assertEqual(out["status"], "reused")
        self.assertEqual(out["cost"], 0)
        after = gw.inspect_group("base")
        self.assertEqual(before["committed"], after["committed"])
        self.assertEqual(before["available"], after["available"])
        view = gw.audience_disposition("again", "lab")
        self.assertTrue(view["result_available"])
        self.assertEqual(view["result"], "q")

    def test_new_audience_needs_independent_budget_judgement(self):
        gw = self._base()
        out = gw.submit("c", "partner", ["a@1"], 3, reuse="q", at=4)
        self.assertEqual(out["status"], "denied")
        self.assertEqual(out["reason"], "insufficient_budget")
        self.assertTrue(out["detail"]["scope_change"]["new_audience"])
        # 额度补足后，新受众独立扣减成功并形成自己的归属。
        gw.set_budget("base", 10, at=5)
        ok = gw.submit("c2", "partner", ["a@1"], 3, reuse="q", at=6)
        self.assertEqual(ok["status"], "accepted")
        c2 = gw.inspect_query("c2")
        self.assertEqual(c2["derived_from"], "a")
        gw.confirm_computation("c2", "q2", at=7)
        gw.confirm_publication("c2", "exposed", at=8)
        self.assertTrue(gw.audience_disposition("c2", "partner")["result_available"])
        # partner 不是原归属 a 的请求方，不能借原单免费读 q。
        self.assertEqual(gw.audience_disposition("a", "partner"),
                         {"found": False, "id": "a"})

    def test_different_revisions_is_scope_change(self):
        gw = self._base()
        gw.announce_catalog("b", 1, ["extra"], ["lab"], at=0)
        gw.set_budget("extra", 10, at=0)
        gw.set_budget("base", 10, at=0)  # 原 a 已 committed 3，补足可用量
        out = gw.submit("d", "lab", ["a@1", "b@1"], 2, reuse="q", at=4)
        self.assertEqual(out["status"], "accepted")
        self.assertTrue(out["scope_change"]["different_revisions"])
        d = gw.inspect_query("d")
        self.assertEqual({h["group"] for h in d["holds"]}, {"base", "extra"})

    def test_reuse_unknown_result_is_rejected_not_charged(self):
        gw = self._base()
        out = gw.submit("e", "lab", ["a@1"], 1, reuse="missing", at=4)
        self.assertEqual(out["status"], "denied")
        self.assertEqual(out["reason"], "reuse_unavailable")

    def test_reuse_before_publication_fails(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        gw.submit("q", "lab", ["s@1"], 1, at=1)
        gw.confirm_computation("q", "r", at=2)
        out = gw.submit("q2", "lab", ["s@1"], 1, reuse="r", at=3)
        self.assertEqual(out["status"], "denied")

    def test_exact_reuse_after_revocation_denied(self):
        gw = self._base()
        gw.withdraw("a", at=4)
        out = gw.submit("again", "lab", ["a@1"], 1, reuse="q", at=5)
        self.assertEqual(out["status"], "denied")
        self.assertEqual(out["reason"], "access_closed")


class ValidationAndIdempotencyTests(unittest.TestCase):
    def test_unknown_revision_denied(self):
        gw = make_gateway()
        out = gw.submit("q", "lab", ["ghost@9"], 1, at=1)
        self.assertEqual(out["status"], "denied")
        self.assertEqual(out["reason"], "unknown_revision")

    def test_malformed_source_raises(self):
        gw = make_gateway()
        with self.assertRaises(ValidationError):
            gw.submit("q", "lab", ["not-a-rev"], 1)

    def test_duplicate_submit_is_idempotent(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        first = gw.submit("q", "lab", ["s@1"], 2, at=1)
        second = gw.submit("q", "lab", ["s@1"], 2, at=9)
        self.assertEqual(first["status"], "accepted")
        self.assertTrue(second.get("idempotent"))
        self.assertEqual(gw.inspect_group("g")["reserved"], 2)  # 没有重复扣

    def test_conflicting_duplicate_id_raises(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        gw.submit("q", "lab", ["s@1"], 2, at=1)
        with self.assertRaises(ConflictError):
            gw.submit("q", "lab", ["s@1"], 3, at=2)

    def test_result_identifier_is_immutable(self):
        gw = make_gateway()
        gw.announce_catalog("s", 1, ["g"], ["lab"], at=0)
        gw.set_budget("g", 5, at=0)
        gw.submit("q", "lab", ["s@1"], 1, at=1)
        gw.confirm_computation("q", "r1", at=2)
        with self.assertRaises(ConflictError):
            gw.confirm_computation("q", "r2", at=3)
        # 相同 result 重放是幂等成功。
        again = gw.confirm_computation("q", "r1", at=4)
        self.assertTrue(again.get("idempotent"))

    def test_handle_returns_error_dict_instead_of_raising(self):
        gw = make_gateway()
        out = gw.handle("submit", {"id": "q", "audience": "lab",
                                   "sources": ["bad"], "unit": 1}, at=1)
        self.assertEqual(out["status"], "error")
        self.assertEqual(out["error"], "invalid_request")


if __name__ == "__main__":
    unittest.main()
