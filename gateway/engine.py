"""查询发布网关核心：预算域协调与发布状态机。

关键不变量
----------
1. 提交即“到达时协调”：在一个 ``BEGIN IMMEDIATE`` 事务内检查所有已确定
   预算域的 ``cap - committed - held >= unit`` 并写入全部 hold；任一域不足
   则整单拒绝、不留任何 hold（联合查询不允许只扣一半）。
2. 未知范围（catalog groups=null）绝不按不相交处理：已确定涉及的域照常
   原子预留，同时查询停在 scope_pending，范围补齐并预留新增域之前不得
   进入可发布状态。
3. hold 只有在发布端给出“明确覆盖该参与者范围的 never 确证”后才释放；
   缺少范围的 never / unknown / 延期回执一律继续占用真实可用量。
4. exposed 一旦确认即成为 committed 公开事实，之后撤销 / 收紧 / 过期只关闭
   后续访问，committed 不回滚。
5. 完全相同且仍有效的已公开结果，原受众、同来源修订的重复读取零成本；
   新受众或来源修订变化走独立的授权 + 当前预算判断。
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from .model import (
    ACCESS_CLOSED,
    AWAITING_PUBLICATION,
    COMMITTED,
    DENIED,
    HELD,
    NEVER_PUBLISHED,
    PREPARING,
    PUBLISHED,
    RELEASED,
    SCOPE_PENDING,
    VERIFYING,
    REASON_ACCESS_CLOSED,
    REASON_CONFLICT,
    REASON_INSUFFICIENT_BUDGET,
    REASON_INVALID_REQUEST,
    REASON_NOT_AUTHORIZED,
    REASON_NOT_PUBLIC_YET,
    REASON_NO_BUDGET_DECLARED,
    REASON_REUSE_UNAVAILABLE,
    REASON_UNKNOWN_REVISION,
    WILDCARD,
    ConflictError,
    GatewayError,
    NotFoundError,
    ValidationError,
    canonical_sources,
    extract_scope,
    parse_source,
    require_int,
)
from .store import Store

#: 零成本重复读取票的状态（不写 hold、不占预算）。
REUSED = "reused"

_QUERY_COLUMNS = (
    "id, audience, sources_json, unit, reuse, derived_from, state, reason_code, "
    "reason_detail_json, result, groups_json, scope_complete, access_mode, "
    "access_allowlist_json, public_fact, fully_settled, settlement, "
    "withdraw_requested, first_exposed_at, created_at, updated_at, record_json"
)


class Gateway:
    def __init__(self, db_path: str = ":memory:", *, clock=None):
        self.db_path = db_path
        self.store = Store(db_path)
        self._lock = __import__("threading").RLock()
        self._clock = clock or (lambda: int(time.time()))

    # ---- 生命周期 ---------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self.store.close()

    def reopen(self) -> "Gateway":
        """关闭并重新打开同一 db 文件，模拟进程内“重启”。

        内存库无法跨连接保留，仅测试中不使用 restart 消息配合 :memory:。
        """
        with self._lock:
            path = self.db_path
            self.store.close()
            if path == ":memory:":
                # 内存库没有可重新加载的持久文件；保留一个全新的空内存库，
                # 明确不声称恢复了数据。演练 runner 始终使用临时文件。
                self.store = Store(":memory:")
            else:
                self.store = Store(path)
        return self

    def restarted(self) -> "Gateway":
        """返回一个从同一 db 路径新开的 Gateway（面向 API 用户的真重启）。"""
        path = self.db_path
        self.close()
        return Gateway(path, clock=self._clock)

    def __enter__(self) -> "Gateway":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _at(self, at: Optional[int]) -> int:
        if at is not None:
            return require_int(at, "at", minimum=0)
        value = self._clock()
        return int(value)

    # ---- 消息分发（场景 / serve 使用） -----------------------------------

    def handle(self, kind: str, record: Optional[dict] = None,
               at: Optional[int] = None) -> Dict[str, Any]:
        record = record if record is not None else {}
        if not isinstance(record, dict):
            return {"status": "error", "error": REASON_INVALID_REQUEST,
                    "message": "record 应为对象"}
        # 收件箱先落盘，保证命令本身可审计。
        try:
            self.store.execute(
                "INSERT INTO inbox(at, kind, record_json) VALUES(?,?,?)",
                (at if isinstance(at, int) else None,
                 str(kind), self.store.dumps(record)),
            )
        except Exception:
            pass

        try:
            if kind == "restart":
                self.reopen()
                return {"status": "restarted", "at": at}
            if kind == "catalog":
                if not all(k in record for k in ("name", "rev", "groups", "audiences")):
                    raise ValidationError(
                        "catalog 需要 name/rev/groups/audiences 字段")
                return self.announce_catalog(
                    record["name"], record["rev"], record["groups"],
                    record["audiences"], at=at)
            if kind == "budget":
                return self.set_budget(record.get("group"), record.get("cap"), at=at)
            if kind == "submit":
                return self.submit(
                    qid=record.get("id"), audience=record.get("audience"),
                    sources=record.get("sources"), unit=record.get("unit"),
                    reuse=record.get("reuse"), at=at)
            if kind == "computed":
                return self.confirm_computation(
                    record.get("id"), record.get("result"), at=at)
            if kind == "published":
                specified, scope = extract_scope(record)
                return self.confirm_publication(
                    record.get("id"), record.get("state"),
                    scope=(scope if specified else None),
                    at=at, raw=record)
            if kind == "withdraw":
                return self.withdraw(
                    record.get("id"), reason=record.get("reason", "revoke"),
                    audiences=record.get("audiences"), at=at)
            if kind == "inspect":
                if record.get("group") is not None:
                    return self.inspect_group(record["group"])
                if record.get("id") is not None:
                    return self.inspect_query(record["id"])
                return self.overview()
            raise ValidationError(f"未支持的消息类型：{kind}")
        except GatewayError as error:
            return {"status": "error", "error": error.code, "message": str(error)}

    # ---- 事件 -------------------------------------------------------------

    def _event(self, conn, at: int, kind: str, qid: Optional[str],
               payload: Dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO event(at, kind, query_id, payload_json) VALUES(?,?,?,?)",
            (at, kind, qid, self.store.dumps(payload)),
        )

    # ---- catalog / budget 维护 ------------------------------------------

    def announce_catalog(self, name: str, rev: int, groups: Optional[List[str]],
                         audiences: List[str], at: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            if not isinstance(name, str) or not name:
                raise ValidationError("catalog.name 必须是非空字符串")
            rev = require_int(rev, "rev", minimum=0)
            if groups is not None:
                if not isinstance(groups, list) or not all(isinstance(g, str) for g in groups):
                    raise ValidationError("catalog.groups 应为字符串数组或 null")
                groups_value: Optional[List[str]] = list(groups)
                groups_known = 1
            else:
                groups_value = None
                groups_known = 0
            if not isinstance(audiences, list) or not all(isinstance(a, str) for a in audiences):
                raise ValidationError("catalog.audiences 应为字符串数组")

            row = self.store.query_one(
                "SELECT * FROM catalog WHERE name=? AND rev=?", (name, rev))
            with self.store.transaction() as conn:
                if row is None:
                    conn.execute(
                        "INSERT INTO catalog(name, rev, groups_known, groups_json, "
                        "audiences_json, updated_at) VALUES(?,?,?,?,?,?)",
                        (name, rev, groups_known,
                         self.store.dumps(groups_value) if groups_known else None,
                         self.store.dumps(list(audiences)), at),
                    )
                    action = "announced"
                else:
                    old_groups_known = int(row["groups_known"])
                    old_groups = self.store.loads(row["groups_json"]) if old_groups_known else None
                    if old_groups_known and not groups_known:
                        raise ConflictError(
                            f"集合修订 {name}@{rev} 的群体范围已完成，不能改回 null；"
                            "集合变化请使用新 rev")
                    if old_groups_known and groups_known and set(old_groups) != set(groups_value):
                        raise ConflictError(
                            f"集合修订 {name}@{rev} 的 groups 已固定为 {old_groups}，"
                            "修订不可变；请使用新 rev")
                    # 允许 null -> 列表 的“范围补齐”。
                    conn.execute(
                        "UPDATE catalog SET groups_known=?, groups_json=?, "
                        "audiences_json=?, updated_at=? WHERE name=? AND rev=?",
                        (groups_known,
                         self.store.dumps(groups_value) if groups_known else None,
                         self.store.dumps(list(audiences)), at, name, rev),
                    )
                    old_audiences = set(self.store.loads(row["audiences_json"], []))
                    if old_audiences != set(audiences):
                        payload = {
                            "source": f"{name}@{rev}",
                            "from": sorted(old_audiences),
                            "to": list(audiences),
                        }
                        self._event(conn, at, "authorization_adjusted", None, payload)
                        # 同步挂到所有引用该修订的查询处置轨迹上，重启后仍可追溯。
                        ref = f"{name}@{rev}"
                        for qrow in conn.execute("SELECT id, sources_json FROM query"):
                            if ref in self.store.loads(qrow["sources_json"], []):
                                self._event(conn, at, "authorization_adjusted",
                                            qrow["id"], payload)
                    action = "completed_scope" if not old_groups_known and groups_known else "updated"

            # 范围补齐后，重新评估所有挂在该修订上的 scope_pending 查询。
            affected: List[str] = []
            if action == "completed_scope":
                affected = self._apply_completed_scope(name, rev, at)
            return {"status": "ok", "kind": "catalog", "action": action,
                    "source": f"{name}@{rev}", "reevaluated": affected}

    def set_budget(self, group: str, cap: int, at: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            if not isinstance(group, str) or not group:
                raise ValidationError("budget.group 必须是非空字符串")
            cap = require_int(cap, "cap")
            with self.store.transaction() as conn:
                row = conn.execute("SELECT cap FROM budget WHERE grp=?", (group,)).fetchone()
                before = None if row is None else int(row["cap"])
                conn.execute(
                    "INSERT INTO budget(grp, cap, updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(grp) DO UPDATE SET cap=excluded.cap, updated_at=excluded.updated_at",
                    (group, cap, at),
                )
                self._event(conn, at, "budget_set", None,
                            {"group": group, "before": before, "cap": cap})
            ledger = self.store.ledger(group)
            return {"status": "ok", "kind": "budget", **ledger}

    # ---- catalog 读取与授权 ----------------------------------------------

    def _catalog_map(self, conn, canon_sources: List[str]):
        """返回 [(name, rev, row|None)]，保持规范来源顺序。"""
        result = []
        for source in canon_sources:
            name, rev = parse_source(source)
            row = conn.execute(
                "SELECT * FROM catalog WHERE name=? AND rev=?", (name, rev)
            ).fetchone()
            result.append((source, name, rev, row))
        return result

    @staticmethod
    def _catalog_audiences(row) -> List[str]:
        return Store.loads(row["audiences_json"], []) if row is not None else []

    def _catalog_allows(self, conn, sources: List[str], audience: str) -> List[str]:
        """返回未授权的来源修订列表（空列表表示全部授权）。"""
        denied_sources = []
        for source in sources:
            name, rev = parse_source(source)
            row = conn.execute(
                "SELECT audiences_json FROM catalog WHERE name=? AND rev=?",
                (name, rev)).fetchone()
            if row is None:
                denied_sources.append(source)
                continue
            if audience not in self.store.loads(row["audiences_json"], []):
                denied_sources.append(source)
        return denied_sources

    # ---- 提交查询 ---------------------------------------------------------

    def submit(self, qid: str, audience: str, sources: List[str], unit: int,
               reuse: Optional[str] = None, at: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            if not isinstance(qid, str) or not qid:
                raise ValidationError("submit.id 必须是非空字符串")
            if not isinstance(audience, str) or not audience:
                raise ValidationError("submit.audience 必须是非空字符串")
            unit = require_int(unit, "unit")
            canon = canonical_sources(sources)
            raw_record = {"id": qid, "audience": audience, "sources": sources,
                          "unit": unit, **({"reuse": reuse} if reuse is not None else {})}

            existing = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if existing is not None:
                return self._duplicate_submit(existing, audience, canon, unit, reuse)

            # 修订必须存在。
            missing = [s for (s, _n, _r, row) in self._catalog_map(self.store.conn, canon)
                       if row is None]
            if missing:
                return self._persist_denial(
                    qid, audience, canon, unit, reuse, REASON_UNKNOWN_REVISION,
                    {"missing_revisions": missing}, at, raw_record, groups=[])

            catalog_info = self._catalog_map(self.store.conn, canon)

            # reuse 分支：先找到“曾公开 result”的归属查询。
            if reuse is not None:
                owner = self._find_result_owner(reuse)
                if owner is None:
                    return self._persist_denial(
                        qid, audience, canon, unit, reuse, REASON_REUSE_UNAVAILABLE,
                        {"result": reuse,
                         "hint": "没有已确认公开并保留归属的该 result"},
                        at, raw_record, groups=self._known_groups(catalog_info))

                same_audience = audience == owner["audience"]
                owner_sources = self.store.loads(owner["sources_json"], [])
                same_sources = canon == owner_sources

                if same_audience and same_sources:
                    allowed, why = self._serve_decision(owner, audience)
                    if allowed:
                        return self._persist_reuse_read(
                            qid, audience, canon, unit, reuse, owner, at, raw_record)
                    reason = why or REASON_NOT_PUBLIC_YET
                    return self._persist_denial(
                        qid, audience, canon, unit, reuse, reason,
                        {"result": reuse, "owner": owner["id"], "exact_reuse": True},
                        at, raw_record, groups=self._known_groups(catalog_info),
                        derived_from=owner["id"])
                # 扩大受众 / 更换修订 => 独立判断授权与当前预算。
                scope_change = {
                    "new_audience": not same_audience,
                    "different_revisions": not same_sources,
                    "owner": owner["id"],
                }
                # result 级撤销 / 过期会阻止任何人再借该 result 引用读取；
                # 但不带 reuse 的全新查询不受此限制。
                if owner["access_mode"] in ("revoked", "expired"):
                    return self._persist_denial(
                        qid, audience, canon, unit, reuse, REASON_ACCESS_CLOSED,
                        {"result": reuse, **scope_change,
                         "access_mode": owner["access_mode"]},
                        at, raw_record, groups=self._known_groups(catalog_info),
                        derived_from=owner["id"])
                return self._fresh_reserve(
                    qid, audience, canon, unit, reuse, at, raw_record,
                    derived_from=owner["id"], scope_change=scope_change,
                    catalog_info=catalog_info)

            return self._fresh_reserve(
                qid, audience, canon, unit, None, at, raw_record,
                derived_from=None, scope_change=None, catalog_info=catalog_info)

    def _find_result_owner(self, result: str):
        # result 标识不可变且原始归属唯一；排除零成本复用票（它们 derived_from
        # 指向原始归属），取最早公开的原始发布。
        return self.store.query_one(
            f"SELECT {_QUERY_COLUMNS} FROM query "
            "WHERE result=? AND public_fact=1 AND state != ? "
            "ORDER BY first_exposed_at ASC, created_at ASC, rowid ASC LIMIT 1",
            (result, REUSED),
        )

    @staticmethod
    def _known_groups(catalog_info) -> List[str]:
        groups = set()
        for _s, _n, _r, row in catalog_info:
            if row is not None and int(row["groups_known"]) == 1:
                groups.update(Store.loads(row["groups_json"], []))
        return sorted(groups)

    def _duplicate_submit(self, row, audience, canon, unit, reuse):
        same = (
            row["audience"] == audience
            and self.store.loads(row["sources_json"], []) == canon
            and int(row["unit"]) == unit
            and (row["reuse"] if row["reuse"] is not None else None)
                == (reuse if reuse is not None else None)
        )
        if not same:
            raise ConflictError(
                f"submit.id={row['id']} 已被不同内容占用；id 必须唯一")
        # 重放返回与首次提交同形的处置结果，并标注 idempotent。
        if row["state"] == DENIED:
            return {"status": "denied", "kind": "submit", "id": row["id"],
                    "reason": row["reason_code"],
                    "detail": self.store.loads(row["reason_detail_json"]),
                    "idempotent": True}
        if row["state"] == REUSED:
            return {"status": "reused", "kind": "submit", "id": row["id"],
                    "result": row["result"], "cost": 0,
                    "owner": row["derived_from"], "idempotent": True,
                    "result_available": True}
        return {"status": "accepted", "kind": "submit", "id": row["id"],
                "state": row["state"], "idempotent": True,
                "groups": self.store.loads(row["groups_json"], [])}

    def _persist_denial(self, qid, audience, canon, unit, reuse, reason_code,
                        detail, at, raw_record, *, groups, derived_from=None):
        scope_complete = 1 if detail.get("missing_revisions") else self._scope_complete(canon)
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT INTO query(id, audience, sources_json, unit, reuse, derived_from, "
                "state, reason_code, reason_detail_json, groups_json, scope_complete, "
                "created_at, updated_at, record_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (qid, audience, self.store.dumps(canon), unit, reuse, derived_from,
                 DENIED, reason_code, self.store.dumps(detail),
                 self.store.dumps(sorted(groups)), scope_complete, at, at,
                 self.store.dumps(raw_record)),
            )
            self._event(conn, at, "submit_denied", qid,
                        {"reason": reason_code, **detail})
        return {"status": "denied", "kind": "submit", "id": qid,
                "reason": reason_code, "detail": detail,
                "disposition_visible_to": audience}

    def _persist_reuse_read(self, qid, audience, canon, unit, reuse, owner,
                            at, raw_record):
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT INTO query(id, audience, sources_json, unit, reuse, derived_from, "
                "state, reason_code, result, groups_json, scope_complete, access_mode, "
                "public_fact, fully_settled, settlement, created_at, updated_at, record_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (qid, audience, self.store.dumps(canon), 0, reuse, owner["id"],
                 REUSED, None, reuse, owner["groups_json"], 1, "active", 1, 1, "full",
                 at, at, self.store.dumps(raw_record)),
            )
            conn.execute(
                "INSERT INTO read_log(at, query_id, audience, via) VALUES(?,?,?,?)",
                (at, qid, audience, "free_exact_reuse"),
            )
            self._event(conn, at, "reuse_read", qid,
                        {"owner": owner["id"], "result": reuse, "cost": 0})
        return {"status": "reused", "kind": "submit", "id": qid, "result": reuse,
                "cost": 0, "owner": owner["id"], "result_available": True}

    def _scope_complete(self, canon: List[str]) -> int:
        for source in canon:
            name, rev = parse_source(source)
            row = self.store.query_one(
                "SELECT groups_known FROM catalog WHERE name=? AND rev=?", (name, rev))
            if row is None or int(row["groups_known"]) == 0:
                return 0
        return 1

    def _fresh_reserve(self, qid, audience, canon, unit, reuse, at, raw_record, *,
                       derived_from, scope_change, catalog_info):
        # 授权：受众必须在每一个来源修订当前公布的 audiences 中。
        unauthorized = []
        groups = set()
        scope_complete = True
        for source, _n, _r, row in catalog_info:
            audiences = self._catalog_audiences(row)
            if audience not in audiences:
                unauthorized.append(source)
            if int(row["groups_known"]) == 0:
                scope_complete = False
            else:
                groups.update(self.store.loads(row["groups_json"], []))
        groups_sorted = sorted(groups)

        with self.store.transaction() as conn:
            if unauthorized:
                detail = {"unauthorized_sources": unauthorized}
                if scope_change:
                    detail["scope_change"] = scope_change
                conn.execute(
                    "INSERT INTO query(id, audience, sources_json, unit, reuse, derived_from, "
                    "state, reason_code, reason_detail_json, groups_json, scope_complete, "
                    "created_at, updated_at, record_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (qid, audience, self.store.dumps(canon), unit, reuse, derived_from,
                     DENIED, REASON_NOT_AUTHORIZED, self.store.dumps(detail),
                     self.store.dumps(groups_sorted), 1 if scope_complete else 0,
                     at, at, self.store.dumps(raw_record)),
                )
                self._event(conn, at, "submit_denied", qid,
                            {"reason": REASON_NOT_AUTHORIZED, **detail})
                return {"status": "denied", "kind": "submit", "id": qid,
                        "reason": REASON_NOT_AUTHORIZED, "detail": detail}

            # 关键：所有预算域在同一个 IMMEDIATE 事务内检查并预留。
            missing_budget: List[str] = []
            insufficient: Dict[str, Dict[str, int]] = {}
            ledgers: Dict[str, dict] = {}
            for group in groups_sorted:
                budget = conn.execute(
                    "SELECT cap FROM budget WHERE grp=?", (group,)).fetchone()
                if budget is None:
                    missing_budget.append(group)
                    continue
                agg = conn.execute(
                    "SELECT "
                    "COALESCE(SUM(CASE WHEN status='committed' THEN amount END),0) AS committed, "
                    "COALESCE(SUM(CASE WHEN status='held' THEN amount END),0) AS reserved "
                    "FROM hold WHERE grp=?", (group,)).fetchone()
                cap = int(budget["cap"])
                available = cap - int(agg["committed"]) - int(agg["reserved"])
                ledgers[group] = {"cap": cap, "available_before": available}
                if available < unit:
                    insufficient[group] = {
                        "cap": cap,
                        "committed": int(agg["committed"]),
                        "reserved": int(agg["reserved"]),
                        "available": available,
                        "unit": unit,
                    }

            if missing_budget or insufficient:
                reason = (REASON_NO_BUDGET_DECLARED if missing_budget
                          and not insufficient else REASON_INSUFFICIENT_BUDGET)
                detail = {"missing_budget": missing_budget,
                          "insufficient": insufficient}
                if scope_change:
                    detail["scope_change"] = scope_change
                conn.execute(
                    "INSERT INTO query(id, audience, sources_json, unit, reuse, derived_from, "
                    "state, reason_code, reason_detail_json, groups_json, scope_complete, "
                    "created_at, updated_at, record_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (qid, audience, self.store.dumps(canon), unit, reuse, derived_from,
                     DENIED, reason, self.store.dumps(detail),
                     self.store.dumps(groups_sorted), 1 if scope_complete else 0,
                     at, at, self.store.dumps(raw_record)),
                )
                self._event(conn, at, "submit_denied", qid,
                            {"reason": reason, **detail})
                return {"status": "denied", "kind": "submit", "id": qid,
                        "reason": reason, "detail": detail}

            state = PREPARING if scope_complete else SCOPE_PENDING
            detail = {"scope_change": scope_change} if scope_change else None
            conn.execute(
                "INSERT INTO query(id, audience, sources_json, unit, reuse, derived_from, "
                "state, reason_detail_json, groups_json, scope_complete, "
                "created_at, updated_at, record_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (qid, audience, self.store.dumps(canon), unit, reuse, derived_from,
                 state, self.store.dumps(detail) if detail else None,
                 self.store.dumps(groups_sorted),
                 1 if scope_complete else 0, at, at, self.store.dumps(raw_record)),
            )
            for group in groups_sorted:
                conn.execute(
                    "INSERT INTO hold(query_id, grp, amount, status, note, updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (qid, group, unit, HELD, "arrival_reservation", at),
                )
            after = {g: self.store.ledger(g) for g in groups_sorted}
            self._event(conn, at, "reserved", qid, {
                "groups": groups_sorted, "unit": unit,
                "scope_complete": scope_complete,
                "availability_after": {g: v["available"] for g, v in after.items()},
            })
            return {
                "status": "accepted", "kind": "submit", "id": qid, "state": state,
                "groups": groups_sorted, "scope_complete": scope_complete,
                "unit": unit, "holds": [{"group": g, "amount": unit, "status": HELD}
                                        for g in groups_sorted],
                "availability": after,
                **({"scope_change": scope_change} if scope_change else {}),
            }

    # ---- 范围补齐后的重新评估 --------------------------------------------

    def _apply_completed_scope(self, name: str, rev: int, at: int) -> List[str]:
        source_ref = f"{name}@{rev}"
        # 除了 scope_pending，已经收到 unknown / withdraw 而进入 verifying、
        # 但范围仍不完整的单也要在补齐时重新评估，避免新域永远漏扣。
        rows = self.store.query_all(
            f"SELECT {_QUERY_COLUMNS} FROM query "
            "WHERE scope_complete=0 AND state IN (?, ?)",
            (SCOPE_PENDING, VERIFYING))
        affected = []
        for q in rows:
            sources = self.store.loads(q["sources_json"], [])
            if source_ref not in sources:
                continue
            affected.append(q["id"])
            self._expand_or_fail(q["id"], at)
        return affected

    def _expand_or_fail(self, qid: str, at: int) -> None:
        with self._lock:
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None or q["state"] in (DENIED, NEVER_PUBLISHED):
                return
            canon = self.store.loads(q["sources_json"], [])
            catalog_info = self._catalog_map(self.store.conn, canon)
            groups = set()
            scope_complete = True
            for _s, _n, _r, row in catalog_info:
                if row is None or int(row["groups_known"]) == 0:
                    scope_complete = False
                else:
                    groups.update(self.store.loads(row["groups_json"], []))
            groups_sorted = sorted(groups)
            unit = int(q["unit"])

            with self.store.transaction() as conn:
                existing = {r["grp"]: r for r in conn.execute(
                    "SELECT grp, status FROM hold WHERE query_id=?", (qid,)).fetchall()}
                new_groups = [g for g in groups_sorted if g not in existing]

                if not scope_complete:
                    # 本次 announce 并未真正补齐全部来源；更新已知群体并调和状态。
                    conn.execute("UPDATE query SET groups_json=?, updated_at=? WHERE id=?",
                                 (self.store.dumps(groups_sorted), at, qid))
                    q2 = conn.execute(
                        f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,)).fetchone()
                    self._reconcile(conn, q2, at)
                    return

                insufficient = {}
                missing_budget = []
                for g in new_groups:
                    b = conn.execute("SELECT cap FROM budget WHERE grp=?", (g,)).fetchone()
                    if b is None:
                        missing_budget.append(g)
                        continue
                    agg = conn.execute(
                        "SELECT COALESCE(SUM(CASE WHEN status='committed' THEN amount END),0) c, "
                        "COALESCE(SUM(CASE WHEN status='held' THEN amount END),0) r "
                        "FROM hold WHERE grp=?", (g,)).fetchone()
                    available = int(b["cap"]) - int(agg["c"]) - int(agg["r"])
                    if available < unit:
                        insufficient[g] = available

                already_public = any(st == COMMITTED for st in
                                     (r["status"] for r in existing.values()))

                if (missing_budget or insufficient) and not already_public:
                    # 尚未发生任何公开事实：联合查询不能半套预留，整单拒绝并
                    # 释放本单全部 held（committed 不存在）。
                    conn.execute(
                        "UPDATE hold SET status=?, note=?, updated_at=? WHERE query_id=? "
                        "AND status=?",
                        (RELEASED, "scope_completion_failed_rollback", at, qid, HELD))
                    conn.execute(
                        "UPDATE query SET state=?, reason_code=?, reason_detail_json=?, "
                        "scope_complete=1, groups_json=?, updated_at=? WHERE id=?",
                        (DENIED, REASON_INSUFFICIENT_BUDGET,
                         self.store.dumps({"missing_budget": missing_budget,
                                           "insufficient": insufficient,
                                           "hint": "范围补齐后新增预算域不足，"
                                                   "已原子释放先前预留"}),
                         self.store.dumps(groups_sorted), at, qid))
                    self._event(conn, at, "scope_expansion_denied", qid,
                                {"missing_budget": missing_budget,
                                 "insufficient": insufficient})
                    return

                for g in new_groups:
                    forced = g in insufficient or g in missing_budget
                    conn.execute(
                        "INSERT INTO hold(query_id, grp, amount, status, note, updated_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (qid, g, unit, HELD,
                         "scope_breach_forced_hold" if forced
                         else "scope_completion_reservation", at))
                conn.execute(
                    "UPDATE query SET groups_json=?, scope_complete=1, updated_at=? "
                    "WHERE id=?",
                    (self.store.dumps(groups_sorted), at, qid))
                if missing_budget or insufficient:
                    # 已有公开事实无法回滚：宁可把 available 记成负数并挂事故，
                    # 也不能假装新域不欠钱。
                    self._event(conn, at, "scope_expansion_breach", qid, {
                        "missing_budget": missing_budget,
                        "insufficient": insufficient,
                        "rule": "公开事实已发生，新增域强制挂账，等待运营补额度",
                    })
                else:
                    self._event(conn, at, "scope_completed", qid,
                                {"groups": groups_sorted, "new_groups": new_groups})
                q2 = conn.execute(
                    f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,)).fetchone()
                self._reconcile(conn, q2, at)

    # ---- 计算确认 ---------------------------------------------------------

    def confirm_computation(self, qid: str, result: str,
                            at: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            if not isinstance(qid, str) or not qid:
                raise ValidationError("computed.id 必须是非空字符串")
            if not isinstance(result, str) or not result:
                raise ValidationError("computed.result 必须是非空的不可变标识")
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None:
                raise NotFoundError(f"未知查询 id：{qid}")
            if q["result"] is not None:
                if q["result"] != result:
                    raise ConflictError(
                        f"{qid} 的 result 已固定为 {q['result']!r}，不可变更为 {result!r}")
                return {"status": "ok", "kind": "computed", "id": qid,
                        "result": result, "idempotent": True, "state": q["state"]}
            if q["state"] == DENIED:
                # 晚到的计算确认不能复活已拒绝请求，也不会产生任何扣减。
                return {"status": "late_ignored", "kind": "computed", "id": qid,
                        "reason": q["reason_code"]}

            with self.store.transaction() as conn:
                conn.execute("UPDATE query SET result=?, updated_at=? WHERE id=?",
                             (result, at, qid))
                self._event(conn, at, "computed", qid, {"result": result})
                q2 = conn.execute(
                    f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,)).fetchone()
                state = self._reconcile(conn, q2, at)
            return {"status": "ok", "kind": "computed", "id": qid,
                    "result": result, "state": state}

    # ---- 撤回 / 收紧 / 过期 ----------------------------------------------

    def withdraw(self, qid: str, reason: str = "revoke",
                 audiences: Optional[List[str]] = None,
                 at: Optional[int] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None:
                raise NotFoundError(f"未知查询 id：{qid}")
            if reason not in ("revoke", "tighten", "expire"):
                raise ValidationError(
                    "withdraw.reason 应为 revoke / tighten / expire")
            if audiences is not None and (
                not isinstance(audiences, list)
                or not all(isinstance(a, str) for a in audiences)
            ):
                raise ValidationError("withdraw.audiences 应为字符串数组")

            with self.store.transaction() as conn:
                if q["state"] == DENIED:
                    return {"status": "noop", "id": qid, "state": DENIED,
                            "reason": q["reason_code"]}
                mode = {"revoke": "revoked", "tighten": "tightened",
                        "expire": "expired"}[reason]
                allowlist = self.store.dumps(audiences) if audiences is not None else None
                conn.execute(
                    "UPDATE query SET withdraw_requested=1, access_mode=?, "
                    "access_allowlist_json=COALESCE(?, access_allowlist_json), "
                    "updated_at=? WHERE id=?",
                    (mode, allowlist, at, qid))
                if int(q["public_fact"]) == 1:
                    kind = "access_closed" if mode in ("revoked", "expired") \
                        else "access_tightened"
                    self._event(conn, at, kind, qid, {
                        "access_mode": mode,
                        "allowlist": audiences,
                        "budget": "committed_preserved",
                    })
                else:
                    # 尚未确认公开：绝不直接释放 hold，等待发布端 never 确证。
                    self._event(conn, at, "withdrawal_pending_verification", qid, {
                        "access_mode": mode,
                        "allowlist": audiences,
                        "rule": "只有发布端确认从未对外开放才释放预留",
                    })
                q2 = conn.execute(
                    f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,)).fetchone()
                state = self._reconcile(conn, q2, at)
            return {"status": "ok", "kind": "withdraw", "id": qid,
                    "state": state, "access_mode": mode,
                    "budget_released": False,
                    "note": "已公开则保留消耗；未确证 never 前预留不释放"}

    # ---- 发布端回执 -------------------------------------------------------

    def confirm_publication(self, qid: str, state: str,
                            scope: Optional[frozenset] = None,
                            at: Optional[int] = None,
                            raw: Optional[dict] = None) -> Dict[str, Any]:
        with self._lock:
            at = self._at(at)
            if state not in ("exposed", "never", "unknown"):
                raise ValidationError("published.state 应为 exposed / never / unknown")
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None:
                raise NotFoundError(f"未知查询 id：{qid}")
            specified = scope is not None
            scope_list = sorted(scope) if specified else None

            with self.store.transaction() as conn:
                conn.execute(
                    "INSERT INTO receipt(at, query_id, state, specified, scope_json, "
                    "raw_json) VALUES(?,?,?,?,?,?)",
                    (at, qid, state, 1 if specified else 0,
                     self.store.dumps(scope_list) if specified else None,
                     self.store.dumps(raw or {"id": qid, "state": state})),
                )
                groups = self.store.loads(q["groups_json"], [])

                if q["state"] != DENIED:
                    if state == "unknown":
                        self._event(conn, at, "receipt_unknown", qid,
                                    {"rule": "unknown 不释放任何预留"})
                    elif state == "exposed":
                        self._apply_exposed(conn, qid, groups,
                                            scope if specified else None, specified, at)
                    elif state == "never":
                        self._apply_never(conn, qid, groups,
                                          scope if specified else None, specified, at)

                q2 = conn.execute(
                    f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,)).fetchone()
                new_state = self._reconcile(conn, q2, at)
            return {"status": "ok", "kind": "published", "id": qid,
                    "receipt": state, "scope": scope_list, "state_after": new_state,
                    **self._budget_summary(qid)}

    def _apply_exposed(self, conn, qid, groups, scope, specified: bool, at: int) -> None:
        # 未给范围时，exposed 按最保守方向覆盖本单全部已登记群体：
        # 公开事实方向的不确定性不能让预算消失。
        targets = set(groups) if not specified else set(scope)
        if WILDCARD in targets:
            targets = set(groups)
        if specified:
            foreign = sorted(g for g in targets if g not in set(groups))
            if foreign:
                self._event(conn, at, "receipt_scope_ignored", qid,
                            {"foreign_groups": foreign})
        for group in groups:
            if group not in targets:
                continue
            hold = conn.execute(
                "SELECT status, amount FROM hold WHERE query_id=? AND grp=?",
                (qid, group)).fetchone()
            if hold is None:
                continue
            if hold["status"] == HELD:
                conn.execute(
                    "UPDATE hold SET status=?, note=?, updated_at=? "
                    "WHERE query_id=? AND grp=?",
                    (COMMITTED, "publisher_confirmed_exposed", at, qid, group))
            elif hold["status"] == RELEASED:
                # 早先被（部分范围的 never）释放，后被 exposed 推翻：
                # 公开事实必须重新记账；这可能使 available 变负，交由运营处置，
                # 但绝不静默抹掉已发生的公开。
                conn.execute(
                    "UPDATE hold SET status=?, note=?, updated_at=? "
                    "WHERE query_id=? AND grp=?",
                    (COMMITTED, "exposed_overturned_earlier_never", at, qid, group))
                ledger = self.store.ledger(group)
                self._event(conn, at, "public_fact_recharged", qid,
                            {"group": group, "amount": int(hold["amount"]),
                             "available_now": ledger["available"]})

    def _apply_never(self, conn, qid, groups, scope, specified: bool, at: int) -> None:
        if not specified:
            # 关键防线：缺少参与者范围的 never 不能证明全部参与者未开放，
            # 待核实部分继续预留，绝不释放。
            self._event(conn, at, "never_without_scope_kept_hold", qid, {
                "rule": "发布端回报缺少参与者范围，待核实部分不能假装已释放",
                "groups_still_held": groups,
            })
            return
        target = set(scope)
        if WILDCARD in target:
            target = set(groups)
        for group in groups:
            if group not in target:
                continue  # 不在回执范围内 => 待核实，继续 held
            hold = conn.execute(
                "SELECT status FROM hold WHERE query_id=? AND grp=?",
                (qid, group)).fetchone()
            if hold is None or hold["status"] != HELD:
                # committed 是已公开事实，never 不能回滚；released 幂等。
                if hold is not None and hold["status"] == COMMITTED:
                    self._event(conn, at, "never_after_exposed_ignored", qid,
                                {"group": group})
                continue
            conn.execute(
                "UPDATE hold SET status=?, note=?, updated_at=? "
                "WHERE query_id=? AND grp=?",
                (RELEASED, "publisher_confirmed_never_for_scope", at, qid, group))
            self._event(conn, at, "hold_released", qid, {"group": group})

    # ---- 状态调和 ---------------------------------------------------------

    def _reconcile(self, conn, q, at: int) -> str:
        qid = q["id"]
        holds = conn.execute(
            "SELECT grp, status FROM hold WHERE query_id=?", (qid,)).fetchall()
        statuses = [h["status"] for h in holds]
        any_committed = COMMITTED in statuses
        any_held = HELD in statuses
        receipt_count = conn.execute(
            "SELECT COUNT(*) AS n FROM receipt WHERE query_id=?", (qid,)).fetchone()["n"]

        old_state = q["state"]
        public_fact = int(q["public_fact"]) == 1 or any_committed
        scope_complete = int(q["scope_complete"]) == 1
        groups = self.store.loads(q["groups_json"], [])

        new_state = old_state
        fully_settled = int(q["fully_settled"])
        settlement = q["settlement"]

        if old_state != DENIED and scope_complete and receipt_count > 0 and not any_held:
            if any_committed:
                committed_groups = [h["grp"] for h in holds if h["status"] == COMMITTED]
                full = (len(holds) == len(groups)
                        and len(committed_groups) == len(groups)
                        and len(groups) > 0)
                settlement = "full" if full else "mixed"
                fully_settled = 1
                new_state = (ACCESS_CLOSED if q["access_mode"] in ("revoked", "expired")
                             else PUBLISHED)
            else:
                settlement = "never"
                fully_settled = 1
                new_state = NEVER_PUBLISHED
        elif old_state != DENIED:
            fully_settled = 0
            if int(q["withdraw_requested"]) == 1 or receipt_count > 0:
                new_state = VERIFYING
            elif q["result"]:
                new_state = AWAITING_PUBLICATION if scope_complete else SCOPE_PENDING
            else:
                new_state = PREPARING if scope_complete else SCOPE_PENDING

        first_exposed_at = q["first_exposed_at"]
        if any_committed and first_exposed_at is None:
            first_exposed_at = at
            self._event(conn, at, "attribution_finalized", qid, {
                "audience": q["audience"],
                "sources": self.store.loads(q["sources_json"], []),
                "groups": groups,
                "unit": int(q["unit"]),
                "result": q["result"],
                "rule": "结果确认公开后，归属与公开消耗永久保留",
            })

        conn.execute(
            f"UPDATE query SET state=?, public_fact=?, fully_settled=?, settlement=?, "
            "first_exposed_at=?, updated_at=? WHERE id=?",
            (new_state, 1 if public_fact else 0, fully_settled, settlement,
             first_exposed_at, at, qid),
        )
        return new_state

    def _budget_summary(self, qid: str) -> Dict[str, Any]:
        rows = self.store.query_all(
            "SELECT grp FROM hold WHERE query_id=? ORDER BY grp", (qid,))
        groups = [r["grp"] for r in rows]
        return {"budgets": {g: self.store.ledger(g) for g in groups}}

    # ---- 结果读取授权（受众视图） ----------------------------------------

    def _effective_public_row(self, q):
        if q["state"] == REUSED and q["derived_from"]:
            owner = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (q["derived_from"],))
            return owner or q
        return q

    def _serve_decision(self, q, audience: str):
        """返回 (allowed, reason_code)。调用方需持锁 / 自行查询。"""
        target = self._effective_public_row(q)
        if target["state"] == DENIED:
            return False, target["reason_code"] or DENIED
        if not int(target["public_fact"]) or not target["result"]:
            return False, REASON_NOT_PUBLIC_YET
        if not int(target["fully_settled"]) or target["settlement"] != "full":
            return False, "settlement_incomplete"
        mode = target["access_mode"]
        if mode in ("revoked", "expired"):
            return False, REASON_ACCESS_CLOSED
        sources = self.store.loads(target["sources_json"], [])
        if mode == "tightened":
            allowlist = self.store.loads(target["access_allowlist_json"], None)
            if allowlist is not None and audience not in allowlist:
                return False, "access_tightened"
            if allowlist is None and audience != target["audience"]:
                return False, "access_tightened"
        elif audience != target["audience"]:
            # 新受众必须持有自己独立计费的发布票，不能蹭原归属。
            return False, "not_original_audience"
        unauthorized = self._catalog_allows(self.store.conn, sources, audience)
        if unauthorized:
            return False, REASON_NOT_AUTHORIZED
        return True, None

    def audience_disposition(self, qid: str, audience: str,
                             at: Optional[int] = None) -> Dict[str, Any]:
        """关闭 / 被拒受众仍可查询“自己请求”的处置，但无权取得 result 引用。"""
        with self._lock:
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None:
                raise NotFoundError(f"未知查询 id：{qid}")
            if audience != q["audience"]:
                # 不向非请求归属方泄露该单的任何细节。
                return {"found": False, "id": qid}
            allowed, reason = self._serve_decision(q, audience)
            return {
                "id": qid,
                "audience": audience,
                "state": q["state"],
                "reason": q["reason_code"] or reason,
                "result_available": allowed,
                # result 标识是结果正文的唯一凭据代理；无权时一律不给出。
                "result": q["result"] if allowed else None,
                "disposition": self._disposition_text(q, allowed, reason),
            }

    @staticmethod
    def _disposition_text(q, allowed: bool, reason: Optional[str]) -> str:
        if allowed:
            return "结果在授权有效期内可读"
        mapping = {
            REASON_INSUFFICIENT_BUDGET: "预算域真实可用量不足，请求已拒绝",
            REASON_NOT_AUTHORIZED: "当前受众未被来源修订授权",
            REASON_UNKNOWN_REVISION: "引用的集合修订不存在",
            REASON_REUSE_UNAVAILABLE: "没有可复用的已公开结果归属",
            REASON_NOT_PUBLIC_YET: "结果尚未经发布端确认对外可读",
            REASON_ACCESS_CLOSED: "发布安排已撤销 / 过期，后续访问关闭",
            "access_tightened": "访问条件已收紧，该受众不在新许可范围内",
            "settlement_incomplete": "联合预算域尚未全部结清，暂不提供结果",
            "not_original_audience": "非原受众，需以独立查询判断预算与授权",
        }
        if q["state"] == SCOPE_PENDING:
            return "交集范围未补齐，未知群体不按不相交处理，暂不可发布"
        if q["state"] == VERIFYING:
            return "发布事实核实中，相关预算预留继续保留"
        if q["state"] == NEVER_PUBLISHED:
            return "已确证从未对外开放，预留已释放"
        return mapping.get(reason or "", "当前无权获取结果正文，处置记录保留")

    # ---- inspect ----------------------------------------------------------

    def inspect_group(self, group: str) -> Dict[str, Any]:
        with self._lock:
            if not isinstance(group, str) or not group:
                raise ValidationError("inspect.group 必须是非空字符串")
            ledger = self.store.ledger(group)
            holds = self.store.query_all(
                """
                SELECT h.query_id, h.amount, h.status, h.note, q.audience, q.state
                FROM hold h JOIN query q ON q.id = h.query_id
                WHERE h.grp=? AND h.status IN (?, ?)
                ORDER BY h.status DESC, h.query_id
                """,
                (group, HELD, COMMITTED),
            )
            ledger["active_charges"] = [
                {"id": r["query_id"], "amount": int(r["amount"]),
                 "status": r["status"], "audience": r["audience"],
                 "query_state": r["state"], "note": r["note"]}
                for r in holds
            ]
            return {"status": "ok", "kind": "inspect", "view": "group", **ledger}

    def inspect_query(self, qid: str) -> Dict[str, Any]:
        with self._lock:
            q = self.store.query_one(
                f"SELECT {_QUERY_COLUMNS} FROM query WHERE id=?", (qid,))
            if q is None:
                raise NotFoundError(f"未知查询 id：{qid}")
            holds = self.store.query_all(
                "SELECT grp, amount, status, note FROM hold WHERE query_id=? ORDER BY grp",
                (qid,))
            receipts = self.store.query_all(
                "SELECT at, state, specified, scope_json FROM receipt "
                "WHERE query_id=? ORDER BY seq", (qid,))
            events = self.store.query_all(
                "SELECT at, kind, payload_json FROM event WHERE query_id=? "
                "ORDER BY seq", (qid,))
            allowed_for_owner, serve_reason = self._serve_decision(q, q["audience"])
            return {
                "status": "ok", "kind": "inspect", "view": "id",
                "id": q["id"], "audience": q["audience"],
                "state": q["state"], "reason": q["reason_code"],
                "reason_detail": self.store.loads(q["reason_detail_json"]),
                "sources": self.store.loads(q["sources_json"], []),
                "groups": self.store.loads(q["groups_json"], []),
                "scope_complete": bool(q["scope_complete"]),
                "unit": int(q["unit"]),
                "reuse": q["reuse"], "derived_from": q["derived_from"],
                "result": q["result"],
                "public_fact": bool(q["public_fact"]),
                "fully_settled": bool(q["fully_settled"]),
                "settlement": q["settlement"],
                "access_mode": q["access_mode"],
                "access_allowlist": self.store.loads(q["access_allowlist_json"]),
                "withdraw_requested": bool(q["withdraw_requested"]),
                "owner_result_available": allowed_for_owner,
                "serve_reason": serve_reason,
                "first_exposed_at": q["first_exposed_at"],
                "holds": [{"group": h["grp"], "amount": int(h["amount"]),
                           "status": h["status"], "note": h["note"]} for h in holds],
                "receipts": [
                    {"at": r["at"], "state": r["state"],
                     "scope_specified": bool(r["specified"]),
                     "scope": self.store.loads(r["scope_json"])}
                    for r in receipts
                ],
                "events": [
                    {"at": e["at"], "kind": e["kind"],
                     "payload": self.store.loads(e["payload_json"])}
                    for e in events
                ],
                "budgets": {h["grp"]: self.store.ledger(h["grp"]) for h in holds},
            }

    def overview(self) -> Dict[str, Any]:
        with self._lock:
            groups = [self.store.ledger(g) for g in self.store.all_groups()]
            counts = {r["state"]: int(r["n"]) for r in self.store.query_all(
                "SELECT state, COUNT(*) n FROM query GROUP BY state")}
            return {"status": "ok", "kind": "inspect", "view": "overview",
                    "groups": groups, "query_states": counts}

    # ---- 可读性别名 -------------------------------------------------------

    def availability(self, group: str) -> Dict[str, Any]:
        """查询某个预算域的真实可用量（cap - committed - held）。"""
        return self.inspect_group(group)

    def query_status(self, qid: str) -> Dict[str, Any]:
        """运营 / 核查视角的单据全量状态。"""
        return self.inspect_query(qid)
