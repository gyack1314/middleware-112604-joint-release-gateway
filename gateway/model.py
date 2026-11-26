"""领域常量、错误类型与输入归一化助手。

本模块不依赖存储与引擎，方便单独阅读“契约中的名词”。
"""

from __future__ import annotations

import re
from typing import Any, FrozenSet, List, Optional, Tuple


# ---- 查询生命周期状态 -----------------------------------------------------

#: 已拒绝（授权 / 预算 / 修订不存在等），终态；处置仍可查。
DENIED = "denied"
#: 至少有一个来源修订的 groups=null，尚不能确定全部预算域；
#: 已确定涉及的域仍在请求到达时原子预留，未知部分绝不按“不相交”放过。
SCOPE_PENDING = "scope_pending"
#: 全部预算域已确定并预留成功，等待仿真端确认计算。
PREPARING = "preparing"
#: 计算已确认（result 标识已登记），等待发布端确认实际可读。
AWAITING_PUBLICATION = "awaiting_publication"
#: 已收到撤回 / unknown 回执 / 部分范围回执，仍有预留未结清，正在核实。
VERIFYING = "verifying"
#: 发布端已确认对外可读且全部域已结清，结果可向当前仍有权的原受众提供。
PUBLISHED = "published"
#: 曾确认公开，但随后被撤销 / 收紧 / 过期；后续访问关闭，预算事实保留。
ACCESS_CLOSED = "access_closed"
#: 所有预留均凭“从未对外开放”的确证释放，终态。
NEVER_PUBLISHED = "never_published"

# ---- 每个群体预留行的状态 -------------------------------------------------

HELD = "held"                # 预留中，占用真实可用量
COMMITTED = "committed"      # 已确认公开，成为不可撤销的公开事实消耗
RELEASED = "released"        # 已凭 never 确证释放

# ---- 拒绝 / 错误原因码 ----------------------------------------------------

REASON_UNKNOWN_REVISION = "unknown_revision"
REASON_NOT_AUTHORIZED = "not_authorized"
REASON_INSUFFICIENT_BUDGET = "insufficient_budget"
REASON_NO_BUDGET_DECLARED = "no_budget_declared"
REASON_INVALID_REQUEST = "invalid_request"
REASON_REUSE_UNAVAILABLE = "reuse_unavailable"
REASON_NOT_PUBLIC_YET = "not_public_yet"
REASON_ACCESS_CLOSED = "access_closed"
REASON_CONFLICT = "conflict"


class GatewayError(Exception):
    """网关业务错误基类。``code`` 是稳定的机器可读原因码。"""

    code = "gateway_error"

    def __init__(self, message: str = "", *, code: Optional[str] = None):
        super().__init__(message or self.__class__.__doc__ or self.code)
        if code:
            self.code = code


class ValidationError(GatewayError):
    code = REASON_INVALID_REQUEST


class ConflictError(GatewayError):
    code = REASON_CONFLICT


class NotFoundError(GatewayError):
    code = "not_found"


_SOURCE_RE = re.compile(r"^(?P<name>.+)@(?P<rev>\d+)$")


def parse_source(source: Any) -> Tuple[str, int]:
    """把 ``"a@1"`` 解析为 ``("a", 1)``。修订必须是非负整数。"""
    if not isinstance(source, str):
        raise ValidationError(f"来源修订必须是字符串 name@rev：{source!r}")
    match = _SOURCE_RE.match(source.strip())
    if not match:
        raise ValidationError(f"来源修订格式应为 name@整数rev：{source!r}")
    return match.group("name"), int(match.group("rev"))


def canonical_sources(sources: Any) -> List[str]:
    """来源修订的规范形式：逐个校验后排序（保留重复引用）。

    “同一来源修订”的比较与提交顺序无关；README 明确允许重复引用，
    因此排序后保留重复项（多重集语义）。
    """
    if not isinstance(sources, list) or not sources:
        raise ValidationError("sources 应为非空数组，元素形如 name@rev")
    canonical: List[str] = []
    for source in sources:
        name, rev = parse_source(source)
        canonical.append(f"{name}@{rev}")
    return sorted(canonical)


def _coerce_group_collection(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, str):
        items = [part.strip() for part in value.split(",") if part.strip()]
        return items
    if isinstance(value, dict):
        # 兼容 {"groups": [...]} / {"scope": {"groups": [...]}} 等嵌套写法。
        for key in ("groups", "participants", "covered_groups", "scope"):
            if key in value:
                return _coerce_group_collection(value[key])
        return None
    if isinstance(value, (list, tuple, set, frozenset)):
        items: List[str] = []
        for item in value:
            if not isinstance(item, str):
                return None
            items.append(item)
        return items
    return None


#: 发布端回执中可能出现的“参与者范围”字段名。资料契约只固定了 state，
#: 对范围字段的常见命名都予以接受，核心语义是“回执结论覆盖哪些群体”。
SCOPE_KEYS = (
    "scope",
    "participant_scope",
    "covered_groups",
    "groups",
    "participants",
)

#: 显式表示“覆盖本单全部参与者 / 群体”的范围写法。
WILDCARD = "*"
_WILDCARD_TOKENS = {"*", "all", "full", "entire", "complete", "全部", "所有", "全量"}


def extract_scope(record: dict) -> Tuple[bool, FrozenSet[str]]:
    """从 published 回执中提取参与者范围（结论覆盖的群体集合）。

    返回 ``(specified, groups)``：

    - ``specified is False``：回执没有给出可用的参与者范围。此时对
      ``state=never`` 绝不能释放预留——“待核实部分不能假装已释放”。
    - ``specified is True``：``groups`` 是回执结论明确覆盖的群体；
      可能为空集（表示明确不覆盖任何群体，同样不能释放）。若调用方不知道
      具体群体名而只给了 all/``*``/全部 等通配写法，集合为 ``{WILDCARD}``，
      由引擎展开为本单全部已登记群体。
    """
    if not isinstance(record, dict):
        return False, frozenset()
    for key in SCOPE_KEYS:
        if key not in record:
            continue
        value = record[key]
        if isinstance(value, str) and value.strip().lower() in _WILDCARD_TOKENS:
            return True, frozenset({WILDCARD})
        if isinstance(value, dict):
            # 例如 {"scope": {"all": true}} / {"coverage": "full"} 的宽松识别。
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, str) and \
                        sub_value.strip().lower() in _WILDCARD_TOKENS:
                    return True, frozenset({WILDCARD})
                if sub_key.lower() in ("all", "full", "complete") and sub_value:
                    return True, frozenset({WILDCARD})
        items = _coerce_group_collection(value)
        if items is None:
            continue
        return True, frozenset(items)
    return False, frozenset()


def require_int(value: Any, field: str, *, minimum: int = 0) -> int:
    # bool 是 int 的子类，但业务上不接受 true/false 充当额度。
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} 必须是整数")
    if value < minimum:
        raise ValidationError(f"{field} 不能小于 {minimum}")
    return value
