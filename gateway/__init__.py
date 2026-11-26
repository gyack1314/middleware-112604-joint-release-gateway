"""协作分析查询发布网关。

职责边界：
- 维护集合修订（catalog）、群体预算域（budget）、查询归属与预留；
- 在请求到达时对所有“已确定涉及”的预算域做原子预留；
- 持久化状态，网关重启后可继续处理计算 / 等发布回执 / 授权调整中的请求；
- 不实现任何统计分析或隐私算法，result 只是仿真端给的不可变标识。

规则出处见仓库 README.md 与 GATEWAY.md。
"""

from .model import (
    ACCESS_CLOSED,
    AWAITING_PUBLICATION,
    DENIED,
    NEVER_PUBLISHED,
    PREPARING,
    PUBLISHED,
    SCOPE_PENDING,
    VERIFYING,
    COMMITTED,
    HELD,
    RELEASED,
    GatewayError,
    ValidationError,
    ConflictError,
    NotFoundError,
    parse_source,
    canonical_sources,
    extract_scope,
)
from .engine import Gateway
from .runner import run_scenario

# 常见命名别名，方便不同调用习惯。
QueryGateway = Gateway
BudgetGateway = Gateway
GatewayService = Gateway

__all__ = [
    "Gateway",
    "QueryGateway",
    "BudgetGateway",
    "GatewayService",
    "run_scenario",
    "ACCESS_CLOSED",
    "AWAITING_PUBLICATION",
    "DENIED",
    "NEVER_PUBLISHED",
    "PREPARING",
    "PUBLISHED",
    "SCOPE_PENDING",
    "VERIFYING",
    "COMMITTED",
    "HELD",
    "RELEASED",
    "GatewayError",
    "ValidationError",
    "ConflictError",
    "NotFoundError",
    "parse_source",
    "canonical_sources",
    "extract_scope",
]

__version__ = "1.0.0"
