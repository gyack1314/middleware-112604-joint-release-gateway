"""把演练 JSONL 按契约顺序喂给 Gateway，产出确定性的处置轨迹。

用法（库）::

    from gateway import run_scenario
    trace = run_scenario("examples/overlap-and-late-publication.jsonl")

restart 消息会关闭并重开同一个 db 文件，因此轨迹中的后续处理完全依赖
磁盘状态，等价于真实重启恢复。
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scenario_reader import iter_messages  # noqa: E402

from .engine import Gateway  # noqa: E402

_DEFAULT_REGISTRY = _REPO_ROOT / "examples" / "message_types.json"


def load_supported_types(registry_path: Optional[Path] = None) -> Set[str]:
    path = Path(registry_path) if registry_path else _DEFAULT_REGISTRY
    return set(json.loads(path.read_text(encoding="utf-8")))


def run_scenario(path: str | Path, *, db_path: Optional[str] = None,
                 keep_db: bool = False,
                 registry_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """运行单个 JSONL 场景，返回每条消息对应的 outcome 轨迹。

    db_path 为 None 时使用临时文件（restart 可真正从磁盘恢复），结束后删除；
    传入显式路径则保留，便于用 CLI / sqlite 继续检查。
    """
    supported = load_supported_types(registry_path)
    messages = list(iter_messages(Path(path), supported))

    temp_dir: Optional[tempfile.TemporaryDirectory] = None
    if db_path is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="gateway-scenario-")
        db_path = str(Path(temp_dir.name) / "gateway.db")
    elif db_path == ":memory:":
        # 场景含 restart 时内存库无法恢复；强制换成临时文件以保证语义真实。
        temp_dir = tempfile.TemporaryDirectory(prefix="gateway-scenario-")
        db_path = str(Path(temp_dir.name) / "gateway.db")

    gateway = Gateway(db_path)
    trace: List[Dict[str, Any]] = []
    try:
        for row in messages:
            outcome = gateway.handle(row["kind"], row["record"], at=row["at"])
            trace.append({"at": row["at"], "kind": row["kind"], "outcome": outcome})
    finally:
        gateway.close()
        if temp_dir is not None and not keep_db:
            temp_dir.cleanup()
    return trace


def trace_to_jsonl(trace: List[Dict[str, Any]]) -> str:
    return "\n".join(json.dumps(item, ensure_ascii=False, sort_keys=True)
                     for item in trace)
