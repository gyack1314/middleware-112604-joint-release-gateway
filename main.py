"""项目入口：回放两个本地演练场景并打印处置轨迹。

更完整的 CLI 用法见 README.md / GATEWAY.md：

    python3 main.py
    python3 -m gateway run examples/overlap-and-late-publication.jsonl --format text
    python3 -m unittest discover -v
"""

from pathlib import Path

from gateway.runner import run_scenario
from gateway.cli import _text_trace

ROOT = Path(__file__).resolve().parent
SCENARIOS = [
    ROOT / "examples" / "overlap-and-late-publication.jsonl",
    ROOT / "examples" / "unknown-overlap-and-scope-change.jsonl",
]


def main() -> int:
    for path in SCENARIOS:
        print("=" * 78)
        print(f"场景：{path.name}")
        print("-" * 78)
        trace = run_scenario(path)
        print(_text_trace(trace))
        print()
    print("全部场景回放完成。运行 python3 -m unittest discover -v 查看规则测试。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
