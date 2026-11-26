"""命令行入口：同一个项目完成 catalog / budget 维护与全部查询操作。

示例
----
跑演练场景（每条输入消息输出一个 outcome JSON）::

    python3 -m gateway run examples/overlap-and-late-publication.jsonl

持久库上的日常操作（默认 ./gateway_state.db）::

    python3 -m gateway catalog --name a --rev 1 --groups shared,left --audiences lab
    python3 -m gateway budget --group shared --cap 4
    python3 -m gateway submit --id one --audience lab --sources a@1,b@1 --unit 3
    python3 -m gateway computed --id one --result digest
    python3 -m gateway published --id one --state exposed
    python3 -m gateway inspect --group shared
    python3 -m gateway disposition --id one --audience lab

stdin 服务模式：逐行读取完整消息 ``{"at":1,"kind":"submit","record":{...}}``，
逐行写回 outcome JSON，状态持久，可随时中断重启::

    python3 -m gateway serve --db gateway_state.db < messages.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, List, Optional

from .engine import Gateway
from .runner import run_scenario, trace_to_jsonl

DEFAULT_DB = "gateway_state.db"


def _print_json(value: Any, *, indent: Optional[int] = 2) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=indent, sort_keys=True))


def _split_csv(value: Optional[str]) -> Optional[List[str]]:
    if value is None:
        return None
    if value.strip().lower() in ("null", "none", "~", ""):
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def _add_db_argument(parser: argparse.ArgumentParser) -> None:
    # SUPPRESS：子命令后给出 --db 时覆盖顶层默认；不给出时保留顶层 --db 的值。
    parser.add_argument("--db", default=argparse.SUPPRESS,
                        help="SQLite 状态库路径（默认 ./gateway_state.db）")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m gateway",
        description="合成演练库的联合预算查询发布网关（仅标准库）")
    parser.add_argument("--db", default=DEFAULT_DB,
                        help=f"SQLite 状态库路径（默认 {DEFAULT_DB}）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="运行 JSONL 演练场景")
    p_run.add_argument("scenario")
    p_run.add_argument("--keep-db", action="store_true",
                       help="保留场景使用的临时 db（或配合 --db 指定路径）")
    p_run.add_argument("--format", choices=("jsonl", "text"), default="jsonl")
    _add_db_argument(p_run)

    p_serve = sub.add_parser("serve", help="stdin JSONL 消息服务模式")
    _add_db_argument(p_serve)

    p_catalog = sub.add_parser("catalog", help="宣告/补齐集合修订")
    p_catalog.add_argument("--name", required=True)
    p_catalog.add_argument("--rev", required=True, type=int)
    p_catalog.add_argument("--groups",
                           help="逗号分隔群体；传 null 表示范围未完成")
    p_catalog.add_argument("--audiences", required=True,
                           help="逗号分隔受众")
    _add_db_argument(p_catalog)

    p_budget = sub.add_parser("budget", help="设置群体整数发布额度")
    p_budget.add_argument("--group", required=True)
    p_budget.add_argument("--cap", required=True, type=int)
    _add_db_argument(p_budget)

    p_submit = sub.add_parser("submit", help="发起查询")
    p_submit.add_argument("--id", required=True)
    p_submit.add_argument("--audience", required=True)
    p_submit.add_argument("--sources", required=True,
                          help="逗号分隔的 name@rev，如 a@1,b@1")
    p_submit.add_argument("--unit", required=True, type=int)
    p_submit.add_argument("--reuse", default=None)
    _add_db_argument(p_submit)

    p_computed = sub.add_parser("computed", help="确认仿真计算与不可变 result 标识")
    p_computed.add_argument("--id", required=True)
    p_computed.add_argument("--result", required=True)
    _add_db_argument(p_computed)

    p_pub = sub.add_parser("published", help="提交发布端回执")
    p_pub.add_argument("--id", required=True)
    p_pub.add_argument("--state", required=True,
                       choices=("exposed", "never", "unknown"))
    p_pub.add_argument("--scope", default=None,
                       help="本回执结论覆盖的群体（逗号分隔）；never 缺范围不释放")
    _add_db_argument(p_pub)

    p_wd = sub.add_parser("withdraw", help="撤销未公开安排或停止今后访问")
    p_wd.add_argument("--id", required=True)
    p_wd.add_argument("--reason", default="revoke",
                      choices=("revoke", "tighten", "expire"))
    p_wd.add_argument("--audiences", default=None,
                      help="tighten 后的新允许受众（逗号分隔，空串=关闭所有人）")
    _add_db_argument(p_wd)

    p_insp = sub.add_parser("inspect", help="查询群体真实可用量或单据状态")
    target = p_insp.add_mutually_exclusive_group(required=True)
    target.add_argument("--group")
    target.add_argument("--id")
    target.add_argument("--all", action="store_true")
    _add_db_argument(p_insp)

    p_disp = sub.add_parser("disposition", help="某受众查看自己请求的处置")
    p_disp.add_argument("--id", required=True)
    p_disp.add_argument("--audience", required=True)
    _add_db_argument(p_disp)

    return parser


def _text_trace(trace) -> str:
    lines = []
    for item in trace:
        out = item["outcome"]
        head = f"[at={item['at']}] {item['kind']}: {out.get('status')}"
        if out.get("id"):
            head += f" id={out['id']}"
        if out.get("state"):
            head += f" state={out['state']}"
        if out.get("reason"):
            head += f" reason={out['reason']}"
        lines.append(head)
        if item["kind"] == "inspect" and out.get("view") == "group":
            lines.append(
                f"    group={out['group']} cap={out['cap']} "
                f"committed={out['committed']} reserved={out['reserved']} "
                f"available={out['available']}")
        if item["kind"] == "inspect" and out.get("view") == "overview":
            for g in out["groups"]:
                lines.append(
                    f"    group={g['group']} cap={g['cap']} "
                    f"committed={g['committed']} reserved={g['reserved']} "
                    f"available={g['available']}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        if args.db != DEFAULT_DB:
            db_path: Optional[str] = args.db
        elif args.keep_db:
            db_path = "gateway_scenario.db"
        else:
            db_path = None
        trace = run_scenario(
            args.scenario, db_path=db_path, keep_db=args.keep_db)
        if args.keep_db and db_path:
            print(f"# 状态库已保留：{db_path}", file=sys.stderr)
        if args.format == "text":
            print(_text_trace(trace))
        else:
            print(trace_to_jsonl(trace))
        return 0

    gateway = Gateway(args.db)
    try:
        if args.command == "serve":
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as error:
                    _print_json({"status": "error", "message": f"JSON 解析失败：{error}"})
                    continue
                kind = message.get("kind")
                if "record" in message:
                    record = message.get("record") or {}
                    at = message.get("at")
                else:
                    # 紧凑形式：{"kind":"budget","group":"x","cap":1}
                    record = {k: v for k, v in message.items()
                              if k not in ("kind", "at")}
                    at = message.get("at")
                outcome = gateway.handle(kind, record, at=at)
                _print_json(outcome, indent=None)
            return 0

        if args.command == "catalog":
            outcome = gateway.announce_catalog(
                args.name, args.rev, _split_csv(args.groups),
                _split_csv(args.audiences) or [])
        elif args.command == "budget":
            outcome = gateway.set_budget(args.group, args.cap)
        elif args.command == "submit":
            outcome = gateway.submit(
                args.id, args.audience,
                [s for s in args.sources.split(",") if s],
                args.unit, reuse=args.reuse)
        elif args.command == "computed":
            outcome = gateway.confirm_computation(args.id, args.result)
        elif args.command == "published":
            scope = _split_csv(args.scope)
            outcome = gateway.confirm_publication(
                args.id, args.state,
                scope=frozenset(scope) if scope is not None else None)
        elif args.command == "withdraw":
            audiences = _split_csv(args.audiences)
            if args.audiences is not None and audiences is None:
                audiences = []  # 显式传空 => 关闭所有受众
            outcome = gateway.withdraw(
                args.id, reason=args.reason, audiences=audiences)
        elif args.command == "inspect":
            if args.group:
                outcome = gateway.inspect_group(args.group)
            elif args.id:
                outcome = gateway.inspect_query(args.id)
            else:
                outcome = gateway.overview()
        elif args.command == "disposition":
            outcome = gateway.audience_disposition(args.id, args.audience)
        else:  # pragma: no cover
            parser.error("unknown command")
            return 2

        _print_json(outcome)
        return 0
    finally:
        gateway.close()


if __name__ == "__main__":
    raise SystemExit(main())
