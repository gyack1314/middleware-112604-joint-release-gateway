# 查询发布网关（Gateway）

纯 Python 3.11 标准库实现的**联合预算协调 + 发布承诺状态机**，持久化在 SQLite。
不包含任何统计分析或隐私算法：`computed` 只登记仿真端给的不可变 `result` 标识，
所有集合、群体、额度、结果均为本地合成演练数据。

## 一分钟跑通

```bash
# 直接回放两个演练场景（文本轨迹）
python3 -m gateway run examples/overlap-and-late-publication.jsonl --format text
python3 -m gateway run examples/unknown-overlap-and-scope-change.jsonl --format text

# 机器可读 JSONL 轨迹（每行一条消息的 outcome）
python3 -m gateway run examples/overlap-and-late-publication.jsonl

# 全量测试（含示例断言、重启恢复、多线程抢最后额度）
python3 -m unittest discover -v
```

作为库：

```python
from gateway import Gateway, run_scenario

gw = Gateway("gateway.db")                 # 传 ":memory:" 用内存库
gw.announce_catalog("a", 1, ["shared", "left"], ["lab"])
gw.set_budget("shared", 4)
print(gw.submit("one", "lab", ["a@1", "b@1"], 3))
print(gw.confirm_computation("one", "digest"))
print(gw.confirm_publication("one", "unknown"))   # unknown 不释放预留
print(gw.inspect_group("shared"))                 # cap/committed/reserved/available
# 进程退出后用 Gateway("gateway.db") 重开，所有预留与中间状态都还在。

trace = run_scenario("examples/unknown-overlap-and-scope-change.jsonl")
```

命令行还支持逐行服务模式与单条维护 / 操作命令，见 `python3 -m gateway --help`。
`--db` 可放在子命令前或后；默认库为当前目录 `gateway_state.db`。

```bash
python3 -m gateway serve --db gateway_state.db < messages.jsonl
python3 -m gateway catalog --name a --rev 1 --groups shared,left --audiences lab
python3 -m gateway budget  --group shared --cap 4 --db gateway_state.db
python3 -m gateway budget  --group left   --cap 10 --db gateway_state.db
python3 -m gateway submit  --id one --audience lab \
        --sources a@1 --unit 3 --db gateway_state.db
python3 -m gateway computed  --id one --result digest --db gateway_state.db
# never 必须带覆盖该群体的 scope 才释放；不带 scope 会继续占用：
python3 -m gateway published --id one --state never --scope shared,left \
        --db gateway_state.db
python3 -m gateway disposition --id one --audience lab --db gateway_state.db
```

## 预算台账

每个群体（预算域）一行整数台账：

```
available = cap − committed − reserved
```

- `cap`：资料方给的整数发布额度（`budget`）。未登记 cap 的域按 **0 额度**处理。
- `reserved`：请求到达时原子写入的 `held` 预留；计算中、等回执、核实中都持续占用。
- `committed`：发布端确认 `exposed` 后转成的公开事实消耗，**永不回滚**。
- 下调 cap 可能让 `available` 为负；这是真实的超额信号，新请求仍会被拒，
  数字不被“美化”。

`unit` 是**每个涉及群体**各自扣减的额度。涉及群体 = 各来源修订 catalog
`groups` 的并集；同一群体在一条查询的多个集合（或重复引用）中只记一次。

## 请求状态机

```
                         范围全部已知?
 submit ── 授权/预算 ── yes ─► preparing ── computed ─► awaiting_publication
   │                     no ─► scope_pending（已确定域仍 held，禁止发布）
   │                              │ catalog 补齐范围并预留新域
   │                              └─► preparing / awaiting_publication
   ├─ 授权失败 ─► denied(not_authorized)
   ├─ 预算不足 ─► denied(insufficient_budget | no_budget_declared)
   └─ 修订缺失 ─► denied(unknown_revision)

 awaiting_publication / preparing / scope_pending
   ── withdraw ─► verifying（withdraw_requested，hold 不释放）
   ── published unknown ─► verifying（hold 不释放）
   ── published never[+scope] ─► 覆盖到的 held 行 released
   │        └─ 全部 hold 释放且范围完整 ─► never_published
   └─ published exposed ─► 覆盖到的行 committed
            └─ 全部域 committed ─► published
                     └─ 随后 revoke/expire ─► access_closed（预算保留）
```

另有零成本复用票状态 `reused`（不写 hold）。所有状态字符串见 `gateway/model.py`。

## 规则 → 行为对照

| 要求 | 实现 |
| --- | --- |
| 请求到达时协调多个预算域 | `submit` 在单个 `BEGIN IMMEDIATE` 事务内检查所有域并写入全部 hold |
| 两个请求不能争走同一最后额度 | “读余量 + 判定 + 插 hold”在同一立即写事务；SQLite 写串行 + `busy_timeout`，进程内另有 `RLock`。测试用 20 线程抢 5 额度、10 个独立连接抢 3 额度验证恰有对应数量赢家 |
| 同群体跨集合只记一次 | 群体走 `set` 并集，hold 主键 `(query_id, grp)`；重复 source 用排序多重集规范化 |
| 交集映射不齐不能把未知当不相交 | `groups:null` → `scope_pending`：**已确定涉及的域照常在到达时预留**（已知风险不会因未知而消失），未知域绝不假设为空 / 不相交；范围补齐前不可发布。补齐时再原子预留新域，新域不足则整单拒绝并释放先前全部 hold |
| 联合查询不能只扣一半 | 所有已知域的余量检查与 hold 插入同事务，任一不足整单回滚，不留半套 hold（有专门断言 g1 reserved=0） |
| 计算后等发布端确认实际可读 | `computed` 只登记不可变 `result` 并进入 `awaiting_publication`；只有 `exposed` 回执产生 `committed` 与归属定稿 |
| 延期回执不能凭空腾出余量 | `unknown` 不改台账；withdraw 本身不释放；晚到 / 重复回执幂等且方向保守 |
| 只有确认“从未对外开放”才释放预留 | 仅 `never` 且回执给出**覆盖该群体的参与者范围**时，对应 `held` 行才 `released` |
| 回执缺少参与者范围 | `never` 不带 `scope`（或范围不可解析）时**一个 hold 都不释放**，写 `never_without_scope_kept_hold` 事件，单据留 `verifying`。接受的范围字段：`scope` / `participant_scope` / `covered_groups` / `groups` / `participants`；`all/*/全部/所有` 视为覆盖本单全部群体 |
| 部分范围回执 | 只释放 / 提交 scope 覆盖的群体，其余继续 held；可被后续回执继续结清 |
| 已开放后撤销 / 收紧 / 过期 | `exposed` 行已 `committed`，撤销只把 `access_mode` 置为 `revoked/tightened/expired` 并关闭后续读取，`committed` 与公开事实保留 |
| 早先 never 后被 exposed 推翻 | `released → committed` 重新记账并写 `public_fact_recharged`；`available` 可能为负，也不抹掉公开事实 |
| 重复读取确切同一份已公开结果 | 按 `result` 找到原始归属；**同受众 + 同来源修订**且访问仍有效 → `reused`，成本 0，写 `read_log` |
| 扩大受众 / 换修订要独立判断 | 受众不同或来源修订多重集不同 → `scope_change`，走全新授权 + 当前预算判断，成功则形成 `derived_from=原单` 的独立归属与扣减，失败则 denied |
| 授权 | 提交受众必须出现在每个来源修订当前的 `audiences` 中；catalog 受众收紧会写 `authorization_adjusted` 并动态使后续读取 / 复用失败 |
| 关闭受众可看处置、不可得正文 | `audience_disposition(id, audience)` 对归属受众始终返回状态 / 原因 / 中文处置说明，但 `result` 仅在公开、全额结清、访问有效且授权通过时给出；非归属受众返回 `found:false` |
| 重启后继续处理计算 / 等发布 / 授权调整 | catalog、budget、query、hold、receipt、event、inbox 全部落 SQLite；重开 `Gateway(path)` 即恢复。场景中的 `restart` 会 close + 重开同一文件。有专门测试覆盖 preparing / scope_pending / awaiting / verifying 与授权收紧跨重启 |
| 同一项目维护集合与限额并完成全部操作 | `python3 -m gateway` 子命令：`catalog / budget / submit / computed / published / withdraw / inspect / disposition / serve / run` |
| 只使用合成本地数据、不做统计 / 隐私算法 | 仅登记外部给的 `result` 标识与整数台账；无任何分析代码 |

## 两个示例的确定结论

**overlap-and-late-publication**：`one` 联合 a@1+b@1，shared/left/right 各预留 3
（shared 4→余 1）；computed 后撤回、回执 `unknown`，3 个预留都不释放；
`two` 只要 shared 2，因真实可用量仅 1 被拒；restart 后 `exposed` 到达，
3 转为 committed，单据 `access_closed`；inspect shared：
`cap=4 committed=3 reserved=0 available=1`。

**unknown-overlap-and-scope-change**：a 花掉 base 3/4 并 exposed；
b 引用 groups=null 的 b@1，进入 `scope_pending`，但在到达时锁走 base 最后 1
（`available=0`）；partner 用 `reuse=q` 扩大受众，授权通过、预算独立判断失败；
inspect base：`committed=3 reserved=1 available=0`。

## 持久化与并发说明

- 状态库默认 `gateway_state.db`（已被 `.gitignore` 忽略），含 WAL sidecar。
- 写事务使用 `BEGIN IMMEDIATE` + `busy_timeout=30s`；同进程用 `RLock` 串行化
  单连接上的事务序列。多进程 / 多连接共享同一 db 时由 SQLite 文件锁保证不超扣。
- `event` 表保存不可变处置轨迹（含 `attribution_finalized` 归属定稿），
  `inbox` 表保存原始命令，`receipt` 表保存全部发布端回执。

## 目录

```
gateway/
  model.py    状态常量、错误、source/scope 规范化
  store.py    SQLite schema、立即写事务、预算台账聚合
  engine.py   Gateway：catalog/budget/submit/computed/published/withdraw/inspect
  runner.py   JSONL 场景回放（复用 scenario_reader 的校验）
  cli.py      python3 -m gateway 命令行
tests at repo root:
  test_gateway_rules.py             全部业务规则单测
  test_gateway_durable_concurrent.py 重启 / 示例轨迹 / 并发 / CLI
```
