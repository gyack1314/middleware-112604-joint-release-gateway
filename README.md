# 协作分析网关的联合隐私预算与结果承诺

预算是合成整数，不是实际隐私参数。集合修订与 scope 中的群体引用可重复。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **catalog**　集合 name/rev 宣告 groups 和 audiences。groups 为 null 表示范围尚未完成。
- **budget**　提供 group 的 cap。
- **submit**　用户用 id 和 audience 对 sources 发查询，unit 是各涉及群体所需额度。 reuse 引用曾公开 result；只有原受众和同一来源修订可按已有发布访问。
- **computed**　仿真计算方为 id 给不可变 result 标识。
- **published**　发布端对 id 响应 exposed、never 或 unknown。
- **withdraw**　运营收回 id 尚未公开的发布安排或停止今后访问。
- **restart**　模拟网关重启。
- **inspect**　查询 group 或 id。

## 示例

- overlap-and-late-publication.jsonl
- unknown-overlap-and-scope-change.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 查询发布网关实现

仓库在上述消息契约之上提供了标准库实现 `gateway/`（SQLite 持久化 + 状态机），
完整规则、状态图与 API/CLI 说明见 [GATEWAY.md](GATEWAY.md)。

```bash
# 回放示例并查看处置轨迹
python3 -m gateway run examples/overlap-and-late-publication.jsonl --format text
python3 -m gateway run examples/unknown-overlap-and-scope-change.jsonl --format text

# 维护集合/额度、发起查询、确认计算、提交回执、查询真实可用量
python3 -m gateway catalog --name a --rev 1 --groups shared,left --audiences lab
python3 -m gateway catalog --name b --rev 1 --groups shared,right --audiences lab
python3 -m gateway budget --group shared --cap 4
python3 -m gateway budget --group left --cap 10
python3 -m gateway budget --group right --cap 10
python3 -m gateway submit --id one --audience lab --sources a@1,b@1 --unit 3
python3 -m gateway computed --id one --result digest
python3 -m gateway published --id one --state exposed
python3 -m gateway inspect --group shared

# 全量测试
python3 -m unittest discover -v
```

核心口径：`available = cap − committed − reserved`；提交时对所有已确定涉及的
预算域做原子预留，未知群体范围（groups=null）不按不相交处理；`unknown` 回执与
缺少参与者范围的 `never` 不释放预留；`exposed` 成为永不回滚的公开事实，撤销只
关闭后续访问；同受众同修订重复读取已公开结果零成本，新受众或换修订需独立判断
授权与预算；所有状态落盘，restart 或重开数据库后可继续推进。
