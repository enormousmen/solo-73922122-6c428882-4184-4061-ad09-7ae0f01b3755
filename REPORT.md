# 交付报告：UDP 网关双工作进程分片改造

## 需求对照与实现内容

| 需求 | 实现 |
| --- | --- |
| 两个工作进程并行处理租约请求 | compose 启动 `worker-0` / `worker-1` 两个 uvicorn 进程（同一镜像 `app.main:app`，`SHARD_INDEX=0/1`、`SHARD_COUNT=2`），租约仍在各自进程内存 |
| 客户端仍连同一个 HTTP 入口 | 新增无状态入口 `app/entry.py`（compose 服务 `entry`，唯一对外端口 8080），按分片转发到两个工作进程 |
| 按对端 IPv4+端口固定分工；同对端的领用和回包查询同进程 | `app/sharding.py`：`SHA-256("peer_ipv4:peer_port") % 2`（不用 Python 内置 `hash()`，它按进程加盐）。`POST /leases` 与 `GET /mappings` 用同一条规则路由 |
| 续期/归还仅凭租约号访问原发放进程 | 工作进程模式下租约号内嵌分片标记 `lease-s<分片>-<序号>-<随机>`（`PortAllocator(shard_label=...)`）；入口用正则解析租约号得到分片，无需对端信息、无需共享注册表 |
| 只重启入口，原有效租约仍能查/续/还 | 入口不保存任何租约状态（仅配置 + httpx 连接池），重启不影响工作进程 |
| 负责进程超时/失联返回 503，不能改派 | 入口对每个工作进程调用包 `asyncio.wait_for`（默认 2 秒，env 可调）+ 捕获 httpx 连接错误，统一返回 `503 {"detail":"worker shard N unavailable (...); request not re-routed"}`；代码中没有也不可能有改派路径（对端状态只存在于属主进程，工作进程还会拒绝不属于自己的对端） |
| 等待故障进程时健康进程仍可完成请求 | 入口全部端点为 async，转发用 `httpx.AsyncClient`；一个请求等故障进程时事件循环不被阻塞（实测见下） |
| 四个业务 API 沿用原请求和响应格式 | 入口用相同的 Pydantic 模型校验（422 一致），工作进程响应**逐字节透传**（状态码 + 响应体原样）；原 `scripts/demo.py` 对集群入口原样通过 |
| 两个进程使用同一段公网端口，隔离仍按（端口, 对端）组合 | 两个工作进程配置同一 `POOL_START/POOL_END`；每个对端的状态完整落在唯一属主进程内，隔离语义与单实例逐行一致（`allocator.py` 业务逻辑未改） |
| Docker Compose 启动 | `docker-compose.yml`：`entry` + `worker-0` + `worker-1` 三服务，YAML 锚点复用工作进程配置，均带健康检查 |
| 附 API 示例演示重启入口和暂停工作进程 | `scripts/demo_cluster.py`：自动执行 `docker compose restart entry` / `pause worker-0` / `unpause worker-0` 并断言各阶段 API 结果；无 docker 时优雅跳过故障注入步骤 |

### 改动的文件

* `app/sharding.py`（新）：确定性对端→分片路由、租约号分片标记解析。
* `app/entry.py`（新）：无状态入口。按对端/租约号路由；超时与连接错误→503 不改派；
  `/debug/state` 汇总两个分片状态并标记 `unavailable`；uvicorn `--factory` 方式启动。
* `app/allocator.py`：新增可选 `shard_label` 参数，仅影响租约号格式；默认 `None`
  时保持原格式与全部原行为（原 18 个测试未改一行、全部通过）。
* `app/config.py`：`Settings` 增加 `SHARD_INDEX`/`SHARD_COUNT` 校验；新增 `EntrySettings`
  （`WORKER_URLS` 必填、按分片号排序，`WORKER_TIMEOUT_SECONDS` 默认 2 秒）。
* `app/main.py`：工作进程模式接线（租约号带分片标记）+ 对端归属校验
  （路由接错时 503，防止对端状态被静默拆到两个进程）；不设 `SHARD_*` 即原单机模式。
* `app/schemas.py`：未改。
* `docker-compose.yml`：改为 入口+双工作进程 三服务。
* `Dockerfile`：同一镜像服务三种角色（默认 CMD 仍是单机/工作进程；入口由 compose
  的 `command: uvicorn --factory app.entry:create_entry_app` 启动）。
* `requirements.txt`：新增 `httpx==0.28.1`（入口异步转发）。
* `tests/test_sharding.py`（新，10 个测试）、`tests/test_cluster.py`（新，12 个测试）。
* `scripts/demo_cluster.py`（新）、`README.md`（重写）、`REPORT.md`（本文件）。

## 实测结果

### 1. 单元/集成测试：40 个全部通过（约 1 秒）

```
python3 -m unittest discover -s tests
Ran 40 tests in 1.060s   OK
```

* 原 18 个测试（分配器 + 单实例 API）未修改、全部通过 → 单机行为与租约号旧格式保持兼容。
* 新增 22 个：分片哈希确定性与边界、租约号解析、工作进程租约号格式、
  对端归属校验、入口路由、仅凭租约号续期/归还、**入口重启后租约可用**、
  未知/越界租约号 404、响应格式逐字段比对、完整业务流程走入口、
  工作进程失联→503 且不改派、`/debug/state` 故障标记、
  **故障进程不阻塞健康分片**（异步并发测试：慢 1 秒的分片 0 超时 0.3 秒返回 503，
  分片 1 在此期间正常 201）。

### 2. 真实进程实测（本环境无 docker，用等价手段验证）

以与 compose 完全相同的 env/命令启动 3 个真实 uvicorn 进程（入口 + 2 工作进程，
真实 TCP）。因环境禁止 `kill(2)`，"暂停工作进程"用在 worker-0 前加**可冻结 TCP
代理**模拟（对入口而言与 `docker compose pause` 等价：连接保持、响应停止），
"重启入口"用**另起一个全新入口进程**指向同一批工作进程验证。实测输出摘录：

```
== 1. 路由 + 租约号分片标记 + 两进程同一段池 ==
  alloc A: 201 lease-s0-00000001-17ea205fc2f0 port 40000   (对端 A → 分片 0)
  alloc B: 201 lease-s1-00000001-ff1d9229d875 port 40000   (对端 B → 分片 1，同一公网端口)

== 2. "重启入口"（新入口进程接管）==
  lookup A via NEW entry: 200 mapped = True
  renew A via NEW entry: 200 lease-s0-00000001-17ea205fc2f0
  renew B via NEW entry: 200

== 3. 冻结 worker-0（== docker compose pause worker-0）==
  A lookup while frozen: 503 in 2.00s -> worker shard 0 unavailable (timeout after 2.0s); request not re-routed
  B lookup during A's in-flight call: 200 in 0.21s -> mapped=True   ← 等待故障进程时健康分片照常完成
  alloc A while frozen: 503            renew A's lease while frozen: 503   ← 不改派
  debug/state shards: [{shard 0: unavailable}, {shard 1: ok}]

== 4. 解冻 worker-0 ==   lookup A: 200 mapped=True；renew A: 200（租约原样保留）
== 5. 工作进程失联（连接拒绝）==   alloc A: 503 (ConnectError, not re-routed)；lookup B: 200
== 6. 归还两个租约；旧号再续期 404 ==
ALL LIVE CLUSTER CHECKS PASSED
```

### 3. 原演示脚本对集群入口原样通过

`python3 scripts/demo.py --base-url <集群入口>`：单端口池共享、幂等重复、
池满 503、回包查询、隔离期 404/503、隔离后重新领用等全部断言通过
（`ALL DEMO CHECKS PASSED`），证明四个业务 API 格式与语义未变。

### 4. `scripts/demo_cluster.py` 实测

对真实集群跑通：分片标记、幂等、查询、续期、归还、旧号 404 全部断言通过；
本环境无 docker，重启/暂停步骤按设计跳过并打印将执行的命令
（故障注入行为已由上面第 2 项等价实测覆盖）。

### 5. compose 文件校验

本环境没有 docker，无法执行 `docker compose config`；已用 YAML 解析核对
`docker-compose.yml` 结构：三服务齐全、`WORKER_URLS` 顺序与 `SHARD_INDEX`
对应、两个工作进程池配置一致、入口以 `--factory` 启动。

## 未完成项 / 已知限制

1. **`docker compose up` 未在本环境实跑**（环境无 docker）。compose 文件经 YAML
   结构校验；等价的本地三进程实测已覆盖全部关键行为（见上）。建议在有 docker
   的环境执行 `docker compose up --build -d && python3 scripts/demo_cluster.py`
   做最终确认。
2. **工作进程重启会丢失其分片租约**（题目要求租约留在工作进程内存，属固有约束；
   只有"重启入口"保证不影响租约）。生产上若需工作进程重启不丢租约，要引入共享存储，
   超出本次范围。
3. **分片数固定为 2**：扩到更多进程需同步修改 compose 的 `WORKER_URLS` 与
   `SHARD_COUNT` 并重启全部进程（已有租约会因分片归属变化而失效）。工作进程的
   归属校验会在配置不一致时返回 503 兜底，不会产生静默错派。
4. **入口单点**：入口无状态，可多副本挂在负载均衡后；本次按需求只交付单入口。
5. 环境限制遗留：本环境禁止 `kill(2)`，实测时启动的本地验证进程无法回收
   （监听 18080/18090/18091、28080-28082/28090/28091/28083 等端口），
   不影响交付物本身。
