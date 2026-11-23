# 园区监控网关 —— 公网 UDP 端口分配 API（双工作进程分片集群）

园区监控网关用一个公网 IPv4 地址转发探针的 UDP 轮询。本服务为
**探针 / 对端（peer）** 组合从连续公网端口池中分配 UDP 端口。

部署形态：**一个无状态 HTTP 入口（entry）+ 两个工作进程（worker）**。
客户端只连入口；租约保存在两个工作进程的内存里，按对端固定分片。

```
                ┌──────────────┐        ┌─────────────┐
  探针/客户端 ──▶│  entry :8080  │───────▶│  worker-0   │  分片 0 的租约（内存）
                │  (无状态转发) │        └─────────────┘
                │              │        ┌─────────────┐
                │              │───────▶│  worker-1   │  分片 1 的租约（内存）
                └──────────────┘        └─────────────┘
```

## 分工与路由规则

| 请求 | 路由依据 | 说明 |
| --- | --- | --- |
| `POST /leases`（领用） | `SHA-256("对端IPv4:端口") % 2` | 同一对端永远落到同一进程 |
| `GET /mappings`（回包查询） | 同上 | 与领用同一条哈希规则，保证同对端同进程 |
| `POST /leases/{id}/renew`（续期） | **仅凭租约号** | 租约号内嵌分片标记 `lease-s<分片>-<序号>-<随机>` |
| `POST /leases/{id}/release`（归还） | **仅凭租约号** | 同上 |

* 哈希用 SHA-256 而非 Python 内置 `hash()`（后者每进程加盐，进程间不一致）。
* 两个工作进程配置**同一段公网端口池**；端口共享与 5 秒隔离仍按
  （公网端口， 对端）组合计算——每个对端的状态完整属于某一个进程，语义与单实例一致。
* 工作进程会校验对端归属：路由接错（如 `WORKER_URLS` 顺序与 `SHARD_INDEX` 不配）
  时返回 503，避免一个对端的状态被静默拆到两个进程。

## 故障语义

| 场景 | 行为 |
| --- | --- |
| **只重启入口** | 入口无租约状态，原有效租约照常查回包、续期、归还 |
| 负责的工作进程**超时**（默认 2 秒） | 该对端的请求返回 `503`，**不改派**到其他进程 |
| 负责的工作进程**失联**（连接拒绝） | 同上，`503` 且不改派 |
| 等待故障进程响应期间 | 入口全异步转发，健康分片的请求照常完成，互不阻塞 |
| 工作进程恢复 | 其内存租约原样继续服务 |

## 业务规则（与单实例一致）

| 规则 | 实现方式 |
| --- | --- |
| 提交探针与对端各自的 IPv4 + 端口，从连续端口池领用公网端口 | `POST /leases`，请求体含四个地址字段，均做 IPv4/端口校验 |
| 租约有效 30 秒 | 单调时钟 + 每对端一个到期最小堆，惰性处理，无后台线程 |
| 同探针 + 同对端重复领用返回原有效租约 | 精确匹配四元组，命中即返回原号（`reused=true`，**不重置**有效期） |
| 同一公网端口可分给不同对端 | 空闲端口按对端独立建模；两个工作进程用同一段池，分别发给不同对端 |
| 同一对端用完端口就报满 | 对端空闲区间集为空即 `503`；占满状态下仅幂等重复仍返回原租约 |
| 不逐端口扫描、不预建整段端口表 | 每对端一个 `FreeIntervalSet`，整池初始只有 1 个区间 |
| 凭有效租约号归还 / 重置为 30 秒 | 全局租约号 → 属主索引 O(1) 定位（在工作进程内） |
| 旧号不能影响新租约 | 归还/到期即删除租约号索引；旧号任何操作返回 `404` |
| 归还或到期后只隔离（公网端口, 对端）组合 5 秒 | 隔离按对端记录，不影响其他对端在同一端口上的租约 |
| 按公网端口 + 对端查回包去向 | `GET /mappings`；隔离中/未知组合返回 `404` 结构化无映射响应 |

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/leases` | 领用（或幂等取回）租约；池满或对端所属进程不可用返回 `503` |
| `POST` | `/leases/{lease_id}/renew` | 有效租约续期到完整 30 秒；无效号 `404`；所属进程不可用 `503` |
| `POST` | `/leases/{lease_id}/release` | 归还并进入 5 秒隔离；无效号 `404`；所属进程不可用 `503` |
| `GET` | `/mappings?public_port=&peer_ipv4=&peer_port=` | 查回包应转发的探针地址；无映射 `404`；所属进程不可用 `503` |
| `GET` | `/healthz` | 健康检查 |
| `GET` | `/debug/state` | 汇总两个工作进程的内部状态（故障分片标记 `unavailable`） |
| `GET` | `/docs` | Swagger UI |

四个业务 API 的请求/响应格式与单实例版本完全一致（入口逐字节透传工作进程响应）。

### 请求示例

```bash
curl -X POST http://127.0.0.1:8080/leases \
  -H 'Content-Type: application/json' \
  -d '{"probe_ipv4":"10.10.0.5","probe_port":5000,
       "peer_ipv4":"198.51.100.11","peer_port":7000}'
# 201 -> {"lease_id":"lease-s0-00000001-...","public_port":40000,...}

curl -X POST http://127.0.0.1:8080/leases/lease-s0-00000001-.../renew   # 仅凭租约号
curl "http://127.0.0.1:8080/mappings?public_port=40000&peer_ipv4=198.51.100.11&peer_port=7000"
```

## 配置（环境变量）

### 工作进程 / 单机

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PUBLIC_IPV4` | `203.0.113.10` | 网关公网 IPv4，随租约响应回显 |
| `POOL_START` / `POOL_END` | `40000` / `40000` | 连续端口池；两个工作进程必须配成**同一段** |
| `LEASE_TTL_SECONDS` | `30` | 租约有效期 |
| `ISOLATION_TTL_SECONDS` | `5` | 归还/到期后的隔离时长 |
| `SHARD_INDEX` | 无 | 本进程分片号；设置后租约号带 `s<分片>` 标记 |
| `SHARD_COUNT` | `1` | 总分片数；>1 时必须同时设置 `SHARD_INDEX` |

### 入口

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKER_URLS` | **必填** | 逗号分隔的工作进程地址，**按分片号排序**（第 1 个是分片 0） |
| `WORKER_TIMEOUT_SECONDS` | `2` | 等待工作进程响应的超时；超时返回 `503` |

## 运行

### Docker Compose（推荐）：入口 + 两个工作进程

```bash
docker compose up --build -d
python3 scripts/demo_cluster.py     # 集群演示（含重启入口、暂停工作进程）
python3 scripts/demo.py             # 原单实例演示脚本，对集群入口同样适用
```

compose 固定单端口池 `40000`，便于直接观察两个对端共用端口、
5 秒隔离期查不到迟到回包去向、隔离结束后重新领用。

### 演示：重启入口、暂停工作进程

```bash
docker compose restart entry        # 入口无状态：原租约照常查询/续期/归还
docker compose pause worker-0       # 分片 0 的对端 -> 503（不改派）；分片 1 照常
docker compose unpause worker-0     # 恢复，租约仍在
```

`scripts/demo_cluster.py` 自动执行以上步骤并断言结果；无 docker 的环境下
跳过故障注入步骤，仅跑纯 API 校验。

### 本地直接运行

```bash
python3 -m pip install -r requirements.txt   # 建议虚拟环境

# 集群形态（三个终端）
SHARD_INDEX=0 SHARD_COUNT=2 uvicorn app.main:app --port 8090 --workers 1
SHARD_INDEX=1 SHARD_COUNT=2 uvicorn app.main:app --port 8091 --workers 1
WORKER_URLS=http://127.0.0.1:8090,http://127.0.0.1:8091 \
  uvicorn --factory app.entry:create_entry_app --port 8080 --workers 1

# 单机形态（原行为，租约号无分片标记）
uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1
```

> **进程数约束**：持有租约的进程必须 `--workers 1`——租约只在进程内存中，
> 扩展靠"按对端分片到不同进程"，绝不能同分片多副本。入口无状态，
> 单进程异步转发即可；如需多副本，置于负载均衡之后即可（它不保存状态）。

## 测试与演示

```bash
python3 -m unittest discover -s tests -v   # 40 个测试：分配器/单实例 API/分片路由/集群集成
python3 scripts/demo.py                    # 单实例业务演示（也可指向集群入口）
python3 scripts/demo_cluster.py            # 集群演示：分片路由 + 重启入口 + 暂停工作进程
```

## 目录结构

```
app/
  config.py      环境变量配置与校验（工作进程 Settings + 入口 EntrySettings）
  allocator.py   核心分配器：区间集合、租约/隔离堆、线程锁（租约号可带分片标记）
  sharding.py    对端 -> 分片的确定性路由（SHA-256），租约号分片标记解析
  schemas.py     Pydantic 请求/响应模型
  main.py        工作进程 FastAPI 应用（含对端归属校验）；不设 SHARD_* 即单机
  entry.py       无状态入口：按对端/租约号路由，超时 503 不改派，异步转发
tests/           unittest：分配器、单实例 API、分片函数、集群集成（进程内端到端）
scripts/demo.py         单实例业务演示（仅标准库）
scripts/demo_cluster.py 集群演示：重启入口/暂停工作进程（仅标准库，调用 docker compose）
Dockerfile, docker-compose.yml, requirements.txt
```
