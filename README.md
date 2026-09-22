# Webhook Event Gate

基于 **FastAPI + SQLite** 的 Webhook 事件闸门 API。客户端按流（`stream_id`）提交带序号的事件，服务为每条流维护连续序号窗口：乱序事件先缓存，缺口补齐或等待超时后按序释放，重复提交幂等去重，所有释放与跳过均落库可查。

## 快速启动

只需一条命令（Docker Compose）：

```bash
docker compose up --build
```

服务监听 **8000** 端口：

- API 入口： http://localhost:8000
- 交互式文档（Swagger UI）： http://localhost:8000/docs
- 健康检查： http://localhost:8000/healthz

SQLite 数据文件持久化在名为 `gate-data` 的卷中（容器内路径 `/data/events.db`）。

## 工作原理

每条流维护 `next_expected`（下一个期望序号）：

| 到达事件 | 行为 |
| --- | --- |
| 流的首个事件 | 定义序号基线，立即释放，`next_expected = seq + 1` |
| `seq == next_expected` | 立即释放，并连带按序释放缓存中连续的事件 |
| `seq > next_expected` | 乱序 → 缓存等待缺口补齐，流进入缺口等待并计时 |
| `seq < next_expected` / `event_id` 或 `seq` 已存在 | 判重，返回 `duplicate`，不重复释放 |

缺口等待超过 `GAP_TIMEOUT_SECONDS`（默认 30 秒）后，后台扫描任务跳过缺失序号（在释放日志中记为 `skipped`），并按序释放已缓存事件；若释放后仍存在新缺口，超时窗口重新计时。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/events` | 提交事件（`stream_id`、`seq`、`event_id`、`payload`） |
| `GET` | `/streams` | 列出所有流的状态 |
| `GET` | `/streams/{stream_id}` | 查询单条流状态（窗口、缓存、缺口计时、统计） |
| `GET` | `/streams/{stream_id}/pending` | 查询待处理（缓存中的乱序）事件 |
| `GET` | `/streams/{stream_id}/releases?limit=100` | 查询释放/跳过日志（最新在前） |
| `GET` | `/healthz` | 健康检查 |

## 示例请求

```bash
# 1. 按序到达 → 立即释放
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"orders","seq":1,"event_id":"evt-1","payload":{"amount":100}}'
# {"status":"released", "released_seqs":[1], "next_expected":2, ...}

# 2. 乱序（缺 seq=2）→ 缓存等待
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"orders","seq":3,"event_id":"evt-3","payload":{"amount":300}}'
# {"status":"buffered", "waiting_for_seq":2, ...}

# 3. 查询流状态与待处理事件
curl -s http://localhost:8000/streams/orders
curl -s http://localhost:8000/streams/orders/pending

# 4. 补齐缺口 → seq 2、3 依次释放
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"orders","seq":2,"event_id":"evt-2","payload":{"amount":200}}'
# {"status":"released", "released_seqs":[2,3], "next_expected":4, ...}

# 5. 重复提交 → 幂等去重
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"orders","seq":3,"event_id":"evt-3","payload":{"amount":300}}'
# {"status":"duplicate", "reason":"event_id_already_seen", ...}

# 6. 查看释放日志
curl -s http://localhost:8000/streams/orders/releases
```

**超时场景**：若步骤 4 的 `seq=2` 一直不到达，等待超过 `GAP_TIMEOUT_SECONDS` 后，服务自动跳过 `seq=2`（日志记为 `{"kind":"skipped","seq":2,"reason":"gap_timeout"}`），并释放缓存的 `seq=3`。想快速观察可把超时调小：

```bash
GAP_TIMEOUT_SECONDS=5 docker compose up --build
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GAP_TIMEOUT_SECONDS` | `30` | 缺口等待超时（秒），超时后跳过缺失序号 |
| `SWEEP_INTERVAL_SECONDS` | `1` | 后台超时扫描间隔（秒） |
| `DATABASE_PATH` | 容器内 `/data/events.db`，本地 `events.db` | SQLite 文件路径 |

## 本地开发

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8000
pytest            # 运行测试
```

## 数据模型（SQLite）

- `streams`：每条流的 `next_expected`、缺口计时 `gap_since`、时间戳。
- `events`：已接受事件（`status` 为 `buffered`/`released`），`(stream_id, seq)` 与 `(stream_id, event_id)` 唯一约束保证去重。
- `releases`：释放日志，`kind=released`（原因 `in_order`/`gap_filled`/`gap_timeout`）或 `kind=skipped`（超时跳过的缺失序号）。

## 项目结构

```
app/
  config.py    # 环境变量配置
  gate.py      # 核心闸门：序号窗口、去重、乱序缓存、超时跳过、释放记录
  schemas.py   # Pydantic 请求/响应模型
  main.py      # FastAPI 路由与后台超时扫描任务
tests/         # 单元测试 + API 集成测试
Dockerfile
docker-compose.yml
```
