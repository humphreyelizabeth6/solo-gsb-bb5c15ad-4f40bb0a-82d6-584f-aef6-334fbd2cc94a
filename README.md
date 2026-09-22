# Webhook Event Gate（事件闸门 API）

基于 **FastAPI + SQLite（aiosqlite）+ Docker** 的 Webhook 事件闸门。
客户端向 `/events` 提交带 `stream_id`、`seq`、`event_id`、`payload` 的事件；
服务为每条流维护连续序号窗口：**去重 → 缓存乱序事件 → 缺口补齐或等待超时后按序释放**，
并完整记录每一次释放结果（事件与超时缺口）。

## 一条命令启动

```bash
docker compose up --build -d
```

- 端口：`8000`（宿主机 `http://localhost:8000` → 容器 `8000`）
- 数据：SQLite 数据库持久化在命名卷 `event-gate-data`（容器内 `/data/gate.db`）
- 日志：`docker compose logs -f event-gate`
- 停止：`docker compose down`（保留数据；加 `-v` 同时删除数据卷）
- 交互式文档：启动后访问 http://localhost:8000/docs （Swagger UI）

## 快速示例

### 1) 按序事件：立即释放

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"order-1","seq":1,"event_id":"evt-1","payload":{"type":"created"}}'
```

返回 `202 Accepted`：

```json
{
  "stream_id": "order-1",
  "seq": 1,
  "event_id": "evt-1",
  "status": "accepted",
  "state": "released",
  "reason": "in_order",
  "next_seq": 2,
  "pending": 0,
  "released_chain": [ { "seq": 1, "event_id": "evt-1", "kind": "event", "reason": "in_order", "...": "..." } ]
}
```

### 2) 乱序事件：先缓存，等缺口补齐后成链释放

先到 `seq=3`（`next_seq=2`，存在缺口）→ 缓存：

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"order-1","seq":3,"event_id":"evt-3","payload":{"type":"shipped"}}'
# → "state":"buffered", "reason":"gap_waiting", "pending":1
```

补齐 `seq=2` → `2`、`3` 一起按序释放：

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"order-1","seq":2,"event_id":"evt-2","payload":{"type":"paid"}}'
# → released_chain: [seq 2 in_order] + [seq 3 gap_filled], next_seq=4
```

### 3) 重复提交：幂等返回 200

相同的 `(stream_id, event_id)` 或相同 `(stream_id, seq)` 重复提交（典型的 webhook 重试）
不会重复释放，返回 `200 OK`，`status` 为 `duplicate`，`reason` 为
`duplicate_buffered` / `duplicate_released`。

### 4) 查询流状态与待处理事件

```bash
# 全部流概览
curl -s http://localhost:8000/streams | python3 -m json.tool

# 单条流状态：序号窗口 next_seq、缺口计时 gap_open/gap_since、待处理数、释放统计
curl -s http://localhost:8000/streams/order-1 | python3 -m json.tool

# 待处理（已缓存、尚未释放）的乱序事件
curl -s http://localhost:8000/streams/order-1/pending | python3 -m json.tool

# 释放结果（事件 + 超时缺口，最近优先，支持 ?limit=&offset=）
curl -s 'http://localhost:8000/streams/order-1/releases?limit=50' | python3 -m json.tool
```

### 5) 超时缺口

缺口等待超过 `GAP_TIMEOUT_SECONDS`（默认 **5 秒**）仍未补齐时，后台扫描器会：

1. 为每个缺失序号写入一条 `kind="gap"`、`reason="timeout_gap"`、`payload=null` 的释放记录；
2. 推进窗口到已缓存的事件，并把后续连续事件以 `reason="timeout_flush"` 一并释放。

例如只发 `seq=2` 且 5 秒内没发 `seq=1`，`/streams/<id>/releases` 将得到：

```json
[
  { "seq": 2, "kind": "event", "reason": "timeout_flush", "payload": { "...": "..." } },
  { "seq": 1, "kind": "gap",   "reason": "timeout_gap",   "payload": null }
]
```

之后流继续正常工作：`seq=3` 到达即按序释放。超时之后才到的旧序号事件（`seq < next_seq`）
返回 `409 Conflict`（同 `event_id` 的重试仍幂等返回 200）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/events` | 提交事件。首次 `202`，重复 `200`，迟到/冲突 `409`，校验失败 `422` |
| `GET` | `/streams` | 列出全部流（序号窗口、缺口、待处理数） |
| `GET` | `/streams/{stream_id}` | 流状态：`next_seq`、`gap_since`、`gap_open`、`pending_count`、释放统计 |
| `GET` | `/streams/{stream_id}/pending` | 待处理（缓存中未释放）事件 |
| `GET` | `/streams/{stream_id}/releases` | 释放结果（含 `event`/`gap`，分页 `limit`/`offset`） |
| `GET` | `/healthz` | 健康检查（容器 HEALTHCHECK） |

### 请求体

```json
{
  "stream_id": "order-1",   // 流标识，1–256 字符
  "seq": 1,                  // 流内单调连续序号，整数且 >= 1，从 1 开始
  "event_id": "evt-1",       // 幂等键：同流内唯一
  "payload": { }             // 任意 JSON 对象
}
```

### 释放原因（reason）

| reason | 含义 |
|---|---|
| `in_order` | 事件序号正好等于 `next_seq`，按序立即释放 |
| `gap_filled` | 缺口补齐后，沿窗口连续释放的缓存事件 |
| `timeout_gap` | 等待超时，缺失序号被记为缺口（`kind=gap`） |
| `timeout_flush` | 超时推进窗口后，顺带释放的缓存事件 |
| `gap_waiting` | 事件已缓存，正在等待缺口补齐（未释放） |
| `duplicate_buffered` / `duplicate_released` | 重复提交，幂等命中 |

## 工作模型

```
                         ┌──────────────────────────────┐
 POST /events ┌────────▶│ 去重 (event_id / seq 唯一)    │
              │          └──────────────┬───────────────┘
              │                         │
        seq == next_seq?        seq > next_seq
              │                         │
              ▼                         ▼
   立即释放 + 沿缓存连续释放      写入 events(status=buffered)
   写 releases(kind=event)       streams.gap_since 开始计时
              │                         │
              └────────┬────────────────┘
                       ▼
          后台 sweeper（默认每 0.5s）
     缺口超过 5s → releases 写 gap 记录 → 推进窗口
                  → 连续缓存事件 timeout_flush
```

- `streams`：每流一行，`next_seq`（下一个期望序号）、`gap_since`（缺口计时起点）
- `events`：去重后的事件，`(stream_id, seq)` 与 `(stream_id, event_id)` 双唯一约束；
  状态 `buffered → released`
- `releases`：释放结果流，`kind=event`（含 payload）或 `kind=gap`（超时缺口，payload 为空）

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `GATE_DB_PATH` | `/data/gate.db` | SQLite 文件路径 |
| `GAP_TIMEOUT_SECONDS` | `5` | 缺口等待超时 |
| `SWEEP_INTERVAL_SECONDS` | `0.5` | 后台超时扫描间隔 |

修改后重建即可，例如 10 秒超时：

```bash
GAP_TIMEOUT_SECONDS=10 docker compose up --build -d
```

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 说明与边界

- 单 uvicorn worker 部署：写操作由应用层全局锁 + SQLite `BEGIN IMMEDIATE` 串行化，
  保证同一连接上的事务不交错；多副本水平扩展需要把锁移到数据库/外部组件，不在当前范围内。
- SQLite 启用 WAL，读接口（状态/待处理/释放记录）不阻塞写。
- 重启容器数据不丢（命名卷）；序号窗口状态从数据库恢复。
