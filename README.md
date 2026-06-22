# MagicQueue

[![CI](https://github.com/JackyZhang8/magicqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/JackyZhang8/magicqueue/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JackyZhang8/magicqueue.svg)](https://pkg.go.dev/github.com/JackyZhang8/magicqueue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

MagicQueue is a lightweight Go message-queue library with pluggable drivers
(Redis / in-memory), optional LevelDB persistence for crash recovery, automatic
retries with exponential backoff, an optional dead-letter queue, and graceful
shutdown.

MagicQueue 是一个轻量的 Go 消息队列库：支持可插拔驱动（Redis / 内存）、基于
LevelDB 的持久化与崩溃恢复、带指数退避的自动重试、可选死信队列，以及优雅关闭。

> **交付语义**：at-least-once（至少一次）。消息可能被重复投递（例如崩溃恢复后），
> 因此**处理器应保证幂等**。

## 多语言版本 / Language Versions

MagicQueue 提供三种语言实现，**API 与语义对齐**（三档优先级、批量入队、多 key `BLPOP`、
持久化与崩溃恢复、指数退避重试、死信队列、优雅关闭，均为 at-least-once）。差异仅在各自生态的
惯用法与持久化后端：

| 语言 | 仓库 / 目录 | 安装                                                                | 内存驱动 | Redis 客户端 | 持久化后端 | 并发模型 |
|------|------------|-------------------------------------------------------------------|----------|--------------|------------|----------|
| **Go**（本仓库） | `github.com/JackyZhang8/magicqueue` | `go get github.com/JackyZhang8/magicqueue`                        | 内置 | [`go-redis/v9`](https://github.com/redis/go-redis) | [LevelDB](https://github.com/syndtr/goleveldb) | goroutine + channel |
| **Rust** | `github.com/JackyZhang8/magicqueue-rs`（crate `magicqueue`） | `cargo add magicqueue`                                            | 内置 | [`redis`](https://crates.io/crates/redis) | [`sled`](https://crates.io/crates/sled) | 线程 + crossbeam channel |
| **Python** | `github.com/JackyZhang8/magicqueue-py`（包 `magicqueue`） | `pip install magicqueue`（Redis：`pip install "magicqueue[redis]"`） | 内置 | [`redis-py`](https://github.com/redis/redis-py) | 标准库 `sqlite3` | `threading` + `queue.Queue` |

> 持久化后端按各语言生态选择无 C 依赖的方案：Go→LevelDB、Rust→sled、Python→sqlite3。
> 三个版本均自带 README、示例、单元/集成测试与 GitHub Actions CI。

## Features / 特性

- 可插拔驱动：生产用 **Redis**（多 key `BLPOP` 阻塞消费，无 `LLEN+LPOP` 轮询空转），开发/测试用 **内存** 队列
- **消息优先级**：高 / 普通 / 低三档（`Payload.Priority` 按符号映射），同档 FIFO
- **批量入队** `EnqueueBatch`：Redis 用 pipeline、LevelDB 用批量写，单次往返提交多条
- **LevelDB 持久化**：进程崩溃后自动恢复未确认的消息
- **自动重试**：指数退避 + 抖动，重试不阻塞 worker
- **死信队列（DLQ）**：重试耗尽的消息可转入死信队列而非丢弃
- 多 topic / group 路由，并发 worker 池
- **优雅关闭**：`Stop()` 等待在途任务完成并释放资源
- panic 恢复与自定义回调
- 可注入 **自定义 Logger**
- 定时队列统计

## 安装 / Install

```bash
go get github.com/JackyZhang8/magicqueue
```

要求 Go 1.22+。Redis 驱动基于 [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis)。

## 快速开始 / Quick Start

```go
package main

import (
	"log"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

type EmailTask struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type EmailHandler struct{}

func (h *EmailHandler) Execute(p *MagicQueue.Payload) *MagicQueue.Result {
	var task EmailTask
	if err := p.ParseBody(&task); err != nil {
		return MagicQueue.NewResult(false, "parse failed", nil)
	}
	log.Printf("sending email to %s", task.To)
	return MagicQueue.NewResult(true, "sent", nil) // 返回 false 会触发重试
}

func main() {
	// 内存队列（开发/测试）。生产环境改用 UseRedis(client)。
	queue := MagicQueue.NewQueue("email_service").UseMemory(nil)
	if err := queue.Err(); err != nil { // 链式配置错误统一在此检查
		log.Fatal(err)
	}

	queue.SetHandler("email", "notification", &EmailHandler{})

	if err := queue.StartWorkers(4); err != nil {
		log.Fatal(err)
	}
	defer queue.Stop() // 优雅关闭

	id, err := queue.Enqueue(&MagicQueue.Payload{
		Topic:    "email",
		Group:    "notification",
		Body:     EmailTask{To: "user@example.com", Subject: "Hi"},
		MaxRetry: 3,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued %s", id)

	time.Sleep(time.Second)
}
```

### 使用 Redis（生产推荐）

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

queue := MagicQueue.NewQueue("email_service").
	UseRedis(rdb).                       // 复用你自己的 client（连接池/TLS 等配置保留）
	UseLevelDb("./data/queue.db")        // 可选：开启持久化
```

> `UseRedis(client)` 会**复用**你传入的 `*redis.Client`，不会丢弃其连接池 / TLS / 超时配置。
> 若希望由库自行建连，可用 `UseRedisConfig(&MagicQueue.RedisConfig{Addr: ...})`。

## 性能基准（内存驱动横向对比）

同一台机器、统一方法下三语言版本的吞吐对比（内存驱动，排除 Redis/磁盘干扰）。

**测试环境**：Intel Xeon Platinum 8375C @ 2.90GHz（2 vCPU）/ 7.8 GiB RAM / Ubuntu 22.04 / Go 1.22、Rust 1.83（release）、CPython 3.12。
**方法**：每项 N = 200,000 消息，取 3 次运行的中位数；批量大小 1000；端到端为 4 workers + 空 handler（pre-enqueue 后计时至全部处理完）。

| 指标 | Go | Rust | Python |
|------|---:|-----:|-------:|
| 单条入队 `enqueue`（条/秒）        | 663,013   | 1,293,833 | 97,660 |
| 批量入队 `enqueue_batch`（条/秒，batch=1000） | 727,392 | 1,109,984 | 98,832 |
| 端到端处理（条/秒，4 workers）     | 404,835   | 1,075,262 | 45,624 |

> 说明：Go/Rust 为编译型、Python 为解释型且受 GIL 限制，量级差异属预期。数字为单台 2 vCPU 云主机上的近似值，仅用于版本间相对比较，绝对值会随硬件波动。
>
> 复现：Go `go run ./bench`、Rust `cargo run --release --example bench`、Python `python bench.py`。

## 完整示例 / Examples

`examples/` 目录下有多个可独立运行的示例：

| 目录 | 说明 | 运行 |
| --- | --- | --- |
| `examples/basic_memory` | 内存队列最简上手 | `go run ./examples/basic_memory` |
| `examples/redis` | Redis 驱动 + 复用 client + 持久化 | `go run ./examples/redis` |
| `examples/retry_deadletter` | 自动重试 + 死信队列 | `go run ./examples/retry_deadletter` |
| `examples/graceful_shutdown` | 监听信号优雅关闭 | `go run ./examples/graceful_shutdown` |
| `examples/multiple_handlers` | 同实例多 topic/group | `go run ./examples/multiple_handlers` |
| `examples/batch_priority` | 批量入队 + 消息优先级 | `go run ./examples/batch_priority` |
| `examples/persistence` | LevelDB 崩溃恢复 | `go run ./examples/persistence write` 然后 `recover` |

## 架构与数据流 / Architecture

```
                 Enqueue(payload)
                        │
        IsPersist? ─────┼──────────► LevelDB (持久层, 崩溃恢复来源)
                        ▼
              QueueDriver (Redis / Memory)      ◄── 投递通道
                        │
        consume(BLPOP / Pop) per topic·group
                        │
                  jobs channel
                        │
            worker pool (N workers)
                        │
                handler.Execute()
            ┌───────────┴───────────┐
        State=true               State=false
            │                        │
   ack: 从 LevelDB 删除      Retry<MaxRetry ? 退避后重投 : (DLQ) + ack
```

要点：
- **持久层是恢复的唯一来源**；`driver` 只是投递通道。
- 启动时 `recoverPersistentMessages` 先把 LevelDB 中未确认的消息重投回 driver，**再**启动消费者，避免与正常消费路径重复处理。
- 重试通过 `time.AfterFunc` 异步重投，**不会阻塞 worker**；退避为指数 + 抖动。
- 内存驱动在进程退出后清空；只有开启 LevelDB 且消息 `IsPersist=true` 时才能跨重启恢复。

## API 概览

构造与配置（均可链式调用）：

| 方法 | 说明 |
| --- | --- |
| `NewQueue(name) *MQueue` | 创建队列实例 |
| `UseRedis(client *redis.Client) *MQueue` | 复用调用方的 Redis client |
| `UseRedisConfig(cfg *RedisConfig) *MQueue` | 由库自建 Redis 驱动 |
| `UseMemory(cfg *MemoryConfig) *MQueue` | 使用内存驱动（`MaxQueueSize` 控制上限） |
| `UseLevelDb(path string) *MQueue` | 开启 LevelDB 持久化 |
| `SetHandler(topic, group, h Queueable) *MQueue` | 注册处理器 |
| `WithOptions(Options) *MQueue` | 设置重试/统计/死信等运行时选项 |
| `SetLogger(Logger) *MQueue` | 注入自定义日志 |
| `RegisterOnInterrupt(RecoveryListener) *MQueue` | 注册 panic 回调 |
| `Err() error` | 返回链式配置过程中累计的第一个错误 |

运行时：

| 方法 | 说明 |
| --- | --- |
| `Enqueue(p *Payload) (string, error)` | 入队，返回消息 ID |
| `EnqueueBatch(ps []*Payload) ([]string, error)` | 批量入队，返回 ID 列表（顺序一致） |
| `StartWorkers(n int) error` | 启动 n 个 worker（非阻塞） |
| `Stop()` | 优雅关闭，等待在途任务并释放资源 |
| `GetQueueSize(topic, group) int64` | 队列长度 |
| `GetDeadLetterSize(topic, group) int64` | 死信队列长度 |

> 注意：`Enqueue` 返回值为 `(string, error)`（id 在前、error 在后），符合 Go 惯例。

### Options

```go
type Options struct {
	PollInterval     time.Duration // 非阻塞驱动的轮询间隔，默认 200ms
	StatsInterval    time.Duration // 统计输出间隔，默认 1m；<=0 关闭
	RetryBaseDelay   time.Duration // 退避基准，默认 500ms
	RetryMaxDelay    time.Duration // 退避上限，默认 30s
	EnableDeadLetter bool          // 重试耗尽是否进入死信队列
}
```

### Payload / Result

```go
type Payload struct {
	ID        string      `json:"id"`         // 由 Enqueue 自动生成
	IsPersist bool        `json:"is_persist"` // 配合 LevelDB 实现崩溃恢复
	Topic     string      `json:"topic"`      // 必填
	Group     string      `json:"group"`
	Body      interface{} `json:"body"`
	Priority  int64       `json:"priority"`   // >0 高, ==0 普通(默认), <0 低
	MaxRetry  int         `json:"max_retry"`
	Retry     int         `json:"retry"`
}

type Result struct {
	State   bool        `json:"state"`   // false 触发重试
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func NewResult(state bool, msg string, data interface{}) *Result
```

> `NewQueueResult` 仍可用，但已标记为 `Deprecated`，请改用 `NewResult`。

## 消息优先级 / Priority

优先级分三档，由 `Payload.Priority` 的符号决定，**同档内保持 FIFO**：

| Priority | 档位 | 说明 |
| --- | --- | --- |
| `> 0` | 高 | 优先处理 |
| `0`（零值默认） | 普通 | 大多数消息 |
| `< 0` | 低 | 后台/可延迟任务 |

```go
queue.Enqueue(&MagicQueue.Payload{Topic: "task", Body: job, Priority: 10})  // 高
queue.Enqueue(&MagicQueue.Payload{Topic: "task", Body: job})                // 普通(0)
queue.Enqueue(&MagicQueue.Payload{Topic: "task", Body: job, Priority: -1})  // 低
```

实现：底层把每个逻辑队列拆成三个子队列（key 后缀 `:p2/:p1/:p0`）。
Redis 端用**多 key `BLPOP`**（`BLPOP key:p2 key:p1 key:p0 timeout`）一次阻塞调用即可
按优先级取消息，既保证优先级又避免 `LLEN+LPOP` 轮询空转。

## 批量入队 / Batch Enqueue

```go
ids, err := queue.EnqueueBatch([]*MagicQueue.Payload{
	{Topic: "task", Body: a, Priority: 10},
	{Topic: "task", Body: b},
	{Topic: "task", Body: c, IsPersist: true},
})
```

- Redis 驱动用 **pipeline** 在一次往返中推送全部消息；
- 持久化消息通过 LevelDB **批量写**（`leveldb.Batch`）原子落盘；
- 任一消息校验失败则整体不入队；推送失败会回滚已持久化的条目。

## 自定义 Logger

```go
type myLogger struct{}
func (myLogger) Printf(format string, args ...interface{}) { /* 接入 zap/logrus 等 */ }

queue.SetLogger(myLogger{})
```

## 开发 / Development

```bash
go build ./...
go vet ./...
go test -race ./...
golangci-lint run ./...
```

CI 在 GitHub Actions 上运行 gofmt / vet / build / race 测试 / golangci-lint，见 `.github/workflows/ci.yml`。

## 注意事项

1. 处理器需保证**幂等**（at-least-once 语义）。
2. Redis 模式确保服务可达；LevelDB 路径需有写权限。
3. 退出前调用 `Stop()` 以避免任务/资源泄漏。

## License

MIT，详见 [LICENSE](LICENSE)。

## Author

[JackyZhang8](https://github.com/JackyZhang8)
