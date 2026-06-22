/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import (
	"context"
	"time"
)

// QueueDriver 定义底层队列驱动接口（内存 / Redis 等实现它）。
//
// queueKey 是逻辑队列名；驱动内部会按优先级把它拆成多个子队列，
// 调用方只需传入基础 key 与消息优先级。
type QueueDriver interface {
	// Push 按优先级将消息推入队列。priority 语义见 priority.go。
	Push(ctx context.Context, queueKey string, message []byte, priority int64) error

	// Pop 按优先级（高 -> 低）取出一条消息；队列为空时返回 (nil, nil)。
	Pop(ctx context.Context, queueKey string) ([]byte, error)

	// Size 返回队列当前消息总数（所有优先级之和）。
	Size(ctx context.Context, queueKey string) (int64, error)

	// Clear 清空指定队列（所有优先级）。
	Clear(ctx context.Context, queueKey string) error

	// Close 释放驱动占用的资源。
	Close() error
}

// BlockingPopper 是可选接口。实现它的驱动（如 Redis 的 BLPOP）可以在没有消息时
// 阻塞等待，从而避免轮询带来的空转与额外延迟，并按优先级返回消息。
type BlockingPopper interface {
	// BPop 阻塞地按优先级取出一条消息，最多等待 timeout。
	// 超时未取到消息时返回 (nil, nil)。
	BPop(ctx context.Context, queueKey string, timeout time.Duration) ([]byte, error)
}

// QueueItem 表示一条待批量入队的消息。
type QueueItem struct {
	QueueKey string
	Message  []byte
	Priority int64
}

// BatchPusher 是可选接口。实现它的驱动（如 Redis pipeline）可以在一次往返中
// 批量推送多条消息，提升批量入队吞吐。
type BatchPusher interface {
	PushBatch(ctx context.Context, items []QueueItem) error
}

// QueueConfig 队列配置接口。
type QueueConfig interface {
	// GetDriver 返回驱动类型标识。
	GetDriver() string
}
