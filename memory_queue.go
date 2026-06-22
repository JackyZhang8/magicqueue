/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrQueueFull 在内存队列达到 MaxQueueSize 上限时由 Push 返回。
var ErrQueueFull = errors.New("memory queue is full")

// MemoryConfig 内存队列配置。
type MemoryConfig struct {
	// MaxQueueSize 单个逻辑队列（按 topic/group 区分，跨所有优先级合计）的
	// 最大容量，0 表示不限制。
	MaxQueueSize int
}

// GetDriver 实现 QueueConfig。
func (c *MemoryConfig) GetDriver() string {
	return "memory"
}

// bucketSet 是一个逻辑队列对应的三档优先级子队列。
type bucketSet struct {
	levels [numLevels]*list.List
}

func newBucketSet() *bucketSet {
	bs := &bucketSet{}
	for i := range bs.levels {
		bs.levels[i] = list.New()
	}
	return bs
}

func (b *bucketSet) total() int {
	n := 0
	for _, l := range b.levels {
		n += l.Len()
	}
	return n
}

// MemoryQueue 是基于内存的优先级队列实现，适用于开发与测试。
// 进程退出后队列内容会丢失（除非启用 LevelDB 持久化）。
type MemoryQueue struct {
	queues  map[string]*bucketSet
	maxSize int
	mutex   sync.RWMutex
}

// NewMemoryQueue 创建内存队列实例。
func NewMemoryQueue(config *MemoryConfig) *MemoryQueue {
	if config == nil {
		config = &MemoryConfig{}
	}
	return &MemoryQueue{
		queues:  make(map[string]*bucketSet),
		maxSize: config.MaxQueueSize,
	}
}

// pushLocked 假定已持锁。
func (q *MemoryQueue) pushLocked(queueKey string, message []byte, priority int64) error {
	bs, exists := q.queues[queueKey]
	if !exists {
		bs = newBucketSet()
		q.queues[queueKey] = bs
	}
	if q.maxSize > 0 && bs.total() >= q.maxSize {
		return ErrQueueFull
	}
	buf := make([]byte, len(message))
	copy(buf, message)
	bs.levels[priorityLevel(priority)].PushBack(buf)
	return nil
}

func (q *MemoryQueue) Push(_ context.Context, queueKey string, message []byte, priority int64) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.pushLocked(queueKey, message, priority)
}

// PushBatch 实现 BatchPusher，在单次加锁内推送多条消息。
func (q *MemoryQueue) PushBatch(_ context.Context, items []QueueItem) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for _, it := range items {
		if err := q.pushLocked(it.QueueKey, it.Message, it.Priority); err != nil {
			return err
		}
	}
	return nil
}

func (q *MemoryQueue) Pop(_ context.Context, queueKey string) ([]byte, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	bs, exists := q.queues[queueKey]
	if !exists {
		return nil, nil
	}
	for _, level := range popOrder {
		l := bs.levels[level]
		if l.Len() > 0 {
			element := l.Front()
			l.Remove(element)
			return element.Value.([]byte), nil
		}
	}
	return nil, nil
}

func (q *MemoryQueue) Size(_ context.Context, queueKey string) (int64, error) {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	if bs, exists := q.queues[queueKey]; exists {
		return int64(bs.total()), nil
	}
	return 0, nil
}

func (q *MemoryQueue) Clear(_ context.Context, queueKey string) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	delete(q.queues, queueKey)
	return nil
}

func (q *MemoryQueue) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.queues = make(map[string]*bucketSet)
	return nil
}
