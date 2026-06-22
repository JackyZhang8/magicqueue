/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis 连接配置。
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// GetDriver 实现 QueueConfig。
func (c *RedisConfig) GetDriver() string {
	return "redis"
}

// RedisQueue 是基于 Redis List 的优先级队列实现，适用于生产与分布式部署。
//
// 每个逻辑队列拆成三个 List（key 加后缀 :p2/:p1/:p0）。消费使用多 key 的
// BLPOP（按 高 -> 普通 -> 低 排列），既保证优先级又是阻塞消费，
// 不再使用 LLEN 轮询 + LPOP。
type RedisQueue struct {
	client *redis.Client
	// ownsClient 表示 client 由 RedisQueue 自己创建，Close 时需要关闭它；
	// 若 client 由调用方传入，则不在此关闭，交还其生命周期给调用方。
	ownsClient bool
}

// NewRedisQueue 根据配置创建 Redis 队列，并测试连通性。
func NewRedisQueue(config *RedisConfig) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &RedisQueue{client: client, ownsClient: true}, nil
}

// NewRedisQueueFromClient 复用调用方已配置好的 *redis.Client，
// 从而保留其连接池、TLS、超时等设置，不会重新建连。
func NewRedisQueueFromClient(client *redis.Client) *RedisQueue {
	return &RedisQueue{client: client, ownsClient: false}
}

// orderedKeys 返回按消费优先级（高 -> 低）排列的子队列 key 列表。
func orderedKeys(queueKey string) []string {
	keys := make([]string, 0, numLevels)
	for _, level := range popOrder {
		keys = append(keys, levelSubKey(queueKey, level))
	}
	return keys
}

func (q *RedisQueue) Push(ctx context.Context, queueKey string, message []byte, priority int64) error {
	return q.client.RPush(ctx, levelSubKey(queueKey, priorityLevel(priority)), message).Err()
}

// PushBatch 实现 BatchPusher，使用 pipeline 在一次往返中推送多条消息。
func (q *RedisQueue) PushBatch(ctx context.Context, items []QueueItem) error {
	pipe := q.client.Pipeline()
	for _, it := range items {
		pipe.RPush(ctx, levelSubKey(it.QueueKey, priorityLevel(it.Priority)), it.Message)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) Pop(ctx context.Context, queueKey string) ([]byte, error) {
	for _, key := range orderedKeys(queueKey) {
		result, err := q.client.LPop(ctx, key).Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, nil
}

// BPop 实现 BlockingPopper：用多 key 的 BLPOP 按优先级阻塞等待。
// BLPOP 会依次检查给定 key，从第一个非空队列弹出，从而实现优先级消费。
func (q *RedisQueue) BPop(ctx context.Context, queueKey string, timeout time.Duration) ([]byte, error) {
	result, err := q.client.BLPop(ctx, timeout, orderedKeys(queueKey)...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// BLPOP 返回 [key, value]。
	if len(result) < 2 {
		return nil, nil
	}
	return []byte(result[1]), nil
}

func (q *RedisQueue) Size(ctx context.Context, queueKey string) (int64, error) {
	pipe := q.client.Pipeline()
	cmds := make([]*redis.IntCmd, 0, numLevels)
	for _, key := range orderedKeys(queueKey) {
		cmds = append(cmds, pipe.LLen(ctx, key))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var total int64
	for _, c := range cmds {
		total += c.Val()
	}
	return total, nil
}

func (q *RedisQueue) Clear(ctx context.Context, queueKey string) error {
	return q.client.Del(ctx, orderedKeys(queueKey)...).Err()
}

func (q *RedisQueue) Close() error {
	if q.ownsClient {
		return q.client.Close()
	}
	return nil
}
