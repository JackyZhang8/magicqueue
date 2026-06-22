/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/syndtr/goleveldb/leveldb"
)

// 常见错误。
var (
	ErrDriverNotSet   = errors.New("queue driver not set")
	ErrEmptyTopic     = errors.New("topic can not be empty")
	ErrAlreadyStarted = errors.New("queue already started")
)

// RecoveryListener 在 worker 处理消息发生 panic 并被恢复时回调，参数为堆栈信息。
type RecoveryListener func(stack string)

// Options 控制队列运行时行为，全部字段都有合理默认值。
type Options struct {
	// PollInterval 是驱动不支持阻塞弹出时的轮询间隔，默认 200ms。
	PollInterval time.Duration
	// StatsInterval 是统计日志输出间隔，默认 1 分钟；<=0 表示关闭统计。
	StatsInterval time.Duration
	// RetryBaseDelay 是重试指数退避的基准时长，默认 500ms。
	RetryBaseDelay time.Duration
	// RetryMaxDelay 是重试退避的上限，默认 30s。
	RetryMaxDelay time.Duration
	// EnableDeadLetter 为 true 时，重试耗尽的消息会被投递到死信队列而非直接丢弃。
	EnableDeadLetter bool
}

func (o *Options) withDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	if o.StatsInterval == 0 {
		o.StatsInterval = time.Minute
	}
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = 500 * time.Millisecond
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = 30 * time.Second
	}
}

// MQueue 是队列的核心类型。典型用法：
//
//	q := MagicQueue.NewQueue("svc").UseMemory(nil)
//	q.SetHandler("topic", "group", handler)
//	if err := q.StartWorkers(4); err != nil { ... }
//	defer q.Stop()
type MQueue struct {
	Name string

	driver QueueDriver
	ldb    *leveldb.DB
	logger Logger
	opts   Options

	handlers   map[string]Queueable
	handlersMu sync.RWMutex

	onRecovery RecoveryListener

	// 运行时状态。
	mu      sync.Mutex
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	jobs    chan Payload
	timers  []*time.Timer

	// err 收集链式调用（UseRedis/UseLevelDb 等）过程中产生的错误，
	// 在 StartWorkers 时统一返回。
	err error
}

// NewQueue 创建一个新的队列实例。
func NewQueue(name string) *MQueue {
	return &MQueue{
		Name:     name,
		logger:   defaultLogger(),
		handlers: make(map[string]Queueable),
	}
}

// SetLogger 注入自定义日志实现，返回自身以支持链式调用。
func (r *MQueue) SetLogger(l Logger) *MQueue {
	if l != nil {
		r.logger = l
	}
	return r
}

// WithOptions 设置运行时选项，返回自身以支持链式调用。
func (r *MQueue) WithOptions(opts Options) *MQueue {
	r.opts = opts
	return r
}

// RegisterOnInterrupt 注册 panic 恢复回调。
func (r *MQueue) RegisterOnInterrupt(listener RecoveryListener) *MQueue {
	r.onRecovery = listener
	return r
}

// Err 返回链式配置过程中累计的第一个错误（如 Redis 连接失败、LevelDB 打开失败）。
func (r *MQueue) Err() error {
	return r.err
}

func (r *MQueue) setErr(err error) {
	if r.err == nil {
		r.err = err
	}
}

// SetHandler 注册 (topic, group) 对应的处理器。
func (r *MQueue) SetHandler(topic string, group string, e Queueable) *MQueue {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()
	r.handlers[r.formatHandlerKey(topic, group)] = e
	return r
}

// UseRedis 使用调用方传入的 *redis.Client 作为队列驱动（复用其连接池/TLS 等配置）。
func (r *MQueue) UseRedis(client *redis.Client) *MQueue {
	if client == nil {
		r.setErr(errors.New("redis client is nil"))
		return r
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		r.setErr(fmt.Errorf("redis ping failed: %w", err))
		return r
	}
	r.driver = NewRedisQueueFromClient(client)
	return r
}

// UseRedisConfig 根据配置自建 Redis 驱动。
func (r *MQueue) UseRedisConfig(config *RedisConfig) *MQueue {
	queue, err := NewRedisQueue(config)
	if err != nil {
		r.setErr(fmt.Errorf("init redis queue: %w", err))
		return r
	}
	r.driver = queue
	return r
}

// UseMemory 使用内存队列驱动。
func (r *MQueue) UseMemory(config *MemoryConfig) *MQueue {
	r.driver = NewMemoryQueue(config)
	return r
}

// UseLevelDb 启用 LevelDB 持久化，用于崩溃恢复。
func (r *MQueue) UseLevelDb(ldbPath string) *MQueue {
	ldb, err := leveldb.OpenFile(ldbPath, nil)
	if err != nil {
		r.setErr(fmt.Errorf("open leveldb %q: %w", ldbPath, err))
		return r
	}
	r.ldb = ldb
	return r
}

func (r *MQueue) formatQueueKey(topic string, group string) string {
	if len(group) > 0 {
		return fmt.Sprintf("%s_%s::%s", r.Name, group, topic)
	}
	return fmt.Sprintf("%s_%s", r.Name, topic)
}

func (r *MQueue) formatHandlerKey(topic string, group string) string {
	if len(topic) > 0 && len(group) > 0 {
		return fmt.Sprintf("%s::%s", group, topic)
	}
	return topic
}

func (r *MQueue) parseHandlerKey(name string) (topic string, group string) {
	index := strings.Index(name, "::")
	if index == -1 {
		return name, ""
	}
	return name[index+2:], name[:index]
}

func (r *MQueue) deadLetterKey(topic string, group string) string {
	return r.formatQueueKey(topic, group) + "::dead"
}

// GetQueueSize 返回指定 (topic, group) 队列的当前消息数量。
func (r *MQueue) GetQueueSize(topic string, group string) int64 {
	if r.driver == nil {
		return 0
	}
	ctx := r.runtimeCtx()
	size, err := r.driver.Size(ctx, r.formatQueueKey(topic, group))
	if err != nil {
		r.logger.Printf("failed to get queue size: %v", err)
		return 0
	}
	return size
}

// GetDeadLetterSize 返回指定 (topic, group) 死信队列的消息数量。
func (r *MQueue) GetDeadLetterSize(topic string, group string) int64 {
	if r.driver == nil {
		return 0
	}
	size, err := r.driver.Size(r.runtimeCtx(), r.deadLetterKey(topic, group))
	if err != nil {
		r.logger.Printf("failed to get dead-letter size: %v", err)
		return 0
	}
	return size
}

func (r *MQueue) runtimeCtx() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// Enqueue 将消息入队，返回消息 ID。
func (r *MQueue) Enqueue(payload *Payload) (string, error) {
	if r.driver == nil {
		return "", ErrDriverNotSet
	}
	if err := r.prepare(payload); err != nil {
		return "", err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	ctx := r.runtimeCtx()

	// 仅当调用方要求持久化且配置了 LevelDB 时才写入持久层。
	if payload.IsPersist {
		if r.ldb == nil {
			r.logger.Printf("warning: IsPersist=true but LevelDB not configured, message %s won't be recoverable", payload.ID)
		} else if err := r.ldb.Put([]byte(payload.ID), data, nil); err != nil {
			return "", fmt.Errorf("persist payload: %w", err)
		}
	}

	if err := r.driver.Push(ctx, r.formatQueueKey(payload.Topic, payload.Group), data, payload.Priority); err != nil {
		// 推送失败时回滚持久层，避免出现“已持久化但从未投递”的孤儿消息。
		if payload.IsPersist && r.ldb != nil {
			_ = r.ldb.Delete([]byte(payload.ID), nil)
		}
		return "", fmt.Errorf("push to driver: %w", err)
	}
	return payload.ID, nil
}

// prepare 校验消息并生成 ID。
func (r *MQueue) prepare(payload *Payload) error {
	if payload == nil {
		return errors.New("payload is nil")
	}
	if len(payload.Topic) == 0 {
		return ErrEmptyTopic
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	payload.ID = id.String()
	return nil
}

// EnqueueBatch 一次性入队多条消息，返回各消息的 ID（顺序与入参一致）。
//
// 若驱动实现了 BatchPusher（如 Redis pipeline），会在一次往返中完成推送；
// 持久化消息会通过 LevelDB 批量写入。任一消息校验失败则整体不入队。
func (r *MQueue) EnqueueBatch(payloads []*Payload) ([]string, error) {
	if r.driver == nil {
		return nil, ErrDriverNotSet
	}
	if len(payloads) == 0 {
		return nil, nil
	}

	ctx := r.runtimeCtx()
	ids := make([]string, len(payloads))
	items := make([]QueueItem, len(payloads))

	batch := new(leveldb.Batch)
	hasPersist := false

	for i, p := range payloads {
		if err := r.prepare(p); err != nil {
			return nil, fmt.Errorf("payload[%d]: %w", i, err)
		}
		data, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("payload[%d] marshal: %w", i, err)
		}
		ids[i] = p.ID
		items[i] = QueueItem{
			QueueKey: r.formatQueueKey(p.Topic, p.Group),
			Message:  data,
			Priority: p.Priority,
		}
		if p.IsPersist {
			if r.ldb == nil {
				r.logger.Printf("warning: IsPersist=true but LevelDB not configured, message %s won't be recoverable", p.ID)
			} else {
				batch.Put([]byte(p.ID), data)
				hasPersist = true
			}
		}
	}

	if hasPersist {
		if err := r.ldb.Write(batch, nil); err != nil {
			return nil, fmt.Errorf("persist batch: %w", err)
		}
	}

	if err := r.pushBatch(ctx, items); err != nil {
		// 回滚持久层，避免孤儿消息。
		if hasPersist {
			rollback := new(leveldb.Batch)
			for i, p := range payloads {
				if p.IsPersist {
					rollback.Delete([]byte(ids[i]))
				}
			}
			_ = r.ldb.Write(rollback, nil)
		}
		return nil, fmt.Errorf("push batch to driver: %w", err)
	}
	return ids, nil
}

// pushBatch 优先使用驱动的 BatchPusher，否则退化为逐条 Push。
func (r *MQueue) pushBatch(ctx context.Context, items []QueueItem) error {
	if bp, ok := r.driver.(BatchPusher); ok {
		return bp.PushBatch(ctx, items)
	}
	for _, it := range items {
		if err := r.driver.Push(ctx, it.QueueKey, it.Message, it.Priority); err != nil {
			return err
		}
	}
	return nil
}

// StartWorkers 启动恢复、消费、worker 与统计协程，立即返回。
// 调用方应在退出前调用 Stop 以优雅关闭。
func (r *MQueue) StartWorkers(workerNum int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return r.err
	}
	if r.driver == nil {
		return ErrDriverNotSet
	}
	if r.started {
		return ErrAlreadyStarted
	}
	if workerNum <= 0 {
		workerNum = 1
	}

	r.opts.withDefaults()
	r.started = true
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.jobs = make(chan Payload, workerNum*2)

	// 先恢复持久化消息（重新投递回驱动），再启动消费者，避免重复消费。
	r.recoverPersistentMessages()

	r.handlersMu.RLock()
	keys := make([]string, 0, len(r.handlers))
	for key := range r.handlers {
		keys = append(keys, key)
	}
	r.handlersMu.RUnlock()

	for _, key := range keys {
		topic, group := r.parseHandlerKey(key)
		r.wg.Add(1)
		go r.consume(topic, group)
	}

	for n := 0; n < workerNum; n++ {
		r.wg.Add(1)
		go r.worker(n)
	}

	if r.opts.StatsInterval > 0 {
		r.wg.Add(1)
		go r.statsReporter()
	}

	r.logger.Printf("queue %q started with %d workers", r.Name, workerNum)
	return nil
}

// Stop 优雅停止队列：取消上下文、停止重试定时器、等待所有协程退出并释放资源。
func (r *MQueue) Stop() {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	r.started = false
	cancel := r.cancel
	timers := r.timers
	r.timers = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, t := range timers {
		t.Stop()
	}
	r.wg.Wait()
	r.cleanup()
	r.logger.Printf("queue %q stopped", r.Name)
}

func (r *MQueue) cleanup() {
	if r.ldb != nil {
		_ = r.ldb.Close()
	}
	if r.driver != nil {
		_ = r.driver.Close()
	}
}

// recoverPersistentMessages 在启动时把 LevelDB 中尚未确认的消息重新投递回驱动。
// 注意：这提供 at-least-once 语义，处理器应保证幂等。
func (r *MQueue) recoverPersistentMessages() {
	if r.ldb == nil {
		return
	}

	iter := r.ldb.NewIterator(nil, nil)
	defer iter.Release()

	recovered := 0
	for iter.Next() {
		var payload Payload
		val := append([]byte(nil), iter.Value()...) // 复制，迭代器底层缓冲会被复用
		if err := json.Unmarshal(val, &payload); err != nil {
			r.logger.Printf("recovery: failed to unmarshal payload: %v", err)
			continue
		}
		if err := r.driver.Push(r.ctx, r.formatQueueKey(payload.Topic, payload.Group), val, payload.Priority); err != nil {
			r.logger.Printf("recovery: failed to requeue %s: %v", payload.ID, err)
			continue
		}
		recovered++
	}
	if err := iter.Error(); err != nil {
		r.logger.Printf("recovery: iteration error: %v", err)
	}
	if recovered > 0 {
		r.logger.Printf("recovered %d persistent message(s)", recovered)
	}
}

// consume 持续从驱动弹出消息并投递到 worker 池。
func (r *MQueue) consume(topic string, group string) {
	defer r.wg.Done()

	queueKey := r.formatQueueKey(topic, group)
	blocking, _ := r.driver.(BlockingPopper)

	for {
		if r.ctx.Err() != nil {
			return
		}

		var (
			data []byte
			err  error
		)
		if blocking != nil {
			data, err = blocking.BPop(r.ctx, queueKey, time.Second)
		} else {
			data, err = r.driver.Pop(r.ctx, queueKey)
		}

		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			r.logger.Printf("consume %s: pop error: %v", queueKey, err)
			if r.sleepCtx(r.opts.PollInterval) {
				return
			}
			continue
		}
		if data == nil {
			if blocking == nil && r.sleepCtx(r.opts.PollInterval) {
				return
			}
			continue
		}

		var payload Payload
		if err := json.Unmarshal(data, &payload); err != nil {
			r.logger.Printf("consume %s: unmarshal error: %v", queueKey, err)
			continue
		}

		select {
		case r.jobs <- payload:
		case <-r.ctx.Done():
			return
		}
	}
}

// sleepCtx 在等待 d 期间若 ctx 被取消则提前返回 true。
func (r *MQueue) sleepCtx(d time.Duration) (cancelled bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return false
	case <-r.ctx.Done():
		return true
	}
}

func (r *MQueue) worker(workerID int) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.jobs:
			r.handle(workerID, job)
		}
	}
}

// handle 执行单条消息，并在处理器 panic 时恢复，避免拖垮整个 worker。
func (r *MQueue) handle(workerID int, job Payload) {
	r.handlersMu.RLock()
	handler, exists := r.handlers[r.formatHandlerKey(job.Topic, job.Group)]
	r.handlersMu.RUnlock()

	if !exists {
		r.logger.Printf("no handler for topic=%q group=%q (job %s)", job.Topic, job.Group, job.ID)
		return
	}

	result := r.safeExecute(handler, &job)

	if result != nil && result.State {
		r.ack(job)
		return
	}

	if job.Retry < job.MaxRetry {
		r.scheduleRetry(job)
		return
	}

	r.logger.Printf("job %s failed permanently after %d retries", job.ID, job.Retry)
	if r.opts.EnableDeadLetter {
		r.toDeadLetter(job)
	}
	r.ack(job)
}

// safeExecute 调用处理器并捕获 panic。
func (r *MQueue) safeExecute(handler Queueable, job *Payload) (result *Result) {
	defer func() {
		if rec := recover(); rec != nil {
			stack := captureStack()
			msg := fmt.Sprintf("panic while processing job %s: %v\n%s", job.ID, rec, stack)
			r.logger.Printf("%s", msg)
			if r.onRecovery != nil {
				r.onRecovery(msg)
			}
			result = NewResult(false, "handler panicked", nil)
		}
	}()
	return handler.Execute(job)
}

// ack 从持久层删除已确认（成功或永久失败）的消息。
func (r *MQueue) ack(job Payload) {
	if job.IsPersist && r.ldb != nil {
		if err := r.ldb.Delete([]byte(job.ID), nil); err != nil {
			r.logger.Printf("failed to delete job %s from LevelDB: %v", job.ID, err)
		}
	}
}

// scheduleRetry 以指数退避（带抖动）的方式异步重投消息，不阻塞 worker。
func (r *MQueue) scheduleRetry(job Payload) {
	job.Retry++
	delay := r.backoff(job.Retry)
	r.logger.Printf("job %s failed, retry %d/%d in %s", job.ID, job.Retry, job.MaxRetry, delay)

	t := time.AfterFunc(delay, func() {
		if r.ctx.Err() != nil {
			return
		}
		if err := r.requeue(job); err != nil {
			r.logger.Printf("failed to requeue job %s: %v", job.ID, err)
		}
	})

	r.mu.Lock()
	if r.started {
		r.timers = append(r.timers, t)
	} else {
		t.Stop()
	}
	r.mu.Unlock()
}

// requeue 重新持久化（更新 retry 计数）并推回驱动。
func (r *MQueue) requeue(job Payload) error {
	data, err := json.Marshal(&job)
	if err != nil {
		return err
	}
	if job.IsPersist && r.ldb != nil {
		if err := r.ldb.Put([]byte(job.ID), data, nil); err != nil {
			return err
		}
	}
	return r.driver.Push(r.ctx, r.formatQueueKey(job.Topic, job.Group), data, job.Priority)
}

func (r *MQueue) toDeadLetter(job Payload) {
	data, err := json.Marshal(&job)
	if err != nil {
		r.logger.Printf("dead-letter marshal error for job %s: %v", job.ID, err)
		return
	}
	if err := r.driver.Push(r.ctx, r.deadLetterKey(job.Topic, job.Group), data, job.Priority); err != nil {
		r.logger.Printf("failed to move job %s to dead-letter: %v", job.ID, err)
	}
}

func (r *MQueue) backoff(retry int) time.Duration {
	if retry < 1 {
		retry = 1
	}
	d := r.opts.RetryBaseDelay << (retry - 1)
	if d <= 0 || d > r.opts.RetryMaxDelay {
		d = r.opts.RetryMaxDelay
	}
	// 抖动：[0.5d, 1.0d]，避免重试风暴。
	jitter := time.Duration(rand.Int63n(int64(d)/2 + 1))
	return d/2 + jitter
}

// QueueStats 是某一时刻各队列的消息数量快照。
type QueueStats struct {
	QueueSizes map[string]int64
}

func (r *MQueue) collectStats() *QueueStats {
	stats := &QueueStats{QueueSizes: make(map[string]int64)}

	r.handlersMu.RLock()
	keys := make([]string, 0, len(r.handlers))
	for key := range r.handlers {
		keys = append(keys, key)
	}
	r.handlersMu.RUnlock()

	for _, key := range keys {
		topic, group := r.parseHandlerKey(key)
		stats.QueueSizes[r.formatQueueKey(topic, group)] = r.GetQueueSize(topic, group)
	}
	return stats
}

func (r *MQueue) statsReporter() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.opts.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			stats := r.collectStats()
			r.logger.Printf("=== Queue Statistics ===")
			for queueKey, size := range stats.QueueSizes {
				r.logger.Printf("Queue %s: %d messages", queueKey, size)
			}
			r.logger.Printf("=====================")
		}
	}
}

func captureStack() string {
	var sb strings.Builder
	for i := 3; ; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fmt.Fprintf(&sb, "%s:%d\n", file, line)
	}
	return sb.String()
}
