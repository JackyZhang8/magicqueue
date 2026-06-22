package MagicQueue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingHandler 记录处理次数并可选地按策略返回成功/失败。
type countingHandler struct {
	calls   int32
	results []bool // 第 n 次调用返回的 State；超出范围按最后一个值
	done    chan struct{}
	once    sync.Once
	target  int32
}

func (h *countingHandler) Execute(p *Payload) *Result {
	n := atomic.AddInt32(&h.calls, 1)
	state := true
	if len(h.results) > 0 {
		idx := int(n - 1)
		if idx >= len(h.results) {
			idx = len(h.results) - 1
		}
		state = h.results[idx]
	}
	if h.done != nil && n >= h.target {
		h.once.Do(func() { close(h.done) })
	}
	return NewResult(state, "", nil)
}

func waitClosed(t *testing.T, ch chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timed out after %s", d)
	}
}

func TestMemoryQueueDriver(t *testing.T) {
	q := NewMemoryQueue(&MemoryConfig{MaxQueueSize: 2})
	ctx := context.Background()

	if err := q.Push(ctx, "k", []byte("a"), 0); err != nil {
		t.Fatalf("push: %v", err)
	}
	if err := q.Push(ctx, "k", []byte("b"), 0); err != nil {
		t.Fatalf("push: %v", err)
	}
	if err := q.Push(ctx, "k", []byte("c"), 0); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	size, _ := q.Size(ctx, "k")
	if size != 2 {
		t.Fatalf("expected size 2, got %d", size)
	}

	v, _ := q.Pop(ctx, "k")
	if string(v) != "a" {
		t.Fatalf("expected FIFO 'a', got %q", v)
	}

	empty, _ := q.Pop(ctx, "missing")
	if empty != nil {
		t.Fatalf("expected nil for empty queue, got %q", empty)
	}
}

func TestMemoryQueuePriority(t *testing.T) {
	q := NewMemoryQueue(nil)
	ctx := context.Background()

	// 入队顺序：normal, low, high, normal2 —— 期望出队 high, normal, normal2, low。
	_ = q.Push(ctx, "k", []byte("normal"), 0)
	_ = q.Push(ctx, "k", []byte("low"), -1)
	_ = q.Push(ctx, "k", []byte("high"), 5)
	_ = q.Push(ctx, "k", []byte("normal2"), 0)

	want := []string{"high", "normal", "normal2", "low"}
	for _, exp := range want {
		got, _ := q.Pop(ctx, "k")
		if string(got) != exp {
			t.Fatalf("priority order: expected %q, got %q", exp, got)
		}
	}
}

func TestMaxQueueSizeAcrossPriorities(t *testing.T) {
	q := NewMemoryQueue(&MemoryConfig{MaxQueueSize: 2})
	ctx := context.Background()
	if err := q.Push(ctx, "k", []byte("a"), 5); err != nil {
		t.Fatalf("push high: %v", err)
	}
	if err := q.Push(ctx, "k", []byte("b"), -1); err != nil {
		t.Fatalf("push low: %v", err)
	}
	// 上限是跨所有优先级合计的。
	if err := q.Push(ctx, "k", []byte("c"), 0); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull across priorities, got %v", err)
	}
}

func TestEnqueueBatch(t *testing.T) {
	h := &countingHandler{done: make(chan struct{}), target: 4}
	q := NewQueue("batch").UseMemory(nil)
	q.SetHandler("t", "", h)
	if err := q.StartWorkers(2); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	ids, err := q.EnqueueBatch([]*Payload{
		{Topic: "t"},
		{Topic: "t"},
		{Topic: "t"},
		{Topic: "t"},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(ids) != 4 {
		t.Fatalf("expected 4 ids, got %d", len(ids))
	}
	for i, id := range ids {
		if id == "" {
			t.Fatalf("id[%d] empty", i)
		}
	}
	waitClosed(t, h.done, 3*time.Second)
}

func TestEnqueueBatchValidation(t *testing.T) {
	q := NewQueue("batch").UseMemory(nil)
	if _, err := q.EnqueueBatch([]*Payload{{Topic: "ok"}, {Topic: ""}}); err == nil {
		t.Fatal("expected error for empty topic in batch")
	}
}

// orderHandler 记录处理顺序。
type orderHandler struct {
	mu    sync.Mutex
	order []string
	done  chan struct{}
	want  int
}

func (h *orderHandler) Execute(p *Payload) *Result {
	var body struct {
		Tag string `json:"tag"`
	}
	_ = p.ParseBody(&body)
	h.mu.Lock()
	h.order = append(h.order, body.Tag)
	if len(h.order) >= h.want {
		h.once()
	}
	h.mu.Unlock()
	return NewResult(true, "", nil)
}

func (h *orderHandler) once() {
	select {
	case <-h.done:
	default:
		close(h.done)
	}
}

func TestEndToEndPriorityOrder(t *testing.T) {
	h := &orderHandler{done: make(chan struct{}), want: 4}
	q := NewQueue("prio").UseMemory(nil)
	q.SetHandler("t", "", h)

	// 先入队（此时无消费者），再用单 worker 处理，顺序确定。
	type body struct {
		Tag string `json:"tag"`
	}
	for _, p := range []*Payload{
		{Topic: "t", Body: body{"normal"}, Priority: 0},
		{Topic: "t", Body: body{"low"}, Priority: -1},
		{Topic: "t", Body: body{"high"}, Priority: 9},
		{Topic: "t", Body: body{"normal2"}, Priority: 0},
	} {
		if _, err := q.Enqueue(p); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	if err := q.StartWorkers(1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	waitClosed(t, h.done, 3*time.Second)
	h.mu.Lock()
	defer h.mu.Unlock()
	want := []string{"high", "normal", "normal2", "low"}
	for i, w := range want {
		if i >= len(h.order) || h.order[i] != w {
			t.Fatalf("expected order %v, got %v", want, h.order)
		}
	}
}

func TestEnqueueValidation(t *testing.T) {
	q := NewQueue("t")
	if _, err := q.Enqueue(&Payload{Topic: "x"}); err != ErrDriverNotSet {
		t.Fatalf("expected ErrDriverNotSet, got %v", err)
	}

	q.UseMemory(nil)
	if _, err := q.Enqueue(&Payload{Topic: ""}); err != ErrEmptyTopic {
		t.Fatalf("expected ErrEmptyTopic, got %v", err)
	}

	id, err := q.Enqueue(&Payload{Topic: "x"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty id")
	}
}

func TestEndToEndMemory(t *testing.T) {
	h := &countingHandler{done: make(chan struct{}), target: 3}
	q := NewQueue("e2e").UseMemory(nil)
	q.SetHandler("topic", "group", h)

	if err := q.StartWorkers(2); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(&Payload{Topic: "topic", Group: "group"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	waitClosed(t, h.done, 3*time.Second)
	if got := atomic.LoadInt32(&h.calls); got != 3 {
		t.Fatalf("expected 3 calls, got %d", got)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	// 前两次失败，第三次成功 -> 处理器应被调用 3 次。
	h := &countingHandler{
		results: []bool{false, false, true},
		done:    make(chan struct{}),
		target:  3,
	}
	q := NewQueue("retry").UseMemory(nil).WithOptions(Options{
		RetryBaseDelay: 10 * time.Millisecond,
		RetryMaxDelay:  50 * time.Millisecond,
	})
	q.SetHandler("t", "", h)
	if err := q.StartWorkers(1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	if _, err := q.Enqueue(&Payload{Topic: "t", MaxRetry: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitClosed(t, h.done, 3*time.Second)
	if got := atomic.LoadInt32(&h.calls); got != 3 {
		t.Fatalf("expected 3 calls, got %d", got)
	}
}

func TestDeadLetter(t *testing.T) {
	h := &countingHandler{
		results: []bool{false}, // 永远失败
		done:    make(chan struct{}),
		target:  3, // 初始 + 2 次重试
	}
	q := NewQueue("dlq").UseMemory(nil).WithOptions(Options{
		RetryBaseDelay:   10 * time.Millisecond,
		RetryMaxDelay:    50 * time.Millisecond,
		EnableDeadLetter: true,
	})
	q.SetHandler("t", "", h)
	if err := q.StartWorkers(1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	if _, err := q.Enqueue(&Payload{Topic: "t", MaxRetry: 2}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitClosed(t, h.done, 3*time.Second)
	// 给框架一点时间把消息搬进死信队列。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if q.GetDeadLetterSize("t", "") == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected 1 message in dead-letter queue, got %d", q.GetDeadLetterSize("t", ""))
}

// panicHandler 会 panic，用于验证 worker 不会被拖垮。
type panicHandler struct{}

func (h *panicHandler) Execute(p *Payload) *Result {
	panic("boom")
}

func TestHandlerPanicRecovery(t *testing.T) {
	recovered := make(chan struct{}, 1)
	q := NewQueue("panic").UseMemory(nil).WithOptions(Options{
		RetryBaseDelay: 10 * time.Millisecond,
	}).RegisterOnInterrupt(func(stack string) {
		select {
		case recovered <- struct{}{}:
		default:
		}
	})
	q.SetHandler("t", "", &panicHandler{})
	if err := q.StartWorkers(1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()

	if _, err := q.Enqueue(&Payload{Topic: "t", MaxRetry: 0}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitClosed(t, recovered, 3*time.Second)
}

func TestPersistenceRecovery(t *testing.T) {
	dir := t.TempDir()

	// 第一阶段：入队持久化消息但不处理，模拟崩溃。
	q1 := NewQueue("p").UseMemory(nil).UseLevelDb(dir)
	if err := q1.Err(); err != nil {
		t.Fatalf("config: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := q1.Enqueue(&Payload{Topic: "job", IsPersist: true}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	q1.cleanup() // 关闭 LevelDB，但不删除数据

	// 第二阶段：新实例 + 全新内存驱动，仅靠 LevelDB 恢复。
	h := &countingHandler{done: make(chan struct{}), target: 3}
	q2 := NewQueue("p").UseMemory(nil).UseLevelDb(dir)
	if err := q2.Err(); err != nil {
		t.Fatalf("config: %v", err)
	}
	q2.SetHandler("job", "", h)
	if err := q2.StartWorkers(2); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q2.Stop()

	waitClosed(t, h.done, 3*time.Second)
	if got := atomic.LoadInt32(&h.calls); got < 3 {
		t.Fatalf("expected >=3 recovered calls, got %d", got)
	}
}

func TestHandlerKeyRoundTrip(t *testing.T) {
	q := NewQueue("n")
	cases := []struct{ topic, group string }{
		{"t", "g"},
		{"t", ""},
	}
	for _, c := range cases {
		key := q.formatHandlerKey(c.topic, c.group)
		topic, group := q.parseHandlerKey(key)
		if topic != c.topic || group != c.group {
			t.Fatalf("round trip mismatch: in (%q,%q) key=%q out (%q,%q)", c.topic, c.group, key, topic, group)
		}
	}
}

func TestStartTwiceFails(t *testing.T) {
	q := NewQueue("dup").UseMemory(nil)
	if err := q.StartWorkers(1); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer q.Stop()
	if err := q.StartWorkers(1); err != ErrAlreadyStarted {
		t.Fatalf("expected ErrAlreadyStarted, got %v", err)
	}
}
