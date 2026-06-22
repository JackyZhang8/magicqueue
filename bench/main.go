// Command bench 是 MagicQueue 的内存驱动微基准，用于三语言版本横向对比。
//
// 运行：go run ./bench   （或 go build -o bench ./bench && ./bench）
package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

const (
	enqueueN = 200_000
	batchN   = 200_000
	batchB   = 1000
	e2eN     = 200_000
	workers  = 4
	reps     = 3
)

type noopHandler struct {
	count *int64
	total int64
	done  chan struct{}
	once  *sync.Once
}

func (h *noopHandler) Execute(_ *MagicQueue.Payload) *MagicQueue.Result {
	if atomic.AddInt64(h.count, 1) == h.total {
		h.once.Do(func() { close(h.done) })
	}
	return MagicQueue.NewResult(true, "", nil)
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

func benchEnqueue() float64 {
	q := MagicQueue.NewQueue("bench").UseMemory(nil)
	start := time.Now()
	for i := 0; i < enqueueN; i++ {
		if _, err := q.Enqueue(&MagicQueue.Payload{Topic: "t"}); err != nil {
			panic(err)
		}
	}
	return float64(enqueueN) / time.Since(start).Seconds()
}

func benchBatch() float64 {
	q := MagicQueue.NewQueue("bench").UseMemory(nil)
	start := time.Now()
	for i := 0; i < batchN; i += batchB {
		batch := make([]*MagicQueue.Payload, 0, batchB)
		for j := 0; j < batchB && i+j < batchN; j++ {
			batch = append(batch, &MagicQueue.Payload{Topic: "t"})
		}
		if _, err := q.EnqueueBatch(batch); err != nil {
			panic(err)
		}
	}
	return float64(batchN) / time.Since(start).Seconds()
}

func benchE2E() float64 {
	var count int64
	var once sync.Once
	done := make(chan struct{})
	q := MagicQueue.NewQueue("bench").UseMemory(nil).
		WithOptions(MagicQueue.Options{StatsInterval: -1})
	q.SetHandler("t", "", &noopHandler{count: &count, total: int64(e2eN), done: done, once: &once})

	for i := 0; i < e2eN; i++ {
		if _, err := q.Enqueue(&MagicQueue.Payload{Topic: "t"}); err != nil {
			panic(err)
		}
	}
	start := time.Now()
	if err := q.StartWorkers(workers); err != nil {
		panic(err)
	}
	<-done
	elapsed := time.Since(start)
	q.Stop()
	return float64(e2eN) / elapsed.Seconds()
}

func run(name string, fn func() float64) {
	results := make([]float64, reps)
	for i := 0; i < reps; i++ {
		results[i] = fn()
	}
	fmt.Printf("%-28s %12.0f ops/s\n", name, median(results))
}

func main() {
	fmt.Printf("MagicQueue Go bench (N=%d, batch=%d, workers=%d, reps=%d)\n", enqueueN, batchB, workers, reps)
	run("enqueue (single, memory)", benchEnqueue)
	run("enqueue (batch=1000, memory)", benchBatch)
	run("end-to-end (4 workers)", benchE2E)
}
