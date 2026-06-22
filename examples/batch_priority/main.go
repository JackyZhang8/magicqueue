// 示例：批量入队 + 消息优先级。
//
// 先在没有消费者时批量入队不同优先级的消息，再用单 worker 处理，
// 可观察到高优先级先被处理（同档 FIFO）。
//
// 运行：go run ./examples/batch_priority
package main

import (
	"log"
	"sync"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

type Job struct {
	Tag string `json:"tag"`
}

type Handler struct {
	wg *sync.WaitGroup
}

func (h *Handler) Execute(p *MagicQueue.Payload) *MagicQueue.Result {
	defer h.wg.Done()
	var job Job
	_ = p.ParseBody(&job)
	log.Printf("processed %-8s (priority=%d)", job.Tag, p.Priority)
	return MagicQueue.NewResult(true, "ok", nil)
}

func main() {
	var wg sync.WaitGroup

	queue := MagicQueue.NewQueue("work").UseMemory(nil)
	queue.SetHandler("task", "", &Handler{wg: &wg})

	// 批量入队：故意按 普通/低/高/普通 的顺序提交。
	payloads := []*MagicQueue.Payload{
		{Topic: "task", Body: Job{"normal-1"}, Priority: 0},
		{Topic: "task", Body: Job{"low-1"}, Priority: -1},
		{Topic: "task", Body: Job{"high-1"}, Priority: 10},
		{Topic: "task", Body: Job{"normal-2"}, Priority: 0},
		{Topic: "task", Body: Job{"high-2"}, Priority: 5},
	}
	wg.Add(len(payloads))

	ids, err := queue.EnqueueBatch(payloads)
	if err != nil {
		log.Fatalf("batch enqueue: %v", err)
	}
	log.Printf("batch-enqueued %d messages: %v", len(ids), ids)

	// 入队后再启动单 worker，处理顺序确定：高(按FIFO) -> 普通(FIFO) -> 低。
	if err := queue.StartWorkers(1); err != nil {
		log.Fatalf("start workers: %v", err)
	}
	defer queue.Stop()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Println("expected order: high-1, high-2, normal-1, normal-2, low-1")
	case <-time.After(5 * time.Second):
		log.Println("timeout")
	}
}
