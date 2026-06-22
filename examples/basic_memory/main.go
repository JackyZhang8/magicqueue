// 示例：使用内存队列处理任务（最简单的上手方式）。
//
// 运行：go run ./examples/basic_memory
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

// GreetTask 是一个简单的业务任务。
type GreetTask struct {
	Name string `json:"name"`
}

// GreetHandler 处理 GreetTask。
type GreetHandler struct {
	wg *sync.WaitGroup
}

func (h *GreetHandler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	defer h.wg.Done()

	var task GreetTask
	if err := payload.ParseBody(&task); err != nil {
		return MagicQueue.NewResult(false, fmt.Sprintf("parse failed: %v", err), nil)
	}
	log.Printf("Hello, %s! (job %s)", task.Name, payload.ID)
	return MagicQueue.NewResult(true, "greeted", nil)
}

func main() {
	var wg sync.WaitGroup

	queue := MagicQueue.NewQueue("greeter").UseMemory(nil)
	queue.SetHandler("greet", "default", &GreetHandler{wg: &wg})

	if err := queue.StartWorkers(2); err != nil {
		log.Fatalf("start workers: %v", err)
	}
	defer queue.Stop()

	names := []string{"Alice", "Bob", "Carol"}
	wg.Add(len(names))
	for _, name := range names {
		id, err := queue.Enqueue(&MagicQueue.Payload{
			Topic: "greet",
			Group: "default",
			Body:  GreetTask{Name: name},
		})
		if err != nil {
			log.Printf("enqueue failed: %v", err)
			wg.Done()
			continue
		}
		log.Printf("enqueued job %s", id)
	}

	// 等待所有任务被处理完（也可用 time.Sleep 简单等待）。
	waitWithTimeout(&wg, 5*time.Second)
	log.Println("all tasks processed")
}

func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		log.Println("timeout waiting for tasks")
	}
}
