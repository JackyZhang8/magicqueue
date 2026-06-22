// 示例：在同一个队列实例上注册多个 topic/group 处理器。
//
// 运行：go run ./examples/multiple_handlers
package main

import (
	"log"
	"sync"
	"time"

	MagicQueue "github.com/JackyZhang8/MagicQueue"
)

type genericHandler struct {
	name string
	wg   *sync.WaitGroup
}

func (h *genericHandler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	defer h.wg.Done()
	log.Printf("[%s] handling job %s (topic=%s group=%s)", h.name, payload.ID, payload.Topic, payload.Group)
	return MagicQueue.NewResult(true, "ok", nil)
}

func main() {
	var wg sync.WaitGroup

	queue := MagicQueue.NewQueue("multi").UseMemory(nil)
	queue.
		SetHandler("email", "notification", &genericHandler{name: "email", wg: &wg}).
		SetHandler("sms", "notification", &genericHandler{name: "sms", wg: &wg}).
		SetHandler("report", "", &genericHandler{name: "report", wg: &wg})

	if err := queue.StartWorkers(4); err != nil {
		log.Fatalf("start workers: %v", err)
	}
	defer queue.Stop()

	jobs := []MagicQueue.Payload{
		{Topic: "email", Group: "notification"},
		{Topic: "sms", Group: "notification"},
		{Topic: "report"},
		{Topic: "email", Group: "notification"},
	}
	wg.Add(len(jobs))
	for i := range jobs {
		if _, err := queue.Enqueue(&jobs[i]); err != nil {
			log.Printf("enqueue failed: %v", err)
			wg.Done()
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Println("all jobs processed")
	case <-time.After(5 * time.Second):
		log.Println("timeout")
	}
}
