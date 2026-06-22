// 示例：自动重试 + 死信队列（DLQ）。
//
// 处理器前两次故意失败，第三次成功，演示指数退避重试；
// 另一条消息一直失败，最终进入死信队列。
//
// 运行：go run ./examples/retry_deadletter
package main

import (
	"log"
	"sync/atomic"
	"time"

	MagicQueue "github.com/JackyZhang8/MagicQueue"
)

type FlakyTask struct {
	ID          string `json:"id"`
	FailForever bool   `json:"fail_forever"`
}

// FlakyHandler 模拟不稳定的下游：前几次失败，之后成功。
type FlakyHandler struct {
	attempts int32
}

func (h *FlakyHandler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	var task FlakyTask
	if err := payload.ParseBody(&task); err != nil {
		return MagicQueue.NewResult(false, "parse failed", nil)
	}

	n := atomic.AddInt32(&h.attempts, 1)
	if task.FailForever {
		log.Printf("job %s: attempt %d -> fail (will exhaust retries)", task.ID, payload.Retry+1)
		return MagicQueue.NewResult(false, "permanent failure", nil)
	}
	if payload.Retry < 2 {
		log.Printf("job %s: attempt %d -> transient failure", task.ID, payload.Retry+1)
		return MagicQueue.NewResult(false, "transient failure", nil)
	}
	log.Printf("job %s: attempt %d -> success (total handler calls=%d)", task.ID, payload.Retry+1, n)
	return MagicQueue.NewResult(true, "ok", nil)
}

func main() {
	queue := MagicQueue.NewQueue("jobs").
		UseMemory(nil).
		WithOptions(MagicQueue.Options{
			RetryBaseDelay:   200 * time.Millisecond,
			RetryMaxDelay:    2 * time.Second,
			EnableDeadLetter: true,
		})

	queue.SetHandler("process", "", &FlakyHandler{})

	if err := queue.StartWorkers(2); err != nil {
		log.Fatalf("start workers: %v", err)
	}
	defer queue.Stop()

	queue.Enqueue(&MagicQueue.Payload{Topic: "process", Body: FlakyTask{ID: "recoverable"}, MaxRetry: 3})
	queue.Enqueue(&MagicQueue.Payload{Topic: "process", Body: FlakyTask{ID: "doomed", FailForever: true}, MaxRetry: 2})

	time.Sleep(5 * time.Second)

	// 查看死信队列大小。
	dead := queue.GetDeadLetterSize("process", "")
	log.Printf("dead-letter queue size: %d", dead)
}
