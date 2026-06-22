// 示例：优雅关闭。监听 SIGINT/SIGTERM，收到信号后调用 Stop()，
// 等待正在处理的任务完成并释放资源（LevelDB、Redis 连接等）。
//
// 运行：go run ./examples/graceful_shutdown ，然后按 Ctrl+C。
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

type SlowHandler struct{}

func (h *SlowHandler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	log.Printf("processing job %s ...", payload.ID)
	time.Sleep(500 * time.Millisecond)
	log.Printf("done job %s", payload.ID)
	return MagicQueue.NewResult(true, "ok", nil)
}

func main() {
	queue := MagicQueue.NewQueue("worker").UseMemory(nil)
	queue.SetHandler("task", "", &SlowHandler{})

	if err := queue.StartWorkers(3); err != nil {
		log.Fatalf("start workers: %v", err)
	}

	// 持续产生任务。
	stopProducer := make(chan struct{})
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProducer:
				return
			case <-ticker.C:
				queue.Enqueue(&MagicQueue.Payload{Topic: "task"})
			}
		}
	}()

	// 等待退出信号。
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down gracefully ...")
	close(stopProducer)
	queue.Stop() // 阻塞直到所有协程退出、资源释放
	log.Println("bye")
}
