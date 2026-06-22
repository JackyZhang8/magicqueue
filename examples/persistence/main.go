// 示例：LevelDB 持久化与崩溃恢复。
//
// 第一次运行（write 模式）只入队带 IsPersist 的消息但不处理，模拟进程在处理前崩溃：
//
//	go run ./examples/persistence write
//
// 第二次运行（recover 模式）启动 worker，框架会从 LevelDB 恢复上次未处理的消息：
//
//	go run ./examples/persistence recover
package main

import (
	"log"
	"os"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
)

const dbPath = "./data/persistence_example.db"

type Handler struct{}

func (h *Handler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	log.Printf("processed recovered/new job %s", payload.ID)
	return MagicQueue.NewResult(true, "ok", nil)
}

func main() {
	mode := "recover"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "write":
		// 注意：内存驱动在进程退出后会清空，这里仅依赖 LevelDB 做恢复演示。
		queue := MagicQueue.NewQueue("persist").UseMemory(nil).UseLevelDb(dbPath)
		if err := queue.Err(); err != nil {
			log.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			id, err := queue.Enqueue(&MagicQueue.Payload{Topic: "job", IsPersist: true})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("persisted job %s (not processed, simulating crash)", id)
		}
		// 不启动 worker，直接退出 —— 模拟“入队后、处理前”崩溃。
		log.Println("now run: go run ./examples/persistence recover")

	case "recover":
		queue := MagicQueue.NewQueue("persist").UseMemory(nil).UseLevelDb(dbPath)
		if err := queue.Err(); err != nil {
			log.Fatal(err)
		}
		queue.SetHandler("job", "", &Handler{})
		if err := queue.StartWorkers(2); err != nil {
			log.Fatal(err)
		}
		defer queue.Stop()

		// 等待恢复的消息被处理。
		time.Sleep(2 * time.Second)
		log.Println("recovery example finished")

	default:
		log.Fatalf("unknown mode %q (use 'write' or 'recover')", mode)
	}
}
