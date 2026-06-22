// 示例：使用 Redis 队列（生产环境推荐），并复用调用方自建的 *redis.Client。
//
// 需要本地有 Redis：docker run -p 6379:6379 redis
// 运行：go run ./examples/redis
package main

import (
	"fmt"
	"log"
	"time"

	MagicQueue "github.com/JackyZhang8/magicqueue"
	"github.com/redis/go-redis/v9"
)

type EmailTask struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type EmailHandler struct{}

func (h *EmailHandler) Execute(payload *MagicQueue.Payload) *MagicQueue.Result {
	var task EmailTask
	if err := payload.ParseBody(&task); err != nil {
		return MagicQueue.NewResult(false, fmt.Sprintf("parse failed: %v", err), nil)
	}
	log.Printf("sending email to %s, subject %q", task.To, task.Subject)
	time.Sleep(200 * time.Millisecond) // 模拟网络耗时
	return MagicQueue.NewResult(true, "sent", nil)
}

func main() {
	// 调用方完全掌控 client 配置（连接池、TLS、超时等），队列会复用它。
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		PoolSize: 20,
	})

	queue := MagicQueue.NewQueue("email_service").
		UseRedis(rdb).
		UseLevelDb("./data/redis_example.db")

	if err := queue.Err(); err != nil {
		log.Fatalf("queue config error: %v", err)
	}

	queue.SetHandler("email", "notification", &EmailHandler{})

	if err := queue.StartWorkers(4); err != nil {
		log.Fatalf("start workers: %v", err)
	}
	defer queue.Stop()

	for i := 1; i <= 5; i++ {
		id, err := queue.Enqueue(&MagicQueue.Payload{
			Topic:     "email",
			Group:     "notification",
			Body:      EmailTask{To: fmt.Sprintf("user%d@example.com", i), Subject: "Hi"},
			IsPersist: true,
			MaxRetry:  3,
		})
		if err != nil {
			log.Printf("enqueue failed: %v", err)
			continue
		}
		log.Printf("enqueued %s", id)
	}

	time.Sleep(3 * time.Second)
}
