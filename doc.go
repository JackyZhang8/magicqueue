/*
Copyright (C) MagicQueue
Author JackyZhang8
*/

// Package MagicQueue 是一个轻量的 Go 消息队列库。
//
// 它提供可插拔的队列驱动（Redis / 内存）、基于 LevelDB 的持久化与崩溃恢复、
// 带指数退避的自动重试、可选死信队列，以及优雅关闭。
//
// 交付语义为 at-least-once（至少一次），消息可能被重复投递，因此处理器应保证幂等。
//
// 典型用法：
//
//	queue := MagicQueue.NewQueue("svc").UseMemory(nil)
//	queue.SetHandler("topic", "group", handler)
//	if err := queue.StartWorkers(4); err != nil {
//		log.Fatal(err)
//	}
//	defer queue.Stop()
//
//	id, err := queue.Enqueue(&MagicQueue.Payload{Topic: "topic", Group: "group", Body: task})
//
// 更多示例见 examples/ 目录。
package MagicQueue
