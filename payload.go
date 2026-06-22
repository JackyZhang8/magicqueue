/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import "encoding/json"

// Queueable 是任务处理器需要实现的接口。
// 每条消息会被分发给与其 (topic, group) 匹配的处理器执行。
type Queueable interface {
	Execute(*Payload) *Result
}

// Payload 表示一条入队消息。
type Payload struct {
	// ID 由 Enqueue 自动生成，调用方无需填写。
	ID string `json:"id"`
	// IsPersist 为 true 且配置了 LevelDB 时，消息会被持久化以支持崩溃恢复。
	IsPersist bool `json:"is_persist"`
	// Topic 消息主题，必填。
	Topic string `json:"topic"`
	// Group 消息分组，可选。
	Group string `json:"group"`
	// Body 业务数据，任意可被 JSON 序列化的类型。
	Body interface{} `json:"body"`
	// Priority 消息优先级：>0 高，==0 普通（默认），<0 低。同档内 FIFO。
	Priority int64 `json:"priority"`
	// MaxRetry 最大重试次数，处理失败时使用。
	MaxRetry int `json:"max_retry"`
	// Retry 当前已重试次数，由框架维护。
	Retry int `json:"retry"`
}

// ParseBody 将 Body 解析到给定的结构体指针中。
func (p *Payload) ParseBody(v interface{}) error {
	jsonData, err := json.Marshal(p.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, v)
}

// Result 表示处理器的执行结果。
// State 为 false 时，框架会按 MaxRetry 进行重试。
type Result struct {
	State   bool        `json:"state"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// NewResult 创建一个处理结果。
func NewResult(state bool, msg string, data interface{}) *Result {
	return &Result{State: state, Message: msg, Data: data}
}

// NewQueueResult 是 NewResult 的别名，保留以兼容旧代码。
//
// Deprecated: 请使用 NewResult。
func NewQueueResult(state bool, msg string, data interface{}) *Result {
	return NewResult(state, msg, data)
}
