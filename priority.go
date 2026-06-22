/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import "fmt"

// 消息优先级分为三档。Payload.Priority 按符号映射：
//
//	Priority  > 0 -> 高优先级 (levelHigh)
//	Priority == 0 -> 普通优先级 (levelNormal，零值默认)
//	Priority  < 0 -> 低优先级 (levelLow)
//
// 同一档内保持 FIFO。底层每个队列拆成三个子队列（key 加后缀 :p2/:p1/:p0），
// 消费时按 高 -> 普通 -> 低 的顺序取消息，从而实现优先级且不破坏阻塞消费。
const (
	levelLow    = 0
	levelNormal = 1
	levelHigh   = 2
	numLevels   = 3
)

// popOrder 定义子队列被消费的优先顺序（高 -> 普通 -> 低）。
var popOrder = [numLevels]int{levelHigh, levelNormal, levelLow}

// priorityLevel 将 Payload.Priority 映射到子队列档位。
func priorityLevel(priority int64) int {
	switch {
	case priority > 0:
		return levelHigh
	case priority < 0:
		return levelLow
	default:
		return levelNormal
	}
}

// levelSubKey 返回某个优先级档位对应的子队列 key。
func levelSubKey(base string, level int) string {
	return fmt.Sprintf("%s:p%d", base, level)
}
