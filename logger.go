/*
Copyright (C) MagicQueue
Author JackyZhang8
*/
package MagicQueue

import (
	"log"
	"os"
)

// Logger 是可插拔的日志接口。注入自定义实现即可把 MagicQueue 的日志
// 接入任意日志框架（zap / logrus / slog 等）。
type Logger interface {
	Printf(format string, v ...any)
}

// stdLogger 是基于标准库 log 的默认实现：带 "[MagicQueue] " 前缀、输出到 stderr。
type stdLogger struct {
	l *log.Logger
}

// Printf 实现 Logger。
func (s *stdLogger) Printf(format string, v ...any) {
	s.l.Printf(format, v...)
}

// defaultLogger 返回默认 Logger 实现。
func defaultLogger() Logger {
	return &stdLogger{l: log.New(os.Stderr, "[MagicQueue] ", log.LstdFlags)}
}
