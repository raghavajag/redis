package main

import (
	"fmt"
	"sync"
	"time"
)

type Logger struct {
	debug bool
	mu    sync.Mutex
}

func NewLogger(debug bool) *Logger {
	return &Logger{debug: debug}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if !l.debug {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[DEBUG] %s - %s\n", timestamp, fmt.Sprintf(format, args...))
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[INFO] %s - %s\n", timestamp, fmt.Sprintf(format, args...))
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[ERROR] %s - %s\n", timestamp, fmt.Sprintf(format, args...))
}
