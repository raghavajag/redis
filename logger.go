package main

import (
	"fmt"
	"sync"
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
	fmt.Printf("[DEBUG] %s\n", fmt.Sprintf(format, args...))
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("[INFO] %s\n", fmt.Sprintf(format, args...))
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("[ERROR] %s\n", fmt.Sprintf(format, args...))
}
