package logger

import (
	"fmt"
	"time"
)

// Layer 1: Logging infrastructure (simple wrapper)

// Logger: Simple structured logging interface
type Logger struct {
	level LogLevel
}

// LogLevel: Log severity levels
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

var levelNames = map[LogLevel]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
}

var currentLogger *Logger

// Initialize logger with level
func Init(levelStr string) {
	level := InfoLevel
	switch levelStr {
	case "debug":
		level = DebugLevel
	case "info":
		level = InfoLevel
	case "warn":
		level = WarnLevel
	case "error":
		level = ErrorLevel
	}

	currentLogger = &Logger{
		level: level,
	}
}

// Get returns the global logger instance
func Get() *Logger {
	if currentLogger == nil {
		currentLogger = &Logger{level: InfoLevel}
	}
	return currentLogger
}

// Private log method
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.level {
		return // Skip if below threshold
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	levelName := levelNames[level]
	message := fmt.Sprintf(format, args...)

	fmt.Printf("[%s] %s: %s\n", timestamp, levelName, message)
}

// Public logging methods

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DebugLevel, format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(InfoLevel, format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WarnLevel, format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ErrorLevel, format, args...)
}

// Convenience functions using global logger

func Debug(format string, args ...interface{}) {
	Get().Debug(format, args...)
}

func Info(format string, args ...interface{}) {
	Get().Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	Get().Warn(format, args...)
}

func Error(format string, args ...interface{}) {
	Get().Error(format, args...)
}

// WithFields: Return a logger with context fields
// TODO: Implement structured logging with fields
func WithFields(fields map[string]interface{}) *Logger {
	return Get() // Simplified for now
}
