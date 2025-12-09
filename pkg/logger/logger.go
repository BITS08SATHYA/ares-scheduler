// File: pkg/logger/logger.go
// Logger: Simple structured logging wrapper
// Implements: Sync() method for graceful logger shutdown

package logger

import (
	"fmt"
	"os"
	"time"
)

// ============================================================================
// LOGGER INTERFACE & TYPES
// ============================================================================

// Logger: Simple structured logging interface
type Logger struct {
	level LogLevel
	name  string
}

// LogLevel: Log severity levels
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// levelNames: Map level to string
var levelNames = map[LogLevel]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
}

// ============================================================================
// GLOBAL LOGGER INSTANCE
// ============================================================================

var globalLogger *Logger

func init() {
	globalLogger = &Logger{
		level: DebugLevel, // turn it on for entering into debugging mode
		//level: InfoLevel, // turn it on for turn off debugging mode
		name: "ares",
	}
}

// Get: Get the global logger instance
func Get() *Logger {
	return globalLogger
}

// ============================================================================
// LOGGING METHODS
// ============================================================================

// Debug: Log debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.log(DebugLevel, format, args...)
	}
}

// Info: Log info message
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.log(InfoLevel, format, args...)
	}
}

// Warn: Log warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WarnLevel {
		l.log(WarnLevel, format, args...)
	}
}

// Error: Log error message
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.log(ErrorLevel, format, args...)
	}
}

// ============================================================================
// INTERNAL LOGGING
// ============================================================================

// log: Internal logging function
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelStr := levelNames[level]

	message := fmt.Sprintf(format, args...)
	output := fmt.Sprintf("[%s] [%s] %s: %s\n", timestamp, l.name, levelStr, message)

	// Write to stdout/stderr based on level
	if level >= ErrorLevel {
		fmt.Fprint(os.Stderr, output)
	} else {
		fmt.Fprint(os.Stdout, output)
	}
}

// ============================================================================
// SYNC METHOD - GRACEFUL SHUTDOWN
// ============================================================================

// Sync: Flush any pending logs and close resources
// This method is called during graceful shutdown
// Error is ignored (best effort) as per logging patterns
func (l *Logger) Sync() error {
	// For a simple logger, Sync flushes stdout/stderr
	// This is safe to call and returns no error

	if err := os.Stdout.Sync(); err != nil {
		// Log sync failed, but we can't log it (would be recursive)
		// Just return the error
		return err
	}

	if err := os.Stderr.Sync(); err != nil {
		return err
	}

	return nil
}

// ============================================================================
// CONFIGURATION METHODS
// ============================================================================

// SetLevel: Set the log level
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// SetLevelStr: Set log level from string
func (l *Logger) SetLevelStr(levelStr string) {
	switch levelStr {
	case "debug":
		l.level = DebugLevel
	case "info":
		l.level = InfoLevel
	case "warn":
		l.level = WarnLevel
	case "error":
		l.level = ErrorLevel
	default:
		l.level = InfoLevel
	}
}

// GetLevel: Get current log level
func (l *Logger) GetLevel() LogLevel {
	return l.level
}

// SetName: Set logger name
func (l *Logger) SetName(name string) {
	l.name = name
}

// ============================================================================
// USAGE EXAMPLES
// ============================================================================

/*
Basic usage:

  log := logger.Get()

  log.Info("Starting application")
  log.Debug("Debug info: %v", value)
  log.Warn("Warning: %s", msg)
  log.Error("Error: %v", err)

  // Graceful shutdown
  defer log.Sync()

In main.go:

  log := initializeLogger(*logLevel)
  defer func() {
    if log != nil {
      _ = log.Sync()  // Safe to call, error is ignored (best effort)
    }
  }()
*/

// ============================================================================
