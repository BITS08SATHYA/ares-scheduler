// File: pkg/logger/logger.go
// Logger: Structured logging wrapper backed by log/slog
// Supports JSON and text output formats via ARES_LOG_FORMAT env var
// Supports log level filtering via ARES_LOG_LEVEL env var

package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// ============================================================================
// LOGGER INTERFACE & TYPES
// ============================================================================

// Logger wraps slog.Logger while preserving the existing Printf-style API
// used by all packages. The slog backend provides structured JSON output
// and proper level filtering.
type Logger struct {
	level  LogLevel
	name   string
	slog   *slog.Logger
	format LogFormat
}

// LogLevel: Log severity levels
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// LogFormat: Output format
type LogFormat int

const (
	TextFormat LogFormat = iota
	JSONFormat
)

// toSlogLevel converts our LogLevel to slog.Level
func toSlogLevel(l LogLevel) slog.Level {
	switch l {
	case DebugLevel:
		return slog.LevelDebug
	case InfoLevel:
		return slog.LevelInfo
	case WarnLevel:
		return slog.LevelWarn
	case ErrorLevel:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ============================================================================
// GLOBAL LOGGER INSTANCE
// ============================================================================

var globalLogger *Logger

func init() {
	level := InfoLevel
	if envLevel := os.Getenv("ARES_LOG_LEVEL"); envLevel != "" {
		switch strings.ToLower(envLevel) {
		case "debug":
			level = DebugLevel
		case "info":
			level = InfoLevel
		case "warn":
			level = WarnLevel
		case "error":
			level = ErrorLevel
		}
	}

	format := TextFormat
	if envFormat := os.Getenv("ARES_LOG_FORMAT"); strings.ToLower(envFormat) == "json" {
		format = JSONFormat
	}

	globalLogger = newLogger("ares", level, format)
}

// newLogger creates a Logger with the given name, level, and format
func newLogger(name string, level LogLevel, format LogFormat) *Logger {
	opts := &slog.HandlerOptions{
		Level: toSlogLevel(level),
	}

	var handler slog.Handler
	if format == JSONFormat {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	// Add component name as a default attribute
	slogger := slog.New(handler).With("component", name)

	return &Logger{
		level:  level,
		name:   name,
		slog:   slogger,
		format: format,
	}
}

// Get: Get the global logger instance
func Get() *Logger {
	return globalLogger
}

// ============================================================================
// LOGGING METHODS (Printf-style API preserved for backward compatibility)
// ============================================================================

// Debug: Log debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.slog.Log(context.Background(), slog.LevelDebug, fmt.Sprintf(format, args...))
	}
}

// Info: Log info message
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.slog.Log(context.Background(), slog.LevelInfo, fmt.Sprintf(format, args...))
	}
}

// Warn: Log warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WarnLevel {
		l.slog.Log(context.Background(), slog.LevelWarn, fmt.Sprintf(format, args...))
	}
}

// Error: Log error message
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.slog.Log(context.Background(), slog.LevelError, fmt.Sprintf(format, args...))
	}
}

// ============================================================================
// STRUCTURED LOGGING METHODS (new slog-style API)
// ============================================================================

// With returns a new Logger with additional structured attributes.
// Example: log.With("job_id", jobID, "cluster", clusterID).Info("scheduled")
func (l *Logger) With(args ...any) *Logger {
	return &Logger{
		level:  l.level,
		name:   l.name,
		slog:   l.slog.With(args...),
		format: l.format,
	}
}

// InfoCtx logs at Info level with context (enables trace propagation)
func (l *Logger) InfoCtx(ctx context.Context, format string, args ...interface{}) {
	l.slog.Log(ctx, slog.LevelInfo, fmt.Sprintf(format, args...))
}

// ErrorCtx logs at Error level with context
func (l *Logger) ErrorCtx(ctx context.Context, format string, args ...interface{}) {
	l.slog.Log(ctx, slog.LevelError, fmt.Sprintf(format, args...))
}

// ============================================================================
// SYNC METHOD - GRACEFUL SHUTDOWN
// ============================================================================

// Sync: Flush any pending logs and close resources
func (l *Logger) Sync() error {
	if err := os.Stdout.Sync(); err != nil {
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
	switch strings.ToLower(levelStr) {
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
	l.slog = l.slog.With("component", name)
}

// Slog returns the underlying slog.Logger for direct slog usage
func (l *Logger) Slog() *slog.Logger {
	return l.slog
}
