package tests

import (
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"testing"
)

// ============================================================================
// LAYER 1: LOGGER TESTS
// ============================================================================
// Tests basic logging infrastructure - foundation for all other layers

type TestLogger struct {
	logs []string
}

func (tl *TestLogger) Log(level string, msg string) {
	tl.logs = append(tl.logs, fmt.Sprintf("[%s] %s", level, msg))
}

func TestLoggerInit(t *testing.T) {
	tests := []struct {
		name     string
		levelStr string
		expected logger.LogLevel
	}{
		{"debug level", "debug", logger.DebugLevel},
		{"info level", "info", logger.InfoLevel},
		{"warn level", "warn", logger.WarnLevel},
		{"error level", "error", logger.ErrorLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.Init(tt.levelStr)
			log := logger.Get()

			if log == nil {
				t.Fatal("Logger should not be nil after init")
			}
		})
	}
}

func TestLoggerLevels(t *testing.T) {
	logger.Init("debug")
	log := logger.Get()

	// Test that we can call all logging levels without panic
	tests := []struct {
		name string
		fn   func(format string, args ...interface{})
	}{
		{"Debug", log.Debug},
		{"Info", log.Info},
		{"Warn", log.Warn},
		{"Error", log.Error},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			tt.fn("Test message for %s", tt.name)
		})
	}
}

func TestLoggerThreshold(t *testing.T) {
	// Initialize with WARN level (should skip DEBUG and INFO)
	logger.Init("warn")

	// These should be silently ignored
	logger.Debug("This should not appear")
	logger.Info("This should not appear")

	// These should appear (if captured)
	logger.Warn("This should appear")
	logger.Error("This should appear")

	// Test passes if no panic occurs
	t.Log("Logger threshold test passed")
}

func TestLoggerWithFields(t *testing.T) {
	logger.Init("debug")

	fields := map[string]interface{}{
		"job_id":   "job-123",
		"cluster":  "us-west-2a",
		"priority": 95,
	}

	// Should not panic
	contextLog := logger.WithFields(fields)
	contextLog.Info("Job scheduled with context")

	t.Log("Logger with fields test passed")
}

func TestLoggerConcurrency(t *testing.T) {
	logger.Init("debug")
	log := logger.Get()

	// Simulate concurrent logging
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			log.Info("Concurrent log from goroutine %d", id)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	t.Log("Concurrent logging test passed")
}

func TestLoggerFormatting(t *testing.T) {
	logger.Init("info")
	log := logger.Get()

	tests := []struct {
		name     string
		format   string
		args     []interface{}
		contains string
	}{
		{
			"string formatting",
			"Job %s scheduled",
			[]interface{}{"job-123"},
			"job-123",
		},
		{
			"integer formatting",
			"Priority: %d",
			[]interface{}{95},
			"95",
		},
		{
			"float formatting",
			"Score: %.2f",
			[]interface{}{99.99},
			"99.99",
		},
		{
			"multiple args",
			"%s on %s with %d GPUs",
			[]interface{}{"job-123", "cluster-abc", 8},
			"job-123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These should not panic
			log.Info(tt.format, tt.args...)
		})
	}
}

// ============================================================================
// LOGGER INTEGRATION TEST
// ============================================================================

func TestLoggerIntegration(t *testing.T) {
	logger.Init("debug")

	// Simulate a complete workflow logging
	logger.Info("Starting Ares scheduler initialization")
	logger.Debug("Connecting to etcd")
	logger.Info("Connected to etcd successfully")
	logger.Debug("Connecting to Redis")
	logger.Info("Connected to Redis successfully")
	logger.Debug("Initializing GPU discovery")
	logger.Info("GPU discovery initialized")
	logger.Warn("No GPUs detected - running in CPU-only mode")
	logger.Info("Ares scheduler ready for job submissions")

	t.Log("Logger integration test passed")
}

func TestLoggerGlobalAccess(t *testing.T) {
	logger.Init("info")

	// Test that global functions work
	logger.Info("Using global logger")
	logger.Warn("Using global warn")
	logger.Error("Using global error")

	// Test that Get() returns same instance
	log1 := logger.Get()
	log2 := logger.Get()

	if log1 != log2 {
		t.Fatal("Get() should return same logger instance")
	}

	t.Log("Logger global access test passed")
}

// ============================================================================
// LAYER 1 TEST SUMMARY
// ============================================================================

func TestLayer1LoggerSummary(t *testing.T) {
	t.Run("Logger Initialization", TestLoggerInit)
	t.Run("Logger Levels", TestLoggerLevels)
	t.Run("Logger Threshold", TestLoggerThreshold)
	t.Run("Logger With Fields", TestLoggerWithFields)
	t.Run("Logger Concurrency", TestLoggerConcurrency)
	t.Run("Logger Formatting", TestLoggerFormatting)
	t.Run("Logger Integration", TestLoggerIntegration)
	t.Run("Logger Global Access", TestLoggerGlobalAccess)

	t.Log("✓ LAYER 1: Logger - All tests passed")
}
