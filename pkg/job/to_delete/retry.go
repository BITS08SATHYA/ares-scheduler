package to_delete

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines its structure or definition
type RetryPolicy struct {
	MaxRetries     int           `json:"max_retries"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
	Multiplier     float64       `json:"multiplier"`
	EnableJitter   bool          `json:"enable_jitter"`
}

// DefaultRetryPolicy returns sensible defaults
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     60 * time.Second,
		Multiplier:     2.0,
		EnableJitter:   true,
	}
}

// CalculateBackoff computes backoff duration for attempt N
func (p *RetryPolicy) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	// Exponential backoff: initialBackoff * (multiplier ^ attempt)
	backoff := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt-1))

	// Cap at max
	if backoff > float64(p.MaxBackoff) {
		backoff = float64(p.MaxBackoff)
	}

	duration := time.Duration(backoff)

	//	Add jitter (adding some randomness to prevent thundering herd) -- say 10 - 25%
	if p.EnableJitter {
		jitterAmount := float64(duration) * 0.25
		jitter := time.Duration((rand.Float64() * 2 * jitterAmount))
		duration += jitter

		//	Ensure non-negative
		if duration < 0 {
			duration = 0
		}
	}

	return duration

}

// ShouldRetry checks if job should be retied
func (p *RetryPolicy) ShouldRetry(currentAttempt int) bool {
	return currentAttempt < p.MaxRetries
}
