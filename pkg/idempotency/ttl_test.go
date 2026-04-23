package idempotency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputeTTL_DefaultForShortJobs(t *testing.T) {
	// Any job with timeout <= 23h gets clamped up to the 24h default so
	// normal retry windows remain covered.
	assert.Equal(t, 24*time.Hour, ComputeTTL(0))
	assert.Equal(t, 24*time.Hour, ComputeTTL(60))      // 1 min job
	assert.Equal(t, 24*time.Hour, ComputeTTL(3600))    // 1h job
	assert.Equal(t, 24*time.Hour, ComputeTTL(23*3600)) // 23h job
	assert.Equal(t, 24*time.Hour, ComputeTTL(-1))      // malformed → default
}

func TestComputeTTL_LongRunningJob(t *testing.T) {
	// 30-hour training job → TTL = 31h (30h + 1h safety margin)
	want := 30*time.Hour + time.Hour
	assert.Equal(t, want, ComputeTTL(int((30 * time.Hour).Seconds())))

	// 48-hour job → 49h.
	assert.Equal(t, 49*time.Hour, ComputeTTL(int((48 * time.Hour).Seconds())))
}

func TestComputeTTL_AtDefaultBoundary(t *testing.T) {
	// A job that takes exactly 23h + 1h = 24h should still hit default,
	// since margin is 1h on top of the timeout. Here 23h + 1h = 24h == default.
	ttl := ComputeTTL(int((23 * time.Hour).Seconds()))
	assert.Equal(t, 24*time.Hour, ttl)
}

func TestWithTTL_OverridesDefault(t *testing.T) {
	cfg := recordConfig{ttl: 10 * time.Minute}
	WithTTL(45 * time.Minute)(&cfg)
	assert.Equal(t, 45*time.Minute, cfg.ttl)
}

func TestWithTTL_ZeroIsIgnored(t *testing.T) {
	// A zero override shouldn't silently reset a caller's default to 0 —
	// that would effectively disable the dedup cache.
	cfg := recordConfig{ttl: 10 * time.Minute}
	WithTTL(0)(&cfg)
	assert.Equal(t, 10*time.Minute, cfg.ttl)
}
