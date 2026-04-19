package crdt

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Property-based tests for CRDT algebraic laws.
// A correct CRDT merge must be:
//   - commutative:  merge(A, B) == merge(B, A)
//   - associative:  merge(merge(A, B), C) == merge(A, merge(B, C))
//   - idempotent:   merge(A, A) == A
//
// These tests generate random CRDT states under a fixed seed and verify the
// laws over many trials. Deterministic seed → reproducible failures.

const (
	propertyTrials = 200
	seed           = 0x5AFEC0DE
)

// ----------------------------------------------------------------------------
// GSet — pure union, easiest case
// ----------------------------------------------------------------------------

func randomGSet(rng *rand.Rand, maxSize int) *GSet {
	gs := NewGSet()
	n := rng.Intn(maxSize + 1)
	for i := 0; i < n; i++ {
		gs.Add(fmt.Sprintf("elem-%d", rng.Intn(maxSize*2)))
	}
	return gs
}

func canonGSet(gs *GSet) string {
	m := gs.Members()
	sort.Strings(m)
	return strings.Join(m, ",")
}

func TestGSet_Commutativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomGSet(rng, 10)
		b := randomGSet(rng, 10)

		ab := cloneGSet(a)
		ab.Merge(b)

		ba := cloneGSet(b)
		ba.Merge(a)

		require.Equalf(t, canonGSet(ab), canonGSet(ba), "trial=%d", i)
	}
}

func TestGSet_Associativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomGSet(rng, 10)
		b := randomGSet(rng, 10)
		c := randomGSet(rng, 10)

		left := cloneGSet(a)
		left.Merge(b)
		left.Merge(c)

		right := cloneGSet(a)
		bc := cloneGSet(b)
		bc.Merge(c)
		right.Merge(bc)

		require.Equalf(t, canonGSet(left), canonGSet(right), "trial=%d", i)
	}
}

func TestGSet_Idempotency(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomGSet(rng, 10)
		before := canonGSet(a)
		a.Merge(cloneGSet(a))
		require.Equalf(t, before, canonGSet(a), "trial=%d", i)
	}
}

func cloneGSet(src *GSet) *GSet {
	dst := NewGSet()
	for _, m := range src.Members() {
		dst.Add(m)
	}
	return dst
}

// ----------------------------------------------------------------------------
// ORSet — add-wins semantics
// ----------------------------------------------------------------------------

func randomORSet(rng *rand.Rand, nodeID string, maxSize int) *ORSet {
	s := NewORSet(nodeID)
	n := rng.Intn(maxSize + 1)
	for i := 0; i < n; i++ {
		elem := fmt.Sprintf("elem-%d", rng.Intn(maxSize*2))
		if rng.Intn(4) == 0 {
			s.Remove(elem)
		} else {
			s.Add(elem)
		}
	}
	return s
}

func canonORSet(s *ORSet) string {
	// OR-set membership is the set of elements with ≥1 tag; canonicalise by
	// listing present elements in sorted order.
	s.mu.RLock()
	present := make([]string, 0, len(s.Elements))
	for elem, tags := range s.Elements {
		if len(tags) > 0 {
			present = append(present, elem)
		}
	}
	s.mu.RUnlock()
	sort.Strings(present)
	return strings.Join(present, ",")
}

func cloneORSet(src *ORSet) *ORSet {
	src.mu.RLock()
	defer src.mu.RUnlock()
	dst := NewORSet(src.nodeID)
	for elem, tags := range src.Elements {
		if len(tags) == 0 {
			continue
		}
		dst.Elements[elem] = make(map[string]bool, len(tags))
		for tag := range tags {
			dst.Elements[elem][tag] = true
		}
	}
	return dst
}

func TestORSet_Commutativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomORSet(rng, "node-a", 8)
		b := randomORSet(rng, "node-b", 8)

		ab := cloneORSet(a)
		ab.Merge(b)

		ba := cloneORSet(b)
		ba.Merge(a)

		require.Equalf(t, canonORSet(ab), canonORSet(ba), "trial=%d", i)
	}
}

func TestORSet_Associativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomORSet(rng, "node-a", 8)
		b := randomORSet(rng, "node-b", 8)
		c := randomORSet(rng, "node-c", 8)

		left := cloneORSet(a)
		left.Merge(b)
		left.Merge(c)

		right := cloneORSet(a)
		bc := cloneORSet(b)
		bc.Merge(c)
		right.Merge(bc)

		require.Equalf(t, canonORSet(left), canonORSet(right), "trial=%d", i)
	}
}

func TestORSet_Idempotency(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomORSet(rng, "node-a", 8)
		before := canonORSet(a)
		a.Merge(cloneORSet(a))
		require.Equalf(t, before, canonORSet(a), "trial=%d", i)
	}
}

// ----------------------------------------------------------------------------
// VectorClock — pointwise max
// ----------------------------------------------------------------------------

func randomVectorClock(rng *rand.Rand, nodes int) *VectorClock {
	vc := NewVectorClock()
	for i := 0; i < nodes; i++ {
		vc.Counters[fmt.Sprintf("node-%d", i)] = uint64(rng.Intn(20))
	}
	return vc
}

func canonVectorClock(vc *VectorClock) string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	keys := make([]string, 0, len(vc.Counters))
	for k := range vc.Counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%d;", k, vc.Counters[k])
	}
	return sb.String()
}

func cloneVectorClock(src *VectorClock) *VectorClock {
	src.mu.RLock()
	defer src.mu.RUnlock()
	dst := NewVectorClock()
	for k, v := range src.Counters {
		dst.Counters[k] = v
	}
	return dst
}

func TestVectorClock_Commutativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomVectorClock(rng, 4)
		b := randomVectorClock(rng, 4)

		ab := cloneVectorClock(a)
		ab.Merge(b)

		ba := cloneVectorClock(b)
		ba.Merge(a)

		require.Equalf(t, canonVectorClock(ab), canonVectorClock(ba), "trial=%d", i)
	}
}

func TestVectorClock_Associativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomVectorClock(rng, 4)
		b := randomVectorClock(rng, 4)
		c := randomVectorClock(rng, 4)

		left := cloneVectorClock(a)
		left.Merge(b)
		left.Merge(c)

		right := cloneVectorClock(a)
		bc := cloneVectorClock(b)
		bc.Merge(c)
		right.Merge(bc)

		require.Equalf(t, canonVectorClock(left), canonVectorClock(right), "trial=%d", i)
	}
}

func TestVectorClock_Idempotency(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomVectorClock(rng, 4)
		before := canonVectorClock(a)
		a.Merge(cloneVectorClock(a))
		require.Equalf(t, before, canonVectorClock(a), "trial=%d", i)
	}
}

// ----------------------------------------------------------------------------
// LWWRegister — last writer wins (by timestamp, nodeID tiebreak)
// ----------------------------------------------------------------------------
//
// Commutativity holds because both orderings converge to the register with the
// later timestamp (or higher nodeID when tied). Associativity likewise.
// Idempotency: merging a register into a clone produces no change.

func randomLWWRegister(rng *rand.Rand) *LWWRegister {
	// Pick a nodeID from a small pool so tiebreaks actually happen
	nodeID := fmt.Sprintf("node-%d", rng.Intn(3))
	value := fmt.Sprintf("val-%d", rng.Intn(100))
	r := NewLWWRegister(nodeID, value)
	// Randomise the timestamp across a small window so concurrent writes
	// collide on timestamp equality (exercises the nodeID tiebreak).
	r.Timestamp = time.Unix(0, int64(rng.Intn(1_000_000_000)))
	return r
}

func canonLWW(r *LWWRegister) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return fmt.Sprintf("ts=%d;node=%s;val=%v", r.Timestamp.UnixNano(), r.NodeID, r.Value)
}

func cloneLWW(src *LWWRegister) *LWWRegister {
	src.mu.RLock()
	defer src.mu.RUnlock()
	return &LWWRegister{
		Value:     src.Value,
		Timestamp: src.Timestamp,
		NodeID:    src.NodeID,
		Clock:     cloneVectorClock(src.Clock),
	}
}

func TestLWWRegister_Commutativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomLWWRegister(rng)
		b := randomLWWRegister(rng)

		ab := cloneLWW(a)
		ab.Merge(b)

		ba := cloneLWW(b)
		ba.Merge(a)

		assert.Equalf(t, canonLWW(ab), canonLWW(ba), "trial=%d", i)
	}
}

func TestLWWRegister_Associativity(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomLWWRegister(rng)
		b := randomLWWRegister(rng)
		c := randomLWWRegister(rng)

		left := cloneLWW(a)
		left.Merge(b)
		left.Merge(c)

		right := cloneLWW(a)
		bc := cloneLWW(b)
		bc.Merge(c)
		right.Merge(bc)

		assert.Equalf(t, canonLWW(left), canonLWW(right), "trial=%d", i)
	}
}

func TestLWWRegister_Idempotency(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < propertyTrials; i++ {
		a := randomLWWRegister(rng)
		before := canonLWW(a)
		a.Merge(cloneLWW(a))
		assert.Equalf(t, before, canonLWW(a), "trial=%d", i)
	}
}

// ----------------------------------------------------------------------------
// Job status transition state machine
// ----------------------------------------------------------------------------

func TestJobStatusTransition_ValidForward(t *testing.T) {
	valid := []struct {
		from, to string
	}{
		{"PENDING", "SCHEDULED"},
		{"PENDING", "QUEUED"},
		{"QUEUED", "SCHEDULED"},
		{"SCHEDULED", "RUNNING"},
		{"RUNNING", "SUCCEEDED"},
		{"RUNNING", "FAILED"},
		{"RUNNING", "CANCELED"},
		{"SCHEDULED", "PREEMPTED"},
		{"PREEMPTED", "PENDING"},
		{"RUNNING", "PREEMPTED"},
		{"SCHEDULED", "RETRYING"},
		{"RETRYING", "SCHEDULED"},
	}
	for _, tc := range valid {
		js := NewCRDTJobState("node-a", "job-1")
		js.Status.Set("node-a", tc.from)
		err := js.UpdateStatus("node-a", tc.to)
		assert.NoErrorf(t, err, "%s -> %s should be allowed", tc.from, tc.to)
	}
}

func TestJobStatusTransition_Invalid(t *testing.T) {
	invalid := []struct {
		from, to string
	}{
		{"SUCCEEDED", "PENDING"},
		{"SUCCEEDED", "RUNNING"},
		{"FAILED", "SCHEDULED"},
		{"CANCELED", "RUNNING"},
		{"PENDING", "RUNNING"},   // must go through SCHEDULED
		{"PENDING", "SUCCEEDED"}, // can't skip all intermediate states
		{"QUEUED", "SUCCEEDED"},
	}
	for _, tc := range invalid {
		js := NewCRDTJobState("node-a", "job-1")
		js.Status.Set("node-a", tc.from)
		err := js.UpdateStatus("node-a", tc.to)
		assert.ErrorIsf(t, err, ErrInvalidTransition, "%s -> %s must be rejected", tc.from, tc.to)
	}
}

func TestJobStatusTransition_SameStateNoop(t *testing.T) {
	js := NewCRDTJobState("node-a", "job-1")
	js.Status.Set("node-a", "RUNNING")
	err := js.UpdateStatus("node-a", "RUNNING")
	assert.NoError(t, err)
}
