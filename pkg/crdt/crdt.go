// File: pkg/crdt/crdt.go
// Feature: Eventual Consistency with CRDTs
//
// CRDTs (Conflict-free Replicated Data Types) enable multiple control planes
// to update state independently and merge without conflicts.
//
// This module implements four CRDT primitives:
//   1. VectorClock  — Causal ordering of events across nodes
//   2. LWWRegister  — Last-Writer-Wins register for mutable fields
//   3. GSet         — Grow-only set (add only, never remove)
//   4. ORSet        — Observed-Remove set (add and remove safely)
//
// These primitives are composed into higher-level CRDTs:
//   - ClusterState  — CRDT-wrapped cluster info (health, load, membership)
//   - JobState      — CRDT-wrapped job record (status, placement, attempts)
//
// Consistency model:
//   - Strong convergence: All replicas that receive the same set of updates
//     will reach the same state, regardless of order.
//   - Causal consistency: Updates respect happened-before relationships
//     via vector clocks.
//
// References:
//   - Shapiro et al., "A comprehensive study of CRDTs" (INRIA, 2011)
//   - Riak DT (Basho), Automerge, Yjs

package crdt

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// VECTOR CLOCK
// ============================================================================

// VectorClock: Tracks causal ordering across distributed nodes.
// Each node maintains a counter; on every local event, increment own counter.
// To compare: VC1 < VC2 iff all entries in VC1 ≤ VC2 and at least one is strictly less.
// Concurrent: neither VC1 < VC2 nor VC2 < VC1.
type VectorClock struct {
	mu       sync.RWMutex
	Counters map[string]uint64 `json:"counters"` // nodeID -> logical timestamp
}

// Comparison result
type ClockRelation int

const (
	Before     ClockRelation = -1 // VC1 happened before VC2
	After      ClockRelation = 1  // VC1 happened after VC2
	Concurrent ClockRelation = 0  // VC1 and VC2 are concurrent (conflict!)
	Equal      ClockRelation = 2  // VC1 and VC2 are identical
)

func NewVectorClock() *VectorClock {
	return &VectorClock{
		Counters: make(map[string]uint64),
	}
}

// Increment: Record a local event on this node
func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.Counters[nodeID]++
}

// Merge: Combine two vector clocks (take max of each entry)
// Used when receiving a message from another node
func (vc *VectorClock) Merge(other *VectorClock) {
	if other == nil {
		return
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, otherCount := range other.Counters {
		if otherCount > vc.Counters[nodeID] {
			vc.Counters[nodeID] = otherCount
		}
	}
}

// Compare: Determine causal relationship between two vector clocks
//
//	Before:     all vc[i] <= other[i], at least one strictly less
//	After:      all vc[i] >= other[i], at least one strictly greater
//	Concurrent: some vc[i] < other[i] AND some vc[j] > other[j]
//	Equal:      all entries identical
func (vc *VectorClock) Compare(other *VectorClock) ClockRelation {
	if other == nil {
		return After
	}

	vc.mu.RLock()
	defer vc.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	hasLess := false
	hasGreater := false

	// Check all keys in both clocks
	allKeys := make(map[string]bool)
	for k := range vc.Counters {
		allKeys[k] = true
	}
	for k := range other.Counters {
		allKeys[k] = true
	}

	for k := range allKeys {
		mine := vc.Counters[k]
		theirs := other.Counters[k]

		if mine < theirs {
			hasLess = true
		}
		if mine > theirs {
			hasGreater = true
		}
	}

	switch {
	case hasLess && hasGreater:
		return Concurrent
	case hasLess:
		return Before
	case hasGreater:
		return After
	default:
		return Equal
	}
}

// Copy: Deep copy of vector clock
func (vc *VectorClock) Copy() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	copy := NewVectorClock()
	for k, v := range vc.Counters {
		copy.Counters[k] = v
	}
	return copy
}

// String: Human-readable representation
func (vc *VectorClock) String() string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	data, _ := json.Marshal(vc.Counters)
	return string(data)
}

// ============================================================================
// LWW REGISTER (Last-Writer-Wins)
// ============================================================================

// LWWRegister: A register where concurrent writes are resolved by timestamp.
// The write with the highest timestamp wins.
// Used for: cluster health status, job status, load metrics
//
// Bias: On exact timestamp tie, we use nodeID as tiebreaker (lexicographic).
// This ensures deterministic convergence even with synchronized clocks.
type LWWRegister struct {
	mu        sync.RWMutex
	Value     interface{}  `json:"value"`
	Timestamp time.Time    `json:"timestamp"`
	NodeID    string       `json:"node_id"` // Who wrote this value
	Clock     *VectorClock `json:"clock"`   // Causal context
}

func NewLWWRegister(nodeID string, value interface{}) *LWWRegister {
	clock := NewVectorClock()
	clock.Increment(nodeID)

	return &LWWRegister{
		Value:     value,
		Timestamp: time.Now(),
		NodeID:    nodeID,
		Clock:     clock,
	}
}

// Set: Update the register value (local write)
func (r *LWWRegister) Set(nodeID string, value interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Value = value
	r.Timestamp = time.Now()
	r.NodeID = nodeID
	r.Clock.Increment(nodeID)
}

// Merge: Merge with a remote register. Last writer wins.
// Returns true if the remote value was adopted.
func (r *LWWRegister) Merge(remote *LWWRegister) bool {
	if remote == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	remote.mu.RLock()
	defer remote.mu.RUnlock()

	// Compare timestamps
	if remote.Timestamp.After(r.Timestamp) {
		r.Value = remote.Value
		r.Timestamp = remote.Timestamp
		r.NodeID = remote.NodeID
		r.Clock.Merge(remote.Clock)
		return true
	}

	// Exact tie: use nodeID as deterministic tiebreaker
	if remote.Timestamp.Equal(r.Timestamp) && remote.NodeID > r.NodeID {
		r.Value = remote.Value
		r.Timestamp = remote.Timestamp
		r.NodeID = remote.NodeID
		r.Clock.Merge(remote.Clock)
		return true
	}

	// Local wins — still merge the clock for causal tracking
	r.Clock.Merge(remote.Clock)
	return false
}

// Get: Read current value
func (r *LWWRegister) Get() interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Value
}

// GetString: Read current value as string
func (r *LWWRegister) GetString() string {
	val := r.Get()
	if s, ok := val.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", val)
}

// GetInt: Read current value as int
func (r *LWWRegister) GetInt() int {
	val := r.Get()
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	default:
		return 0
	}
}

// GetBool: Read current value as bool
func (r *LWWRegister) GetBool() bool {
	val := r.Get()
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}

// ============================================================================
// G-SET (Grow-Only Set)
// ============================================================================

// GSet: A set that only supports additions, never removals.
// Merge = union. Always converges because union is commutative and idempotent.
// Used for: cluster membership history, completed job IDs, event logs
type GSet struct {
	mu       sync.RWMutex
	Elements map[string]bool `json:"elements"`
}

func NewGSet() *GSet {
	return &GSet{
		Elements: make(map[string]bool),
	}
}

// Add: Add an element to the set
func (gs *GSet) Add(element string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.Elements[element] = true
}

// Contains: Check if element exists
func (gs *GSet) Contains(element string) bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.Elements[element]
}

// Merge: Union of two G-Sets (always safe, always converges)
func (gs *GSet) Merge(other *GSet) {
	if other == nil {
		return
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for element := range other.Elements {
		gs.Elements[element] = true
	}
}

// Size: Number of elements
func (gs *GSet) Size() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return len(gs.Elements)
}

// Members: Get all elements
func (gs *GSet) Members() []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	result := make([]string, 0, len(gs.Elements))
	for element := range gs.Elements {
		result = append(result, element)
	}
	return result
}

// ============================================================================
// OR-SET (Observed-Remove Set)
// ============================================================================

// ORSet: A set that supports both add and remove without conflicts.
// Each add gets a unique tag. Remove only removes observed tags.
// Concurrent add + remove of same element: add wins (add-wins semantics).
//
// Used for: active cluster membership (clusters can join AND leave),
// active job tracking, tenant lists
//
// Implementation: Each element is stored with a set of unique tags.
// Add creates a new tag. Remove deletes all currently observed tags.
// An element is in the set iff it has at least one tag.
type ORSet struct {
	mu sync.RWMutex
	// element -> set of unique tags
	Elements map[string]map[string]bool `json:"elements"`
	// Counter for generating unique tags
	tagCounter uint64
	nodeID     string
}

func NewORSet(nodeID string) *ORSet {
	return &ORSet{
		Elements: make(map[string]map[string]bool),
		nodeID:   nodeID,
	}
}

// Add: Add element with a new unique tag
func (os *ORSet) Add(element string) {
	os.mu.Lock()
	defer os.mu.Unlock()

	os.tagCounter++
	tag := fmt.Sprintf("%s:%d:%d", os.nodeID, time.Now().UnixNano(), os.tagCounter)

	if os.Elements[element] == nil {
		os.Elements[element] = make(map[string]bool)
	}
	os.Elements[element][tag] = true
}

// Remove: Remove element by clearing all its observed tags
func (os *ORSet) Remove(element string) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// Remove all tags for this element
	delete(os.Elements, element)
}

// Contains: Check if element is in the set (has at least one tag)
func (os *ORSet) Contains(element string) bool {
	os.mu.RLock()
	defer os.mu.RUnlock()

	tags := os.Elements[element]
	return len(tags) > 0
}

// Merge: Merge two OR-Sets
// For each element: union of tags from both sets, minus tags removed by either
// Simplified: union of all tag sets (add-wins semantics)
func (os *ORSet) Merge(other *ORSet) {
	if other == nil {
		return
	}

	os.mu.Lock()
	defer os.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for element, otherTags := range other.Elements {
		if os.Elements[element] == nil {
			os.Elements[element] = make(map[string]bool)
		}
		for tag := range otherTags {
			os.Elements[element][tag] = true
		}
	}
}

// Size: Number of elements currently in the set
func (os *ORSet) Size() int {
	os.mu.RLock()
	defer os.mu.RUnlock()

	count := 0
	for _, tags := range os.Elements {
		if len(tags) > 0 {
			count++
		}
	}
	return count
}

// Members: Get all elements currently in the set
func (os *ORSet) Members() []string {
	os.mu.RLock()
	defer os.mu.RUnlock()

	result := make([]string, 0)
	for element, tags := range os.Elements {
		if len(tags) > 0 {
			result = append(result, element)
		}
	}
	return result
}
