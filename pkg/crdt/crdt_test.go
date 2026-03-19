package crdt

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// VECTOR CLOCK
// ============================================================================

func TestVectorClock_NewIsEmpty(t *testing.T) {
	vc := NewVectorClock()
	assert.Empty(t, vc.Counters)
}

func TestVectorClock_Increment(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("node-a")
	vc.Increment("node-a")
	vc.Increment("node-b")

	assert.Equal(t, uint64(2), vc.Counters["node-a"])
	assert.Equal(t, uint64(1), vc.Counters["node-b"])
}

func TestVectorClock_Merge(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 3
	vc1.Counters["b"] = 1

	vc2 := NewVectorClock()
	vc2.Counters["a"] = 1
	vc2.Counters["b"] = 5
	vc2.Counters["c"] = 2

	vc1.Merge(vc2)

	assert.Equal(t, uint64(3), vc1.Counters["a"]) // max(3,1)
	assert.Equal(t, uint64(5), vc1.Counters["b"]) // max(1,5)
	assert.Equal(t, uint64(2), vc1.Counters["c"]) // max(0,2)
}

func TestVectorClock_MergeNil(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("a")
	vc.Merge(nil) // should not panic
	assert.Equal(t, uint64(1), vc.Counters["a"])
}

func TestVectorClock_Compare_Equal(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 3
	vc1.Counters["b"] = 2

	vc2 := NewVectorClock()
	vc2.Counters["a"] = 3
	vc2.Counters["b"] = 2

	assert.Equal(t, Equal, vc1.Compare(vc2))
}

func TestVectorClock_Compare_Before(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 1
	vc1.Counters["b"] = 2

	vc2 := NewVectorClock()
	vc2.Counters["a"] = 3
	vc2.Counters["b"] = 2

	assert.Equal(t, Before, vc1.Compare(vc2))
}

func TestVectorClock_Compare_After(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 5
	vc1.Counters["b"] = 3

	vc2 := NewVectorClock()
	vc2.Counters["a"] = 3
	vc2.Counters["b"] = 2

	assert.Equal(t, After, vc1.Compare(vc2))
}

func TestVectorClock_Compare_Concurrent(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 3
	vc1.Counters["b"] = 1

	vc2 := NewVectorClock()
	vc2.Counters["a"] = 1
	vc2.Counters["b"] = 3

	assert.Equal(t, Concurrent, vc1.Compare(vc2))
}

func TestVectorClock_Compare_NilOther(t *testing.T) {
	vc := NewVectorClock()
	vc.Increment("a")
	assert.Equal(t, After, vc.Compare(nil))
}

func TestVectorClock_Compare_MissingKeys(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Counters["a"] = 1

	vc2 := NewVectorClock()
	vc2.Counters["b"] = 1

	// vc1 has a=1,b=0; vc2 has a=0,b=1 → concurrent
	assert.Equal(t, Concurrent, vc1.Compare(vc2))
}

func TestVectorClock_Compare_EmptyClocks(t *testing.T) {
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()
	assert.Equal(t, Equal, vc1.Compare(vc2))
}

func TestVectorClock_Copy(t *testing.T) {
	vc := NewVectorClock()
	vc.Counters["a"] = 5
	vc.Counters["b"] = 3

	cp := vc.Copy()
	assert.Equal(t, uint64(5), cp.Counters["a"])

	// Mutating copy shouldn't affect original
	cp.Counters["a"] = 99
	assert.Equal(t, uint64(5), vc.Counters["a"])
}

func TestVectorClock_String(t *testing.T) {
	vc := NewVectorClock()
	vc.Counters["a"] = 1
	s := vc.String()
	assert.Contains(t, s, `"a":1`)
}

func TestVectorClock_ConcurrentMerge_NoDeadlock(t *testing.T) {
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()
	vc1.Counters["a"] = 1
	vc2.Counters["b"] = 1

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			vc1.Merge(vc2)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			vc2.Merge(vc1)
		}
	}()
	wg.Wait()
	// If we get here without deadlock, the test passes
}

// ============================================================================
// LWW REGISTER
// ============================================================================

func TestLWWRegister_New(t *testing.T) {
	r := NewLWWRegister("node-a", "hello")
	assert.Equal(t, "hello", r.Get())
	assert.Equal(t, "node-a", r.NodeID)
}

func TestLWWRegister_Set(t *testing.T) {
	r := NewLWWRegister("node-a", "v1")
	time.Sleep(time.Millisecond) // ensure different timestamp
	r.Set("node-a", "v2")
	assert.Equal(t, "v2", r.Get())
}

func TestLWWRegister_Merge_RemoteWins(t *testing.T) {
	local := NewLWWRegister("node-a", "old")
	time.Sleep(2 * time.Millisecond)
	remote := NewLWWRegister("node-b", "new")

	adopted := local.Merge(remote)
	assert.True(t, adopted)
	assert.Equal(t, "new", local.Get())
}

func TestLWWRegister_Merge_LocalWins(t *testing.T) {
	remote := NewLWWRegister("node-b", "old")
	time.Sleep(2 * time.Millisecond)
	local := NewLWWRegister("node-a", "new")

	adopted := local.Merge(remote)
	assert.False(t, adopted)
	assert.Equal(t, "new", local.Get())
}

func TestLWWRegister_Merge_Tiebreaker(t *testing.T) {
	// Same timestamp scenario: higher nodeID wins
	r1 := NewLWWRegister("node-a", "val-a")
	r2 := &LWWRegister{
		Value:     "val-b",
		Timestamp: r1.Timestamp, // same timestamp
		NodeID:    "node-z",     // higher lexicographically
		Clock:     NewVectorClock(),
	}

	adopted := r1.Merge(r2)
	assert.True(t, adopted, "higher nodeID should win on tie")
	assert.Equal(t, "val-b", r1.Get())
}

func TestLWWRegister_Merge_TiebreakerLocalWins(t *testing.T) {
	r1 := NewLWWRegister("node-z", "val-z") // higher nodeID
	r2 := &LWWRegister{
		Value:     "val-a",
		Timestamp: r1.Timestamp,
		NodeID:    "node-a", // lower nodeID
		Clock:     NewVectorClock(),
	}

	adopted := r1.Merge(r2)
	assert.False(t, adopted, "lower nodeID should lose on tie")
	assert.Equal(t, "val-z", r1.Get())
}

func TestLWWRegister_MergeNil(t *testing.T) {
	r := NewLWWRegister("a", "val")
	adopted := r.Merge(nil)
	assert.False(t, adopted)
	assert.Equal(t, "val", r.Get())
}

func TestLWWRegister_GetString(t *testing.T) {
	r := NewLWWRegister("a", "hello")
	assert.Equal(t, "hello", r.GetString())
}

func TestLWWRegister_GetString_NonString(t *testing.T) {
	r := NewLWWRegister("a", 42)
	assert.Equal(t, "42", r.GetString())
}

func TestLWWRegister_GetInt(t *testing.T) {
	r := NewLWWRegister("a", 42)
	assert.Equal(t, 42, r.GetInt())
}

func TestLWWRegister_GetInt_Float64(t *testing.T) {
	r := NewLWWRegister("a", 42.0)
	assert.Equal(t, 42, r.GetInt())
}

func TestLWWRegister_GetInt_Int64(t *testing.T) {
	r := NewLWWRegister("a", int64(42))
	assert.Equal(t, 42, r.GetInt())
}

func TestLWWRegister_GetInt_Invalid(t *testing.T) {
	r := NewLWWRegister("a", "not-a-number")
	assert.Equal(t, 0, r.GetInt())
}

func TestLWWRegister_GetBool(t *testing.T) {
	r := NewLWWRegister("a", true)
	assert.True(t, r.GetBool())
}

func TestLWWRegister_GetBool_NonBool(t *testing.T) {
	r := NewLWWRegister("a", "yes")
	assert.False(t, r.GetBool())
}

// ============================================================================
// G-SET
// ============================================================================

func TestGSet_New(t *testing.T) {
	gs := NewGSet()
	assert.Equal(t, 0, gs.Size())
}

func TestGSet_Add_Contains(t *testing.T) {
	gs := NewGSet()
	gs.Add("cluster-a")
	gs.Add("cluster-b")

	assert.True(t, gs.Contains("cluster-a"))
	assert.True(t, gs.Contains("cluster-b"))
	assert.False(t, gs.Contains("cluster-c"))
	assert.Equal(t, 2, gs.Size())
}

func TestGSet_Add_Idempotent(t *testing.T) {
	gs := NewGSet()
	gs.Add("a")
	gs.Add("a")
	gs.Add("a")
	assert.Equal(t, 1, gs.Size())
}

func TestGSet_Members(t *testing.T) {
	gs := NewGSet()
	gs.Add("x")
	gs.Add("y")

	members := gs.Members()
	assert.Len(t, members, 2)
	assert.Contains(t, members, "x")
	assert.Contains(t, members, "y")
}

func TestGSet_Merge(t *testing.T) {
	gs1 := NewGSet()
	gs1.Add("a")
	gs1.Add("b")

	gs2 := NewGSet()
	gs2.Add("b")
	gs2.Add("c")

	gs1.Merge(gs2)
	assert.Equal(t, 3, gs1.Size())
	assert.True(t, gs1.Contains("a"))
	assert.True(t, gs1.Contains("b"))
	assert.True(t, gs1.Contains("c"))
}

func TestGSet_Merge_Nil(t *testing.T) {
	gs := NewGSet()
	gs.Add("a")
	gs.Merge(nil)
	assert.Equal(t, 1, gs.Size())
}

func TestGSet_Merge_IsCommutative(t *testing.T) {
	gs1 := NewGSet()
	gs1.Add("a")
	gs1.Add("b")

	gs2 := NewGSet()
	gs2.Add("c")
	gs2.Add("d")

	// Merge both ways into copies
	r1 := NewGSet()
	r1.Merge(gs1)
	r1.Merge(gs2)

	r2 := NewGSet()
	r2.Merge(gs2)
	r2.Merge(gs1)

	assert.Equal(t, r1.Size(), r2.Size())
	for _, m := range r1.Members() {
		assert.True(t, r2.Contains(m))
	}
}

func TestGSet_ConcurrentAdds(t *testing.T) {
	gs := NewGSet()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			gs.Add(string(rune('A' + n%26)))
		}(i)
	}
	wg.Wait()
	assert.LessOrEqual(t, gs.Size(), 26)
}

// ============================================================================
// OR-SET
// ============================================================================

func TestORSet_New(t *testing.T) {
	os := NewORSet("node-a")
	assert.Equal(t, 0, os.Size())
}

func TestORSet_Add_Contains(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("cluster-1")
	os.Add("cluster-2")

	assert.True(t, os.Contains("cluster-1"))
	assert.True(t, os.Contains("cluster-2"))
	assert.False(t, os.Contains("cluster-3"))
	assert.Equal(t, 2, os.Size())
}

func TestORSet_Remove(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("x")
	os.Add("y")
	os.Remove("x")

	assert.False(t, os.Contains("x"))
	assert.True(t, os.Contains("y"))
	assert.Equal(t, 1, os.Size())
}

func TestORSet_Remove_NonExistent(t *testing.T) {
	os := NewORSet("node-a")
	os.Remove("doesnt-exist") // should not panic
	assert.Equal(t, 0, os.Size())
}

func TestORSet_Add_After_Remove(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("x")
	os.Remove("x")
	os.Add("x") // re-add after remove

	assert.True(t, os.Contains("x"))
	assert.Equal(t, 1, os.Size())
}

func TestORSet_Members(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("a")
	os.Add("b")
	os.Add("c")
	os.Remove("b")

	members := os.Members()
	assert.Len(t, members, 2)
	assert.Contains(t, members, "a")
	assert.Contains(t, members, "c")
}

func TestORSet_Merge(t *testing.T) {
	os1 := NewORSet("node-a")
	os1.Add("x")
	os1.Add("y")

	os2 := NewORSet("node-b")
	os2.Add("y")
	os2.Add("z")

	os1.Merge(os2)

	assert.True(t, os1.Contains("x"))
	assert.True(t, os1.Contains("y"))
	assert.True(t, os1.Contains("z"))
}

func TestORSet_Merge_AddWins(t *testing.T) {
	// Concurrent add on one node and remove on another → add wins
	os1 := NewORSet("node-a")
	os1.Add("x")

	os2 := NewORSet("node-b")
	os2.Add("x")
	os2.Remove("x") // os2 removes x
	// But os1 still has x with its own tag

	os2.Merge(os1) // Merge os1's add into os2 → add-wins
	assert.True(t, os2.Contains("x"), "add-wins semantics: concurrent add should survive remove")
}

func TestORSet_Merge_Nil(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("x")
	os.Merge(nil)
	assert.True(t, os.Contains("x"))
}

func TestORSet_MultipleAdds_CreateMultipleTags(t *testing.T) {
	os := NewORSet("node-a")
	os.Add("x")
	os.Add("x") // second add creates a new unique tag

	// Element should have 2 tags
	require.True(t, os.Contains("x"))
	assert.Len(t, os.Elements["x"], 2)

	// Remove clears all tags
	os.Remove("x")
	assert.False(t, os.Contains("x"))
}

func TestORSet_ConcurrentOps(t *testing.T) {
	os := NewORSet("node-a")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := string(rune('a' + n%5))
			os.Add(key)
			if n%3 == 0 {
				os.Remove(key)
			}
		}(i)
	}
	wg.Wait()
	// No panic or race = success
}
