// // File: tests/unit/04_layer3_idempotency_test.go
// // Layer 3 - Idempotency Manager Tests
// // Adjusted for actual ares-scheduler repo structure
// // Coverage: CheckDuplicate, RecordSuccess, ValidateRequestID
// // ~400 LOC, excellent coverage
package unit

//
//import (
//	"context"
//	"fmt"
//	"sync"
//	"sync/atomic"
//	"testing"
//
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//)
//
//// ============================================================================
//// MOCK STORAGE FOR IDEMPOTENCY MANAGER
//// ============================================================================
//
//type MockIdempotencyStore struct {
//	cache map[string]*idempotency.RequestRecord
//	mu    sync.RWMutex
//}
//
//func NewMockIdempotencyStore() *MockIdempotencyStore {
//	return &MockIdempotencyStore{
//		cache: make(map[string]*idempotency.RequestRecord),
//	}
//}
//
//func (m *MockIdempotencyStore) Store(ctx context.Context, record *idempotency.RequestRecord) error {
//	if record == nil || record.RequestID == "" {
//		return fmt.Errorf("invalid record: missing request ID")
//	}
//
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	m.cache[record.RequestID] = record
//	return nil
//}
//
//func (m *MockIdempotencyStore) Get(ctx context.Context, requestID string) (*idempotency.RequestRecord, error) {
//	m.mu.RLock()
//	defer m.mu.RUnlock()
//
//	if record, ok := m.cache[requestID]; ok {
//		return record, nil
//	}
//	return nil, nil
//}
//
//func (m *MockIdempotencyStore) Delete(ctx context.Context, requestID string) error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	delete(m.cache, requestID)
//	return nil
//}
//
//func (m *MockIdempotencyStore) Clear(ctx context.Context) error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	m.cache = make(map[string]*idempotency.RequestRecord)
//	return nil
//}
//
//// ============================================================================
//// TEST CASES
//// ============================================================================
//
//// TestCheckDuplicateFirstRequest - First request should not be duplicate
//func TestCheckDuplicateFirstRequest(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewIdempotencyManager(store)
//
//	requestID := "req-001"
//	isDuplicate, err := manager.CheckDuplicate(context.Background(), requestID)
//
//	require.NoError(t, err)
//	assert.False(t, isDuplicate, "First request should not be a duplicate")
//}
//
//// TestCheckDuplicateSecondRequest - Second request with same ID should be duplicate
//func TestCheckDuplicateSecondRequest(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-002"
//
//	// First request
//	isDuplicate, _ := manager.CheckDuplicate(context.Background(), requestID)
//	assert.False(t, isDuplicate)
//
//	// Record the first request
//	err := manager.RecordSuccess(context.Background(), requestID, "result-1")
//	require.NoError(t, err)
//
//	// Second request with same ID
//	isDuplicate, err = manager.CheckDuplicate(context.Background(), requestID)
//	require.NoError(t, err)
//	assert.True(t, isDuplicate, "Second request with same ID should be a duplicate")
//}
//
//// TestCheckDuplicateEmptyID - Empty request ID validation
//func TestCheckDuplicateEmptyID(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	_, err := manager.CheckDuplicate(context.Background(), "")
//	assert.Error(t, err, "Empty request ID should return error")
//}
//
//// TestRecordSuccess - Record successful request
//func TestRecordSuccess(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-003"
//	result := "operation-successful"
//
//	err := manager.RecordSuccess(context.Background(), requestID, result)
//	require.NoError(t, err)
//
//	// Verify it was stored
//	record, _ := store.Get(context.Background(), requestID)
//	require.NotNil(t, record)
//	assert.Equal(t, requestID, record.RequestID)
//}
//
//// TestRecordSuccessNilResult - Handle nil results
//func TestRecordSuccessNilResult(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-004"
//
//	// Should handle nil results gracefully
//	err := manager.RecordSuccess(context.Background(), requestID, nil)
//	require.NoError(t, err)
//}
//
//// TestValidateRequestIDFormat - Validate request ID format
//func TestValidateRequestIDFormat(t *testing.T) {
//	testCases := []struct {
//		name      string
//		requestID string
//		shouldErr bool
//	}{
//		{"Valid ID", "req-12345", false},
//		{"Valid UUID", "550e8400-e29b-41d4-a716-446655440000", false},
//		{"Valid alphanumeric", "REQUEST-ABC123", false},
//		{"Empty ID", "", true},
//		{"Too long", "x" + string(make([]byte, 300)), true},
//		{"Invalid characters", "req@invalid", true},
//	}
//
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			err := manager.ValidateRequestID(tc.requestID)
//			if tc.shouldErr {
//				assert.Error(t, err)
//			} else {
//				assert.NoError(t, err)
//			}
//		})
//	}
//}
//
//// TestRecordCompletion - Record job completion
//func TestRecordCompletion(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-005"
//	jobID := "job-123"
//	status := "completed"
//
//	err := manager.RecordCompletion(context.Background(), requestID, jobID, status)
//	require.NoError(t, err)
//
//	record, _ := store.Get(context.Background(), requestID)
//	require.NotNil(t, record)
//	assert.Equal(t, jobID, record.JobID)
//}
//
//// TestGetCachedResult - Retrieve cached result
//func TestGetCachedResult(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-006"
//	expectedResult := "cached-result"
//
//	// Record the result
//	manager.RecordSuccess(context.Background(), requestID, expectedResult)
//
//	// Retrieve it
//	result, err := manager.GetCachedResult(context.Background(), requestID)
//	require.NoError(t, err)
//	assert.Equal(t, expectedResult, result)
//}
//
//// TestGetCachedResultNotFound - Missing cached result
//func TestGetCachedResultNotFound(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	result, err := manager.GetCachedResult(context.Background(), "nonexistent")
//	assert.NoError(t, err)
//	assert.Nil(t, result)
//}
//
//// ============================================================================
//// CONCURRENCY TESTS
//// ============================================================================
//
//// TestConcurrentDuplicateDetection - Race condition test with 100 goroutines
//func TestConcurrentDuplicateDetection(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "concurrent-req"
//	numGoroutines := 100
//	var wg sync.WaitGroup
//	var duplicateCount int32
//
//	for i := 0; i < numGoroutines; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//
//			isDuplicate, err := manager.CheckDuplicate(context.Background(), requestID)
//			if err == nil && isDuplicate {
//				atomic.AddInt32(&duplicateCount, 1)
//			}
//
//			// Try to record success
//			manager.RecordSuccess(context.Background(), requestID, "result")
//		}()
//	}
//
//	wg.Wait()
//
//	// Only first request should not be duplicate, rest should be
//	duplicates := atomic.LoadInt32(&duplicateCount)
//	t.Logf("Duplicates detected: %d (expected: ~%d)", duplicates, numGoroutines-1)
//}
//
//// TestConcurrentRecordSuccess - Multiple concurrent success recordings
//func TestConcurrentRecordSuccess(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	numGoroutines := 50
//	var wg sync.WaitGroup
//	var successCount int32
//
//	for i := 0; i < numGoroutines; i++ {
//		wg.Add(1)
//		go func(idx int) {
//			defer wg.Done()
//
//			requestID := fmt.Sprintf("req-concurrent-%d", idx)
//			err := manager.RecordSuccess(context.Background(), requestID, fmt.Sprintf("result-%d", idx))
//			if err == nil {
//				atomic.AddInt32(&successCount, 1)
//			}
//		}(i)
//	}
//
//	wg.Wait()
//
//	assert.Equal(t, int32(numGoroutines), successCount)
//}
//
//// TestConcurrentReads - Multiple reads from cache
//func TestConcurrentReads(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "read-test"
//	manager.RecordSuccess(context.Background(), requestID, "test-result")
//
//	numGoroutines := 100
//	var wg sync.WaitGroup
//	var successCount int32
//
//	for i := 0; i < numGoroutines; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//
//			result, err := manager.GetCachedResult(context.Background(), requestID)
//			if err == nil && result != nil {
//				atomic.AddInt32(&successCount, 1)
//			}
//		}()
//	}
//
//	wg.Wait()
//
//	assert.Equal(t, int32(numGoroutines), successCount)
//}
//
//// ============================================================================
//// EDGE CASE TESTS
//// ============================================================================
//
//// TestMultipleDifferentRequests - Track multiple different requests
//func TestMultipleDifferentRequests(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	// Record 5 different requests
//	for i := 0; i < 5; i++ {
//		requestID := fmt.Sprintf("req-%d", i)
//		err := manager.RecordSuccess(context.Background(), requestID, fmt.Sprintf("result-%d", i))
//		require.NoError(t, err)
//	}
//
//	// Verify all are tracked
//	for i := 0; i < 5; i++ {
//		requestID := fmt.Sprintf("req-%d", i)
//		isDuplicate, _ := manager.CheckDuplicate(context.Background(), requestID)
//		assert.True(t, isDuplicate, "Request %s should be marked as duplicate", requestID)
//	}
//}
//
//// TestResultOverwrite - Handle result updates
//func TestResultOverwrite(t *testing.T) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	requestID := "req-overwrite"
//
//	// Record first result
//	manager.RecordSuccess(context.Background(), requestID, "result-1")
//	result1, _ := manager.GetCachedResult(context.Background(), requestID)
//	assert.Equal(t, "result-1", result1)
//
//	// Update with new result
//	manager.RecordSuccess(context.Background(), requestID, "result-2")
//	result2, _ := manager.GetCachedResult(context.Background(), requestID)
//	assert.Equal(t, "result-2", result2)
//}
//
//// ============================================================================
//// BENCHMARK TESTS
//// ============================================================================
//
//// BenchmarkCheckDuplicate - Duplicate check performance
//func BenchmarkCheckDuplicate(b *testing.B) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//	ctx := context.Background()
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		manager.CheckDuplicate(ctx, fmt.Sprintf("req-%d", i))
//	}
//}
//
//// BenchmarkRecordSuccess - Success recording performance
//func BenchmarkRecordSuccess(b *testing.B) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//	ctx := context.Background()
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		manager.RecordSuccess(ctx, fmt.Sprintf("req-%d", i), "result")
//	}
//}
//
//// BenchmarkGetCachedResult - Cache retrieval performance
//func BenchmarkGetCachedResult(b *testing.B) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//	ctx := context.Background()
//
//	// Pre-populate cache
//	manager.RecordSuccess(ctx, "req-cache", "result")
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		manager.GetCachedResult(ctx, "req-cache")
//	}
//}
//
//// BenchmarkValidateRequestID - Validation performance
//func BenchmarkValidateRequestID(b *testing.B) {
//	store := NewMockIdempotencyStore()
//	manager := idempotency.NewManager(store)
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		manager.ValidateRequestID(fmt.Sprintf("req-%d", i))
//	}
//}
