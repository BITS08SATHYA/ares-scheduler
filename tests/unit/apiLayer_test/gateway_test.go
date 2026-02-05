package gateway_test

//
//import (
//	"bytes"
//	"encoding/json"
//	"net/http"
//	"net/http/httptest"
//	"testing"
//	"time"
//
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/api/gateway"
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//)
//
//// ============================================================================
//// SECTION 1: VALIDATION TESTS (Pure Unit Tests - No Dependencies)
//// ============================================================================
//
//// TestAPIRequestValidation tests request validation logic via HTTP handler
//func TestAPIRequestValidation(t *testing.T) {
//	// Create minimal gateway for validation testing
//	config := &gateway.GatewayConfig{
//		Port:           8080,
//		RequestTimeout: 30 * time.Second,
//	}
//
//	// Create mock scheduler (minimal, for validation tests)
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err, "Failed to create gateway")
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Valid request passes validation", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "A100",
//			MemoryMB:  16384,
//			Priority:  50,
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		// Should not return 400 (validation error)
//		assert.NotEqual(t, http.StatusBadRequest, w.Code, "Valid request should pass validation")
//	})
//
//	t.Run("Missing request_id fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "", // Missing!
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "A100",
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail without request_id")
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "request_id", "Error should mention request_id")
//	})
//
//	t.Run("Missing name fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "", // Missing!
//			GPUCount:  4,
//			GPUType:   "A100",
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail without name")
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "name", "Error should mention name")
//	})
//
//	t.Run("Invalid GPU count fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//			GPUCount:  999, // Too many!
//			GPUType:   "A100",
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid GPU count")
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "gpu_count", "Error should mention gpu_count")
//	})
//
//	t.Run("Invalid GPU type fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "FAKE-GPU", // Invalid!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid GPU type")
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "gpu_type", "Error should mention gpu_type")
//	})
//
//	t.Run("Missing GPU type when GPUs requested fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "", // Missing!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should require GPU type when GPUs requested")
//	})
//
//	t.Run("Invalid priority fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//			Priority:  999, // Out of range!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid priority")
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "priority", "Error should mention priority")
//	})
//
//	t.Run("Invalid region fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID:    "test-req-123",
//			Name:         "test-job",
//			PreferRegion: "invalid-region", // Invalid!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid region")
//	})
//
//	t.Run("Invalid timeout fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID:   "test-req-123",
//			Name:        "test-job",
//			TimeoutSecs: 9999, // Too long!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid timeout")
//	})
//
//	t.Run("Invalid max retries fails", func(t *testing.T) {
//		req := &gateway.APIRequest{
//			RequestID:  "test-req-123",
//			Name:       "test-job",
//			MaxRetries: 999, // Too many!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code, "Should fail with invalid max_retries")
//	})
//}
//
//// ============================================================================
//// SECTION 2: HTTP HANDLER TESTS (Integration-Style with httptest)
//// ============================================================================
//
//// TestHealthCheckHandler tests the /health endpoint
//func TestHealthCheckHandler(t *testing.T) {
//	// Create gateway
//	config := &gateway.GatewayConfig{
//		Port:            8080,
//		HealthCheckPath: "/health",
//	}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	// Register routes
//	mux := gw.RegisterRoutes()
//
//	t.Run("GET /health returns 200 OK", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/health", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusOK, w.Code, "Should return 200 OK")
//
//		// Parse response
//		var response gateway.HealthCheckResponse
//		err := json.Unmarshal(w.Body.Bytes(), &response)
//		assert.NoError(t, err, "Response should be valid JSON")
//
//		// Verify fields
//		assert.Equal(t, "healthy", response.Status)
//		assert.NotEmpty(t, response.Timestamp)
//	})
//
//	t.Run("POST /health returns 405 Method Not Allowed", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodPost, "/health", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusMethodNotAllowed, w.Code, "Should return 405")
//	})
//}
//
//// TestScheduleJobHandler tests the /schedule endpoint
//func TestScheduleJobHandler(t *testing.T) {
//	// Create gateway
//	config := &gateway.GatewayConfig{
//		Port:           8080,
//		RequestTimeout: 30 * time.Second,
//		MaxRequestSize: 1 << 20,
//	}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Valid job request", func(t *testing.T) {
//		// Create valid request
//		reqBody := &gateway.APIRequest{
//			RequestID: "test-req-valid-123",
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "A100",
//			MemoryMB:  16384,
//			Priority:  50,
//		}
//
//		body, _ := json.Marshal(reqBody)
//		req := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		// Note: This might fail if no clusters registered, but validation should pass
//		// Check that we at least get past validation
//		assert.NotEqual(t, http.StatusBadRequest, w.Code, "Should pass validation")
//	})
//
//	t.Run("Invalid method returns 405", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/schedule", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
//	})
//
//	t.Run("Missing Content-Type returns 400", func(t *testing.T) {
//		reqBody := &gateway.APIRequest{
//			RequestID: "test-req-123",
//			Name:      "test-job",
//		}
//
//		body, _ := json.Marshal(reqBody)
//		req := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		// No Content-Type header!
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//	})
//
//	t.Run("Invalid JSON returns 400", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodPost, "/schedule",
//			bytes.NewBufferString("invalid json {{{"))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//	})
//
//	t.Run("Empty body returns 400", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodPost, "/schedule", nil)
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//	})
//
//	t.Run("Missing request_id fails validation", func(t *testing.T) {
//		reqBody := &gateway.APIRequest{
//			RequestID: "", // Missing!
//			Name:      "test-job",
//			GPUCount:  4,
//			GPUType:   "A100",
//		}
//
//		body, _ := json.Marshal(reqBody)
//		req := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//
//		// Check error message
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "request_id")
//	})
//}
//
//// ============================================================================
//// SECTION 3: CLUSTER REGISTRATION TESTS
//// ============================================================================
//
//// TestClusterRegistrationValidation tests cluster registration validation
//func TestClusterRegistrationValidation(t *testing.T) {
//	config := &gateway.GatewayConfig{Port: 8080}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Valid cluster registration", func(t *testing.T) {
//		req := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "cluster-us-west-1",
//			Region:             "us-west",
//			Zone:               "us-west-1a",
//			LocalSchedulerAddr: "http://localhost:9090",
//			TotalGPUs:          8,
//			TotalCPUs:          64,
//			TotalMemoryGB:      256.0,
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		// Should not return 400 (validation error)
//		assert.NotEqual(t, http.StatusBadRequest, w.Code, "Valid request should pass")
//	})
//
//	t.Run("Missing cluster_id fails", func(t *testing.T) {
//		req := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "", // Missing!
//			Region:             "us-west",
//			LocalSchedulerAddr: "http://localhost:9090",
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "cluster_id")
//	})
//
//	t.Run("Missing region fails", func(t *testing.T) {
//		req := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "cluster-1",
//			Region:             "", // Missing!
//			LocalSchedulerAddr: "http://localhost:9090",
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "region")
//	})
//
//	t.Run("Missing scheduler address fails", func(t *testing.T) {
//		req := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "cluster-1",
//			Region:             "us-west",
//			LocalSchedulerAddr: "", // Missing!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "local_scheduler_addr")
//	})
//
//	t.Run("Invalid GPU count fails", func(t *testing.T) {
//		req := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "cluster-1",
//			Region:             "us-west",
//			LocalSchedulerAddr: "http://localhost:9090",
//			TotalGPUs:          9999, // Too many!
//		}
//
//		body, _ := json.Marshal(req)
//		httpReq := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		httpReq.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, httpReq)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//
//		var errResp gateway.ErrorResponse
//		json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.Contains(t, errResp.Message, "total_gpus")
//	})
//}
//
//// TestClusterRegistrationHandler tests the /clusters/register endpoint
//func TestClusterRegistrationHandler(t *testing.T) {
//	config := &gateway.GatewayConfig{Port: 8080}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Valid cluster registration", func(t *testing.T) {
//		reqBody := &gateway.ClusterRegistrationRequest{
//			ClusterID:          "cluster-test-1",
//			Region:             "us-west",
//			Zone:               "us-west-1a",
//			LocalSchedulerAddr: "http://localhost:9090",
//			TotalGPUs:          8,
//			TotalCPUs:          64,
//			TotalMemoryGB:      256.0,
//		}
//
//		body, _ := json.Marshal(reqBody)
//		req := httptest.NewRequest(http.MethodPost, "/clusters/register", bytes.NewBuffer(body))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		// Should return 200 or 201
//		assert.True(t, w.Code == http.StatusOK || w.Code == http.StatusCreated,
//			"Should return success status")
//
//		// Parse response
//		var response gateway.ClusterRegistrationResponse
//		json.Unmarshal(w.Body.Bytes(), &response)
//		assert.True(t, response.Success)
//		assert.Equal(t, "cluster-test-1", response.ClusterID)
//	})
//
//	t.Run("Invalid method returns 405", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/clusters/register", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
//	})
//
//	t.Run("Invalid JSON returns 400", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodPost, "/clusters/register",
//			bytes.NewBufferString("invalid json"))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusBadRequest, w.Code)
//	})
//}
//
//// ============================================================================
//// SECTION 4: APIGATEWAY LIFECYCLE TESTS
//// ============================================================================
//
//// TestAPIGatewayCreation tests gateway initialization
//func TestAPIGatewayCreation(t *testing.T) {
//	t.Run("Create with valid config", func(t *testing.T) {
//		config := &gateway.GatewayConfig{
//			Port:           8080,
//			RequestTimeout: 30 * time.Second,
//		}
//
//		redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//		clusterManager := cluster.NewClusterManager(redisClient, nil)
//		globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//		gw, err := gateway.NewAPIGateway(globalScheduler, config)
//		assert.NoError(t, err)
//		assert.NotNil(t, gw)
//	})
//
//	t.Run("Create with nil scheduler fails", func(t *testing.T) {
//		config := &gateway.GatewayConfig{Port: 8080}
//
//		gw, err := gateway.NewAPIGateway(nil, config)
//		assert.Error(t, err)
//		assert.Nil(t, gw)
//		assert.Contains(t, err.Error(), "scheduler")
//	})
//
//	t.Run("Create with nil config uses defaults", func(t *testing.T) {
//		redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//		clusterManager := cluster.NewClusterManager(redisClient, nil)
//		globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//		gw, err := gateway.NewAPIGateway(globalScheduler, nil)
//		assert.NoError(t, err)
//		assert.NotNil(t, gw)
//	})
//
//	t.Run("Create with invalid port fails", func(t *testing.T) {
//		config := &gateway.GatewayConfig{Port: 99999} // Invalid!
//
//		redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//		clusterManager := cluster.NewClusterManager(redisClient, nil)
//		globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//		gw, err := gateway.NewAPIGateway(globalScheduler, config)
//		assert.Error(t, err)
//		assert.Nil(t, gw)
//	})
//}
//
//// TestAPIGatewayStart tests server start/stop
//func TestAPIGatewayStart(t *testing.T) {
//	t.Run("Start and stop server", func(t *testing.T) {
//		config := &gateway.GatewayConfig{
//			Port:           18080, // Non-standard port for testing
//			RequestTimeout: 5 * time.Second,
//		}
//
//		redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//		clusterManager := cluster.NewClusterManager(redisClient, nil)
//		globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//		gw, err := gateway.NewAPIGateway(globalScheduler, config)
//		require.NoError(t, err)
//
//		// Start server
//		err = gw.Start()
//		assert.NoError(t, err, "Should start successfully")
//
//		// Wait for server to be ready
//		time.Sleep(200 * time.Millisecond)
//
//		// Try to connect
//		resp, err := http.Get("http://localhost:18080/health")
//		if err == nil {
//			assert.Equal(t, http.StatusOK, resp.StatusCode)
//			resp.Body.Close()
//		}
//
//		// Stop server
//		err = gw.Stop(5 * time.Second)
//		assert.NoError(t, err, "Should stop successfully")
//	})
//}
//
//// ============================================================================
//// SECTION 5: MIDDLEWARE TESTS
//// ============================================================================
//
//// TestCORSMiddleware tests CORS header handling
//func TestCORSMiddleware(t *testing.T) {
//	config := &gateway.GatewayConfig{
//		Port:       8080,
//		EnableCORS: true,
//	}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("CORS headers present when enabled", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/health", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		// Check CORS headers
//		assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"))
//		assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Methods"))
//	})
//
//	t.Run("OPTIONS request returns 200", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodOptions, "/schedule", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusOK, w.Code)
//	})
//}
//
//// ============================================================================
//// SECTION 6: METRICS TESTS
//// ============================================================================
//
//// TestMetricsCollection tests metrics recording
//func TestMetricsCollection(t *testing.T) {
//	config := &gateway.GatewayConfig{
//		Port:             8080,
//		EnablePrometheus: true,
//		MetricsPath:      "/metrics",
//	}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Metrics endpoint returns 200", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusOK, w.Code)
//	})
//
//	t.Run("Stats collection works", func(t *testing.T) {
//		// Make a few requests
//		for i := 0; i < 5; i++ {
//			req := httptest.NewRequest(http.MethodGet, "/health", nil)
//			w := httptest.NewRecorder()
//			mux.ServeHTTP(w, req)
//		}
//
//		// Get stats
//		stats := gw.GetStats()
//		assert.NotNil(t, stats)
//		assert.GreaterOrEqual(t, stats["total_requests"].(uint64), uint64(5))
//	})
//
//	t.Run("Reset metrics works", func(t *testing.T) {
//		// Make requests
//		req := httptest.NewRequest(http.MethodGet, "/health", nil)
//		w := httptest.NewRecorder()
//		mux.ServeHTTP(w, req)
//
//		// Reset
//		gw.Reset()
//
//		// Stats should be zero
//		stats := gw.GetStats()
//		assert.Equal(t, uint64(0), stats["total_requests"].(uint64))
//	})
//}
//
//// ============================================================================
//// SECTION 7: ERROR HANDLING TESTS
//// ============================================================================
//
//// TestErrorResponses tests error response formatting
//func TestErrorResponses(t *testing.T) {
//	config := &gateway.GatewayConfig{Port: 8080}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("404 for unknown route", func(t *testing.T) {
//		req := httptest.NewRequest(http.MethodGet, "/unknown-route", nil)
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		assert.Equal(t, http.StatusNotFound, w.Code)
//	})
//
//	t.Run("Error response has correct format", func(t *testing.T) {
//		// Trigger validation error
//		reqBody := &gateway.APIRequest{
//			RequestID: "", // Missing!
//			Name:      "test",
//		}
//
//		body, _ := json.Marshal(reqBody)
//		req := httptest.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(body))
//		req.Header.Set("Content-Type", "application/json")
//		w := httptest.NewRecorder()
//
//		mux.ServeHTTP(w, req)
//
//		// Parse error response
//		var errResp gateway.ErrorResponse
//		err := json.Unmarshal(w.Body.Bytes(), &errResp)
//		assert.NoError(t, err)
//
//		// Verify error format
//		assert.False(t, errResp.Success)
//		assert.NotEmpty(t, errResp.ErrorCode)
//		assert.NotEmpty(t, errResp.Message)
//		assert.NotEmpty(t, errResp.Timestamp)
//	})
//}
//
//// ============================================================================
//// SECTION 8: CONCURRENT REQUEST TESTS
//// ============================================================================
//
//// TestConcurrentRequests tests thread-safety
//func TestConcurrentRequests(t *testing.T) {
//	config := &gateway.GatewayConfig{
//		Port:           8080,
//		RequestTimeout: 5 * time.Second,
//	}
//
//	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
//	clusterManager := cluster.NewClusterManager(redisClient, nil)
//	globalScheduler := global.NewGlobalScheduler("test", redisClient, clusterManager)
//
//	gw, err := gateway.NewAPIGateway(globalScheduler, config)
//	require.NoError(t, err)
//
//	mux := gw.RegisterRoutes()
//
//	t.Run("Handle 100 concurrent health checks", func(t *testing.T) {
//		numRequests := 100
//		done := make(chan bool, numRequests)
//
//		for i := 0; i < numRequests; i++ {
//			go func() {
//				req := httptest.NewRequest(http.MethodGet, "/health", nil)
//				w := httptest.NewRecorder()
//				mux.ServeHTTP(w, req)
//				assert.Equal(t, http.StatusOK, w.Code)
//				done <- true
//			}()
//		}
//
//		// Wait for all requests
//		for i := 0; i < numRequests; i++ {
//			<-done
//		}
//
//		// Verify stats
//		stats := gw.GetStats()
//		assert.GreaterOrEqual(t, stats["total_requests"].(uint64), uint64(numRequests))
//	})
//}
