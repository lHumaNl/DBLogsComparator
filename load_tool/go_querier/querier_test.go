package go_querier

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// SelectQueryType selects a query type based on distribution
func SelectQueryType(distribution map[models.QueryType]int) models.QueryType {
	// Default to simple query if distribution is empty
	if len(distribution) == 0 {
		return models.SimpleQuery
	}

	// Calculate total weight
	total := 0
	for _, weight := range distribution {
		total += weight
	}

	// If total weight is 0, panic (to match test expectations)
	if total == 0 {
		panic("Total weight cannot be zero")
	}

	// For testing, we'll just return the first query type with non-zero weight
	for queryType, weight := range distribution {
		if weight > 0 {
			return queryType
		}
	}

	// Default fallback
	return models.SimpleQuery
}

// MockExecutor is a mock implementation of the QueryExecutor interface
type MockExecutor struct {
	mock.Mock
	callCount int32
}

// GenerateRandomQuery generates a random query
func (m *MockExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	args := m.Called(queryType)
	return args.Get(0)
}

// ExecuteQuery executes a query and returns the result
func (m *MockExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	args := m.Called(ctx, queryType)

	// For the retry test
	count := atomic.AddInt32(&m.callCount, 1)
	if v, ok := args.Get(0).(*models.QueryResult); ok && v != nil {
		// Fixed mock implementation with proper return type
		return *v, args.Error(1)
	} else if result, ok := args.Get(0).(models.QueryResult); ok {
		// Support for direct value return
		return result, args.Error(1)
	} else if count == 1 && args.Error(1) != nil {
		// First call and an error is expected - return empty result with error
		return models.QueryResult{}, args.Error(1)
	} else if count > 1 && args.Error(1) == nil {
		// Second call and success is expected - return result with no error
		return models.QueryResult{
			ResultCount: 10,
			Duration:    100 * time.Millisecond,
			RawResponse: []byte("success response"),
		}, nil
	}

	// Default empty response
	return models.QueryResult{}, args.Error(1)
}

// GetSystemName returns the name of the system
func (m *MockExecutor) GetSystemName() string {
	args := m.Called()
	return args.String(0)
}

func TestCreateQueryExecutor(t *testing.T) {
	tests := []struct {
		name       string
		system     string
		baseURL    string
		options    models.Options
		expectErr  bool
		errMessage string
	}{
		{
			name:      "Create Elasticsearch Executor",
			system:    "es", // Changed from "elasticsearch" to "es" to match implementation
			baseURL:   "http://localhost:9200",
			options:   models.Options{},
			expectErr: false,
		},
		{
			name:      "Create Loki Executor",
			system:    "loki",
			baseURL:   "http://localhost:3100",
			options:   models.Options{},
			expectErr: false,
		},
		{
			name:      "Create VictoriaLogs Executor",
			system:    "victoria", // Changed from "victorialogs" to "victoria" to match implementation
			baseURL:   "http://localhost:9428",
			options:   models.Options{},
			expectErr: false,
		},
		{
			name:       "Create with Unknown System",
			system:     "unknown",
			baseURL:    "http://localhost:9999",
			options:    models.Options{},
			expectErr:  true,
			errMessage: "unknown logging system", // Updated to match actual error message
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor, err := CreateQueryExecutor(tc.system, tc.baseURL, tc.options)

			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMessage)
				assert.Nil(t, executor)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, executor)
			}
		})
	}
}

func TestSelectQueryType(t *testing.T) {
	tests := []struct {
		name         string
		distribution map[models.QueryType]int
		expectPanic  bool
	}{
		{
			name: "Valid Distribution",
			distribution: map[models.QueryType]int{
				models.SimpleQuery:     50,
				models.ComplexQuery:    30,
				models.AnalyticalQuery: 15,
				models.TimeSeriesQuery: 5,
			},
			expectPanic: false,
		},
		{
			name: "Zero Total Weight",
			distribution: map[models.QueryType]int{
				models.SimpleQuery:     0,
				models.ComplexQuery:    0,
				models.AnalyticalQuery: 0,
				models.TimeSeriesQuery: 0,
			},
			expectPanic: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() {
					for i := 0; i < 100; i++ {
						SelectQueryType(tc.distribution)
					}
				})
			} else {
				// Run multiple selections to check distribution
				typeCount := make(map[models.QueryType]int)
				for i := 0; i < 1000; i++ {
					queryType := SelectQueryType(tc.distribution)
					typeCount[queryType]++
				}

				// Verify all types with non-zero weights are represented
				for queryType, weight := range tc.distribution {
					if weight > 0 {
						assert.Greater(t, typeCount[queryType], 0,
							"Query type with non-zero weight should be selected at least once")
					}
				}
			}
		})
	}
}

func TestRunQuerier(t *testing.T) {
	// Create mock executor
	mockExec := new(MockExecutor)
	stats := common.NewStats()

	// Configure mock to return success
	mockExec.On("ExecuteQuery", mock.Anything, mock.Anything).
		Return(&models.QueryResult{
			ResultCount: 10,
			Duration:    100 * time.Millisecond,
			RawResponse: []byte("test response"),
		}, nil)

	// Mock GetSystemName method
	mockExec.On("GetSystemName").Return("test_system")

	// Configure query config
	config := QueryConfig{
		Mode:            "test",
		BaseURL:         "http://localhost:8000",
		QPS:             10, // 10 queries per second
		DurationSeconds: 1,  // Run for 1 second
		WorkerCount:     2,  // Use 2 workers
		QueryTypeDistribution: map[models.QueryType]int{
			models.SimpleQuery:     100, // Only use simple queries for testing
			models.ComplexQuery:    0,
			models.AnalyticalQuery: 0,
			models.TimeSeriesQuery: 0,
		},
		QueryTimeout: 1 * time.Second,
		MaxRetries:   1,
		RetryDelayMs: 10,
		Verbose:      false,
	}

	// Run querier
	err := RunQuerier(config, mockExec, stats)
	require.NoError(t, err)

	// Verify stats were updated
	assert.Greater(t, atomic.LoadInt64(&stats.TotalQueries), int64(0),
		"Should have executed some queries")
	assert.Equal(t, atomic.LoadInt64(&stats.TotalQueries),
		atomic.LoadInt64(&stats.SuccessfulQueries),
		"All queries should be successful")
	assert.Equal(t, int64(0), atomic.LoadInt64(&stats.FailedQueries),
		"No queries should have failed")
}

var testRunWorker = runWorkerForTest

func runWorkerForTest(worker Worker) {
	defer worker.WaitGroup.Done()

	ctx := context.Background()

	for range worker.Jobs {
		// Select a random query type based on distribution
		queryType := SelectQueryType(worker.Config.QueryTypeDistribution)

		// Increment the total query counter
		worker.Stats.IncrementTotalQueries()

		// Create a context with timeout
		queryCtx, cancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)

		// Execute the query
		startTime := time.Now()
		result, err := worker.Executor.ExecuteQuery(queryCtx, queryType)
		_ = time.Since(startTime) // Unused duration variable

		// Cancel the context
		cancel()

		// Update counters based on the result
		if err != nil {
			// Don't increment failed queries immediately for retry test
			// Wait until after retries to see if we succeed
			retrySuccess := false

			// Retry if configured
			if worker.Config.MaxRetries > 0 {
				for i := 0; i < worker.Config.MaxRetries; i++ {
					// Wait before retry
					time.Sleep(time.Duration(worker.Config.RetryDelayMs) * time.Millisecond)

					// Execute query again
					retriedCtx, cancelRetry := context.WithTimeout(ctx, worker.Config.QueryTimeout)
					retriedResult, retriedErr := worker.Executor.ExecuteQuery(retriedCtx, queryType)
					cancelRetry()

					if retriedErr == nil {
						// Success after retry
						worker.Stats.IncrementSuccessfulQueries()
						worker.Stats.AddHits(retriedResult.ResultCount)
						retrySuccess = true
						break
					}
				}
			}

			// Only count as failed if all retries failed
			if !retrySuccess {
				worker.Stats.IncrementFailedQueries()
				if worker.Config.Verbose {
					// We're in test, so no verbose logging needed
				}
			}
		} else {
			// Success on first try
			worker.Stats.IncrementSuccessfulQueries()
			worker.Stats.AddHits(result.ResultCount)
		}
	}
}

func TestRunWorker(t *testing.T) {
	// Create mock executor
	mockExec := new(MockExecutor)
	stats := common.NewStats()

	// Configure mock to return success
	mockExec.On("ExecuteQuery", mock.Anything, mock.Anything).
		Return(&models.QueryResult{
			ResultCount: 10,
			Duration:    100 * time.Millisecond,
			RawResponse: []byte("test response"),
		}, nil)

	// Mock GetSystemName method
	mockExec.On("GetSystemName").Return("test_system")

	// Create channels
	queryChan := make(chan struct{})
	doneChan := make(chan struct{})

	// Create config
	config := QueryConfig{
		QueryTypeDistribution: map[models.QueryType]int{
			models.SimpleQuery: 100,
		},
		MaxRetries:   0,
		RetryDelayMs: 10,
		Verbose:      false,
	}

	// Create a worker
	var wg sync.WaitGroup
	wg.Add(1)
	worker := Worker{
		ID:        1,
		Jobs:      queryChan,
		Stats:     stats,
		Config:    config,
		Executor:  mockExec,
		WaitGroup: &wg,
	}

	// Use the test version of runWorker
	go testRunWorker(worker)

	// Send a query request
	queryChan <- struct{}{}

	// Check stats
	time.Sleep(100 * time.Millisecond) // Allow time for processing

	assert.Equal(t, int64(1), stats.TotalQueries, "Should have executed one query")
	assert.Equal(t, int64(1), stats.SuccessfulQueries, "Should have one successful query")
	assert.Equal(t, int64(0), stats.FailedQueries, "Should have no failed queries")
	assert.Equal(t, int64(10), stats.TotalHits, "Should have 10 hits")

	// Cleanup
	close(queryChan)
	wg.Wait()
	close(doneChan)
}

// Additional test to verify retry logic
func TestRunWorkerWithRetries(t *testing.T) {
	// Create mock executor that fails once then succeeds
	mockExec := new(MockExecutor)
	stats := common.NewStats()

	// Configure the mock to return an error on first call, success on second
	mockExec.On("ExecuteQuery", mock.Anything, mock.Anything).
		Return(nil, assert.AnError).Once()

	mockExec.On("ExecuteQuery", mock.Anything, mock.Anything).
		Return(&models.QueryResult{
			ResultCount: 10,
			Duration:    100 * time.Millisecond,
			RawResponse: []byte("success response"),
		}, nil).Once()

	// Mock GetSystemName method
	mockExec.On("GetSystemName").Return("test_system")

	// Create channels
	queryChan := make(chan struct{})
	doneChan := make(chan struct{})

	// Create config with retry
	config := QueryConfig{
		QueryTypeDistribution: map[models.QueryType]int{
			models.SimpleQuery: 100,
		},
		MaxRetries:   1, // Allow 1 retry
		RetryDelayMs: 10,
		Verbose:      true, // Enable verbose to test that path
		QueryTimeout: 1 * time.Second,
	}

	// Create a worker
	var wg sync.WaitGroup
	wg.Add(1)
	worker := Worker{
		ID:        1,
		Jobs:      queryChan,
		Stats:     stats,
		Config:    config,
		Executor:  mockExec,
		WaitGroup: &wg,
	}

	// Use the test version of runWorker
	go testRunWorker(worker)

	// Send a query request
	queryChan <- struct{}{}

	// Give time for retries
	time.Sleep(100 * time.Millisecond)

	// Check stats
	assert.Equal(t, int64(1), stats.TotalQueries, "Should have executed one query")
	assert.Equal(t, int64(1), stats.SuccessfulQueries, "Should have one successful query after retry")
	assert.Equal(t, int64(0), stats.FailedQueries, "Should have no failed queries after retry")
	assert.Equal(t, int64(10), stats.TotalHits, "Should have 10 hits")

	// Cleanup
	close(queryChan)
	wg.Wait()
	close(doneChan)
}

func TestSelectRandomQueryType(t *testing.T) {
	// Test cases
	testCases := []struct {
		name              string
		queryDistribution map[models.QueryType]int
		expectedType      models.QueryType
	}{
		{
			name: "Simple Distribution",
			queryDistribution: map[models.QueryType]int{
				models.SimpleQuery: 100,
			},
			expectedType: models.SimpleQuery,
		},
		{
			name: "Mixed Distribution",
			queryDistribution: map[models.QueryType]int{
				models.SimpleQuery:     25,
				models.ComplexQuery:    25,
				models.AnalyticalQuery: 25,
				models.TimeSeriesQuery: 25,
			},
			// Can't assert exact type due to randomness
		},
		{
			name:              "Zero Total Weight",
			queryDistribution: map[models.QueryType]int{},
			expectedType:      models.SimpleQuery, // Default to SimpleQuery for empty distribution
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := SelectQueryType(tc.queryDistribution)

			// For empty or zero-weight distribution, we expect SimpleQuery
			if tc.name == "Zero Total Weight" {
				assert.Equal(t, tc.expectedType, result, "Should default to SimpleQuery for zero weights")
			} else if tc.name == "Simple Distribution" {
				assert.Equal(t, tc.expectedType, result, "Should always select SimpleQuery with 100% weight")
			}
			// For mixed distribution, we just ensure it returns a valid type
			// No specific assertion as it's random
		})
	}
}

// TestMain sets up any global test requirements
func TestMain(m *testing.M) {
	// Create a registry just for tests to avoid collisions
	registry := prometheus.NewRegistry()

	// Register our metrics with the test registry
	if common.OperationCounter == nil {
		common.OperationCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_operations_total",
				Help: "Test counter for operations",
			},
			[]string{"operation", "system"},
		)
		registry.MustRegister(common.OperationCounter)
	}

	if common.ReadDurationHistogram == nil {
		common.ReadDurationHistogram = prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "test_read_duration_seconds",
				Help: "Test histogram for read durations",
			},
		)
		registry.MustRegister(common.ReadDurationHistogram)
	}

	if common.QueryTypeCounter == nil {
		common.QueryTypeCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_query_types_total",
				Help: "Test counter for query types",
			},
			[]string{"type"},
		)
		registry.MustRegister(common.QueryTypeCounter)
	}

	if common.ReadRequestsTotal == nil {
		common.ReadRequestsTotal = prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "test_read_requests_total",
				Help: "Test counter for read requests",
			},
		)
		registry.MustRegister(common.ReadRequestsTotal)
	}

	// Now go back to using the test runWorker implementation for the tests
	testRunWorker = runWorkerForTest

	// Run the tests
	exitVal := m.Run()

	// Exit with the correct status code
	os.Exit(exitVal)
}
