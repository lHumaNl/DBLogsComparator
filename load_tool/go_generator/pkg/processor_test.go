package pkg

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// MockLogDB - mock database for testing
type MockLogDB struct {
	name       string
	sendCalled int
	shouldFail bool
	metrics    map[string]float64
}

func NewMockLogDB(name string, shouldFail bool) *MockLogDB {
	return &MockLogDB{
		name:       name,
		shouldFail: shouldFail,
		metrics:    make(map[string]float64),
	}
}

func (m *MockLogDB) Name() string {
	return m.name
}

func (m *MockLogDB) Initialize() error {
	return nil
}

func (m *MockLogDB) SendLogs(logs []logdb.LogEntry) error {
	m.sendCalled++
	if m.shouldFail {
		return fmt.Errorf("test error: failed to send logs")
	}
	return nil
}

func (m *MockLogDB) Close() error {
	return nil
}

func (m *MockLogDB) Metrics() map[string]float64 {
	return m.metrics
}

func (m *MockLogDB) FormatPayload(logs []logdb.LogEntry) (string, string) {
	return "", "application/json"
}

func TestCreateBulkPayload(t *testing.T) {
	// Testing log package creation
	config := Config{
		Mode:                "test",
		BulkSize:            10,
		LogTypeDistribution: map[string]int{"web_access": 100}, // Only one type for simplicity
	}

	bufferPool := NewBufferPool()
	logs := CreateBulkPayload(config, bufferPool)

	// Check package size
	if len(logs) != config.BulkSize {
		t.Errorf("Package size should be %d, got %d", config.BulkSize, len(logs))
	}

	// Check log type and structure
	for _, log := range logs {
		if logType, ok := log["log_type"].(string); !ok || logType != "web_access" {
			t.Errorf("Expected log type 'web_access', got '%v'", logType)
		}

		if _, ok := log["timestamp"].(string); !ok {
			t.Error("Log should have a 'timestamp' field of type string")
		}
	}
}

func TestWorker(t *testing.T) {
	// Testing Worker function
	config := Config{
		BulkSize:            5,
		LogTypeDistribution: map[string]int{"web_access": 100},
		Verbose:             false,
		EnableMetrics:       false,
	}

	stats := &Stats{}
	bufferPool := NewBufferPool()

	// Create a channel for tasks
	jobs := make(chan struct{}, 2)
	jobs <- struct{}{} // Add two tasks
	jobs <- struct{}{}
	close(jobs) // Close the channel so the Worker finishes

	var wg sync.WaitGroup
	wg.Add(1)

	// Start Worker with mock database
	db := NewMockLogDB("mockdb", false)
	go Worker(1, jobs, stats, config, db, bufferPool, &wg)

	// Wait for completion
	wg.Wait()

	// Check statistics
	if db.sendCalled != 2 {
		t.Errorf("SendLogs method should be called 2 times, actually: %d", db.sendCalled)
	}

	if atomic.LoadInt64(&stats.TotalRequests) != 2 {
		t.Errorf("TotalRequests should be 2, got %d", stats.TotalRequests)
	}

	if atomic.LoadInt64(&stats.SuccessfulRequests) != 2 {
		t.Errorf("SuccessfulRequests should be 2, got %d", stats.SuccessfulRequests)
	}

	if atomic.LoadInt64(&stats.TotalLogs) != int64(2*config.BulkSize) {
		t.Errorf("TotalLogs should be %d, got %d", 2*config.BulkSize, stats.TotalLogs)
	}

	// Testing with error
	stats = &Stats{}
	jobs = make(chan struct{}, 1)
	jobs <- struct{}{}
	close(jobs)

	wg.Add(1)
	db = NewMockLogDB("mockdb", true) // This database will return an error
	go Worker(1, jobs, stats, config, db, bufferPool, &wg)

	wg.Wait()

	if atomic.LoadInt64(&stats.FailedRequests) != 1 {
		t.Errorf("FailedRequests should be 1, got %d", stats.FailedRequests)
	}
}

func TestStatsReporter(t *testing.T) {
	// Testing StatsReporter function
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Set some initial statistic values
	atomic.StoreInt64(&stats.TotalRequests, 100)
	atomic.StoreInt64(&stats.SuccessfulRequests, 95)
	atomic.StoreInt64(&stats.FailedRequests, 5)
	atomic.StoreInt64(&stats.TotalLogs, 1000)

	// Create a channel for stopping
	stopChan := make(chan struct{})

	config := Config{
		EnableMetrics: false,
	}

	// Run StatsReporter for a short time
	go StatsReporter(stats, stopChan, config)

	// Let it work for a bit (enough for one tick)
	time.Sleep(1100 * time.Millisecond)

	// Stop
	close(stopChan)

	// No explicit checks since StatsReporter just outputs information
	// We could redirect stdout and check the output, but that's more complex
	// In a real test we can verify that the function doesn't panic
	// and correctly stops when the channel is closed
}
