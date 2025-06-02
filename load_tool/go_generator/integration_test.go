package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

// TestMain - entry point for running tests
func TestMain(m *testing.M) {
	// Here we can perform setup before running tests
	setupTestEnvironment()

	// Run tests
	code := m.Run()

	// Cleanup after tests execution
	teardownTestEnvironment()

	// Exit with test execution code
	os.Exit(code)
}

// Test environment setup
func setupTestEnvironment() {
	// Here we can prepare the test environment:
	// - Set environment variables
	// - Initialize resources
	// - Create temporary files
	// Здесь можно подготовить тестовое окружение:
	// - Установить переменные окружения
	// - Инициализировать ресурсы
	// - Создать временные файлы
}

// Test environment cleanup
func teardownTestEnvironment() {
	// Here we can clean up the test environment:
	// - Remove temporary files
	// - Free up resources
	// Здесь можно очистить тестовое окружение:
	// - Удалить временные файлы
	// - Освободить ресурсы
}

// Integration test to verify core functionality
func TestLogGeneratorIntegration(t *testing.T) {
	// Skip this test during normal execution,
	// as it requires external dependencies and configured environment
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Integration tests skipped. Set RUN_INTEGRATION_TESTS=true to run them.")
	}

	// Create test configuration with short execution time
	config := pkg.Config{
		Mode:        "mock",                  // Using mock for testing
		BaseURL:     "http://localhost:8000", // Mock server address
		URL:         "http://localhost:8000",
		RPS:         10,
		Duration:    100 * time.Millisecond, // Short duration for the test
		BulkSize:    5,
		WorkerCount: 2,
		LogTypeDistribution: map[string]int{
			"web_access":  60,
			"web_error":   10,
			"application": 20,
			"metric":      5,
			"event":       5,
		},
		Verbose:       true,
		MaxRetries:    1,
		RetryDelay:    50 * time.Millisecond,
		EnableMetrics: false,
	}

	// Create mock database for testing
	mockDB := NewMockLogDB("integration_test")

	// Start generator with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan error)
	go func() {
		err := pkg.RunGenerator(config, mockDB)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Error running generator: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Test exceeded timeout")
	}

	// Check that logs were sent
	if mockDB.sendCalled == 0 {
		t.Error("No calls to send logs")
	}
}

// MockLogDB for integration tests
type MockLogDB struct {
	name       string
	sendCalled int
	metrics    map[string]float64
}

// Create new MockLogDB instance for integration tests
func NewMockLogDB(name string) *MockLogDB {
	return &MockLogDB{
		name:       name,
		sendCalled: 0,
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
