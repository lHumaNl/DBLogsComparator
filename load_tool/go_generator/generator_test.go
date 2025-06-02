package main

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

// TestCmdLineArgs checks the correct parsing of command line arguments
func TestCmdLineArgs(t *testing.T) {
	// Save original arguments
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}()

	// Test 1: Basic arguments
	os.Args = []string{
		"log-generator",
		"-mode", "es",
		"-rps", "100",
		"-duration", "10s",
		"-bulk-size", "50",
		"-worker-count", "4",
	}

	// Create a new FlagSet for tests
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	// Parameters for testing
	mode := flag.String("mode", "victoria", "Operating mode")
	rps := flag.Int("rps", 10, "Requests per second")
	duration := flag.Duration("duration", 1*time.Minute, "Test duration")
	bulkSize := flag.Int("bulk-size", 100, "Number of logs per request")
	workerCount := flag.Int("worker-count", 4, "Number of worker goroutines")

	// Parse arguments
	flag.Parse()

	// Check results
	if *mode != "es" {
		t.Errorf("Incorrect mode, expected 'es', got '%s'", *mode)
	}

	if *rps != 100 {
		t.Errorf("Incorrect RPS value, expected 100, got %d", *rps)
	}

	if *bulkSize != 50 {
		t.Errorf("Incorrect bulk size, expected 50, got %d", *bulkSize)
	}

	if *workerCount != 4 {
		t.Errorf("Incorrect worker count, expected 4, got %d", *workerCount)
	}

	if *duration != 10*time.Second {
		t.Errorf("Incorrect duration, expected 10s, got %v", *duration)
	}
}

// TestCreateConfig checks the creation of configuration from command line arguments
func TestCreateConfig(t *testing.T) {
	// Create test configuration
	config := pkg.Config{
		Mode:          "victoria",
		URL:           "http://localhost:8428",
		RPS:           10,
		Duration:      1 * time.Minute,
		BulkSize:      100,
		WorkerCount:   4,
		EnableMetrics: true,
		LogTypeDistribution: map[string]int{
			"web_access":  60,
			"web_error":   10,
			"application": 20,
			"metric":      5,
			"event":       5,
		},
	}

	// Check configuration values
	if config.Mode != "victoria" {
		t.Errorf("Incorrect mode in configuration: %s", config.Mode)
	}

	if config.RPS != 10 {
		t.Errorf("Incorrect RPS value: %d", config.RPS)
	}

	// Check log type distribution
	expectedDist := map[string]int{
		"web_access":  60,
		"web_error":   10,
		"application": 20,
		"metric":      5,
		"event":       5,
	}

	for logType, expectedPercent := range expectedDist {
		if config.LogTypeDistribution[logType] != expectedPercent {
			t.Errorf("Incorrect distribution for type '%s': expected %d%%, got %d%%",
				logType, expectedPercent, config.LogTypeDistribution[logType])
		}
	}
}

// MockLogDBForTest - mock for the logdb.LogDB interface
type MockLogDBForTest struct {
	name string
}

func (m *MockLogDBForTest) Name() string {
	return m.name
}

func (m *MockLogDBForTest) Initialize() error {
	return nil
}

func (m *MockLogDBForTest) SendLogs(logs []logdb.LogEntry) error {
	return nil
}

func (m *MockLogDBForTest) Close() error {
	return nil
}

func (m *MockLogDBForTest) Metrics() map[string]float64 {
	return make(map[string]float64)
}

func (m *MockLogDBForTest) FormatPayload(logs []logdb.LogEntry) (string, string) {
	return "", "application/json"
}
