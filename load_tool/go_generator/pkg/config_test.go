package pkg

import (
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	// Check creation and configuration setup
	config := Config{
		Mode:                "victoria",
		BaseURL:             "http://localhost:8428",
		URL:                 "http://localhost:8428",
		RPS:                 100,
		Duration:            1 * time.Minute,
		BulkSize:            50,
		WorkerCount:         4,
		ConnectionCount:     10,
		LogTypeDistribution: map[string]int{"web_access": 60, "web_error": 10, "application": 20, "metric": 5, "event": 5},
		Verbose:             true,
		MaxRetries:          3,
		RetryDelay:          500 * time.Millisecond,
		EnableMetrics:       true,
		MetricsPort:         9090,
	}

	// Check configuration values
	if config.Mode != "victoria" {
		t.Errorf("Expected Mode to be 'victoria', got '%s'", config.Mode)
	}

	if config.RPS != 100 {
		t.Errorf("Expected RPS to be 100, got %d", config.RPS)
	}

	if config.BulkSize != 50 {
		t.Errorf("Expected BulkSize to be 50, got %d", config.BulkSize)
	}

	if config.WorkerCount != 4 {
		t.Errorf("Expected WorkerCount to be 4, got %d", config.WorkerCount)
	}

	if config.LogTypeDistribution["web_access"] != 60 {
		t.Errorf("Expected LogTypeDistribution['web_access'] to be 60, got %d", config.LogTypeDistribution["web_access"])
	}
}

func TestStats(t *testing.T) {
	// Check statistics initialization and updates
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Check initial values
	if stats.TotalRequests != 0 {
		t.Errorf("Initial TotalRequests should be 0, got %d", stats.TotalRequests)
	}

	if stats.SuccessfulRequests != 0 {
		t.Errorf("Initial SuccessfulRequests should be 0, got %d", stats.SuccessfulRequests)
	}

	if stats.FailedRequests != 0 {
		t.Errorf("Initial FailedRequests should be 0, got %d", stats.FailedRequests)
	}
}
