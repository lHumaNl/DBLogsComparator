package pkg

import (
	"time"
)

// Config - load configuration
type Config struct {
	Mode                string
	BaseURL             string
	URL                 string
	RPS                 int
	Duration            time.Duration
	BulkSize            int
	WorkerCount         int
	ConnectionCount     int
	LogTypeDistribution map[string]int
	Verbose             bool
	MaxRetries          int
	RetryDelay          time.Duration
	EnableMetrics       bool
	MetricsPort         int
}

// Stats - execution statistics
type Stats struct {
	TotalRequests      int64
	SuccessfulRequests int64
	FailedRequests     int64
	TotalLogs          int64
	RetriedRequests    int64
	StartTime          time.Time
}
