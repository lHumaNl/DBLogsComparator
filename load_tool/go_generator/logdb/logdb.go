package logdb

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

// LogEntry represents a generic log entry structure
type LogEntry map[string]interface{}

// Options contains parameters for log database initialization
type Options struct {
	BatchSize       int
	Timeout         time.Duration
	RetryCount      int
	RetryDelay      time.Duration
	Verbose         bool
	Debug           bool // Debug mode for request body logging
	ConnectionCount int  // Number of HTTP connections for the pool
}

// LogDB - interface for all log databases
type LogDB interface {
	// Initialize initializes the database connection
	Initialize() error

	// SendLogs sends a batch of logs to the database
	SendLogs(logs []LogEntry) error

	// Close closes the database connection
	Close() error

	// Name returns the database name
	Name() string

	// Metrics returns database operation metrics
	Metrics() map[string]float64

	// FormatPayload formats log entries into the required format for sending
	FormatPayload(logs []LogEntry) (string, string)

	// FormatPayloadWithBuffer formats log entries using provided buffer for efficiency
	FormatPayloadWithBuffer(logs []LogEntry, buf *bytes.Buffer) (string, string)

	// SendLogsWithBuffer sends logs using buffer pool for better performance
	SendLogsWithBuffer(logs []LogEntry, buf *bytes.Buffer) error
}

// BaseLogDB - base structure with common functionality for all log databases
type BaseLogDB struct {
	URL          string
	BatchSize    int
	Timeout      time.Duration
	RetryCount   int
	RetryDelay   time.Duration
	MetricsData  map[string]float64
	metricsMutex sync.RWMutex
	Verbose      bool
	Debug        bool
}

// NewBaseLogDB creates a new BaseLogDB instance
func NewBaseLogDB(url string, options Options) *BaseLogDB {
	return &BaseLogDB{
		URL:         url,
		BatchSize:   options.BatchSize,
		Timeout:     options.Timeout,
		RetryCount:  options.RetryCount,
		RetryDelay:  options.RetryDelay,
		Verbose:     options.Verbose,
		Debug:       options.Debug,
		MetricsData: make(map[string]float64),
	}
}

// Metrics returns database operation metrics
func (db *BaseLogDB) Metrics() map[string]float64 {
	db.metricsMutex.RLock()
	defer db.metricsMutex.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[string]float64)
	for k, v := range db.MetricsData {
		result[k] = v
	}
	return result
}

// UpdateMetric safely updates a metric value
func (db *BaseLogDB) UpdateMetric(key string, value float64) {
	db.metricsMutex.Lock()
	defer db.metricsMutex.Unlock()
	db.MetricsData[key] = value
}

// UpdateMetricWithContext safely updates a metric value with context check
func (db *BaseLogDB) UpdateMetricWithContext(ctx context.Context, key string, value float64) {
	select {
	case <-ctx.Done():
		// Context cancelled, skip metric update
		return
	default:
		db.UpdateMetric(key, value)
	}
}

// IncrementMetric safely increments a metric value
func (db *BaseLogDB) IncrementMetric(key string, delta float64) {
	db.metricsMutex.Lock()
	defer db.metricsMutex.Unlock()
	db.MetricsData[key] += delta
}

// IncrementMetricWithContext safely increments a metric value with context check
func (db *BaseLogDB) IncrementMetricWithContext(ctx context.Context, key string, delta float64) {
	select {
	case <-ctx.Done():
		// Context cancelled, skip metric update
		return
	default:
		db.IncrementMetric(key, delta)
	}
}

// CreateLogDB - factory method for creating LogDB instances
func CreateLogDB(mode string, baseURL string, options Options) (LogDB, error) {
	switch mode {
	case "victoria", "victorialogs":
		return NewVictoriaLogsDB(baseURL, options)
	case "es", "elasticsearch", "elk":
		return NewElasticsearchDB(baseURL, options)
	case "loki":
		return NewLokiDB(baseURL, options)
	default:
		return nil, fmt.Errorf("unsupported log DB type: %s", mode)
	}
}
