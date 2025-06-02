package logdb

import (
	"fmt"
	"time"
)

// LogEntry represents a generic log entry structure
type LogEntry map[string]interface{}

// Options contains parameters for log database initialization
type Options struct {
	BatchSize  int
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
	Verbose    bool
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
}

// BaseLogDB - base structure with common functionality for all log databases
type BaseLogDB struct {
	URL         string
	BatchSize   int
	Timeout     time.Duration
	RetryCount  int
	RetryDelay  time.Duration
	MetricsData map[string]float64
	Verbose     bool
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
		MetricsData: make(map[string]float64),
	}
}

// Metrics returns database operation metrics
func (db *BaseLogDB) Metrics() map[string]float64 {
	return db.MetricsData
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
