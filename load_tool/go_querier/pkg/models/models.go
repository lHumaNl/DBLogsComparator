package models

import (
	"context"
	"time"
)

// QueryType defines the type of query
type QueryType string

const (
	SimpleQuery     QueryType = "simple"     // Simple search by keyword or field
	ComplexQuery    QueryType = "complex"    // Complex search with multiple conditions
	AnalyticalQuery QueryType = "analytical" // Query with aggregations
	TimeSeriesQuery QueryType = "timeseries" // Time series query
)

// QueryResult represents the result of a query execution
type QueryResult struct {
	Duration  time.Duration // Execution time
	HitCount  int           // Number of documents found
	BytesRead int64         // Number of bytes read
	Status    string        // Query status
}

// QueryExecutor interface for executing queries
type QueryExecutor interface {
	// ExecuteQuery executes a query of the specified type and returns the result
	ExecuteQuery(ctx context.Context, queryType QueryType) (QueryResult, error)

	// GenerateRandomQuery creates a random query of the specified type
	GenerateRandomQuery(queryType QueryType) interface{}

	// GetSystemName returns the name of the system
	GetSystemName() string
}

// Options settings for the query executor
type Options struct {
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
	Verbose    bool
}
