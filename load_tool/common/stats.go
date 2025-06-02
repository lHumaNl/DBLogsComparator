package common

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats represents test execution statistics
type Stats struct {
	StartTime          time.Time
	TotalRequests      int64 // Total number of write requests
	SuccessfulRequests int64 // Successful write requests
	FailedRequests     int64 // Failed write requests
	TotalLogs          int64 // Total number of logs sent
	RetriedRequests    int64 // Retried write requests

	// Read request statistics
	TotalQueries      int64 // Total number of read requests
	SuccessfulQueries int64 // Successful read requests
	FailedQueries     int64 // Failed read requests
	RetriedQueries    int64 // Retried read requests

	// Query performance statistics
	QueryLatencySum int64 // Sum of query latencies (in microseconds)
	QueryCount      int64 // Number of queries for calculating average latency

	// Counts by query types
	SimpleQueryCount     int64
	ComplexQueryCount    int64
	AnalyticalQueryCount int64
	TimeSeriesQueryCount int64

	// Query result statistics
	TotalHits      int64 // Total number of documents found
	TotalBytesRead int64 // Total number of bytes read
}

// NewStats creates a new statistics instance
func NewStats() *Stats {
	return &Stats{
		StartTime: time.Now(),
	}
}

// IncrementTotalRequests increments the total write requests counter
func (s *Stats) IncrementTotalRequests() {
	atomic.AddInt64(&s.TotalRequests, 1)
}

// IncrementSuccessfulRequests increments the successful write requests counter
func (s *Stats) IncrementSuccessfulRequests() {
	atomic.AddInt64(&s.SuccessfulRequests, 1)
}

// IncrementFailedRequests increments the failed write requests counter
func (s *Stats) IncrementFailedRequests() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

// AddLogs adds the number of logs sent
func (s *Stats) AddLogs(count int) {
	atomic.AddInt64(&s.TotalLogs, int64(count))
}

// IncrementRetriedRequests increments the retried write requests counter
func (s *Stats) IncrementRetriedRequests() {
	atomic.AddInt64(&s.RetriedRequests, 1)
}

// IncrementTotalQueries increments the total read requests counter
func (s *Stats) IncrementTotalQueries() {
	atomic.AddInt64(&s.TotalQueries, 1)
}

// IncrementSuccessfulQueries increments the successful read requests counter
func (s *Stats) IncrementSuccessfulQueries() {
	atomic.AddInt64(&s.SuccessfulQueries, 1)
}

// IncrementFailedQueries increments the failed read requests counter
func (s *Stats) IncrementFailedQueries() {
	atomic.AddInt64(&s.FailedQueries, 1)
}

// IncrementRetriedQueries increments the retried read requests counter
func (s *Stats) IncrementRetriedQueries() {
	atomic.AddInt64(&s.RetriedQueries, 1)
}

// AddQueryLatency adds the query latency value (in microseconds)
func (s *Stats) AddQueryLatency(latencyMicros int64) {
	atomic.AddInt64(&s.QueryLatencySum, latencyMicros)
	atomic.AddInt64(&s.QueryCount, 1)
}

// IncrementQueryTypeCount increments the counter for a specific query type
func (s *Stats) IncrementQueryTypeCount(queryType string) {
	switch queryType {
	case "simple":
		atomic.AddInt64(&s.SimpleQueryCount, 1)
	case "complex":
		atomic.AddInt64(&s.ComplexQueryCount, 1)
	case "analytical":
		atomic.AddInt64(&s.AnalyticalQueryCount, 1)
	case "timeseries":
		atomic.AddInt64(&s.TimeSeriesQueryCount, 1)
	}
}

// AddHits adds the number of documents found
func (s *Stats) AddHits(count int) {
	atomic.AddInt64(&s.TotalHits, int64(count))
}

// AddBytesRead adds the number of bytes read
func (s *Stats) AddBytesRead(bytes int64) {
	atomic.AddInt64(&s.TotalBytesRead, bytes)
}

// PrintSummary prints a summary of the statistics
func (s *Stats) PrintSummary() {
	elapsed := time.Since(s.StartTime).Seconds()

	fmt.Printf("\n=== Test Results ===\n")
	fmt.Printf("Duration: %.2f seconds\n", elapsed)

	// Write statistics
	if s.TotalRequests > 0 {
		writeRPS := float64(s.TotalRequests) / elapsed
		writeLPS := float64(s.TotalLogs) / elapsed

		fmt.Printf("\n--- Write Statistics ---\n")
		fmt.Printf("Total write requests: %d (%.2f/s)\n", s.TotalRequests, writeRPS)
		fmt.Printf("Total logs sent: %d (%.2f/s)\n", s.TotalLogs, writeLPS)
		fmt.Printf("Successful requests: %d (%.2f%%)\n", s.SuccessfulRequests,
			float64(s.SuccessfulRequests)/float64(s.TotalRequests)*100)
		fmt.Printf("Failed requests: %d (%.2f%%)\n", s.FailedRequests,
			float64(s.FailedRequests)/float64(s.TotalRequests)*100)
		fmt.Printf("Retried requests: %d\n", s.RetriedRequests)
	}

	// Read statistics
	if s.TotalQueries > 0 {
		readQPS := float64(s.TotalQueries) / elapsed
		avgLatencyMs := float64(s.QueryLatencySum) / float64(s.QueryCount) / 1000.0
		avgHitsPerQuery := float64(s.TotalHits) / float64(s.TotalQueries)
		avgBytesPerQuery := float64(s.TotalBytesRead) / float64(s.TotalQueries) / 1024.0 // KB

		fmt.Printf("\n--- Read Statistics ---\n")
		fmt.Printf("Total read requests: %d (%.2f/s)\n", s.TotalQueries, readQPS)
		fmt.Printf("Successful requests: %d (%.2f%%)\n", s.SuccessfulQueries,
			float64(s.SuccessfulQueries)/float64(s.TotalQueries)*100)
		fmt.Printf("Failed requests: %d (%.2f%%)\n", s.FailedQueries,
			float64(s.FailedQueries)/float64(s.TotalQueries)*100)
		fmt.Printf("Retried requests: %d\n", s.RetriedQueries)

		// Query performance
		fmt.Printf("\nAverage query latency: %.2f ms\n", avgLatencyMs)
		fmt.Printf("Average hits per query: %.2f documents/query\n", avgHitsPerQuery)
		fmt.Printf("Average result size: %.2f KB/query\n", avgBytesPerQuery)

		// Query type distribution
		if s.QueryCount > 0 {
			fmt.Printf("\nQuery type distribution:\n")
			fmt.Printf("  Simple: %d (%.2f%%)\n", s.SimpleQueryCount,
				float64(s.SimpleQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Complex: %d (%.2f%%)\n", s.ComplexQueryCount,
				float64(s.ComplexQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Analytical: %d (%.2f%%)\n", s.AnalyticalQueryCount,
				float64(s.AnalyticalQueryCount)/float64(s.QueryCount)*100)
			fmt.Printf("  Time series: %d (%.2f%%)\n", s.TimeSeriesQueryCount,
				float64(s.TimeSeriesQueryCount)/float64(s.QueryCount)*100)
		}
	}

	// Overall statistics
	if s.TotalRequests > 0 && s.TotalQueries > 0 {
		totalOps := s.TotalRequests + s.TotalQueries
		opsPerSec := float64(totalOps) / elapsed

		fmt.Printf("\n--- Overall Statistics ---\n")
		fmt.Printf("Total operations: %d (%.2f/s)\n", totalOps, opsPerSec)
		fmt.Printf("Read/write ratio: %.2f\n",
			float64(s.TotalQueries)/float64(s.TotalRequests))
	}
}
