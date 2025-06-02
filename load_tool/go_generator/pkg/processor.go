package pkg

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// CreateBulkPayload creates a batch of logs to send to the database
func CreateBulkPayload(config Config, bufferPool *BufferPool) []logdb.LogEntry {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	logs := make([]logdb.LogEntry, 0, config.BulkSize)

	for i := 0; i < config.BulkSize; i++ {
		logType := SelectRandomLogType(config.LogTypeDistribution)
		logObject := GenerateLog(logType, timestamp)

		// Convert to JSON and back to map[string]interface{}
		logJSON, err := json.Marshal(logObject)
		if err != nil {
			continue
		}

		var logEntry logdb.LogEntry
		if err := json.Unmarshal(logJSON, &logEntry); err != nil {
			continue
		}

		logs = append(logs, logEntry)
	}

	return logs
}

// Worker processes tasks from the jobs channel
func Worker(id int, jobs <-chan struct{}, stats *Stats, config Config, db logdb.LogDB, bufferPool *BufferPool, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
		// Create a batch of logs
		logs := CreateBulkPayload(config, bufferPool)

		// Send logs to the database
		startTime := time.Now()
		err := db.SendLogs(logs)
		duration := time.Since(startTime).Seconds()

		// Update metrics and statistics
		if err != nil {
			atomic.AddInt64(&stats.FailedRequests, 1)
			if config.Verbose {
				fmt.Printf("Worker %d: Error sending logs: %v\n", id, err)
			}

			if config.EnableMetrics {
				RequestDuration.WithLabelValues("error", db.Name()).Observe(duration)
				RequestsTotal.WithLabelValues("error", db.Name()).Inc()
			}
		} else {
			atomic.AddInt64(&stats.SuccessfulRequests, 1)
			atomic.AddInt64(&stats.TotalLogs, int64(len(logs)))

			if config.EnableMetrics {
				RequestDuration.WithLabelValues("success", db.Name()).Observe(duration)
				RequestsTotal.WithLabelValues("success", db.Name()).Inc()

				// Increment counters for each log type
				logCounts := make(map[string]int)
				for _, log := range logs {
					if logType, ok := log["log_type"].(string); ok {
						logCounts[logType]++
					}
				}

				for logType, count := range logCounts {
					LogsTotal.WithLabelValues(logType, db.Name()).Add(float64(count))
				}
			}
		}

		atomic.AddInt64(&stats.TotalRequests, 1)
	}
}

// StatsReporter outputs operational statistics
func StatsReporter(stats *Stats, stopChan <-chan struct{}, config Config) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Calculate current values
			elapsed := time.Since(stats.StartTime).Seconds()
			totalReqs := atomic.LoadInt64(&stats.TotalRequests)
			successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
			failedReqs := atomic.LoadInt64(&stats.FailedRequests)
			totalLogs := atomic.LoadInt64(&stats.TotalLogs)
			retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

			currentRPS := float64(totalReqs) / elapsed
			currentLPS := float64(totalLogs) / elapsed

			// Update metrics
			if config.EnableMetrics {
				RPSGauge.Set(currentRPS)
				LPSGauge.Set(currentLPS)
			}

			// Output statistics
			fmt.Printf("\rTime: %.1f s | Requests: %d (%.1f/s) | Logs: %d (%.1f/s) | Success: %.1f%% | Errors: %.1f%% | Retries: %d  ",
				elapsed, totalReqs, currentRPS, totalLogs, currentLPS,
				float64(successReqs)/float64(totalReqs+1)*100,
				float64(failedReqs)/float64(totalReqs+1)*100,
				retriedReqs)

		case <-stopChan:
			return
		}
	}
}

// RunGenerator starts the log generator
func RunGenerator(config Config, db logdb.LogDB) error {
	// Initialize statistics
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Initialize pools
	bufferPool := NewBufferPool()

	// Initialize channels and goroutines
	jobs := make(chan struct{}, config.RPS*2)
	stopChan := make(chan struct{})

	// Start metrics server if enabled
	if config.EnableMetrics {
		StartMetricsServer(config.MetricsPort, config)
		InitPrometheus(config)
	}

	// Start goroutine for displaying statistics
	go StatsReporter(stats, stopChan, config)

	// Start worker goroutines
	var wg sync.WaitGroup
	for w := 1; w <= config.WorkerCount; w++ {
		wg.Add(1)
		go Worker(w, jobs, stats, config, db, bufferPool, &wg)
	}

	// Main load generation loop
	tickInterval := time.Second / time.Duration(config.RPS)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	endTime := time.Now().Add(config.Duration)

	for time.Now().Before(endTime) {
		<-ticker.C
		jobs <- struct{}{}
	}

	close(jobs)
	wg.Wait()
	close(stopChan)

	// Output final statistics
	elapsed := time.Since(stats.StartTime).Seconds()
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
	failedReqs := atomic.LoadInt64(&stats.FailedRequests)
	totalLogs := atomic.LoadInt64(&stats.TotalLogs)
	retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

	currentRPS := float64(totalReqs) / elapsed
	currentLPS := float64(totalLogs) / elapsed

	fmt.Printf("\n\nTest completed!\n")
	fmt.Printf("Duration: %.2f seconds\n", elapsed)
	fmt.Printf("Total requests: %d (%.2f/s)\n", totalReqs, currentRPS)
	fmt.Printf("Total logs: %d (%.2f/s)\n", totalLogs, currentLPS)
	fmt.Printf("Successful requests: %d (%.2f%%)\n", successReqs, float64(successReqs)/float64(totalReqs)*100)
	fmt.Printf("Failed requests: %d (%.2f%%)\n", failedReqs, float64(failedReqs)/float64(totalReqs)*100)
	fmt.Printf("Retry attempts: %d\n", retriedReqs)

	return nil
}
