package pkg

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
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
				common.ObserveWriteDuration(db.Name(), "error", duration)
				common.IncrementFailedWrite(db.Name(), "send_error")
			}
		} else {
			atomic.AddInt64(&stats.SuccessfulRequests, 1)
			atomic.AddInt64(&stats.TotalLogs, int64(len(logs)))

			if config.EnableMetrics {
				common.ObserveWriteDuration(db.Name(), "success", duration)
				common.IncrementSuccessfulWrite(db.Name())

				// Instead of using "bulk" type, we use a special function to account for
				// batch operations, which also increments the atomic writeRequestsCount counter for RPS calculation
				common.IncrementWriteOperation(db.Name())

				// Increment counters for each log type
				logCounts := make(map[string]int)
				for _, log := range logs {
					if logType, ok := log["log_type"].(string); ok {
						logCounts[logType]++

						// Update the log counter by types in stats using mutex
						stats.LogsByTypeMutex.Lock()
						stats.LogsByType[logType]++
						stats.LogsByTypeMutex.Unlock()
					}
				}

				for logType, count := range logCounts {
					common.WriteLogsTotal.WithLabelValues(logType, db.Name()).Add(float64(count))
				}
			}
		}

		atomic.AddInt64(&stats.TotalRequests, 1)
	}
}

// StatsReporter periodically outputs execution statistics
func StatsReporter(stats *Stats, stopChan <-chan struct{}, config Config, dbName string) {
	// Create ticker for periodic statistics output
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Save previous values for delta calculation
	lastTotalRequests := int64(0)
	lastTotalLogs := int64(0)
	lastTime := time.Now()
	lastLogsByType := make(map[string]int64) // For storing previous values by log types

	// Pre-initialize lastLogsByType for all log types
	for logType := range config.LogTypeDistribution {
		lastLogsByType[logType] = 0
	}

	for {
		select {
		case <-stopChan:
			// Output final statistics
			elapsed := time.Since(stats.StartTime).Seconds()
			successRate := 0.0
			if stats.TotalRequests > 0 {
				successRate = float64(stats.SuccessfulRequests) / float64(stats.TotalRequests) * 100
			}

			fmt.Printf("\n=== Final Statistics ===\n")
			fmt.Printf("Total requests: %d\n", stats.TotalRequests)
			fmt.Printf("Successful requests: %d (%.2f%%)\n", stats.SuccessfulRequests, successRate)
			fmt.Printf("Failed requests: %d\n", stats.FailedRequests)
			fmt.Printf("Retried requests: %d\n", stats.RetriedRequests)
			fmt.Printf("Total logs sent: %d\n", stats.TotalLogs)
			fmt.Printf("Average RPS: %.2f\n", float64(stats.TotalRequests)/elapsed)
			fmt.Printf("Average LPS: %.2f\n", float64(stats.TotalLogs)/elapsed)

			// Output statistics by log types
			stats.LogsByTypeMutex.RLock()
			fmt.Printf("Logs by type: %v\n", stats.LogsByType)
			stats.LogsByTypeMutex.RUnlock()

			return

		case <-ticker.C:
			// Calculate RPS and LPS (requests and logs per second)
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			lastTime = now

			// Get current values from atomic counters
			totalRequests := atomic.LoadInt64(&stats.TotalRequests)
			totalLogs := atomic.LoadInt64(&stats.TotalLogs)

			// Calculate delta for total requests and logs
			deltaRequests := totalRequests - lastTotalRequests
			deltaLogs := totalLogs - lastTotalLogs

			// Calculate RPS and LPS
			rps := float64(deltaRequests) / elapsed
			lps := float64(deltaLogs) / elapsed

			fmt.Printf("\rRPS: %.2f | LPS: %.2f | Success: %d | Failed: %d | Retried: %d | Total logs: %d",
				rps, lps, stats.SuccessfulRequests, stats.FailedRequests, stats.RetriedRequests, stats.TotalLogs)

			if config.EnableMetrics {
				// Update RPS and LPS metrics
				// Here we set values obtained from delta calculation for the last period
				common.GeneratorThroughput.WithLabelValues(dbName, "all").Set(lps)

				// Update metrics by log types
				stats.LogsByTypeMutex.RLock()
				for logType, count := range stats.LogsByType {
					deltaLogType := count - lastLogsByType[logType]
					lpsLogType := float64(deltaLogType) / elapsed

					// Set throughput for each log type
					common.GeneratorThroughput.WithLabelValues(dbName, logType).Set(lpsLogType)
					lastLogsByType[logType] = count
				}
				stats.LogsByTypeMutex.RUnlock()
			}

			// Save current values for the next cycle
			lastTotalRequests = totalRequests
			lastTotalLogs = totalLogs
		}
	}
}

// RunGenerator starts the log generator
func RunGenerator(config Config, db logdb.LogDB) error {
	// Initialize statistics
	stats := &Stats{
		StartTime:  time.Now(),
		LogsByType: make(map[string]int64), // Initialize LogsByType map
	}

	// Pre-initialize counters for all log types
	for logType := range config.LogTypeDistribution {
		stats.LogsByType[logType] = 0
	}

	// Initialize pools
	bufferPool := NewBufferPool()

	// Initialize channels and goroutines
	jobs := make(chan struct{}, config.RPS*2)
	stopChan := make(chan struct{})

	// Start metrics server if enabled
	if config.EnableMetrics {
		common.InitPrometheus(config.BulkSize)

		// Initialize metrics for each log type
		for logType := range config.LogTypeDistribution {
			// Pre-register metrics for all log types with zero values
			common.GeneratorThroughput.WithLabelValues(db.Name(), logType).Set(0)
			common.WriteLogsTotal.WithLabelValues(logType, db.Name()).Add(0)
		}

		// Register metrics for write operations and requests
		common.OperationCounter.WithLabelValues("write", db.Name()).Add(0)
		// Set initial RPS values
		common.CurrentRPS.WithLabelValues("generator").Set(0)
	}

	// Start goroutine for displaying statistics
	go StatsReporter(stats, stopChan, config, db.Name())

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
