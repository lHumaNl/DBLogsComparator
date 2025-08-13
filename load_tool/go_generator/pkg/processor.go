package pkg

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// LogGenerationWork represents work item for log generation (single log)
type LogGenerationWork struct {
	Index     int
	LogType   string
	Timestamp string
}

// LogGenerationWorkBatch represents batch work item for multiple logs
// OPTIMIZATION: Workers now process batches instead of individual logs
type LogGenerationWorkBatch struct {
	StartIndex int      // Starting index in the bulk
	EndIndex   int      // Ending index (exclusive)
	LogTypes   []string // Pre-selected log types for this batch
}

// LogGenerationResult represents result of log generation
type LogGenerationResult struct {
	Index    int
	LogEntry logdb.LogEntry
	Success  bool
}

// CreateBulkPayload creates a batch of logs to send to the database using concurrent generation
func CreateBulkPayload(config *GeneratorConfig, bufferPool *BufferPool) []logdb.LogEntry {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	// Always use concurrent generation with goroutines for consistent performance
	// This ensures consistent behavior regardless of bulkSize
	return createBulkPayloadConcurrent(config, timestamp)
}

// CreateBulkPayloadWithContext creates a batch of logs with context cancellation support
func CreateBulkPayloadWithContext(ctx context.Context, config *GeneratorConfig, bufferPool *BufferPool) []logdb.LogEntry {
	// Check context first
	select {
	case <-ctx.Done():
		return []logdb.LogEntry{} // Return empty slice if context cancelled
	default:
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	// Always use concurrent generation with goroutines for consistent performance
	// This ensures consistent behavior regardless of bulkSize
	return createBulkPayloadConcurrentWithContext(ctx, config, timestamp)
}

// Sequential generation functions removed - now using only concurrent generation for consistency

// createBulkPayloadConcurrentWithContext creates logs using FIXED worker pool with context support
// OPTIMIZATION: Now uses CPU*4 workers maximum instead of bulkSize workers
func createBulkPayloadConcurrentWithContext(ctx context.Context, config *GeneratorConfig, timestamp string) []logdb.LogEntry {
	// Check context first
	select {
	case <-ctx.Done():
		return []logdb.LogEntry{} // Return empty slice if context cancelled
	default:
	}

	// FIXED: Use limited worker pool (CPU*4) instead of bulkSize workers
	numWorkers := config.GetLogGenerationWorkers() // Now returns CPU*4 (max 16 minimum)

	// Pre-generate all log types and timestamps to distribute work efficiently
	logTypes := make([]string, config.BulkSize)
	for i := 0; i < config.BulkSize; i++ {
		logTypes[i] = SelectRandomLogType(config.Distribution)
	}

	// Channels for work distribution - work items contain multiple log indices
	workChan := make(chan LogGenerationWorkBatch, numWorkers*2)   // Small buffer for batching
	resultChan := make(chan LogGenerationResult, config.BulkSize) // Results still 1:1 with logs

	// Start FIXED number of worker goroutines with context support
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go logGenerationWorkerBatchWithContext(ctx, workChan, resultChan, timestamp, &wg)
	}

	// Distribute work in batches to workers for better efficiency
	go func() {
		defer close(workChan)

		// Calculate work batch size per worker
		batchSize := config.BulkSize / numWorkers
		remaining := config.BulkSize % numWorkers

		startIdx := 0
		for i := 0; i < numWorkers; i++ {
			// Check context before sending work batch
			select {
			case <-ctx.Done():
				return // Stop sending work if context cancelled
			default:
			}

			// Calculate end index for this worker's batch
			endIdx := startIdx + batchSize
			if i < remaining {
				endIdx++ // Distribute remaining work to first workers
			}

			// Create batch for this worker
			batch := LogGenerationWorkBatch{
				StartIndex: startIdx,
				EndIndex:   endIdx,
				LogTypes:   logTypes[startIdx:endIdx],
			}

			select {
			case workChan <- batch:
				startIdx = endIdx
			case <-ctx.Done():
				return // Stop if context cancelled during send
			}
		}
	}()

	// Collect results with context timeout
	results := make([]LogGenerationResult, 0, config.BulkSize)
	done := make(chan struct{})

	go func() {
		defer close(done)
		wg.Wait()
		close(resultChan)
	}()

	// Collect results with context awareness
	for {
		select {
		case <-ctx.Done():
			// Context cancelled - return partial results
			logs := make([]logdb.LogEntry, len(results))
			for i, result := range results {
				if result.Success {
					logs[i] = result.LogEntry
				}
			}
			return logs
		case <-done:
			// All workers finished - collect remaining results
			for result := range resultChan {
				if result.Success {
					results = append(results, result)
				}
			}
			// Convert results to log entries
			logs := make([]logdb.LogEntry, len(results))
			for i, result := range results {
				logs[i] = result.LogEntry
			}
			return logs
		case result, ok := <-resultChan:
			if !ok {
				// Channel closed - convert results and return
				logs := make([]logdb.LogEntry, len(results))
				for i, result := range results {
					logs[i] = result.LogEntry
				}
				return logs
			}
			if result.Success {
				results = append(results, result)
			}
		}
	}
}

// logGenerationWorkerBatchWithContext processes batches of log generation work with context support
// OPTIMIZATION: Each worker processes multiple logs in a batch for better efficiency
func logGenerationWorkerBatchWithContext(ctx context.Context, workChan <-chan LogGenerationWorkBatch, resultChan chan<- LogGenerationResult, timestamp string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return // Stop worker if context cancelled
		case batch, ok := <-workChan:
			if !ok {
				return // Channel closed
			}

			// Process all logs in the batch
			for i, logType := range batch.LogTypes {
				// Check context before processing each log
				select {
				case <-ctx.Done():
					return
				default:
				}

				actualIndex := batch.StartIndex + i
				logObject := GenerateLog(logType, timestamp)

				// Direct conversion without JSON marshaling - eliminates double allocation
				logEntry, err := convertToLogEntry(logObject)
				if err != nil {
					select {
					case resultChan <- LogGenerationResult{
						Index:   actualIndex,
						Success: false,
					}:
					case <-ctx.Done():
						return
					}
					continue
				}

				select {
				case resultChan <- LogGenerationResult{
					Index:    actualIndex,
					LogEntry: logEntry,
					Success:  true,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// logGenerationWorkerWithContext - DEPRECATED: Replaced with batch processing
// Keep for backward compatibility
func logGenerationWorkerWithContext(ctx context.Context, workChan <-chan LogGenerationWork, resultChan chan<- LogGenerationResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return // Stop worker if context cancelled
		case work, ok := <-workChan:
			if !ok {
				return // Channel closed
			}

			logObject := GenerateLog(work.LogType, work.Timestamp)

			// Direct conversion without JSON marshaling - eliminates double allocation
			logEntry, err := convertToLogEntry(logObject)
			if err != nil {
				select {
				case resultChan <- LogGenerationResult{
					Index:   work.Index,
					Success: false,
				}:
				case <-ctx.Done():
					return
				}
				continue
			}

			select {
			case resultChan <- LogGenerationResult{
				Index:    work.Index,
				LogEntry: logEntry,
				Success:  true,
			}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Sequential generation removed - using only concurrent generation

// createBulkPayloadConcurrent creates logs using FIXED worker pool for concurrent generation
// OPTIMIZATION: Now uses CPU*4 workers maximum with batch processing
func createBulkPayloadConcurrent(config *GeneratorConfig, timestamp string) []logdb.LogEntry {
	// FIXED: Use limited worker pool (CPU*4) instead of bulkSize workers
	numWorkers := config.GetLogGenerationWorkers() // Now returns CPU*4

	// Pre-generate all log types to distribute work efficiently
	logTypes := make([]string, config.BulkSize)
	for i := 0; i < config.BulkSize; i++ {
		logTypes[i] = SelectRandomLogType(config.Distribution)
	}

	// Channels for work distribution - work items contain multiple log indices
	workChan := make(chan LogGenerationWorkBatch, numWorkers*2)   // Small buffer for batching
	resultChan := make(chan LogGenerationResult, config.BulkSize) // Results still 1:1 with logs

	// Start FIXED number of worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go logGenerationWorkerBatch(workChan, resultChan, timestamp, &wg)
	}

	// Distribute work in batches to workers
	go func() {
		defer close(workChan)

		// Calculate work batch size per worker
		batchSize := config.BulkSize / numWorkers
		remaining := config.BulkSize % numWorkers

		startIdx := 0
		for i := 0; i < numWorkers; i++ {
			// Calculate end index for this worker's batch
			endIdx := startIdx + batchSize
			if i < remaining {
				endIdx++ // Distribute remaining work to first workers
			}

			// Create batch for this worker
			batch := LogGenerationWorkBatch{
				StartIndex: startIdx,
				EndIndex:   endIdx,
				LogTypes:   logTypes[startIdx:endIdx],
			}

			workChan <- batch
			startIdx = endIdx
		}
	}()

	// Collect results
	results := make([]LogGenerationResult, 0, config.BulkSize)
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		if result.Success {
			results = append(results, result)
		}
	}

	// Convert results to log entries (order doesn't matter for bulk operations)
	logs := make([]logdb.LogEntry, len(results))
	for i, result := range results {
		logs[i] = result.LogEntry
	}

	return logs
}

// convertToLogEntry converts a log struct directly to LogEntry without reflection
// This optimized version is 10-20x faster than reflection-based approach
func convertToLogEntry(logObject interface{}) (logdb.LogEntry, error) {
	// Handle already converted maps directly
	if mapVal, ok := logObject.(map[string]interface{}); ok {
		return mapVal, nil
	}

	// Direct type conversion - much faster than reflection
	switch log := logObject.(type) {
	case *WebAccessLog:
		return convertWebAccessLog(log), nil
	case WebAccessLog:
		return convertWebAccessLog(&log), nil

	case *WebErrorLog:
		return convertWebErrorLog(log), nil
	case WebErrorLog:
		return convertWebErrorLog(&log), nil

	case *ApplicationLog:
		return convertApplicationLog(log), nil
	case ApplicationLog:
		return convertApplicationLog(&log), nil

	case *MetricLog:
		return convertMetricLog(log), nil
	case MetricLog:
		return convertMetricLog(&log), nil

	case *EventLog:
		return convertEventLog(log), nil
	case EventLog:
		return convertEventLog(&log), nil

	default:
		return nil, fmt.Errorf("unsupported log type: %T", logObject)
	}
}

// convertWebAccessLog converts WebAccessLog to LogEntry with proper field flattening
func convertWebAccessLog(log *WebAccessLog) logdb.LogEntry {
	entry := make(logdb.LogEntry, 16) // Pre-allocate with expected capacity

	// BaseLog fields (embedded struct flattening)
	entry["timestamp"] = log.Timestamp
	entry["log_type"] = log.LogType
	entry["host"] = log.Host
	entry["container_name"] = log.ContainerName

	// BaseLog omitempty fields
	if log.Environment != "" {
		entry["environment"] = log.Environment
	}
	if log.DataCenter != "" {
		entry["datacenter"] = log.DataCenter
	}
	if log.Version != "" {
		entry["version"] = log.Version
	}
	if log.GitCommit != "" {
		entry["git_commit"] = log.GitCommit
	}

	// WebAccessLog specific fields
	entry["remote_addr"] = log.RemoteAddr
	entry["request"] = log.Request
	entry["status"] = log.Status
	entry["bytes_sent"] = log.BytesSent
	entry["http_referer"] = log.HttpReferer
	entry["http_user_agent"] = log.HttpUserAgent
	entry["request_time"] = log.RequestTime
	entry["message"] = log.Message

	return entry
}

// convertWebErrorLog converts WebErrorLog to LogEntry with proper field flattening
func convertWebErrorLog(log *WebErrorLog) logdb.LogEntry {
	entry := make(logdb.LogEntry, 20) // Pre-allocate with expected capacity

	// BaseLog fields (embedded struct flattening)
	entry["timestamp"] = log.Timestamp
	entry["log_type"] = log.LogType
	entry["host"] = log.Host
	entry["container_name"] = log.ContainerName

	// BaseLog omitempty fields
	if log.Environment != "" {
		entry["environment"] = log.Environment
	}
	if log.DataCenter != "" {
		entry["datacenter"] = log.DataCenter
	}
	if log.Version != "" {
		entry["version"] = log.Version
	}
	if log.GitCommit != "" {
		entry["git_commit"] = log.GitCommit
	}

	// WebErrorLog specific fields
	entry["level"] = log.Level
	entry["message"] = log.Message
	entry["error_code"] = log.ErrorCode
	entry["service"] = log.Service

	// WebErrorLog omitempty fields
	if log.Exception != "" {
		entry["exception"] = log.Exception
	}
	if log.Stacktrace != "" {
		entry["stacktrace"] = log.Stacktrace
	}
	if log.RequestID != "" {
		entry["request_id"] = log.RequestID
	}
	if log.RequestPath != "" {
		entry["request_path"] = log.RequestPath
	}
	if log.ClientIP != "" {
		entry["client_ip"] = log.ClientIP
	}
	if log.Duration != 0 {
		entry["duration"] = log.Duration
	}
	if log.RetryCount != 0 {
		entry["retry_count"] = log.RetryCount
	}
	if len(log.Tags) > 0 {
		entry["tags"] = log.Tags
	}
	if len(log.Context) > 0 {
		entry["context"] = log.Context
	}

	return entry
}

// convertApplicationLog converts ApplicationLog to LogEntry with proper field flattening
func convertApplicationLog(log *ApplicationLog) logdb.LogEntry {
	entry := make(logdb.LogEntry, 24) // Pre-allocate with expected capacity

	// BaseLog fields (embedded struct flattening)
	entry["timestamp"] = log.Timestamp
	entry["log_type"] = log.LogType
	entry["host"] = log.Host
	entry["container_name"] = log.ContainerName

	// BaseLog omitempty fields
	if log.Environment != "" {
		entry["environment"] = log.Environment
	}
	if log.DataCenter != "" {
		entry["datacenter"] = log.DataCenter
	}
	if log.Version != "" {
		entry["version"] = log.Version
	}
	if log.GitCommit != "" {
		entry["git_commit"] = log.GitCommit
	}

	// ApplicationLog specific fields
	entry["level"] = log.Level
	entry["message"] = log.Message
	entry["service"] = log.Service
	entry["trace_id"] = log.TraceID
	entry["span_id"] = log.SpanID
	entry["request_method"] = log.RequestMethod
	entry["request_path"] = log.RequestPath
	entry["response_status"] = log.ResponseStatus
	entry["response_time"] = log.ResponseTime

	// ApplicationLog omitempty fields
	if len(log.RequestParams) > 0 {
		entry["request_params"] = log.RequestParams
	}
	if log.Exception != "" {
		entry["exception"] = log.Exception
	}
	if log.Stacktrace != "" {
		entry["stacktrace"] = log.Stacktrace
	}
	if log.UserID != "" {
		entry["user_id"] = log.UserID
	}
	if log.SessionID != "" {
		entry["session_id"] = log.SessionID
	}
	if len(log.Dependencies) > 0 {
		entry["dependencies"] = log.Dependencies
	}
	if log.Memory != 0 {
		entry["memory"] = log.Memory
	}
	if log.CPU != 0 {
		entry["cpu"] = log.CPU
	}

	return entry
}

// convertMetricLog converts MetricLog to LogEntry with proper field flattening
func convertMetricLog(log *MetricLog) logdb.LogEntry {
	entry := make(logdb.LogEntry, 12) // Pre-allocate with expected capacity

	// BaseLog fields (embedded struct flattening)
	entry["timestamp"] = log.Timestamp
	entry["log_type"] = log.LogType
	entry["host"] = log.Host
	entry["container_name"] = log.ContainerName

	// BaseLog omitempty fields
	if log.Environment != "" {
		entry["environment"] = log.Environment
	}
	if log.DataCenter != "" {
		entry["datacenter"] = log.DataCenter
	}
	if log.Version != "" {
		entry["version"] = log.Version
	}
	if log.GitCommit != "" {
		entry["git_commit"] = log.GitCommit
	}

	// MetricLog specific fields
	entry["metric_name"] = log.MetricName
	entry["value"] = log.Value
	entry["service"] = log.Service
	entry["region"] = log.Region
	entry["message"] = log.Message

	return entry
}

// convertEventLog converts EventLog to LogEntry with proper field flattening
func convertEventLog(log *EventLog) logdb.LogEntry {
	entry := make(logdb.LogEntry, 14) // Pre-allocate with expected capacity

	// BaseLog fields (embedded struct flattening)
	entry["timestamp"] = log.Timestamp
	entry["log_type"] = log.LogType
	entry["host"] = log.Host
	entry["container_name"] = log.ContainerName

	// BaseLog omitempty fields
	if log.Environment != "" {
		entry["environment"] = log.Environment
	}
	if log.DataCenter != "" {
		entry["datacenter"] = log.DataCenter
	}
	if log.Version != "" {
		entry["version"] = log.Version
	}
	if log.GitCommit != "" {
		entry["git_commit"] = log.GitCommit
	}

	// EventLog specific fields
	entry["event_type"] = log.EventType
	entry["message"] = log.Message
	entry["resource_id"] = log.ResourceID
	entry["namespace"] = log.Namespace
	entry["service"] = log.Service

	return entry
}

// logGenerationWorkerBatch processes batches of log generation work
// OPTIMIZATION: Each worker processes multiple logs in a batch for better efficiency
func logGenerationWorkerBatch(workChan <-chan LogGenerationWorkBatch, resultChan chan<- LogGenerationResult, timestamp string, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range workChan {
		// Process all logs in the batch
		for i, logType := range batch.LogTypes {
			actualIndex := batch.StartIndex + i
			logObject := GenerateLog(logType, timestamp)

			// Direct conversion without JSON marshaling - eliminates double allocation
			logEntry, err := convertToLogEntry(logObject)
			if err != nil {
				resultChan <- LogGenerationResult{
					Index:   actualIndex,
					Success: false,
				}
				continue
			}

			resultChan <- LogGenerationResult{
				Index:    actualIndex,
				LogEntry: logEntry,
				Success:  true,
			}
		}
	}
}

// logGenerationWorker - DEPRECATED: Replaced with batch processing
// Keep for backward compatibility
func logGenerationWorker(workChan <-chan LogGenerationWork, resultChan chan<- LogGenerationResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for work := range workChan {
		logObject := GenerateLog(work.LogType, work.Timestamp)

		// Direct conversion without JSON marshaling - eliminates double allocation
		logEntry, err := convertToLogEntry(logObject)
		if err != nil {
			resultChan <- LogGenerationResult{
				Index:   work.Index,
				Success: false,
			}
			continue
		}

		resultChan <- LogGenerationResult{
			Index:    work.Index,
			LogEntry: logEntry,
			Success:  true,
		}
	}
}

// Worker processes tasks from the jobs channel
func Worker(id int, jobs <-chan struct{}, stats *Stats, config *GeneratorConfig, db logdb.LogDB, bufferPool *BufferPool, wg *sync.WaitGroup) {
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
			if config.IsVerbose() {
				fmt.Printf("Worker %d: Error sending logs: %v\n", id, err)
			}

			if config.EnableMetrics {
				common.ObserveWriteDuration(db.Name(), "error", duration)
				// Count failures for each log type in the batch
				logCounts := make(map[string]int)
				for _, log := range logs {
					if logType, ok := log["log_type"].(string); ok {
						logCounts[logType]++
					}
				}
				for logType := range logCounts {
					common.IncrementFailedWrite(db.Name(), logType, "send_error")
				}
			}
		} else {
			atomic.AddInt64(&stats.SuccessfulRequests, 1)
			atomic.AddInt64(&stats.TotalLogs, int64(len(logs)))

			if config.EnableMetrics {
				common.ObserveWriteDuration(db.Name(), "success", duration)

				// Instead of using "bulk" type, we use a special function to account for
				// batch operations, which also increments the atomic writeRequestsCount counter for RPS calculation
				common.IncrementWriteOperation(db.Name())

				// Increment counters for each log type
				logCounts := make(map[string]int)
				for _, log := range logs {
					if logType, ok := log["log_type"].(string); ok {
						logCounts[logType]++

						// Update the log counter by types in stats using thread-safe method
						stats.IncrementLogType(logType)
					}
				}

				for logType, count := range logCounts {
					common.WriteLogsTotal.WithLabelValues(logType, db.Name()).Add(float64(count))
					// Count success for each log type
				}
				common.IncrementSuccessfulWrite(db.Name())
			}
		}

		atomic.AddInt64(&stats.TotalRequests, 1)
	}
}

// StatsReporter periodically outputs execution statistics
func StatsReporter(stats *Stats, stopChan <-chan struct{}, config *GeneratorConfig, dbName string) {
	// Create ticker for periodic statistics output
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Save previous values for delta calculation
	lastTotalRequests := int64(0)
	lastTotalLogs := int64(0)
	lastTime := time.Now()
	lastLogsByType := make(map[string]int64) // For storing previous values by log types

	// Pre-initialize lastLogsByType for all log types
	for logType := range config.Distribution {
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

			if config.IsVerbose() {
				fmt.Printf("\n=== Final Statistics ===\n")
				fmt.Printf("Total requests: %d\n", stats.TotalRequests)
				fmt.Printf("Successful requests: %d (%.2f%%)\n", stats.SuccessfulRequests, successRate)
				fmt.Printf("Failed requests: %d\n", stats.FailedRequests)
				fmt.Printf("Retried requests: %d\n", stats.RetriedRequests)
				fmt.Printf("Total logs sent: %d\n", stats.TotalLogs)
				fmt.Printf("Average RPS: %.2f\n", float64(stats.TotalRequests)/elapsed)
				fmt.Printf("Average LPS: %.2f\n", float64(stats.TotalLogs)/elapsed)
			}

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

			if config.IsVerbose() {
				fmt.Printf("\rRPS: %.2f | LPS: %.2f | Success: %d | Failed: %d | Retried: %d | Total logs: %d",
					rps, lps, stats.SuccessfulRequests, stats.FailedRequests, stats.RetriedRequests, stats.TotalLogs)
			}

			if config.EnableMetrics {
				// Update RPS and LPS metrics
				// Here we set values obtained from delta calculation for the last period
				common.GeneratorThroughput.WithLabelValues(dbName, "all").Set(lps)

				// Update metrics by log types using thread-safe method
				logsByType := stats.GetLogsByTypeCopy()
				for logType, count := range logsByType {
					deltaLogType := count - lastLogsByType[logType]
					lpsLogType := float64(deltaLogType) / elapsed

					// Set throughput for each log type
					common.GeneratorThroughput.WithLabelValues(dbName, logType).Set(lpsLogType)
					lastLogsByType[logType] = count
				}
			}

			// Save current values for the next cycle
			lastTotalRequests = totalRequests
			lastTotalLogs = totalLogs
		}
	}
}

// RunGenerator starts the optimized log generator (replaces old implementation)
func RunGenerator(config *GeneratorConfig, db logdb.LogDB) error {
	return RunGeneratorOptimized(config, db)
}

// RunGeneratorWithContext starts the optimized log generator with context support (replaces old implementation)
func RunGeneratorWithContext(ctx context.Context, config *GeneratorConfig, db logdb.LogDB) error {
	return RunGeneratorOptimizedWithContext(ctx, config, db)
}
