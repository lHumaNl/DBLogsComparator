package pkg

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// OptimizedWorker represents a single worker with idle state tracking
type OptimizedWorker struct {
	id         int
	idle       int64 // atomic flag: 1 = idle, 0 = working
	requests   chan struct{}
	ctx        context.Context
	wg         *sync.WaitGroup
	config     *GeneratorConfig
	db         logdb.LogDB
	stats      *Stats
	bufferPool *BufferPool
	retryPool  *RetryPool
}

// OptimizedWorkerPool manages the main worker pool with fixed size
type OptimizedWorkerPool struct {
	workers      []*OptimizedWorker
	requestChan  chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	retryPool    *RetryPool
	numWorkers   int
	shutdownOnce sync.Once
}

// NewOptimizedWorkerPool creates a new optimized worker pool with fixed size
func NewOptimizedWorkerPool(ctx context.Context, config *GeneratorConfig, db logdb.LogDB, stats *Stats, bufferPool *BufferPool) *OptimizedWorkerPool {
	// Worker count based on architecture requirements: CPU cores * 4
	cpuCores := runtime.NumCPU()
	numWorkers := cpuCores * 4

	// Minimum 16 workers as per architecture spec, regardless of load
	if numWorkers < 16 {
		numWorkers = 16
	}

	poolCtx, cancel := context.WithCancel(ctx)

	// Create retry pool (separate from main workers)
	retryWorkers := runtime.NumCPU() // Retry workers = CPU count
	if retryWorkers < 2 {
		retryWorkers = 2
	}
	retryPool := NewRetryPool(poolCtx, retryWorkers)

	// Channel size for backpressure protection
	chanSize := int(config.GetRPS() * 10)
	if chanSize < 100 {
		chanSize = 100
	}
	if chanSize > 10000 {
		chanSize = 10000
	}

	pool := &OptimizedWorkerPool{
		workers:     make([]*OptimizedWorker, numWorkers),
		requestChan: make(chan struct{}, chanSize),
		ctx:         poolCtx,
		cancel:      cancel,
		retryPool:   retryPool,
		numWorkers:  numWorkers,
	}

	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		worker := &OptimizedWorker{
			id:         i,
			idle:       1, // Start in idle state
			requests:   pool.requestChan,
			ctx:        poolCtx,
			wg:         &pool.wg,
			config:     config,
			db:         db,
			stats:      stats,
			bufferPool: bufferPool,
			retryPool:  retryPool,
		}
		pool.workers[i] = worker

		pool.wg.Add(1)
		go worker.run()
	}

	return pool
}

// SubmitRequest submits a request to the worker pool (non-blocking)
func (wp *OptimizedWorkerPool) SubmitRequest() bool {
	select {
	case wp.requestChan <- struct{}{}:
		return true
	default:
		return false // Channel full, backpressure applied
	}
}

// GetIdleWorkerCount returns the number of idle main workers
func (wp *OptimizedWorkerPool) GetIdleWorkerCount() int {
	idle := 0
	for _, worker := range wp.workers {
		if atomic.LoadInt64(&worker.idle) == 1 {
			idle++
		}
	}
	return idle
}

// GetStats returns worker pool statistics
func (wp *OptimizedWorkerPool) GetStats() (mainTotal, mainIdle, retryTotal, retryIdle int) {
	mainTotal = wp.numWorkers
	mainIdle = wp.GetIdleWorkerCount()
	retryTotal, retryIdle, _ = wp.retryPool.GetStats()
	return
}

// Shutdown gracefully stops all workers
func (wp *OptimizedWorkerPool) Shutdown(timeout time.Duration) {
	wp.shutdownOnce.Do(func() {
		// Signal all workers to stop
		wp.cancel()

		// IMPORTANT: Close request channel AFTER cancelling context
		// This ensures workers see context cancellation first
		time.Sleep(100 * time.Millisecond) // Give workers time to see context cancellation
		close(wp.requestChan)

		// Wait for main workers with timeout
		done := make(chan struct{})
		go func() {
			wp.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Main workers shutdown completed gracefully
		case <-time.After(timeout):
			// Main workers shutdown timeout
		}

		// Shutdown retry pool
		wp.retryPool.Shutdown(timeout)
	})
}

// run is the main loop for workers with proper idle behavior
func (w *OptimizedWorker) run() {
	defer w.wg.Done()

	for {
		// Start in idle state, blocking on channel
		atomic.StoreInt64(&w.idle, 1)

		// CRITICAL FIX: Check context cancellation BEFORE blocking on channel
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// Now safely block on channel with context awareness
		select {
		case <-w.ctx.Done():
			return
		case _, ok := <-w.requests:
			if !ok {
				return
			}

			// Switch to working state
			atomic.StoreInt64(&w.idle, 0)

			// Process the request with context awareness
			w.processRequest()

			// CRITICAL FIX: Don't check context after processing!
			// Let the worker return to the top of the loop where it will:
			// 1. Set idle=1 (this is what we want to see in metrics)
			// 2. Then check context cancellation properly
			// This ensures correct idle state reporting before shutdown
		}
	}
}

// processRequest handles the main request processing with optimized retry handling
func (w *OptimizedWorker) processRequest() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[ERROR] Main worker %d panic: %v\n", w.id, r)
			atomic.AddInt64(&w.stats.FailedRequests, 1)
			atomic.AddInt64(&w.stats.TotalRequests, 1)
		}
	}()

	// Create batch of logs with context support
	logs := CreateBulkPayloadWithContext(w.ctx, w.config, w.bufferPool)

	if len(logs) == 0 {
		return
	}

	// First attempt - try to send directly
	startTime := time.Now()
	var err error

	// Use buffered sending if available
	buf := w.bufferPool.GetLarge()
	defer w.bufferPool.Put(buf)

	// Try context-aware methods first
	if contextDB, ok := w.db.(interface {
		SendLogsWithContext(context.Context, []logdb.LogEntry) error
	}); ok {
		err = contextDB.SendLogsWithContext(w.ctx, logs)
	} else if bufferDB, ok := w.db.(interface {
		SendLogsWithBuffer([]logdb.LogEntry, *bytes.Buffer) error
	}); ok {
		// Use optimized buffer method with context timeout
		done := make(chan error, 1)
		go func() {
			err := bufferDB.SendLogsWithBuffer(logs, buf)
			done <- err
		}()

		select {
		case err = <-done:
		case <-w.ctx.Done():
			err = fmt.Errorf("SendLogsWithBuffer cancelled by context")
			// Don't return - let function complete normally so worker can return to idle state
		case <-time.After(20 * time.Second):
			err = fmt.Errorf("SendLogsWithBuffer timeout")
		}
	} else {
		// Fallback to standard method with context timeout
		done := make(chan error, 1)
		go func() {
			err := w.db.SendLogs(logs)
			done <- err
		}()

		select {
		case err = <-done:
		case <-w.ctx.Done():
			err = fmt.Errorf("SendLogs cancelled by context")
			// Don't return - let function complete normally so worker can return to idle state
		case <-time.After(20 * time.Second):
			err = fmt.Errorf("SendLogs timeout")
		}
	}

	duration := time.Since(startTime).Seconds()

	if err == nil {
		// Success - update metrics immediately
		atomic.AddInt64(&w.stats.SuccessfulRequests, 1)
		atomic.AddInt64(&w.stats.TotalLogs, int64(len(logs)))
		atomic.AddInt64(&w.stats.TotalRequests, 1)

		if w.config.EnableMetrics {
			common.ObserveWriteDuration(w.db.Name(), "success", duration)
			common.IncrementWriteOperation(w.db.Name())

			// Update log counters by type
			logCounts := make(map[string]int)
			for _, log := range logs {
				if logType, ok := log["log_type"].(string); ok {
					logCounts[logType]++
					w.stats.IncrementLogType(logType)
				}
			}

			for logType, count := range logCounts {
				common.WriteLogsTotal.WithLabelValues(logType, w.db.Name()).Add(float64(count))
			}
			common.IncrementSuccessfulWrite(w.db.Name())
		}

	} else {
		// First attempt failed - ALWAYS record failure metrics immediately
		atomic.AddInt64(&w.stats.FailedRequests, 1)
		atomic.AddInt64(&w.stats.TotalRequests, 1)

		if w.config.EnableMetrics {
			common.ObserveWriteDuration(w.db.Name(), "error", duration)

			// Record failure metrics for each log type in the batch
			logCounts := make(map[string]int)
			for _, log := range logs {
				if logType, ok := log["log_type"].(string); ok {
					logCounts[logType]++
				}
			}
			for logType := range logCounts {
				common.IncrementFailedWrite(w.db.Name(), logType, "send_error")
			}
		}

		// Then try retry if configured
		if w.config.MaxRetries > 0 {
			retryReq := RetryRequest{
				Logs:         logs,
				Attempt:      1,
				MaxRetries:   w.config.MaxRetries,
				RetryDelay:   w.config.RetryDelay(),
				DB:           w.db,
				Stats:        w.stats,
				Config:       w.config,
				BufferPool:   w.bufferPool,
				ProcessorID:  w.id,
				ResponseChan: nil, // Fire and forget for now
			}

			submitted := w.retryPool.SubmitRetry(retryReq)
			if submitted {
				// Successfully submitted for retry - record retry metric
				if w.config.EnableMetrics {
					common.WriteRequestsRetried.WithLabelValues(w.db.Name(), "1").Inc()
				}
			} else {
				// Retry pool full - log additional error
				payload, _ := w.db.FormatPayload(logs)
				retryErr := fmt.Errorf("retry pool full, original error: %v", err)
				common.LogGeneratorError(w.id, w.db.Name(), retryErr, payload)
			}
		} else {
			// No retries configured - log error to file
			payload, _ := w.db.FormatPayload(logs)
			common.LogGeneratorError(w.id, w.db.Name(), err, payload)
		}
	}
}

// recordFailure records a final failure with proper metrics
func (w *OptimizedWorker) recordFailure(logs []logdb.LogEntry, err error) {
	atomic.AddInt64(&w.stats.FailedRequests, 1)
	atomic.AddInt64(&w.stats.TotalRequests, 1)

	if w.config.EnableMetrics {
		// Count failures for each log type in the batch
		logCounts := make(map[string]int)
		for _, log := range logs {
			if logType, ok := log["log_type"].(string); ok {
				logCounts[logType]++
			}
		}
		for logType := range logCounts {
			common.IncrementFailedWrite(w.db.Name(), logType, "send_error")
		}
	}

	// Log error to file
	payload, _ := w.db.FormatPayload(logs)
	common.LogGeneratorError(w.id, w.db.Name(), err, payload)
}

// RunGeneratorOptimized starts the optimized log generator with smart timing
func RunGeneratorOptimized(config *GeneratorConfig, db logdb.LogDB) error {
	return RunGeneratorOptimizedWithContext(context.Background(), config, db)
}

// RunGeneratorOptimizedWithContext starts the optimized generator with smart RPS timing
func RunGeneratorOptimizedWithContext(ctx context.Context, config *GeneratorConfig, db logdb.LogDB) error {

	// Initialize statistics (StartTime will be set later)
	stats := &Stats{}

	// Initialize pools
	bufferPool := NewBufferPool()

	// Variables for accurate timing calculation
	var actualEndTime time.Time
	var actualDuration time.Duration

	// Create context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start metrics server if enabled
	if config.EnableMetrics {
		common.InitPrometheus(db.Name(), config.BulkSize)

		// Initialize metrics for each log type
		for logType := range config.Distribution {
			common.GeneratorThroughput.WithLabelValues(db.Name(), logType).Set(0)
			common.WriteLogsTotal.WithLabelValues(logType, db.Name()).Add(0)
		}

		common.OperationCounter.WithLabelValues("write", db.Name()).Add(0)
		normalizedSystem := common.NormalizeSystemName(db.Name())
		common.CurrentRPS.WithLabelValues("generator", normalizedSystem).Set(0)
	}

	// Create optimized worker pool
	workerPool := NewOptimizedWorkerPool(ctx, config, db, stats, bufferPool)

	// Start statistics reporter
	stopChan := make(chan struct{})
	go StatsReporter(stats, stopChan, config, db.Name())

	// CRITICAL FIX: Set StartTime NOW - just before the main loop starts (excluding initialization)
	stats.StartTime = time.Now()
	endTime := stats.StartTime.Add(config.Duration)

	// Smart RPS timing - no busy waiting!
	// Use nanosecond precision for better accuracy
	targetIntervalNs := float64(time.Second) / config.GetRPS()
	targetInterval := time.Duration(targetIntervalNs)
	backpressureCount := int64(0)

	// Debug: show actual timing calculation
	if config.IsVerbose() {
		actualRPS := float64(time.Second) / float64(targetInterval)
		fmt.Printf("Target RPS: %.4f, Calculated interval: %v, Actual RPS from interval: %.4f\n",
			config.GetRPS(), targetInterval, actualRPS)
	}

	// Main RPS loop with precise timing
	for {
		iterationStart := time.Now()

		// Check if we should stop BEFORE processing
		if iterationStart.After(endTime) {
			break
		}

		// Submit request (non-blocking)
		submitted := workerPool.SubmitRequest()
		if !submitted {
			// Backpressure - request channel full
			backpressureCount++
			if config.IsVerbose() && backpressureCount%100 == 0 {
				fmt.Printf("\nWarning: Backpressure applied %d times - system overloaded\n", backpressureCount)
			}
			atomic.AddInt64(&stats.FailedRequests, 1)
			atomic.AddInt64(&stats.TotalRequests, 1)
		}

		// Smart timing - sleep only the remaining time
		iterationEnd := time.Now()
		elapsed := iterationEnd.Sub(iterationStart)

		if elapsed < targetInterval {
			sleepDuration := targetInterval - elapsed

			select {
			case <-time.After(sleepDuration):
				// Sleep completed
			case <-ctx.Done():
				goto cleanup
			}
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			goto cleanup
		default:
			// Continue
		}
	}

cleanup:
	// CRITICAL FIX: Proper shutdown sequence
	// STEP 1: Stop the main RPS loop (already done by breaking out of loop)
	// STEP 2: Calculate final timing BEFORE any shutdown operations
	actualEndTime = time.Now()
	actualDuration = actualEndTime.Sub(stats.StartTime)

	// STEP 3: Stop stats reporter BEFORE cancelling context to get accurate final stats
	close(stopChan)

	// STEP 4: Now signal workers to stop
	cancel() // Signal all workers to stop

	// STEP 4.5: Give workers time to return to idle state before measuring statistics
	time.Sleep(200 * time.Millisecond) // Allow workers to complete current cycle and set idle=1

	// STEP 5: Get worker statistics BEFORE shutdown (when they should be in idle state)
	mainTotal, mainIdle, retryTotal, retryIdle := workerPool.GetStats()

	// STEP 6: Now shutdown worker pool with timeout
	workerPool.Shutdown(5 * time.Second)

	// CRITICAL FIX: Use actual test duration (excluding initialization time)
	// elapsed now reflects only the actual test execution time
	elapsed := actualDuration.Seconds()
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
	failedReqs := atomic.LoadInt64(&stats.FailedRequests)
	totalLogs := atomic.LoadInt64(&stats.TotalLogs)
	retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

	// Now RPS calculation uses pure test execution time
	currentRPS := float64(totalReqs) / elapsed
	currentLPS := float64(totalLogs) / elapsed

	// Calculate percentages safely
	var successPercent, failedPercent float64
	if totalReqs > 0 {
		successPercent = float64(successReqs) / float64(totalReqs) * 100
		failedPercent = float64(failedReqs) / float64(totalReqs) * 100
	}

	// Always show final statistics with timing fix confirmation
	fmt.Printf("\n\n=== Test Results ===\n")
	fmt.Printf("Duration: %.2f seconds (pure test execution, initialization excluded)\n", elapsed)
	fmt.Printf("Total requests: %d (%.2f/s)\n", totalReqs, currentRPS)
	fmt.Printf("Total logs: %d (%.2f/s)\n", totalLogs, currentLPS)
	fmt.Printf("Successful requests: %d (%.2f%%)\n", successReqs, successPercent)
	fmt.Printf("Failed requests: %d (%.2f%%)\n", failedReqs, failedPercent)
	fmt.Printf("Retry attempts: %d\n", retriedReqs)
	if backpressureCount > 0 {
		fmt.Printf("Backpressure events: %d\n", backpressureCount)
	}

	// Worker statistics (already obtained above)
	fmt.Printf("Final worker states: Main=%d/%d idle, Retry=%d/%d idle\n",
		mainIdle, mainTotal, retryIdle, retryTotal)

	// Stop metrics server
	common.StopMetricsServer()

	return nil
}

// RunGeneratorOptimizedWithContextAndStatsLogger starts the optimized generator with context and StatsLogger support
func RunGeneratorOptimizedWithContextAndStatsLogger(ctx context.Context, config *GeneratorConfig, db logdb.LogDB,
	statsLogger *common.StatsLogger, step, totalSteps int) error {
	// Initialize statistics (StartTime will be set later)
	stats := &Stats{}

	// Initialize pools
	bufferPool := NewBufferPool()

	// Variables for accurate timing calculation
	var actualEndTime time.Time
	var actualDuration time.Duration

	// Create context for coordinated shutdown
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// Create optimized worker pool with context
	wp := NewOptimizedWorkerPool(workerCtx, config, db, stats, bufferPool)
	defer wp.Shutdown(30 * time.Second)

	if config.IsVerbose() {
		fmt.Printf("Starting optimized generator with %d main workers\n", wp.numWorkers)
		fmt.Printf("Target RPS: %.2f\n", config.GetRPS())
	}

	stats.StartTime = time.Now()
	actualStartTime := stats.StartTime

	// Smart timing calculation for more efficient CPU usage
	targetIntervalNs := float64(time.Second) / config.GetRPS()
	targetInterval := time.Duration(targetIntervalNs)

	// RPS timing loop - more CPU efficient than ticker
	endTime := actualStartTime.Add(config.Duration)

	if config.IsVerbose() {
		fmt.Printf("Starting RPS loop with target interval: %v (%.2f RPS)\n", targetInterval, config.GetRPS())
	}

	for time.Now().Before(endTime) {
		iterationStart := time.Now()

		select {
		case <-ctx.Done():
			if config.IsVerbose() {
				fmt.Printf("\nGenerator stopping due to context cancellation\n")
			}
			goto cleanup
		default:
			// Submit request to worker pool (non-blocking)
			submitted := wp.SubmitRequest()
			if !submitted {
				atomic.AddInt64(&stats.BackpressureEvents, 1)
			}
		}

		// Smart sleep - only sleep remaining time if needed
		elapsed := time.Since(iterationStart)
		if elapsed < targetInterval {
			sleepDuration := targetInterval - elapsed
			time.Sleep(sleepDuration)
		}
	}

cleanup:
	// Record actual end time for precise duration calculation
	actualEndTime = time.Now()
	actualDuration = actualEndTime.Sub(actualStartTime)

	// Graceful shutdown
	wp.Shutdown(30 * time.Second)

	if config.IsVerbose() {
		fmt.Printf("\nOptimized generator stopped. Collecting final statistics...\n")
	}

	// Final statistics calculation using ACTUAL duration (not config duration)
	elapsed := actualDuration.Seconds()

	// Get final counters
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
	failedReqs := atomic.LoadInt64(&stats.FailedRequests)
	totalLogs := atomic.LoadInt64(&stats.TotalLogs)
	retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)
	backpressureCount := atomic.LoadInt64(&stats.BackpressureEvents)

	// Worker statistics
	mainTotal, mainIdle, retryTotal, retryIdle := wp.GetStats()

	// Calculate final statistics
	currentRPS := float64(totalReqs) / elapsed
	currentLPS := float64(totalLogs) / elapsed

	// Calculate percentages safely
	var successPercent, failedPercent float64
	if totalReqs > 0 {
		successPercent = float64(successReqs) / float64(totalReqs) * 100
		failedPercent = float64(failedReqs) / float64(totalReqs) * 100
	}

	// Print final statistics
	fmt.Printf("\n\n=== Test Results ===\n")
	fmt.Printf("Duration: %.2f seconds (pure test execution, initialization excluded)\n", elapsed)
	fmt.Printf("Total requests: %d (%.2f/s)\n", totalReqs, currentRPS)
	fmt.Printf("Total logs: %d (%.2f/s)\n", totalLogs, currentLPS)
	fmt.Printf("Successful requests: %d (%.2f%%)\n", successReqs, successPercent)
	fmt.Printf("Failed requests: %d (%.2f%%)\n", failedReqs, failedPercent)
	fmt.Printf("Retry attempts: %d\n", retriedReqs)
	if backpressureCount > 0 {
		fmt.Printf("Backpressure events: %d\n", backpressureCount)
	}

	// Worker statistics
	fmt.Printf("Final worker states: Main=%d/%d idle, Retry=%d/%d idle\n",
		mainIdle, mainTotal, retryIdle, retryTotal)

	// Log to StatsLogger if provided
	if statsLogger != nil && step > 0 && totalSteps > 0 {
		statsLogger.LogGeneratorStats(step, totalSteps, config.System, elapsed,
			config.GetRPS(), currentRPS, totalReqs, successReqs, failedReqs,
			totalLogs)
	}

	return nil
}

// runSimpleOptimizedGenerator - simplified version for debugging
func runSimpleOptimizedGenerator(config *GeneratorConfig, db logdb.LogDB) error {
	fmt.Printf("[DEBUG] SIMPLE: Starting with RPS=%.2f, bulkSize=%d\n", config.GetRPS(), config.BulkSize)

	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()

	bufferPool := NewBufferPool()

	// Simple timing loop
	ticker := time.NewTicker(time.Duration(float64(time.Second) / config.GetRPS()))
	defer ticker.Stop()

	requestCount := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[DEBUG] SIMPLE: Test completed after %d requests\n", requestCount)
			return nil
		case <-ticker.C:
			requestCount++
			fmt.Printf("[DEBUG] SIMPLE: Processing request #%d\n", requestCount)

			// Simple log generation
			logs := CreateBulkPayloadWithContext(ctx, config, bufferPool)
			fmt.Printf("[DEBUG] SIMPLE: Generated %d logs\n", len(logs))

			if len(logs) > 0 {
				// Simple send
				err := db.SendLogs(logs)
				if err != nil {
					fmt.Printf("[DEBUG] SIMPLE: Send error: %v\n", err)
				} else {
					fmt.Printf("[DEBUG] SIMPLE: Send success!\n")
				}
			}
		}
	}
}
