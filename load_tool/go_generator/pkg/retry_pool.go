package pkg

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
)

// RetryRequest represents a request that needs to be retried
type RetryRequest struct {
	Logs         []logdb.LogEntry
	Attempt      int
	MaxRetries   int
	RetryDelay   time.Duration
	DB           logdb.LogDB
	Stats        *Stats
	Config       *GeneratorConfig
	BufferPool   *BufferPool
	ProcessorID  int
	ResponseChan chan RetryResponse // Channel to send result back
}

// RetryResponse represents the result of a retry attempt
type RetryResponse struct {
	Success bool
	Error   error
	Attempt int
}

// RetryWorker represents a single retry worker with idle state tracking
type RetryWorker struct {
	id       int
	idle     int64 // atomic flag: 1 = idle, 0 = working
	requests chan RetryRequest
	ctx      context.Context
	wg       *sync.WaitGroup
}

// RetryPool manages a pool of retry workers with idle behavior
type RetryPool struct {
	workers    []*RetryWorker
	workChan   chan RetryRequest
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	numWorkers int
}

// NewRetryPool creates a new retry pool with specified number of workers
func NewRetryPool(ctx context.Context, numWorkers int) *RetryPool {
	if numWorkers <= 0 {
		numWorkers = 2 // Minimum 2 retry workers
	}

	poolCtx, cancel := context.WithCancel(ctx)

	pool := &RetryPool{
		workers:    make([]*RetryWorker, numWorkers),
		workChan:   make(chan RetryRequest, numWorkers*10), // Buffer for backpressure
		ctx:        poolCtx,
		cancel:     cancel,
		numWorkers: numWorkers,
	}

	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		worker := &RetryWorker{
			id:       i,
			idle:     1, // Start in idle state
			requests: pool.workChan,
			ctx:      poolCtx,
			wg:       &pool.wg,
		}
		pool.workers[i] = worker

		pool.wg.Add(1)
		go worker.run()
	}

	return pool
}

// SubmitRetry submits a retry request to the pool (non-blocking)
func (rp *RetryPool) SubmitRetry(req RetryRequest) bool {
	select {
	case rp.workChan <- req:
		return true
	default:
		// Pool is overloaded, drop the retry request
		return false
	}
}

// SubmitRetryBlocking submits a retry request to the pool (blocking with timeout)
func (rp *RetryPool) SubmitRetryBlocking(req RetryRequest, timeout time.Duration) bool {
	select {
	case rp.workChan <- req:
		return true
	case <-time.After(timeout):
		return false
	case <-rp.ctx.Done():
		return false
	}
}

// GetIdleWorkerCount returns the number of idle workers
func (rp *RetryPool) GetIdleWorkerCount() int {
	idle := 0
	for _, worker := range rp.workers {
		if atomic.LoadInt64(&worker.idle) == 1 {
			idle++
		}
	}
	return idle
}

// GetStats returns retry pool statistics
func (rp *RetryPool) GetStats() (total, idle, working int) {
	total = rp.numWorkers
	idle = rp.GetIdleWorkerCount()
	working = total - idle
	return
}

// Shutdown gracefully stops all retry workers
func (rp *RetryPool) Shutdown(timeout time.Duration) {
	// Signal all workers to stop
	rp.cancel()

	// Close work channel to prevent new requests
	close(rp.workChan)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		rp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Retry pool shutdown completed gracefully
	case <-time.After(timeout):
		// Retry pool shutdown timeout - some workers may not have finished
	}
}

// run is the main loop for retry workers with idle state management
func (rw *RetryWorker) run() {
	defer rw.wg.Done()

	for {
		// Start in idle state, blocking on channel
		atomic.StoreInt64(&rw.idle, 1)

		select {
		case <-rw.ctx.Done():
			return
		case req, ok := <-rw.requests:
			if !ok {
				return
			}

			// Switch to working state
			atomic.StoreInt64(&rw.idle, 0)

			// Process the retry request
			rw.processRetryRequest(req)
		}
	}
}

// processRetryRequest handles the actual retry logic
func (rw *RetryWorker) processRetryRequest(req RetryRequest) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[ERROR] Retry worker %d panic: %v\n", rw.id, r)
			// Send failure response
			if req.ResponseChan != nil {
				select {
				case req.ResponseChan <- RetryResponse{
					Success: false,
					Error:   fmt.Errorf("panic during retry: %v", r),
					Attempt: req.Attempt,
				}:
				default:
				}
			}
		}
	}()

	// Apply exponential backoff delay
	if req.Attempt > 0 {
		backoffDelay := req.RetryDelay * time.Duration(1<<(req.Attempt-1))

		select {
		case <-time.After(backoffDelay):
			// Delay completed
		case <-rw.ctx.Done():
			// Context cancelled during delay
			if req.ResponseChan != nil {
				select {
				case req.ResponseChan <- RetryResponse{
					Success: false,
					Error:   fmt.Errorf("context cancelled during retry delay"),
					Attempt: req.Attempt,
				}:
				default:
				}
			}
			return
		}
	}

	// Attempt to send logs
	startTime := time.Now()
	var err error

	// Use buffered sending if available
	buf := req.BufferPool.GetLarge()
	defer req.BufferPool.Put(buf)

	// Try context-aware methods first
	if contextDB, ok := req.DB.(interface {
		SendLogsWithContext(context.Context, []logdb.LogEntry) error
	}); ok {
		err = contextDB.SendLogsWithContext(rw.ctx, req.Logs)
	} else if bufferDB, ok := req.DB.(interface {
		SendLogsWithBuffer([]logdb.LogEntry, *bytes.Buffer) error
	}); ok {
		// Use optimized buffer method with context timeout
		done := make(chan error, 1)
		go func() {
			done <- bufferDB.SendLogsWithBuffer(req.Logs, buf)
		}()

		select {
		case err = <-done:
			// Request completed
		case <-rw.ctx.Done():
			err = fmt.Errorf("context cancelled during SendLogsWithBuffer")
		case <-time.After(15 * time.Second):
			err = fmt.Errorf("SendLogsWithBuffer timeout")
		}
	} else {
		// Fallback to standard method with context timeout
		done := make(chan error, 1)
		go func() {
			done <- req.DB.SendLogs(req.Logs)
		}()

		select {
		case err = <-done:
			// Request completed
		case <-rw.ctx.Done():
			err = fmt.Errorf("context cancelled during SendLogs")
		case <-time.After(15 * time.Second):
			err = fmt.Errorf("SendLogs timeout")
		}
	}

	duration := time.Since(startTime).Seconds()

	// Handle result
	if err == nil {
		// Retry successful

		// Update success metrics
		atomic.AddInt64(&req.Stats.SuccessfulRequests, 1)
		atomic.AddInt64(&req.Stats.TotalLogs, int64(len(req.Logs)))
		atomic.AddInt64(&req.Stats.TotalRequests, 1)

		if req.Config.EnableMetrics {
			common.ObserveWriteDuration(req.DB.Name(), "success", duration)
			common.IncrementWriteOperation(req.DB.Name())

			// Update log counters by type
			logCounts := make(map[string]int)
			for _, log := range req.Logs {
				if logType, ok := log["log_type"].(string); ok {
					logCounts[logType]++
					req.Stats.IncrementLogType(logType)
				}
			}

			for logType, count := range logCounts {
				common.WriteLogsTotal.WithLabelValues(logType, req.DB.Name()).Add(float64(count))
			}
			common.IncrementSuccessfulWrite(req.DB.Name())
		}

		// Send success response
		if req.ResponseChan != nil {
			select {
			case req.ResponseChan <- RetryResponse{
				Success: true,
				Error:   nil,
				Attempt: req.Attempt,
			}:
			default:
			}
		}

	} else {
		// Retry failed

		atomic.AddInt64(&req.Stats.RetriedRequests, 1)

		if req.Config.EnableMetrics {
			common.ObserveWriteDuration(req.DB.Name(), "error", duration)
		}

		// Check if we should retry again
		if req.Attempt < req.MaxRetries {
			// Submit for another retry attempt
			newReq := req
			newReq.Attempt++

			select {
			case <-rw.ctx.Done():
				// Context cancelled, send final failure
				rw.sendFinalFailure(req, err)
			default:
				// Try to submit for another retry
				submitted := false
				select {
				case rw.requests <- newReq:
					submitted = true
				case <-time.After(100 * time.Millisecond):
					// Retry queue full, send failure
				}

				if !submitted {
					rw.sendFinalFailure(req, fmt.Errorf("retry queue full after attempt %d: %v", req.Attempt, err))
				}
			}
		} else {
			// Max retries exceeded, send final failure
			rw.sendFinalFailure(req, err)
		}
	}
}

// sendFinalFailure sends final failure response and updates metrics
func (rw *RetryWorker) sendFinalFailure(req RetryRequest, err error) {
	// Update failure metrics
	atomic.AddInt64(&req.Stats.FailedRequests, 1)
	atomic.AddInt64(&req.Stats.TotalRequests, 1)

	if req.Config.EnableMetrics {
		// Count failures for each log type in the batch
		logCounts := make(map[string]int)
		for _, log := range req.Logs {
			if logType, ok := log["log_type"].(string); ok {
				logCounts[logType]++
			}
		}
		for logType := range logCounts {
			common.IncrementFailedWrite(req.DB.Name(), logType, "retry_exhausted")
		}
	}

	// Send failure response
	if req.ResponseChan != nil {
		select {
		case req.ResponseChan <- RetryResponse{
			Success: false,
			Error:   err,
			Attempt: req.Attempt,
		}:
		default:
		}
	}

	// Log error to file
	payload, _ := req.DB.FormatPayload(req.Logs)
	common.LogGeneratorError(req.ProcessorID, req.DB.Name(), err, payload)
}
