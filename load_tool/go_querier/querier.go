package go_querier

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	queriercommon "github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/executors"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"

	"github.com/sirupsen/logrus"
)

// QueryType defines the type of query
type QueryType string

const (
	SimpleQuery     QueryType = "simple"     // Simple search by keyword or field
	ComplexQuery    QueryType = "complex"    // Complex search with multiple conditions
	AnalyticalQuery QueryType = "analytical" // Query with aggregations
	TimeSeriesQuery QueryType = "timeseries" // Time series query
)

// QueryResult represents the result of executing a query
type QueryResult struct {
	Duration    time.Duration // Execution time
	HitCount    int           // Number of documents found
	BytesRead   int64         // Number of bytes read
	Status      string        // Query status
	RawResponse []byte        // Raw response from the query
	QueryString string        // Query string
	StartTime   time.Time     // Start time of the query
	EndTime     time.Time     // End time of the query
	Limit       string        // Limit of the query
	Step        string        // Step of the query
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

// QueryConfig configuration for the query module
type QueryConfig struct {
	Mode            string
	BaseURL         string
	QPS             float64
	DurationSeconds int
	// WorkerCount removed - using runtime.NumCPU() * 4
	QueryTypeDistribution map[models.QueryType]int
	QueryTimeout          time.Duration
	MaxRetries            int
	RetryDelayMs          int
	Verbose               bool
	DisableVerboseLogging bool
}

// Duration returns the working time in time.Duration format
func (q *QueryConfig) Duration() time.Duration {
	if q.DurationSeconds <= 0 {
		return 0 // infinite operation
	}
	return time.Duration(q.DurationSeconds) * time.Second
}

// RetryDelay returns the delay between retries in time.Duration format
func (q *QueryConfig) RetryDelay() time.Duration {
	return time.Duration(q.RetryDelayMs) * time.Millisecond
}

// Worker represents a worker goroutine
type Worker struct {
	ID        int
	Jobs      <-chan struct{}
	Stats     *common.Stats
	Config    QueryConfig
	Executor  models.QueryExecutor
	WaitGroup *sync.WaitGroup
}

// CreateQueryExecutor creates a query executor for the specified system
func CreateQueryExecutor(mode, baseURL string, options models.Options, workerID int) (models.QueryExecutor, error) {
	switch mode {
	case "victoria", "victorialogs":
		return executors.NewVictoriaLogsExecutor(baseURL, options, workerID), nil
	case "es", "elasticsearch", "elk":
		return executors.NewElasticsearchExecutor(baseURL, options), nil
	case "loki":
		return executors.NewLokiExecutor(baseURL, options), nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown logging system: %s", mode))
	}
}

// CreateQueryExecutorWithTimeConfig creates a query executor for the specified system with time configuration
func CreateQueryExecutorWithTimeConfig(mode, baseURL string, options models.Options, workerID int, timeConfig *common.TimeRangeConfig) (models.QueryExecutor, error) {
	switch mode {
	case "victoria", "victorialogs":
		return executors.NewVictoriaLogsExecutorWithTimeConfig(baseURL, options, workerID, timeConfig), nil
	case "es", "elasticsearch", "elk":
		return executors.NewElasticsearchExecutorWithTimeConfig(baseURL, options, timeConfig), nil
	case "loki":
		return executors.NewLokiExecutorWithTimeConfig(baseURL, options, timeConfig), nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown logging system: %s", mode))
	}
}

// RunQuerier starts the query module with asynchronous processing
func RunQuerier(config QueryConfig, executor models.QueryExecutor, stats *common.Stats) error {
	return RunQuerierWithContext(context.Background(), config, executor, stats, nil, 0, 0)
}

// RunQuerierWithStatsLogger starts the query module with StatsLogger support
func RunQuerierWithStatsLogger(config QueryConfig, executor models.QueryExecutor, stats *common.Stats,
	statsLogger *common.StatsLogger, step, totalSteps int) error {
	return RunQuerierWithContext(context.Background(), config, executor, stats, statsLogger, step, totalSteps)
}

// RunQuerierWithContext starts the query module with context support for graceful shutdown
func RunQuerierWithContext(ctx context.Context, config QueryConfig, executor models.QueryExecutor, stats *common.Stats,
	statsLogger *common.StatsLogger, step, totalSteps int) error {
	// Record test start time
	testStartTime := time.Now()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// No longer using async channels - synchronous execution for precise RPS timing

	// Start periodic statistics reporter
	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go func() {
		defer statsWg.Done()
		statsReporter(ctx, stats, executor.GetSystemName(), config.QPS, testStartTime, config.Verbose)
	}()

	if config.Verbose {
		fmt.Printf("Using synchronous query execution to maintain precise RPS timing\n")
	}

	// QPS timing loop - more CPU efficient than ticker
	targetIntervalNs := float64(time.Second) / config.QPS
	targetInterval := time.Duration(targetIntervalNs)

	endTime := time.Now().Add(config.Duration())

	if config.Verbose {
		fmt.Printf("Starting QPS loop with target interval: %v (%.2f QPS)\n", targetInterval, config.QPS)
	}

	for time.Now().Before(endTime) {
		iterationStart := time.Now()

		select {
		case <-ctx.Done():
			if config.Verbose {
				fmt.Printf("\nQuerier stopping due to context cancellation\n")
			}
			goto cleanup
		default:
			// Execute query synchronously to maintain precise RPS timing
			processQuery(0, stats, config, executor)
		}

		// Smart sleep - only sleep remaining time if needed
		elapsed := time.Since(iterationStart)
		if elapsed < targetInterval {
			sleepDuration := targetInterval - elapsed
			time.Sleep(sleepDuration)
		}
	}

cleanup:
	// Graceful shutdown
	cancel() // Cancel context to stop stats reporter

	// Wait for stats reporter to finish
	statsWg.Wait()

	// Print final statistics with StatsLogger support
	printFinalQueryStatisticsWithLogger(stats, testStartTime, executor.GetSystemName(), config.QPS, statsLogger, step, totalSteps)

	return nil
}

// processQuery processes a single query without retry logic
func processQuery(processorID int, stats *common.Stats, config QueryConfig, executor models.QueryExecutor) {
	// Select a random query type based on distribution
	queryType := selectRandomQueryType(config.QueryTypeDistribution)

	// Generate time range beforehand to have it for error metrics
	timeRangeInfo := executor.GenerateTimeRange()
	var timeStringRepr string

	// Extract StringRepr from timeRange (all executors return the same type)
	if timeRange, ok := timeRangeInfo.(queriercommon.TimeRange); ok {
		timeStringRepr = timeRange.StringRepr
	}
	if timeStringRepr == "" {
		timeStringRepr = "unknown"
	}

	// Increment the total query counter
	stats.IncrementTotalQueries()
	common.IncrementReadRequests(executor.GetSystemName())

	// Create a context with timeout for the query
	queryCtx, cancel := context.WithTimeout(context.Background(), config.QueryTimeout)
	defer cancel()

	// Execute the query (no retries in querier)
	startTime := time.Now()
	result, err := executor.ExecuteQuery(queryCtx, queryType)
	duration := time.Since(startTime)

	common.IncrementQueryType(executor.GetSystemName(), string(queryType))

	if err == nil {
		// Success - update metrics
		stats.IncrementSuccessfulQueries()
		common.IncrementSuccessfulRead(executor.GetSystemName(), string(queryType))

		// Update metrics with time and type information
		if result.TimeStringRepr != "" {
			common.ObserveReadDurationWithTimeAndType(executor.GetSystemName(), "success", result.TimeStringRepr, string(queryType), duration.Seconds())
		} else {
			common.ObserveReadDurationWithTimeAndType(executor.GetSystemName(), "success", "unknown", string(queryType), duration.Seconds())
		}

		// Update statistics based on results
		stats.AddHits(result.HitCount)
		stats.AddBytesRead(result.BytesRead)

		// Update Prometheus metrics
		normalizedSystem := common.NormalizeSystemName(executor.GetSystemName())
		common.ResultHitsSummary.WithLabelValues(normalizedSystem, string(queryType)).Observe(float64(result.HitCount))
		common.ResultSizeSummary.WithLabelValues(normalizedSystem, string(queryType)).Observe(float64(result.BytesRead))

		// Output query information if verbose mode is enabled
		if config.Verbose {
			// Format timerange for display
			timeRangeStr := ""
			if !result.StartTime.IsZero() && !result.EndTime.IsZero() {
				startStr := result.StartTime.Format("2006-01-02 15:04:05")
				endStr := result.EndTime.Format("2006-01-02 15:04:05")
				timeRangeStr = fmt.Sprintf(" [%s to %s]", startStr, endStr)
			}

			// Format limit if available
			limitStr := ""
			if result.Limit != "" {
				limitStr = fmt.Sprintf(" limit:%s", result.Limit)
			}

			// Format step if available
			stepStr := ""
			if result.Step != "" {
				stepStr = fmt.Sprintf(" step:%s", result.Step)
			}

			// Log with query details
			fmt.Printf("[Processor %d] Query %s: %s%s%s%s found %d records, read %d bytes, time %v\n",
				processorID, queryType, result.QueryString, timeRangeStr, limitStr, stepStr, result.HitCount, result.BytesRead, duration)
		}

		return
	}

	// Query failed - no retries in querier
	stats.IncrementFailedQueries()
	common.IncrementFailedRead(executor.GetSystemName(), string(queryType), "query_error")

	// Update metrics with time and type information for failed query
	// Use timeStringRepr generated beforehand, fallback to result if available
	finalTimeStringRepr := timeStringRepr
	if result.TimeStringRepr != "" {
		finalTimeStringRepr = result.TimeStringRepr
	}
	common.ObserveReadDurationWithTimeAndType(executor.GetSystemName(), "failed", finalTimeStringRepr, string(queryType), duration.Seconds())

	// Log error using structured logging with query string
	// Use generated query string or result query string if available
	common.LogQueryError(processorID, string(queryType), executor.GetSystemName(), err, result.QueryString)
}

// runWorker starts a worker goroutine
func runWorker(worker Worker) {
	defer worker.WaitGroup.Done()

	ctx := context.Background()

	for range worker.Jobs {
		// Select a random query type based on distribution
		queryType := selectRandomQueryType(worker.Config.QueryTypeDistribution)

		// Generate time range beforehand to have it for error metrics
		timeRangeInfo := worker.Executor.GenerateTimeRange()
		var timeStringRepr string

		// Extract StringRepr from timeRange (all executors return the same type)
		if timeRange, ok := timeRangeInfo.(queriercommon.TimeRange); ok {
			timeStringRepr = timeRange.StringRepr
		}
		if timeStringRepr == "" {
			timeStringRepr = "unknown"
		}

		// Increment the total query counter
		worker.Stats.IncrementTotalQueries()
		common.IncrementReadRequests(worker.Executor.GetSystemName())

		// Create a context with timeout
		queryCtx, cancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)

		// Execute the query
		startTime := time.Now()
		result, err := worker.Executor.ExecuteQuery(queryCtx, queryType)
		duration := time.Since(startTime)

		// Cancel the context
		cancel()

		common.IncrementQueryType(worker.Executor.GetSystemName(), string(queryType))

		// Update counters based on the result
		if err != nil {
			worker.Stats.IncrementFailedQueries()
			common.IncrementFailedRead(worker.Executor.GetSystemName(), string(queryType), "query_error")

			// Update metrics with time and type information for failed query
			// Use timeStringRepr generated beforehand, fallback to result if available
			finalTimeStringRepr := timeStringRepr
			if result.TimeStringRepr != "" {
				finalTimeStringRepr = result.TimeStringRepr
			}
			common.ObserveReadDurationWithTimeAndType(worker.Executor.GetSystemName(), "failed", finalTimeStringRepr, string(queryType), duration.Seconds())
		}

		// If the query is ultimately successful, update counters
		if err == nil {
			worker.Stats.IncrementSuccessfulQueries()
			common.IncrementSuccessfulRead(worker.Executor.GetSystemName(), string(queryType))

			// Update metrics with time and type information for successful query (if not already done in retry)
			// Use timeStringRepr generated beforehand, fallback to result if available
			successFinalTimeStringRepr := timeStringRepr
			if result.TimeStringRepr != "" {
				successFinalTimeStringRepr = result.TimeStringRepr
			}
			common.ObserveReadDurationWithTimeAndType(worker.Executor.GetSystemName(), "success", successFinalTimeStringRepr, string(queryType), duration.Seconds())

			// Update statistics based on results
			worker.Stats.AddHits(result.HitCount)
			worker.Stats.AddBytesRead(result.BytesRead)

			// Update Prometheus metrics
			normalizedSystem := common.NormalizeSystemName(worker.Executor.GetSystemName())
			common.ResultHitsSummary.WithLabelValues(normalizedSystem, string(queryType)).Observe(float64(result.HitCount))
			common.ResultSizeSummary.WithLabelValues(normalizedSystem, string(queryType)).Observe(float64(result.BytesRead))

			// Output query information if verbose mode is enabled
			if worker.Config.Verbose {
				// Format timerange for display
				timeRangeStr := ""
				if !result.StartTime.IsZero() && !result.EndTime.IsZero() {
					startStr := result.StartTime.Format("2006-01-02 15:04:05")
					endStr := result.EndTime.Format("2006-01-02 15:04:05")
					timeRangeStr = fmt.Sprintf(" [%s to %s]", startStr, endStr)
				}

				// Format limit if available
				limitStr := ""
				if result.Limit != "" {
					limitStr = fmt.Sprintf(" limit:%s", result.Limit)
				}

				// Format step if available
				stepStr := ""
				if result.Step != "" {
					stepStr = fmt.Sprintf(" step:%s", result.Step)
				}

				// Log with query details
				fmt.Printf("[Worker %d] Query %s: %s%s%s%s found %d records, read %d bytes, time %v\n",
					worker.ID, queryType, result.QueryString, timeRangeStr, limitStr, stepStr, result.HitCount, result.BytesRead, duration)
			}
		} else {
			// Always log errors for diagnostics using structured logging
			queryString := result.QueryString
			if queryString == "" {
				queryString = "query_generation_failed"
			}
			common.LogQueryError(worker.ID, string(queryType), worker.Executor.GetSystemName(), err, queryString)
		}
	}
}

// selectRandomQueryType selects a random query type based on distribution
func selectRandomQueryType(distribution map[models.QueryType]int) models.QueryType {
	// Calculate the total weight of all query types
	totalWeight := 0
	for _, weight := range distribution {
		totalWeight += weight
	}

	// If there is no distribution, return a simple query
	if totalWeight == 0 {
		return models.SimpleQuery
	}

	// Generate a random number from 0 to total weight
	randomNum := randInt(0, totalWeight)

	// Select the query type based on its weight
	currentWeight := 0
	for queryType, weight := range distribution {
		currentWeight += weight
		if randomNum < currentWeight {
			return queryType
		}
	}

	// By default, return a simple query
	return models.SimpleQuery
}

// randInt returns a random number in the range [min, max)
func randInt(min, max int) int {
	return min + logdata.RandomIntn(max-min)
}

func init() {
	// No need for rand.Seed since Go 1.20 - global generator is auto-seeded
}

// createExecutor creates a new query executor for the specified system
func createExecutor(system, host string, port int, timeout time.Duration, retryCount int, verbose bool, workerID int) (models.QueryExecutor, error) {
	baseURL := fmt.Sprintf("http://%s:%d", host, port)

	options := models.Options{
		Timeout:    timeout,
		RetryCount: retryCount,
		RetryDelay: 500 * time.Millisecond,
		Verbose:    verbose,
	}

	switch strings.ToLower(system) {
	case "elasticsearch", "elk":
		return executors.NewElasticsearchExecutor(baseURL, options), nil
	case "loki":
		return executors.NewLokiExecutor(baseURL, options), nil
	case "victorialogs", "victoria":
		return executors.NewVictoriaLogsExecutor(baseURL, options, workerID), nil
	default:
		return nil, errors.New("unsupported log system")
	}
}

func main() {
	// Parse command-line flags
	serverHost := flag.String("host", "localhost", "Server host")
	serverPort := flag.Int("port", 9200, "Server port")
	system := flag.String("system", "elasticsearch", "Log system to query (elasticsearch, loki, victorialogs, etc)")
	// workersCount flag removed - using runtime.NumCPU() * 4
	intervalMillis := flag.Int("interval", 1000, "Query interval in milliseconds")
	rps := flag.Int("rps", 0, "Requests per second (overrides interval if set)")
	timeoutMs := flag.Int("timeout", 5000, "Query timeout in milliseconds")
	retryCount := flag.Int("retries", 3, "Query retry count")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warn, error, fatal, panic)")
	logFormat := flag.String("log-format", "", "Log format (text, json)")
	verbose := flag.Bool("verbose", false, "Verbose output")
	queryType := flag.String("query-type", "", "Query type to use (simple, complex, analytical, timeseries, stat, topk). If empty, use all types.")

	flag.Parse()

	// Configure logger
	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	if *logLevel != "" {
		level, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			fmt.Printf("Invalid log level: %s\n", *logLevel)
			os.Exit(1)
		}
		logger.SetLevel(level)
	}

	if *logFormat == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	if *verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	// Create the executor
	executor, err := createExecutor(*system, *serverHost, *serverPort, time.Duration(*timeoutMs)*time.Millisecond, *retryCount, *verbose, 1)
	if err != nil {
		logger.Fatalf("Failed to create executor: %v", err)
	}

	// Calculate query interval
	var interval time.Duration
	if *rps > 0 {
		interval = time.Second / time.Duration(*rps)
	} else {
		interval = time.Duration(*intervalMillis) * time.Millisecond
	}

	// Parse query type if specified
	var selectedQueryType models.QueryType
	if *queryType != "" {
		switch strings.ToLower(*queryType) {
		case "simple":
			selectedQueryType = models.SimpleQuery
		case "complex":
			selectedQueryType = models.ComplexQuery
		case "analytical":
			selectedQueryType = models.AnalyticalQuery
		case "timeseries":
			selectedQueryType = models.TimeSeriesQuery
		case "stat":
			selectedQueryType = models.StatQuery
		case "topk":
			selectedQueryType = models.TopKQuery
		default:
			logger.Fatalf("Invalid query type: %s. Must be simple, complex, analytical, timeseries, stat, or topk", *queryType)
		}
		logger.Infof("Using specified query type: %s", *queryType)
	}

	// Create workers
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Start workers
	// Start workers based on CPU count
	numWorkers := runtime.NumCPU() * 4
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logger.Infof("Starting worker %d", workerID)

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					logger.Infof("Worker %d stopping", workerID)
					return
				case <-ticker.C:
					// Determine query type
					var qt models.QueryType
					if *queryType != "" {
						qt = selectedQueryType
					} else {
						// Randomly choose query type if not specified
						rnd := int(time.Now().UnixNano() % 100)
						switch {
						case rnd < 50:
							qt = models.SimpleQuery // 50%
						case rnd < 65:
							qt = models.ComplexQuery // 15%
						case rnd < 80:
							qt = models.AnalyticalQuery // 15%
						case rnd < 90:
							qt = models.TimeSeriesQuery // 10%
						case rnd < 95:
							qt = models.StatQuery // 5%
						default:
							qt = models.TopKQuery // 5%
						}
					}

					// Execute the query
					queryStartTime := time.Now()
					result, err := executor.ExecuteQuery(ctx, qt)
					queryDuration := time.Since(queryStartTime)

					if err != nil {
						logger.Errorf("[Worker %d] Query failed: %v", workerID, err)
					} else {
						// Extract query details from result
						queryText := ""
						timeRange := ""
						limit := ""
						if result.RawResponse != nil {
							// Try to extract query and time range from the raw response
							var respMap map[string]interface{}
							if jsonErr := json.Unmarshal(result.RawResponse, &respMap); jsonErr == nil {
								if data, ok := respMap["config"].(map[string]interface{}); ok {
									if q, ok := data["query"].(string); ok {
										queryText = q
									}
									// Extract start and end times
									if start, ok := data["start"].(string); ok {
										if end, ok := data["end"].(string); ok {
											// Format time strings to make them more readable if they're nanoseconds
											startTime, startErr := parseNanosecondTime(start)
											endTime, endErr := parseNanosecondTime(end)

											if startErr == nil && endErr == nil {
												timeRange = fmt.Sprintf("[%s to %s]",
													startTime.Format("2006-01-02 15:04:05"),
													endTime.Format("2006-01-02 15:04:05"))
											} else {
												timeRange = fmt.Sprintf("[%s to %s]", start, end)
											}
										}
									}
									// Extract limit
									if limitVal, ok := data["limit"].(string); ok {
										limit = fmt.Sprintf("limit:%s", limitVal)
									}
								}
							}
						}

						// Output query results with full details
						if *verbose {
							fmt.Printf("[Worker %d] Query %s: %s %s %s found %d records, read %d bytes, time %v\n",
								workerID, qt, queryText, timeRange, limit, result.HitCount, result.BytesRead, queryDuration)
						}
					}
				}
			}
		}(i)
	}

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	logger.Info("Stopping querier...")
	cancel()
	wg.Wait()
	logger.Info("Querier stopped")
}

// parseNanosecondTime attempts to parse a string as a nanosecond timestamp
// Returns the time or an error
func parseNanosecondTime(timeStr string) (time.Time, error) {
	// Try to convert to int64 nanoseconds
	ns, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %v", err)
	}
	return time.Unix(0, ns), nil
}

// statsReporter periodically reports query statistics during execution
func statsReporter(ctx context.Context, stats *common.Stats, systemName string, targetQPS float64, startTime time.Time, verbose bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastTotalQueries, lastSuccessful, lastFailed int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentTotal := atomic.LoadInt64(&stats.TotalQueries)
			currentSuccessful := atomic.LoadInt64(&stats.SuccessfulQueries)
			currentFailed := atomic.LoadInt64(&stats.FailedQueries)

			// Calculate deltas for the last 5 seconds
			deltaTotal := currentTotal - lastTotalQueries
			deltaSuccessful := currentSuccessful - lastSuccessful
			deltaFailed := currentFailed - lastFailed

			actualQPS := float64(deltaTotal) / 5.0 // 5 second intervals
			elapsed := time.Since(startTime).Seconds()

			if verbose && deltaTotal > 0 {
				fmt.Printf("[%s] %.0fs: %d queries (%.1f QPS, target: %.1f), success: %d, failed: %d\n",
					systemName, elapsed, deltaTotal, actualQPS, targetQPS, deltaSuccessful, deltaFailed)
			}

			// Update last values
			lastTotalQueries = currentTotal
			lastSuccessful = currentSuccessful
			lastFailed = currentFailed
		}
	}
}

// printFinalQueryStatistics prints comprehensive final statistics for the querier
func printFinalQueryStatistics(stats *common.Stats, startTime time.Time, systemName string, targetQPS float64) {
	printFinalQueryStatisticsWithLogger(stats, startTime, systemName, targetQPS, nil, 0, 0)
}

// printFinalQueryStatisticsWithLogger prints comprehensive final statistics and optionally logs to StatsLogger
func printFinalQueryStatisticsWithLogger(stats *common.Stats, startTime time.Time, systemName string, targetQPS float64,
	statsLogger *common.StatsLogger, step, totalSteps int) {
	elapsed := time.Since(startTime).Seconds()

	totalQueries := atomic.LoadInt64(&stats.TotalQueries)
	successfulQueries := atomic.LoadInt64(&stats.SuccessfulQueries)
	failedQueries := atomic.LoadInt64(&stats.FailedQueries)
	totalHits := atomic.LoadInt64(&stats.TotalHits)
	totalBytesRead := atomic.LoadInt64(&stats.TotalBytesRead)

	// Calculate rates
	actualQPS := float64(totalQueries) / elapsed
	successPercent := 0.0
	failedPercent := 0.0

	if totalQueries > 0 {
		successPercent = float64(successfulQueries) / float64(totalQueries) * 100
		failedPercent = float64(failedQueries) / float64(totalQueries) * 100
	}

	avgHitsPerQuery := 0.0
	avgBytesPerQuery := 0.0

	if totalQueries > 0 {
		avgHitsPerQuery = float64(totalHits) / float64(totalQueries)
		avgBytesPerQuery = float64(totalBytesRead) / float64(totalQueries) / 1024.0 // KB
	}

	// Print final statistics in generator-like format
	fmt.Printf("\n\n=== Query Test Results ===\n")
	fmt.Printf("System: %s\n", systemName)
	fmt.Printf("Duration: %.2f seconds\n", elapsed)
	fmt.Printf("Target QPS: %.2f\n", targetQPS)
	fmt.Printf("Actual QPS: %.2f\n", actualQPS)
	fmt.Printf("Total queries: %d (%.2f/s)\n", totalQueries, actualQPS)
	fmt.Printf("Successful queries: %d (%.2f%%)\n", successfulQueries, successPercent)
	fmt.Printf("Failed queries: %d (%.2f%%)\n", failedQueries, failedPercent)

	if totalQueries > 0 {
		fmt.Printf("Total results found: %d documents\n", totalHits)
		fmt.Printf("Total data read: %.2f MB\n", float64(totalBytesRead)/1024.0/1024.0)
		fmt.Printf("Average results per query: %.1f documents\n", avgHitsPerQuery)
		fmt.Printf("Average data per query: %.2f KB\n", avgBytesPerQuery)
	}

	// Calculate efficiency
	var efficiency float64 = 100.0
	if targetQPS > 0 {
		efficiency = (actualQPS / targetQPS) * 100
		fmt.Printf("QPS Efficiency: %.1f%% (actual/target)\n", efficiency)
	}

	fmt.Printf("=== Query Test Completed ===\n\n")

	// Log to StatsLogger if provided
	if statsLogger != nil && step > 0 && totalSteps > 0 {
		statsLogger.LogQuerierStats(step, totalSteps, systemName, elapsed,
			targetQPS, actualQPS, totalQueries, successfulQueries, failedQueries,
			totalHits, efficiency)
	}
}
