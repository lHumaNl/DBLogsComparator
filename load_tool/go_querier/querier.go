package go_querier

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/executors"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
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

// QueryConfig configuration for the query module
type QueryConfig struct {
	Mode                  string
	BaseURL               string
	QPS                   int
	DurationSeconds       int
	WorkerCount           int
	QueryTypeDistribution map[models.QueryType]int
	QueryTimeout          time.Duration
	MaxRetries            int
	RetryDelayMs          int
	Verbose               bool
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
func CreateQueryExecutor(mode, baseURL string, options models.Options) (models.QueryExecutor, error) {
	switch mode {
	case "victoria":
		return executors.NewVictoriaLogsExecutor(baseURL, options), nil
	case "es":
		return executors.NewElasticsearchExecutor(baseURL, options), nil
	case "loki":
		return executors.NewLokiExecutor(baseURL, options), nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown logging system: %s", mode))
	}
}

// RunQuerier starts the query module
func RunQuerier(config QueryConfig, executor models.QueryExecutor, stats *common.Stats) error {
	// Initialize channels and goroutines
	jobs := make(chan struct{}, config.QPS*2)
	stopChan := make(chan struct{})

	// Start worker goroutines
	var wg sync.WaitGroup
	for w := 1; w <= config.WorkerCount; w++ {
		wg.Add(1)
		worker := Worker{
			ID:        w,
			Jobs:      jobs,
			Stats:     stats,
			Config:    config,
			Executor:  executor,
			WaitGroup: &wg,
		}
		go runWorker(worker)
	}

	// Main load generation loop
	tickInterval := time.Second / time.Duration(config.QPS)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	endTime := time.Now().Add(config.Duration())

	for time.Now().Before(endTime) {
		<-ticker.C
		jobs <- struct{}{}
	}

	close(jobs)
	wg.Wait()
	close(stopChan)

	return nil
}

// runWorker starts a worker goroutine
func runWorker(worker Worker) {
	defer worker.WaitGroup.Done()

	ctx := context.Background()

	for range worker.Jobs {
		// Select a random query type based on distribution
		queryType := selectRandomQueryType(worker.Config.QueryTypeDistribution)

		// Increment the total query counter
		worker.Stats.IncrementTotalQueries()
		common.IncrementReadRequests()
		common.OperationCounter.WithLabelValues("query", worker.Executor.GetSystemName()).Inc()

		// Create a context with timeout
		queryCtx, cancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)

		// Execute the query
		startTime := time.Now()
		result, err := worker.Executor.ExecuteQuery(queryCtx, queryType)
		duration := time.Since(startTime)

		// Cancel the context
		cancel()

		// Update metrics
		common.ReadDurationHistogram.Observe(duration.Seconds())
		common.QueryTypeCounter.WithLabelValues(string(queryType)).Inc()

		// Update counters based on the result
		if err != nil {
			worker.Stats.IncrementFailedQueries()
			common.IncrementFailedRead()

			// If the error is not related to timeout or context, retry the query
			if err != context.DeadlineExceeded && err != context.Canceled {
				for i := 0; i < worker.Config.MaxRetries; i++ {
					worker.Stats.IncrementRetriedQueries()
					common.ReadRequestsRetried.Inc()

					time.Sleep(worker.Config.RetryDelay())

					// Create a new context for the retry
					retryCtx, retryCancel := context.WithTimeout(ctx, worker.Config.QueryTimeout)

					// Retry the query
					retryStartTime := time.Now()
					result, err = worker.Executor.ExecuteQuery(retryCtx, queryType)
					retryDuration := time.Since(retryStartTime)

					// Cancel the context
					retryCancel()

					// Update metrics
					common.ReadDurationHistogram.Observe(retryDuration.Seconds())

					// If the query is successful, break the retry loop
					if err == nil {
						break
					}
				}
			}
		}

		// If the query is ultimately successful, update counters
		if err == nil {
			worker.Stats.IncrementSuccessfulQueries()
			common.IncrementSuccessfulRead()

			// Update statistics based on results
			worker.Stats.AddHits(result.HitCount)
			worker.Stats.AddBytesRead(result.BytesRead)

			// Update Prometheus metrics
			common.ResultHitsHistogram.Observe(float64(result.HitCount))
			common.ResultSizeHistogram.Observe(float64(result.BytesRead))

			// Output query information if verbose mode is enabled
			if worker.Config.Verbose {
				fmt.Printf("[Worker %d] Query %s: found %d records, read %d bytes, time %v\n",
					worker.ID, queryType, result.HitCount, result.BytesRead, duration)
			}
		} else {
			// Output error information
			if worker.Config.Verbose {
				fmt.Printf("[Worker %d] Query error %s: %v\n", worker.ID, queryType, err)
			}
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
	return min + rand.Intn(max-min)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
