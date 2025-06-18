package common

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// All metrics use a unified prefix dblogscomp_ for standardization and easier filtering in monitoring systems

var (
	// Metrics for write operations

	// WriteRequestsTotal counts the total number of log write requests
	// regardless of their success or type
	WriteRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_write_requests_total",
		Help: "Total number of log write requests",
	}, []string{"system"})

	// WriteRequestsSuccess counts the number of successfully executed log write requests
	WriteRequestsSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_write_requests_success",
		Help: "Number of successful log write requests",
	}, []string{"system"})

	// WriteRequestsFailure counts the number of failed log write requests
	WriteRequestsFailure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_write_requests_failure",
		Help: "Number of failed log write requests",
	}, []string{"system", "error_type"})

	// WriteLogsTotal counts the total number of sent logs
	// by types and destination systems
	WriteLogsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_generator_logs_sent_total", // renamed for clarity
		Help: "Total number of logs sent by generator",
	}, []string{"log_type", "system"})

	// WriteRequestsRetried counts the number of retried write requests
	WriteRequestsRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_write_requests_retried",
		Help: "Number of retried write requests",
	}, []string{"system", "retry_attempt"})

	// WriteDurationHistogram measures the execution time of write requests
	WriteDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dblogscomp_write_duration_seconds",
		Help:    "Histogram of write request execution times",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	}, []string{"system", "status"})

	// WriteBatchSizeGauge shows the current log batch size
	WriteBatchSizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dblogscomp_generator_batch_size",
		Help: "Current log batch size in generator",
	})

	// Metrics for read operations
	ReadRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_read_requests_total",
		Help: "Total number of log read requests",
	}, []string{"system"})

	ReadRequestsSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_read_requests_success",
		Help: "Number of successful log read requests",
	}, []string{"system"})

	ReadRequestsFailure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_read_requests_failure",
		Help: "Number of failed log read requests",
	}, []string{"system", "error_type"})

	ReadRequestsRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_read_requests_retried",
		Help: "Number of retried read requests",
	}, []string{"system", "retry_attempt"})

	ReadDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dblogscomp_read_duration_seconds",
		Help:    "Histogram of read request execution times",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	}, []string{"system", "status"})

	QueryTypeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_querier_query_type_total", // renamed for clarity
		Help: "Number of queries by type in querier",
	}, []string{"type", "system"})

	// New metric to track failed queries by type
	FailedQueryTypeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_querier_failed_query_types",
		Help: "Number of failed queries by type in querier",
	}, []string{"type", "system", "error_type"})

	ResultSizeHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dblogscomp_querier_result_size_bytes", // renamed for clarity
		Help:    "Histogram of query result sizes in querier",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 10), // from 1KB to ~1MB
	}, []string{"system"})

	ResultHitsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dblogscomp_querier_result_hits", // renamed for clarity
		Help:    "Histogram of query result hits in querier",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15), // from 1 to ~16K with a multiplier of 2
	}, []string{"system"})

	// Metrics by systems
	OperationCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_operations_total",
		Help: "Number of operations by type and system",
	}, []string{"type", "system"})

	// General performance metrics
	CurrentRPS = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dblogscomp_current_rps",
		Help: "Current requests per second",
	}, []string{"component"})

	// New metric to track generator throughput
	GeneratorThroughput = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dblogscomp_generator_throughput_logs_per_second",
		Help: "Current generator throughput in logs per second",
	}, []string{"system", "log_type"})

	// New metric to track connection errors
	ConnectionErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_connection_errors",
		Help: "Number of connection errors with logging systems",
	}, []string{"system", "error_type"})

	// Replacing existing metrics with unified ones
	LogTypeQueryCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_querier_log_type_queries",
		Help: "Number of queries by log type in querier",
	}, []string{"log_type", "system"})

	ErrorTypeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dblogscomp_error_total",
		Help: "Number of errors by type",
	}, []string{"error_type", "operation", "system"})

	SystemLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dblogscomp_system_latency_seconds",
		Help:    "Comparison of latency between different logging systems",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"system", "operation_type"})

	ResourceUsageGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dblogscomp_resource_usage",
		Help: "Resource usage during test execution",
	}, []string{"resource_type"}) // CPU, memory, goroutines, etc.
)

var (
	writeRequestsCount int64
	readRequestsCount  int64

	// Flag indicating that the metrics server has already been started
	metricsServerStarted bool
)

// rpsIncrementingMetric wrapper structure for CounterVec that increases RPS counter when Inc() is called
type rpsIncrementingMetric struct {
	*prometheus.CounterVec
}

func (r *rpsIncrementingMetric) WithLabelValues(labelValues ...string) prometheus.Counter {
	return &rpsIncrementingCounter{
		Counter: r.CounterVec.WithLabelValues(labelValues...),
	}
}

// rpsIncrementingCounter wrapper structure for Counter that increases RPS counter when Inc() is called
type rpsIncrementingCounter struct {
	prometheus.Counter
}

func (c *rpsIncrementingCounter) Inc() {
	c.Counter.Inc()
	atomic.AddInt64(&writeRequestsCount, 1)
}

func (c *rpsIncrementingCounter) Add(val float64) {
	c.Counter.Add(val)
	atomic.AddInt64(&writeRequestsCount, 1)
}

// IncrementWriteRequests increases the write requests counter
// system - logging system name (elasticsearch, loki, victoria)
// component - component (generator or querier)
func IncrementWriteRequests(system string) {
	if system == "" {
		system = "unknown"
	}
	WriteRequestsTotal.WithLabelValues(system).Inc()
	atomic.AddInt64(&writeRequestsCount, 1)
}

// IncrementReadRequests increases the read requests counter
// system - logging system name (elasticsearch, loki, victoria)
// component - component (generator or querier)
func IncrementReadRequests(system string) {
	if system == "" {
		system = "unknown"
	}
	ReadRequestsTotal.WithLabelValues(system).Inc()
	atomic.AddInt64(&readRequestsCount, 1)
}

// IncrementSuccessfulWrite increases the successful write requests counter
// system - logging system name (elasticsearch, loki, victoria)
// component - component (generator or querier)
func IncrementSuccessfulWrite(system string) {
	if system == "" {
		system = "unknown"
	}
	WriteRequestsSuccess.WithLabelValues(system).Inc()
}

// IncrementFailedWrite increases the failed write requests counter
// system - logging system name, errorType - error type
// component - component (generator or querier)
func IncrementFailedWrite(system, errorType string) {
	if system == "" {
		system = "unknown"
	}
	if errorType == "" {
		errorType = "unknown"
	}
	WriteRequestsFailure.WithLabelValues(system, errorType).Inc()
	ErrorTypeCounter.WithLabelValues(errorType, "write", system).Inc()
}

// IncrementSuccessfulRead increases the successful read requests counter
// system - logging system name (elasticsearch, loki, victoria)
// component - component (generator or querier)
func IncrementSuccessfulRead(system string) {
	if system == "" {
		system = "unknown"
	}
	ReadRequestsSuccess.WithLabelValues(system).Inc()
}

// IncrementFailedRead increases the failed read requests counter
// system - logging system name, errorType - error type
// component - component (generator or querier)
func IncrementFailedRead(system, errorType string) {
	if system == "" {
		system = "unknown"
	}
	if errorType == "" {
		errorType = "unknown"
	}
	ReadRequestsFailure.WithLabelValues(system, errorType).Inc()
	ErrorTypeCounter.WithLabelValues(errorType, "read", system).Inc()
}

// IncrementConnectionError increases the connection error counter
// system - logging system name, errorType - error type
// component - component (generator or querier)
func IncrementConnectionError(system, errorType string) {
	if system == "" {
		system = "unknown"
	}
	if errorType == "" {
		errorType = "unknown"
	}
	ConnectionErrors.WithLabelValues(system, errorType).Inc()
}

// ObserveWriteDuration records the execution time of a write request
// system - logging system name, status - request status (success/failure)
// component - component (generator or querier)
func ObserveWriteDuration(system, status string, duration float64) {
	if system == "" {
		system = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	WriteDurationHistogram.WithLabelValues(system, status).Observe(duration)
	SystemLatencyHistogram.WithLabelValues(system, "write").Observe(duration)
}

// ObserveReadDuration records the execution time of a read request
// system - logging system name, status - request status (success/failure)
// component - component (generator or querier)
func ObserveReadDuration(system, status string, duration float64) {
	if system == "" {
		system = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	ReadDurationHistogram.WithLabelValues(system, status).Observe(duration)
	SystemLatencyHistogram.WithLabelValues(system, "read").Observe(duration)
}

// IncrementQueryType increases the counter of queries by type
// system - logging system name, queryType - query type
func IncrementQueryType(system, queryType string) {
	if queryType == "" {
		queryType = "unknown"
	}
	if system == "" {
		system = "unknown"
	}
	QueryTypeCounter.WithLabelValues(queryType, system).Inc()
	OperationCounter.WithLabelValues("query", system).Inc()
}

// IncrementFailedQueryType increases the counter of failed queries by type
// system - logging system name, queryType - query type, errorType - error type
func IncrementFailedQueryType(system, queryType string, errorType string) {
	if queryType == "" {
		queryType = "unknown"
	}
	if system == "" {
		system = "unknown"
	}
	if errorType == "" {
		errorType = "unknown"
	}
	FailedQueryTypeCounter.WithLabelValues(queryType, system, errorType).Inc()
}

// IncrementLogType increases the counter of sent logs by type
// system - logging system name, logType - log type
func IncrementLogType(system, logType string) {
	if system == "" {
		system = "unknown"
	}
	if logType == "" {
		logType = "unknown"
	}
	WriteLogsTotal.WithLabelValues(logType, system).Inc()
	OperationCounter.WithLabelValues("write", system).Inc()
	// Count each log write operation as a write request
	atomic.AddInt64(&writeRequestsCount, 1)
}

// IncrementLogTypeQuery increases the counter of queries by log type
// system - logging system name, logType - log type
func IncrementLogTypeQuery(system, logType string) {
	if system == "" {
		system = "unknown"
	}
	if logType == "" {
		logType = "unknown"
	}
	LogTypeQueryCounter.WithLabelValues(logType, system).Inc()
}

// IncrementWriteOperation increases write operation counters without adding log type metrics
// Used for accounting batch operations without creating special "bulk" type metric
func IncrementWriteOperation(system string) {
	if system == "" {
		system = "unknown"
	}
	OperationCounter.WithLabelValues("write", system).Inc()
	// Count each write operation as a write request for RPS calculation
	atomic.AddInt64(&writeRequestsCount, 1)
}

// UpdateGeneratorThroughput updates the generator throughput metric
// system - logging system name, logType - log type, logsPerSec - logs per second
func UpdateGeneratorThroughput(system, logType string, logsPerSec float64) {
	if system == "" {
		system = "unknown"
	}
	if logType == "" {
		logType = "unknown"
	}
	GeneratorThroughput.WithLabelValues(system, logType).Set(logsPerSec)
}

// RecordWriteBatchSize records the log batch size
func RecordWriteBatchSize(size int) {
	WriteBatchSizeGauge.Set(float64(size))
}

// RecordResourceUsage records resource usage
// resourceType - resource type (cpu, memory, goroutines)
// value - usage value
// component - component (generator, querier or general)
func RecordResourceUsage(resourceType string, value float64) {
	if resourceType == "" {
		resourceType = "unknown"
	}
	ResourceUsageGauge.WithLabelValues(resourceType).Set(value)
}

// InitPrometheus initializes metrics registration
func InitPrometheus(bulkSize int) {
	// Start a separate goroutine to update metrics in real time
	go updateRealTimeMetrics()

	// Set the initial value for the log batch size if it's a generator
	if bulkSize > 0 {
		RecordWriteBatchSize(bulkSize)
	}

}

// updateRealTimeMetrics updates metrics in real time
func updateRealTimeMetrics() {
	lastWriteRequests := int64(0)
	lastReadRequests := int64(0)
	lastTime := time.Now()

	for {
		time.Sleep(1 * time.Second)

		now := time.Now()
		elapsed := now.Sub(lastTime).Seconds()
		lastTime = now

		// Get the current values from atomic counters
		currentWriteRequests := atomic.LoadInt64(&writeRequestsCount)
		currentReadRequests := atomic.LoadInt64(&readRequestsCount)

		// Calculate RPS and QPS
		writeRequests := float64(currentWriteRequests - lastWriteRequests)
		readRequests := float64(currentReadRequests - lastReadRequests)

		rps := writeRequests / elapsed
		qps := readRequests / elapsed

		// Update metrics with component labels
		CurrentRPS.WithLabelValues("generator").Set(rps)
		CurrentRPS.WithLabelValues("querier").Set(qps)

		// Update general operation counters if there's activity
		// Removed updating OperationCounter for system="total" to fix dblogscomp_operations_total metric

		// Store values for the next cycle
		lastWriteRequests = currentWriteRequests
		lastReadRequests = currentReadRequests
	}
}

// StartMetricsServer starts the HTTP server for Prometheus metrics
func StartMetricsServer(port int) {
	// Check if the metrics server has already been started
	if metricsServerStarted {
		fmt.Println("Metrics server is already running, skipping initialization")
		return
	}

	// IMPORTANT: use the standard promhttp.Handler() instead of HandlerFor
	// for proper registration of promhttp_* metrics in the registry
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		addr := fmt.Sprintf(":%d", port)
		fmt.Printf("Metrics server started at %s/metrics\n", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("Error starting metrics server: %v\n", err)
		}
	}()

	// Set the flag indicating that the metrics server has been started
	metricsServerStarted = true
}
