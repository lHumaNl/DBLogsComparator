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

var (
	// Metrics for write operations
	WriteRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_total",
		Help: "Total number of write requests",
	})

	WriteRequestsSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_success",
		Help: "Number of successful write requests",
	})

	WriteRequestsFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_failure",
		Help: "Number of failed write requests",
	})

	WriteLogsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_logs_total",
		Help: "Total number of logs sent",
	})

	WriteRequestsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_write_requests_retried",
		Help: "Number of retried write requests",
	})

	WriteDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_write_duration_seconds",
		Help:    "Histogram of write request durations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	})

	// Metrics for read operations
	ReadRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_total",
		Help: "Total number of read requests",
	})

	ReadRequestsSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_success",
		Help: "Number of successful read requests",
	})

	ReadRequestsFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_failure",
		Help: "Number of failed read requests",
	})

	ReadRequestsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "load_test_read_requests_retried",
		Help: "Number of retried read requests",
	})

	ReadDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_read_duration_seconds",
		Help:    "Histogram of read request durations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	})

	QueryTypeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_test_query_type_total",
		Help: "Number of requests by type",
	}, []string{"type"})

	ResultSizeHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_result_size_bytes",
		Help:    "Histogram of query result sizes",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 10), // from 1KB to ~1MB
	})

	ResultHitsHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "load_test_result_hits",
		Help:    "Histogram of number of results in queries",
		Buckets: prometheus.LinearBuckets(0, 10, 10), // from 0 to 90 with step 10
	})

	// Metrics by system
	OperationCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "load_test_operations_total",
		Help: "Number of operations by type and system",
	}, []string{"type", "system"})

	// General performance metrics
	CurrentRPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_rps",
		Help: "Current number of write requests per second",
	})

	CurrentQPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_qps",
		Help: "Current number of read requests per second",
	})

	CurrentOPS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "load_test_current_ops",
		Help: "Total current number of operations per second",
	})

	// Log generator metrics from pkg/metrics.go
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_requests_total",
			Help: "Total number of requests sent by the log generator",
		},
		[]string{"status", "destination"},
	)

	LogsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_logs_total",
			Help: "Total number of generated logs by type",
		},
		[]string{"log_type", "destination"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "log_generator_request_duration_seconds",
			Help:    "Request execution time",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status", "destination"},
	)

	RetryCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "log_generator_retry_count",
			Help: "Number of request retries",
		},
	)

	RPSGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_rps",
			Help: "Current number of requests per second",
		},
	)

	LPSGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_lps",
			Help: "Current number of logs per second",
		},
	)

	BatchSizeGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_batch_size",
			Help: "Log batch size",
		},
	)
)

var (
	writeRequestsCount int64
	readRequestsCount  int64

	// Flag indicating whether the metrics server has already been started
	metricsServerStarted bool
)

// IncrementWriteRequests increments the write requests counter
func IncrementWriteRequests() {
	WriteRequestsTotal.Inc()
	atomic.AddInt64(&writeRequestsCount, 1)
}

// IncrementReadRequests increments the read requests counter
func IncrementReadRequests() {
	ReadRequestsTotal.Inc()
	atomic.AddInt64(&readRequestsCount, 1)
}

// IncrementSuccessfulWrite increments the successful write requests counter
func IncrementSuccessfulWrite() {
	WriteRequestsSuccess.Inc()
}

// IncrementFailedWrite increments the failed write requests counter
func IncrementFailedWrite() {
	WriteRequestsFailure.Inc()
}

// IncrementSuccessfulRead increments the successful read requests counter
func IncrementSuccessfulRead() {
	ReadRequestsSuccess.Inc()
}

// IncrementFailedRead increments the failed read requests counter
func IncrementFailedRead() {
	ReadRequestsFailure.Inc()
}

// InitPrometheus initializes metrics registration
func InitPrometheus() {
	// Start a separate goroutine for real-time metrics updates
	go updateRealTimeMetrics()
}

// updateRealTimeMetrics updates metrics in real-time
func updateRealTimeMetrics() {
	lastWriteRequests := int64(0)
	lastReadRequests := int64(0)
	lastTime := time.Now()

	for {
		time.Sleep(1 * time.Second)

		now := time.Now()
		elapsed := now.Sub(lastTime).Seconds()
		lastTime = now

		// Get current values from Prometheus counters
		// Use metrics from the common package instead of direct Prometheus access
		currentWriteRequests := writeRequestsCount
		currentReadRequests := readRequestsCount

		// Calculate RPS and QPS
		writeRequests := float64(currentWriteRequests - lastWriteRequests)
		readRequests := float64(currentReadRequests - lastReadRequests)

		rps := writeRequests / elapsed
		qps := readRequests / elapsed
		ops := (writeRequests + readRequests) / elapsed

		// Update metrics
		CurrentRPS.Set(rps)
		CurrentQPS.Set(qps)
		CurrentOPS.Set(ops)

		// Remember values for the next cycle
		lastWriteRequests = currentWriteRequests
		lastReadRequests = currentReadRequests
	}
}

// StartMetricsServer starts an HTTP server for Prometheus metrics
func StartMetricsServer(port int) {
	// Check if the metrics server has already been started
	if metricsServerStarted {
		fmt.Println("Metrics server already started, skipping initialization")
		return
	}

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

// InitGeneratorMetrics initializes metrics for the log generator
func InitGeneratorMetrics(config *Config) {
	// Set the initial value for the log batch size
	if config.Generator.BulkSize > 0 {
		BatchSizeGauge.Set(float64(config.Generator.BulkSize))
	}
}
