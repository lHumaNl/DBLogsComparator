package pkg

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
)

// Declare variables for metrics, but don't initialize them immediately
// This will help avoid conflicts during initialization
var (
	RequestsTotal   *prometheus.CounterVec
	LogsTotal       *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	RetryCounter    prometheus.Counter
	RPSGauge        prometheus.Gauge
	LPSGauge        prometheus.Gauge
	BatchSizeGauge  prometheus.Gauge

	// Flag indicating whether metrics have been initialized
	metricsInitialized bool
)

// InitPrometheus initializes Prometheus metrics
func InitPrometheus(config Config) {
	// Check if metrics have already been initialized
	if metricsInitialized {
		return
	}

	// Use metrics from common if they're already initialized
	// This prevents duplicate registration
	RequestsTotal = common.RequestsTotal
	LogsTotal = common.LogsTotal
	RequestDuration = common.RequestDuration
	RetryCounter = common.RetryCounter
	RPSGauge = common.RPSGauge
	LPSGauge = common.LPSGauge
	BatchSizeGauge = common.BatchSizeGauge

	// Call metrics initialization from the common package
	commonConfig := &common.Config{
		Generator: common.GeneratorConfig{
			BulkSize: config.BulkSize,
		},
	}
	common.InitGeneratorMetrics(commonConfig)

	// Set initialization flag
	metricsInitialized = true
}

// StartMetricsServer starts an HTTP server for Prometheus metrics
func StartMetricsServer(metricsPort int, config Config) {
	// Use common implementation of metrics server
	common.StartMetricsServer(metricsPort)
}
