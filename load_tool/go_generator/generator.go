package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
)

func main() {
	// Set maximum number of processors
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Determine optimal number of workers
	// By default - number of available CPUs * 2
	cpuCount := runtime.NumCPU()
	defaultWorkers := cpuCount * 2

	// If CPU limit is set for container, use it
	cpuLimit := os.Getenv("CPU_LIMIT")
	if cpuLimit != "" {
		if limit, err := strconv.ParseFloat(cpuLimit, 64); err == nil && limit > 0 {
			defaultWorkers = int(limit * 2)
		}
	}

	// Parse command line arguments
	mode := flag.String("mode", "victoria", "Operation mode: victoria (VictoriaLogs), es (Elasticsearch), loki (Loki)")
	baseURL := flag.String("url", "http://localhost:8428", "Base URL for sending logs")
	rps := flag.Int("rps", 10, "Requests per second")
	duration := flag.Duration("duration", 1*time.Minute, "Test duration")
	bulkSize := flag.Int("bulk-size", 100, "Number of logs in one request")
	// workerCount flag removed - using runtime.NumCPU() * 4
	connectionCount := flag.Int("connection-count", 10, "Number of HTTP connections")

	// Log type distribution
	webAccessWeight := flag.Int("web-access-weight", 60, "Weight for web_access logs")
	webErrorWeight := flag.Int("web-error-weight", 10, "Weight for web_error logs")
	applicationWeight := flag.Int("application-weight", 20, "Weight for application logs")
	metricWeight := flag.Int("metric-weight", 5, "Weight for metric logs")
	eventWeight := flag.Int("event-weight", 5, "Weight for event logs")

	verbose := flag.Bool("verbose", false, "Verbose output")
	maxRetries := flag.Int("max-retries", 3, "Maximum number of retry attempts")
	retryDelay := flag.Duration("retry-delay", 500*time.Millisecond, "Delay between retry attempts")

	enableMetrics := flag.Bool("enable-metrics", false, "Enable Prometheus metrics")
	metricsPort := flag.Int("metrics-port", 9090, "Prometheus metrics port")

	flag.Parse()

	// Configure log type distribution
	logTypeDistribution := map[string]int{
		"web_access":  *webAccessWeight,
		"web_error":   *webErrorWeight,
		"application": *applicationWeight,
		"metric":      *metricWeight,
		"event":       *eventWeight,
	}

	// Create configuration
	config := pkg.Config{
		Mode:     *mode,
		BaseURL:  *baseURL,
		URL:      *baseURL,
		RPS:      *rps,
		Duration: *duration,
		BulkSize: *bulkSize,
		// WorkerCount removed - using CPU-based allocation
		// ConnectionCount removed - using dynamic CPU-based allocation
		LogTypeDistribution: logTypeDistribution,
		Verbose:             *verbose,
		MaxRetries:          *maxRetries,
		RetryDelay:          *retryDelay,
		EnableMetrics:       *enableMetrics,
		MetricsPort:         *metricsPort,
	}

	// Output configuration information
	if config.Verbose {
		fmt.Printf("=== Log Generator ===\n")
		fmt.Printf("Mode: %s\n", config.Mode)
		fmt.Printf("URL: %s\n", config.URL)
		fmt.Printf("RPS: %d\n", config.RPS)
		fmt.Printf("Duration: %s\n", config.Duration)
		fmt.Printf("Bulk size: %d\n", config.BulkSize)
		// Worker count info removed - dynamic allocation
		fmt.Printf("HTTP connections: %d\n", config.GetConnectionCount())
		fmt.Printf("Log type distribution: %v\n", config.LogTypeDistribution)
		fmt.Printf("Retry attempts: %d\n", config.MaxRetries)
		fmt.Printf("Retry delay: %s\n", config.RetryDelay)
		fmt.Printf("Prometheus metrics: %v\n", config.EnableMetrics)
		if config.EnableMetrics {
			fmt.Printf("Metrics port: %d\n", config.MetricsPort)
		}
		fmt.Printf("=======================\n\n")
	}

	// Create appropriate log database
	db, err := logdb.CreateLogDB(config.Mode, config.BaseURL, logdb.Options{
		BatchSize:  config.BulkSize,
		Timeout:    10 * time.Second,
		RetryCount: config.MaxRetries,
		RetryDelay: config.RetryDelay,
		Verbose:    config.Verbose,
	})

	if err != nil {
		log.Fatalf("Error creating log database: %v", err)
	}

	// Start log generator
	if err := pkg.RunGenerator(config, db); err != nil {
		log.Fatalf("Error starting generator: %v", err)
	}
}
