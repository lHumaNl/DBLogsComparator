package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/logdb"
	generator "github.com/dblogscomparator/DBLogsComparator/load_tool/go_generator/pkg"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

const (
	modeGeneratorOnly = "generator"
	modeQuerierOnly   = "querier"
	modeCombined      = "combined"

	systemLoki     = "loki"
	systemES       = "elasticsearch"
	systemVictoria = "victoria"
)

// Default command line parameters
const (
	defaultConfigPath = "config.yaml"
	defaultHostsPath  = "db_hosts.yaml"
)

func main() {
	// Command line parameters
	configPath := flag.String("config", defaultConfigPath, "Path to YAML configuration file")
	hostsPath := flag.String("hosts", defaultHostsPath, "Path to file with logging system URLs")
	legacyMode := flag.Bool("legacy", false, "Use command line arguments mode instead of YAML")
	defaultConfig := flag.Bool("default-config", false, "Create default configuration and exit")

	// Add new flags
	system := flag.String("system", "", "Logging system: loki, elasticsearch, victoria (required parameter)")
	metricsPort := flag.Int("metrics-port", 9090, "Port for Prometheus metrics server")
	mode := flag.String("mode", "generator", "Operation mode: generator, querier, combined")

	flag.Parse()

	// Check for required system parameter
	if *system == "" && !*defaultConfig && !*legacyMode {
		// Check if LOG_DB environment variable exists
		envSystem := os.Getenv("LOG_DB")
		if envSystem != "" {
			*system = envSystem
			fmt.Printf("Using logging system from LOG_DB environment variable: %s\n", *system)
		} else {
			fmt.Println("Error: logging system not specified")
			fmt.Println("Specify system using -system flag or LOG_DB environment variable")
			fmt.Println("Available systems: loki, elasticsearch, victoria")
			flag.Usage()
			os.Exit(1)
		}
	}

	// Create default configuration if requested
	if *defaultConfig {
		config := getDefaultConfig()
		if err := common.SaveConfig(config, *configPath); err != nil {
			log.Fatalf("Error creating default configuration: %v", err)
		}
		fmt.Printf("Default configuration created in file: %s\n", *configPath)
		return
	}

	// Determine directory with current executable
	execDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalf("Failed to determine current directory: %v", err)
	}

	// If legacy mode requested, use command line arguments
	if *legacyMode {
		log.Println("Starting in legacy mode using command line arguments")
		runInLegacyMode()
		return
	}

	// Load configuration from YAML
	configFullPath := *configPath
	if !filepath.IsAbs(configFullPath) {
		configFullPath = filepath.Join(execDir, *configPath)
	}

	config, err := common.LoadConfig(configFullPath)
	if err != nil {
		// If file doesn't exist and it's the default path, create default configuration
		if os.IsNotExist(err) && *configPath == defaultConfigPath {
			fmt.Println("Configuration file not found, creating default configuration")
			config = getDefaultConfig()
			if err := common.SaveConfig(config, configFullPath); err != nil {
				log.Fatalf("Error creating default configuration: %v", err)
			}
			fmt.Printf("Default configuration created in file: %s\n", configFullPath)
		} else {
			log.Fatalf("Error loading configuration: %v", err)
		}
	}

	// Set system from command line or environment variable
	if *system != "" {
		// Check that a valid system is specified
		switch *system {
		case systemLoki:
			config.System = *system
			fmt.Printf("Using logging system: %s\n", *system)
		case systemES, "es", "elk":
			config.System = systemES
			fmt.Printf("Using logging system: %s\n", systemES)
		case systemVictoria, "victorialogs":
			config.System = systemVictoria
			fmt.Printf("Using logging system: %s\n", systemVictoria)
		default:
			log.Fatalf("Unknown logging system: %s", *system)
		}
	}

	// Check that system is specified
	if config.System == "" {
		log.Fatalf("Logging system not specified. Use -system flag or specify in configuration file")
	}

	// Set operation mode from command line
	if *mode != "" {
		// Check that a valid mode is specified
		switch *mode {
		case modeGeneratorOnly, modeQuerierOnly, modeCombined:
			config.Mode = *mode
			fmt.Printf("Using operation mode: %s\n", *mode)
		default:
			log.Fatalf("Unknown operation mode: %s", *mode)
		}
	}

	// Set metrics port from command line
	if *metricsPort != 0 {
		config.MetricsPort = *metricsPort
	}

	// Load hosts configuration from file
	hostsFullPath := *hostsPath
	if !filepath.IsAbs(hostsFullPath) {
		hostsFullPath = filepath.Join(execDir, *hostsPath)
	}

	hostsConfig, err := common.LoadHostsConfig(hostsFullPath)
	if err != nil {
		log.Printf("Warning: failed to load hosts file: %v", err)
		log.Printf("URLs from main configuration file or default values will be used")
	} else {
		// Apply loaded URLs to main configuration
		// In Go, structures cannot be nil, so we check the operation mode
		if config.Mode == modeGeneratorOnly || config.Mode == modeCombined {
			config.Generator.URLLoki = hostsConfig.URLLoki
			config.Generator.URLES = hostsConfig.URLES
			config.Generator.URLVictoria = hostsConfig.URLVictoria
		}
		if config.Mode == modeQuerierOnly || config.Mode == modeCombined {
			config.Querier.URLLoki = hostsConfig.URLLoki
			config.Querier.URLES = hostsConfig.URLES
			config.Querier.URLVictoria = hostsConfig.URLVictoria
		}
		fmt.Printf("System URLs loaded from file: %s\n", hostsFullPath)
	}

	// Create structure for statistics
	stats := &common.Stats{
		StartTime: time.Now(),
	}

	// Initialize Prometheus metrics if enabled
	if config.Metrics {
		common.InitPrometheus(config.Generator.BulkSize)
		common.StartMetricsServer(config.MetricsPort)
	}

	// Start required components depending on mode
	switch config.Mode {
	case modeGeneratorOnly:
		fmt.Println("Starting in log generator mode")
		runGeneratorWithConfig(config, stats)
	case modeQuerierOnly:
		fmt.Println("Starting in query client mode")
		if err := runQuerierWithConfig(config, stats); err != nil {
			log.Fatalf("Error starting query client: %v", err)
		}
	case modeCombined:
		fmt.Println("Starting in combined mode (generator and queries)")
		go runGeneratorWithConfig(config, stats)
		if err := runQuerierWithConfig(config, stats); err != nil {
			log.Fatalf("Error starting query client: %v", err)
		}
	default:
		log.Fatalf("Unknown operation mode: %s", config.Mode)
	}
}

// Create default configuration
func getDefaultConfig() *common.Config {
	return &common.Config{
		Mode:            "generator",
		System:          "", // Must be specified via CLI or environment variable
		DurationSeconds: 60,
		Metrics:         true,
		MetricsPort:     9090,
		Generator: common.GeneratorConfig{
			URLLoki:         "http://loki:3100",
			URLES:           "http://elasticsearch:9200",
			URLVictoria:     "http://victorialogs:9428",
			RPS:             100,
			BulkSize:        10,
			WorkerCount:     4,
			ConnectionCount: 10,
			Distribution: map[string]int{
				"web_access":  60,
				"web_error":   10,
				"application": 20,
				"event":       5,
				"metric":      5,
			},
			MaxRetries:   3,
			RetryDelayMs: 500,
			Verbose:      false,
		},
		Querier: common.QuerierConfig{
			URLLoki:      "http://loki:3100",
			URLES:        "http://elasticsearch:9200",
			URLVictoria:  "http://victorialogs:9428",
			RPS:          10,
			WorkerCount:  2,
			MaxRetries:   3,
			RetryDelayMs: 500,
			Verbose:      false,
			Distribution: map[string]int{
				"exact_match":   30,
				"time_range":    40,
				"label_filter":  20,
				"complex_query": 10,
			},
		},
	}
}

// Running in command line arguments mode (legacy)
func runInLegacyMode() {
	// Main flags
	mode := flag.String("mode", "generator", "Operation mode: generator (generation only), querier (queries only), combined (generation and queries)")
	system := flag.String("system", "victoria", "Logging system: loki, elasticsearch, victoria")

	// Common parameters
	duration := flag.Int("duration", 60, "Operation duration (in seconds, 0 = infinite)")

	// Parameters for generation mode
	rps := flag.Int("rps", 100, "Requests per second (for generator)")
	bulkSize := flag.Int("bulk-size", 10, "Number of logs in one request")
	workerCount := flag.Int("worker-count", 4, "Number of parallel workers")
	connectionCount := flag.Int("connection-count", 10, "Number of connections")
	maxRetries := flag.Int("max-retries", 3, "Maximum number of retry attempts")
	retryDelay := flag.Int("retry-delay", 500, "Delay between retry attempts (in milliseconds)")
	verbose := flag.Bool("verbose", false, "Verbose output")

	// Log type weights
	webAccessWeight := flag.Int("web-access-weight", 60, "Weight of web_access logs")
	webErrorWeight := flag.Int("web-error-weight", 10, "Weight of web_error logs")
	applicationWeight := flag.Int("application-weight", 20, "Weight of application logs")
	eventWeight := flag.Int("event-weight", 5, "Weight of event logs")
	metricWeight := flag.Int("metric-weight", 5, "Weight of metric logs")

	// Parameters for query mode
	qps := flag.Int("qps", 10, "Queries per second (for query client)")

	// Query type weights
	exactMatchWeight := flag.Int("exact-match-weight", 30, "Weight of exact_match queries")
	timeRangeWeight := flag.Int("time-range-weight", 40, "Weight of time_range queries")
	labelFilterWeight := flag.Int("label-filter-weight", 20, "Weight of label_filter queries")
	complexQueryWeight := flag.Int("complex-query-weight", 10, "Weight of complex_query queries")

	// Metrics parameters
	metricsEnabled := flag.Bool("enable-metrics", true, "Enable Prometheus metrics")
	metricsPort := flag.Int("metrics-port", 9090, "Port for Prometheus metrics server")

	// Re-parse flags, as they were already parsed above
	flag.CommandLine.Parse(os.Args[1:])

	// Create configuration based on command line arguments
	config := &common.Config{
		Mode:            *mode,
		System:          *system,
		DurationSeconds: *duration,
		Metrics:         *metricsEnabled,
		MetricsPort:     *metricsPort,
		Generator: common.GeneratorConfig{
			URLLoki:         "http://loki:3100",
			URLES:           "http://elasticsearch:9200",
			URLVictoria:     "http://victorialogs:9428",
			RPS:             *rps,
			BulkSize:        *bulkSize,
			WorkerCount:     *workerCount,
			ConnectionCount: *connectionCount,
			Distribution: map[string]int{
				"web_access":  *webAccessWeight,
				"web_error":   *webErrorWeight,
				"application": *applicationWeight,
				"event":       *eventWeight,
				"metric":      *metricWeight,
			},
			MaxRetries:   *maxRetries,
			RetryDelayMs: *retryDelay,
			Verbose:      *verbose,
		},
		Querier: common.QuerierConfig{
			URLLoki:      "http://loki:3100",
			URLES:        "http://elasticsearch:9200",
			URLVictoria:  "http://victorialogs:9428",
			RPS:          *qps,
			WorkerCount:  *workerCount,
			MaxRetries:   *maxRetries,
			RetryDelayMs: *retryDelay,
			Verbose:      *verbose,
			Distribution: map[string]int{
				"exact_match":   *exactMatchWeight,
				"time_range":    *timeRangeWeight,
				"label_filter":  *labelFilterWeight,
				"complex_query": *complexQueryWeight,
			},
		},
	}

	// Create structure for statistics
	stats := &common.Stats{
		StartTime: time.Now(),
	}

	// Start required components depending on mode
	switch *mode {
	case modeGeneratorOnly:
		fmt.Println("Starting in log generator mode (legacy)")
		runGeneratorWithConfig(config, stats)
	case modeQuerierOnly:
		fmt.Println("Starting in query client mode (legacy)")
		runQuerierWithConfig(config, stats)
	case modeCombined:
		fmt.Println("Starting in combined mode - generator and queries (legacy)")
		go runGeneratorWithConfig(config, stats)
		runQuerierWithConfig(config, stats)
	default:
		log.Fatalf("Unknown operation mode: %s", *mode)
	}
}

// Function to start log generator with new configuration
func runGeneratorWithConfig(cfg *common.Config, stats *common.Stats) {
	generatorConfig := generator.Config{
		Mode:                "default",
		BaseURL:             cfg.Generator.GetURL(cfg.System),
		URL:                 cfg.Generator.GetURL(cfg.System),
		RPS:                 cfg.Generator.RPS,
		Duration:            time.Duration(cfg.DurationSeconds) * time.Second,
		BulkSize:            cfg.Generator.BulkSize,
		WorkerCount:         cfg.Generator.WorkerCount,
		ConnectionCount:     cfg.Generator.ConnectionCount,
		LogTypeDistribution: cfg.Generator.Distribution, // Adding log type distribution from configuration
		MaxRetries:          cfg.Generator.MaxRetries,
		RetryDelay:          time.Duration(cfg.Generator.RetryDelayMs) * time.Millisecond,
		Verbose:             cfg.Generator.Verbose,
		EnableMetrics:       cfg.Metrics,
		MetricsPort:         cfg.MetricsPort,
	}

	// Output information about log type distribution
	fmt.Println("Log type distribution:", generatorConfig.LogTypeDistribution)

	// Enable metrics, if needed
	if cfg.Metrics {
		common.InitPrometheus(cfg.Generator.BulkSize)
		// Removing StartMetricsServer call, as it's already started in main.go
		// common.StartMetricsServer(cfg.MetricsPort)
	}

	// Create database client
	options := logdb.Options{
		BatchSize:  generatorConfig.BulkSize,
		Timeout:    30 * time.Second, // Request timeout
		RetryCount: generatorConfig.MaxRetries,
		RetryDelay: generatorConfig.RetryDelay,
		Verbose:    generatorConfig.Verbose,
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.URL, options)
	if err != nil {
		log.Fatalf("Error creating database client: %v", err)
	}

	fmt.Printf("[Generator] Starting with RPS=%d, system=%s\n", cfg.Generator.RPS, cfg.System)

	// Start generator
	if err := generator.RunGenerator(generatorConfig, db); err != nil {
		log.Fatalf("Error starting generator: %v", err)
	}
}

// runQuerierWithConfig starts the query client with new configuration from YAML
func runQuerierWithConfig(config *common.Config, stats *common.Stats) error {
	log.Printf("Starting query client for system %s\n", config.System)

	// Determine URL for selected system
	var baseURL string
	var systemForExecutor string
	switch config.System {
	case "victorialogs", "victoria":
		baseURL = config.Querier.URLVictoria
		systemForExecutor = "victoria"
	case "elasticsearch", "es", "elk":
		baseURL = config.Querier.URLES
		systemForExecutor = "es"
	case "loki":
		baseURL = config.Querier.URLLoki
		systemForExecutor = "loki"
	default:
		return fmt.Errorf("unknown logging system: %s", config.System)
	}

	// Create options for query executor
	options := models.Options{
		Timeout:    10 * time.Second, // Default 10 seconds
		RetryCount: config.Querier.MaxRetries,
		RetryDelay: time.Duration(config.Querier.RetryDelayMs) * time.Millisecond,
		Verbose:    config.Querier.Verbose,
	}

	// Create query executor for selected system
	log.Printf("Debug: Creating query executor with system=%s, baseURL=%s", systemForExecutor, baseURL)
	executor, err := go_querier.CreateQueryExecutor(systemForExecutor, baseURL, options, 1)
	if err != nil {
		return fmt.Errorf("error creating query executor: %v", err)
	}

	// Create configuration for query module
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             config.Querier.RPS,
		DurationSeconds: config.DurationSeconds,
		WorkerCount:     config.Querier.WorkerCount,
		QueryTypeDistribution: map[models.QueryType]int{
			models.SimpleQuery:     config.Querier.Distribution["simple"],
			models.ComplexQuery:    config.Querier.Distribution["complex"],
			models.AnalyticalQuery: config.Querier.Distribution["analytical"],
			models.TimeSeriesQuery: config.Querier.Distribution["timeseries"],
			models.StatQuery:       config.Querier.Distribution["stat"],
			models.TopKQuery:       config.Querier.Distribution["topk"],
		},
		QueryTimeout: 10 * time.Second, // Default 10 seconds
		MaxRetries:   config.Querier.MaxRetries,
		RetryDelayMs: config.Querier.RetryDelayMs,
		Verbose:      config.Querier.Verbose,
	}

	log.Printf("[Queries] Starting with QPS=%d, system=%s\n", config.Querier.RPS, config.System)

	// Start query module
	return go_querier.RunQuerier(queryConfig, executor, stats)
}
