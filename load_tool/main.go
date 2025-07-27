package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
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

	// Load testing modes
	loadModeStability = "stability"
	loadModeMaxPerf   = "maxPerf"
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
	loadMode := flag.String("load-mode", "stability", "Load testing mode: stability, maxPerf")

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

	// Set and validate load mode from command line
	if *loadMode != "" {
		switch *loadMode {
		case loadModeStability, loadModeMaxPerf:
			config.LoadMode = *loadMode
			fmt.Printf("Using load testing mode: %s\n", *loadMode)
		default:
			log.Fatalf("Unknown load testing mode: %s. Valid options: stability, maxPerf", *loadMode)
		}
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

	// Initialize structured logger
	verbose := (config.Mode == modeGeneratorOnly && config.Generator.Verbose) ||
		(config.Mode == modeQuerierOnly && config.Querier.Verbose) ||
		(config.Mode == modeCombined && (config.Generator.Verbose || config.Querier.Verbose))
	common.InitTextLogger(verbose)

	// Initialize Prometheus metrics if enabled
	if config.Metrics {
		common.InitPrometheus(config.System, config.Generator.BulkSize)
		common.StartMetricsServer(config.MetricsPort)
		fmt.Printf("Prometheus metrics available at: http://localhost:%d/metrics\n", config.MetricsPort)
	}

	// Start required components depending on mode and load testing mode
	switch config.LoadMode {
	case loadModeMaxPerf:
		fmt.Println("Starting maxPerf load testing mode")
		if err := runMaxPerfTest(config, stats); err != nil {
			log.Fatalf("Error in maxPerf test: %v", err)
		}
	case loadModeStability:
		fmt.Println("Starting stability load testing mode")
		if err := runStabilityTest(config, stats); err != nil {
			log.Fatalf("Error in stability test: %v", err)
		}
	default:
		// Fallback to original mode logic for compatibility
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

			// Create context for coordinating both components
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup

			// Start generator in background
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer log.Println("Generator stopped")
				runGeneratorWithContextConfig(ctx, config, stats)
			}()

			// Start querier in foreground
			if err := runQuerierWithConfig(config, stats); err != nil {
				log.Printf("Querier error: %v", err)
				cancel() // Stop generator
			} else {
				cancel() // Stop generator when querier finishes normally
			}

			// Wait for generator to finish
			wg.Wait()
			log.Println("Combined mode finished")
		default:
			log.Fatalf("Unknown operation mode: %s", config.Mode)
		}
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
			URLLoki:     "http://loki:3100",
			URLES:       "http://elasticsearch:9200",
			URLVictoria: "http://victorialogs:9428",
			RPS:         100,
			BulkSize:    10,
			// WorkerCount removed - using CPU-based allocation
			// ConnectionCount removed - using dynamic CPU-based allocation (runtime.NumCPU() * 4)
			Distribution: map[string]int{
				"web_access":  60,
				"web_error":   10,
				"application": 20,
				"event":       5,
				"metric":      5,
			},
			MaxRetries:   3,
			RetryDelayMs: 500,
			TimeoutMs:    30000, // 30 seconds
			Verbose:      false,
			// Load testing configurations
			MaxPerf: common.LoadTestConfig{
				Steps:            20,
				StepDuration:     30,
				Impact:           5,
				BaseRPS:          10,
				StartPercent:     10,
				IncrementPercent: 10,
			},
			Stability: common.LoadTestConfig{
				StepDuration: 60,
				Impact:       5,
				BaseRPS:      10,
				StepPercent:  100,
			},
		},
		Querier: common.QuerierConfig{
			URLLoki:     "http://loki:3100",
			URLES:       "http://elasticsearch:9200",
			URLVictoria: "http://victorialogs:9428",
			RPS:         10,
			// WorkerCount removed - using CPU-based allocation
			MaxRetries:   3,
			RetryDelayMs: 500,
			TimeoutMs:    10000, // 10 seconds
			Verbose:      false,
			Distribution: map[string]int{
				"exact_match":   30,
				"time_range":    40,
				"label_filter":  20,
				"complex_query": 10,
			},
			// Load testing configurations
			MaxPerf: common.LoadTestConfig{
				Steps:            20,
				StepDuration:     30,
				Impact:           5,
				BaseRPS:          10,
				StartPercent:     10,
				IncrementPercent: 10,
			},
			Stability: common.LoadTestConfig{
				StepDuration: 60,
				Impact:       5,
				BaseRPS:      10,
				StepPercent:  100,
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
	// workerCount flag removed - using CPU-based allocation
	// connectionCount flag removed - using dynamic CPU-based allocation
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
			URLLoki:     "http://loki:3100",
			URLES:       "http://elasticsearch:9200",
			URLVictoria: "http://victorialogs:9428",
			RPS:         *rps,
			BulkSize:    *bulkSize,
			// WorkerCount removed - using CPU-based allocation
			// ConnectionCount removed - using dynamic CPU-based allocation
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
			URLLoki:     "http://loki:3100",
			URLES:       "http://elasticsearch:9200",
			URLVictoria: "http://victorialogs:9428",
			RPS:         *qps,
			// WorkerCount removed - using CPU-based allocation
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

		// Create context for coordinating both components
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup

		// Start generator in background
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer log.Println("Generator stopped (legacy)")
			runGeneratorWithContextConfig(ctx, config, stats)
		}()

		// Start querier in foreground
		if err := runQuerierWithConfig(config, stats); err != nil {
			log.Printf("Querier error (legacy): %v", err)
			cancel() // Stop generator
		} else {
			cancel() // Stop generator when querier finishes normally
		}

		// Wait for generator to finish
		wg.Wait()
		log.Println("Combined mode finished (legacy)")
	default:
		log.Fatalf("Unknown operation mode: %s", *mode)
	}
}

// Function to start log generator with new configuration
func runGeneratorWithConfig(cfg *common.Config, stats *common.Stats) {
	generatorConfig := generator.Config{
		Mode:     "default",
		BaseURL:  cfg.Generator.GetURL(cfg.System),
		URL:      cfg.Generator.GetURL(cfg.System),
		RPS:      cfg.Generator.RPS,
		Duration: time.Duration(cfg.DurationSeconds) * time.Second,
		BulkSize: cfg.Generator.BulkSize,
		// WorkerCount removed - using CPU-based allocation
		// ConnectionCount removed - using dynamic CPU-based allocation in logdb
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
		common.InitPrometheus(cfg.System, cfg.Generator.BulkSize)
		// Removing StartMetricsServer call, as it's already started in main.go
		// common.StartMetricsServer(cfg.MetricsPort)
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         cfg.Generator.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay,
		Verbose:         generatorConfig.Verbose,
		ConnectionCount: cfg.Generator.GetConnectionCount(),
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

// runGeneratorWithContextConfig starts the generator with context support for graceful shutdown
func runGeneratorWithContextConfig(ctx context.Context, cfg *common.Config, stats *common.Stats) {
	generatorConfig := generator.Config{
		Mode:     "default",
		BaseURL:  cfg.Generator.GetURL(cfg.System),
		URL:      cfg.Generator.GetURL(cfg.System),
		RPS:      cfg.Generator.RPS,
		Duration: time.Duration(cfg.DurationSeconds) * time.Second,
		BulkSize: cfg.Generator.BulkSize,
		// WorkerCount removed - using CPU-based allocation
		// ConnectionCount removed - using dynamic CPU-based allocation in logdb
		LogTypeDistribution: cfg.Generator.Distribution,
		MaxRetries:          cfg.Generator.MaxRetries,
		RetryDelay:          time.Duration(cfg.Generator.RetryDelayMs) * time.Millisecond,
		Verbose:             cfg.Generator.Verbose,
		EnableMetrics:       cfg.Metrics,
		MetricsPort:         cfg.MetricsPort,
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         cfg.Generator.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay,
		Verbose:         generatorConfig.Verbose,
		ConnectionCount: cfg.Generator.GetConnectionCount(),
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.URL, options)
	if err != nil {
		log.Printf("Error creating database client: %v", err)
		return
	}

	fmt.Printf("[Generator] Starting with RPS=%d, system=%s\n", cfg.Generator.RPS, cfg.System)

	// Start generator with context support
	// TODO: Implement RunGeneratorWithContext in generator package for proper context cancellation
	// For now, using regular RunGenerator - context cancellation will not be immediate
	select {
	case <-ctx.Done():
		log.Printf("Generator stopped by context cancellation before start")
		return
	default:
		if err := generator.RunGenerator(generatorConfig, db); err != nil {
			log.Printf("Error in generator: %v", err)
		}
	}
}

// runMaxPerfTest executes maxPerf load testing mode with step-by-step RPS increase
func runMaxPerfTest(cfg *common.Config, stats *common.Stats) error {
	var loadTestConfig common.LoadTestConfig

	// Get load test configuration based on mode
	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		loadTestConfig = cfg.Generator.GetLoadTestConfig("maxPerf")
	} else if cfg.Mode == modeQuerierOnly {
		loadTestConfig = cfg.Querier.GetLoadTestConfig("maxPerf")
	} else {
		return fmt.Errorf("unsupported mode for load testing: %s", cfg.Mode)
	}

	// Initialize load test logger
	logger, err := common.NewLoadTestLogger()
	if err != nil {
		return fmt.Errorf("failed to initialize load test logger: %v", err)
	}
	defer logger.Close()

	// Log test start
	logger.LogTestStart("maxPerf")
	fmt.Printf("Starting maxPerf test: %d steps, base RPS=%d, start=%d%%, increment=%d%%\n",
		loadTestConfig.Steps, loadTestConfig.BaseRPS, loadTestConfig.StartPercent, loadTestConfig.IncrementPercent)

	// Execute each step
	for step := 1; step <= loadTestConfig.Steps; step++ {
		// Calculate RPS for this step
		currentPercent := float64(loadTestConfig.StartPercent + (step-1)*loadTestConfig.IncrementPercent)
		currentRPS := int(float64(loadTestConfig.BaseRPS) * currentPercent / 100.0)

		fmt.Printf("Step %d/%d: %.1f%% of base RPS = %d RPS\n", step, loadTestConfig.Steps, currentPercent, currentRPS)

		// Log step start
		logger.LogStepStart(step, float64(currentRPS))

		// Update configuration with current RPS
		if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
			cfg.Generator.RPS = currentRPS
		}
		if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
			cfg.Querier.RPS = currentRPS
		}

		// Set step duration for the current step
		cfg.DurationSeconds = loadTestConfig.Impact + loadTestConfig.StepDuration

		// Start load with impact period
		fmt.Printf("Starting impact period (%d seconds)...\n", loadTestConfig.Impact)

		// Create context for this step
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup

		// Start appropriate load generators based on mode
		if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
			wg.Add(1)
			go func() {
				defer wg.Done()
				runGeneratorWithContextConfig(ctx, cfg, stats)
			}()
		}

		if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Start querier in background for this step
				if err := runQuerierWithConfig(cfg, stats); err != nil {
					log.Printf("Querier error in step %d: %v", step, err)
				}
			}()
		}

		// Wait for impact period to end
		time.Sleep(time.Duration(loadTestConfig.Impact) * time.Second)

		// Log end of impact period
		logger.LogImpactEnd(step)
		fmt.Printf("Impact period ended, starting stable load period (%d seconds)...\n", loadTestConfig.StepDuration)

		// Wait for stable load period to complete
		time.Sleep(time.Duration(loadTestConfig.StepDuration) * time.Second)

		// Stop all load generators for this step
		cancel()
		wg.Wait()

		// Log step end
		logger.LogStepEnd(step)
		fmt.Printf("Step %d completed\n", step)
	}

	// Log test completion
	logger.LogTestEnd()

	// Generate YAML report
	if err := logger.GenerateYAMLReport("maxPerf", loadTestConfig); err != nil {
		log.Printf("Warning: failed to generate YAML report: %v", err)
	}

	fmt.Printf("MaxPerf test completed successfully\n")
	return nil
}

// runStabilityTest executes stability load testing mode with constant load
func runStabilityTest(cfg *common.Config, stats *common.Stats) error {
	var loadTestConfig common.LoadTestConfig

	// Get load test configuration based on mode
	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		loadTestConfig = cfg.Generator.GetLoadTestConfig("stability")
	} else if cfg.Mode == modeQuerierOnly {
		loadTestConfig = cfg.Querier.GetLoadTestConfig("stability")
	} else {
		return fmt.Errorf("unsupported mode for load testing: %s", cfg.Mode)
	}

	// Initialize load test logger
	logger, err := common.NewLoadTestLogger()
	if err != nil {
		return fmt.Errorf("failed to initialize load test logger: %v", err)
	}
	defer logger.Close()

	// Calculate RPS for stability test
	currentRPS := int(float64(loadTestConfig.BaseRPS) * float64(loadTestConfig.StepPercent) / 100.0)

	// Log test start
	logger.LogTestStart("stability")
	fmt.Printf("Starting stability test: %d%% of base RPS = %d RPS, duration=%d+%d seconds\n",
		loadTestConfig.StepPercent, currentRPS, loadTestConfig.Impact, loadTestConfig.StepDuration)

	// Log step start (stability test has only one "step")
	logger.LogStepStart(1, float64(currentRPS))

	// Update configuration with calculated RPS
	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		cfg.Generator.RPS = currentRPS
	}
	if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
		cfg.Querier.RPS = currentRPS
	}

	// Set total duration
	cfg.DurationSeconds = loadTestConfig.Impact + loadTestConfig.StepDuration

	// Create context for the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// Start appropriate load generators based on mode
	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runGeneratorWithContextConfig(ctx, cfg, stats)
		}()
	}

	if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runQuerierWithConfig(cfg, stats); err != nil {
				log.Printf("Querier error in stability test: %v", err)
			}
		}()
	}

	// Wait for impact period to end
	fmt.Printf("Starting impact period (%d seconds)...\n", loadTestConfig.Impact)
	time.Sleep(time.Duration(loadTestConfig.Impact) * time.Second)

	// Log end of impact period
	logger.LogImpactEnd(1)
	fmt.Printf("Impact period ended, starting stable load period (%d seconds)...\n", loadTestConfig.StepDuration)

	// Wait for stable load period to complete
	time.Sleep(time.Duration(loadTestConfig.StepDuration) * time.Second)

	// Stop all load generators
	cancel()
	wg.Wait()

	// Stop metrics server gracefully
	common.StopMetricsServer()

	// Log step and test end
	logger.LogStepEnd(1)
	logger.LogTestEnd()

	// Generate YAML report
	if err := logger.GenerateYAMLReport("stability", loadTestConfig); err != nil {
		log.Printf("Warning: failed to generate YAML report: %v", err)
	}

	fmt.Printf("Stability test completed successfully\n")
	return nil
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
		Timeout:         config.Querier.Timeout(), // Timeout from config
		RetryCount:      config.Querier.MaxRetries,
		RetryDelay:      time.Duration(config.Querier.RetryDelayMs) * time.Millisecond,
		Verbose:         config.Querier.Verbose,
		ConnectionCount: config.Querier.GetConnectionCount(),
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
		// WorkerCount removed - using CPU-based allocation
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
