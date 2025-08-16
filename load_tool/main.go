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

	// Add new flags
	system := flag.String("system", "", "Logging system: loki, elasticsearch, victoria (required parameter)")
	metricsPort := flag.Int("metrics-port", 9090, "Port for Prometheus metrics server")
	mode := flag.String("mode", "generator", "Operation mode: generator, querier, combined")
	loadMode := flag.String("load-mode", "stability", "Load testing mode: stability, maxPerf")
	errorLogging := flag.Bool("error", true, "Enable error logging (default: true)")

	flag.Parse()

	// Check for required system parameter
	if *system == "" {
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

	// Determine directory with current executable
	execDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalf("Failed to determine current directory: %v", err)
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

	// Set error logging from command line (only if explicitly set)
	var errorFlagSet bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "error" {
			errorFlagSet = true
		}
	})
	if errorFlagSet {
		config.Error = errorLogging
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
		// StartTime will be set by the generator just before starting
	}

	// Initialize structured logger
	verbose := (config.Mode == modeGeneratorOnly && config.Generator.IsVerbose(config.Verbose)) ||
		(config.Mode == modeQuerierOnly && config.Querier.IsVerbose(config.Verbose)) ||
		(config.Mode == modeCombined && (config.Generator.IsVerbose(config.Verbose) || config.Querier.IsVerbose(config.Verbose)))
	common.InitTextLogger(verbose)

	// Set error logging based on configuration
	errorLoggingEnabled := (config.Mode == modeGeneratorOnly && config.Generator.IsError(config.Error)) ||
		(config.Mode == modeQuerierOnly && config.Querier.IsError(config.Error)) ||
		(config.Mode == modeCombined && (config.Generator.IsError(config.Error) || config.Querier.IsError(config.Error)))
	common.SetErrorLogging(errorLoggingEnabled)

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

		// IMPORTANT: Stop metrics server in fallback mode (when not using load test modes)
		common.StopMetricsServer()
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
		Combined: common.CombinedConfig{
			MaxPerf: common.LoadTestConfig{
				Steps:        3,
				StepDuration: 10,
				Impact:       2,
				// Note: BaseRPS, StartPercent, IncrementPercent taken from generator/querier sections
			},
			Stability: common.LoadTestConfig{
				StepDuration: 10,
				Impact:       5,
				// Note: BaseRPS, StepPercent taken from generator/querier sections
			},
		},
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
			Verbose:      nil,   // Use global setting
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
			Verbose:      nil,   // Use global setting
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

// Function to start log generator with new configuration
func runGeneratorWithConfig(cfg *common.Config, stats *common.Stats) {
	generatorConfig := &generator.GeneratorConfig{
		GeneratorConfig: &cfg.Generator,
		System:          cfg.System,
		Duration:        time.Duration(cfg.DurationSeconds) * time.Second,
		EnableMetrics:   cfg.Metrics,
		MetricsPort:     cfg.MetricsPort,
		GlobalVerbose:   cfg.Verbose,
		GlobalDebug:     cfg.Debug,
		GlobalError:     cfg.Error,
	}

	// Output information about log type distribution
	fmt.Println("Log type distribution:", generatorConfig.Distribution)

	// Enable metrics, if needed
	if cfg.Metrics {
		common.InitPrometheus(cfg.System, cfg.Generator.BulkSize)
		// Removing StartMetricsServer call, as it's already started in main.go
		// common.StartMetricsServer(cfg.MetricsPort)
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         generatorConfig.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay(),
		Verbose:         generatorConfig.IsVerbose(),
		Debug:           cfg.Generator.IsDebug(cfg.Debug),
		Error:           cfg.Generator.IsError(cfg.Error),
		ConnectionCount: cfg.Generator.GetConnectionCount(),
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.GetURL(), options)
	if err != nil {
		log.Fatalf("Error creating database client: %v", err)
	}

	fmt.Printf("[Generator] Starting with RPS=%.2f, system=%s\n", generatorConfig.GetRPS(), cfg.System)

	// Start optimized generator
	if err := generator.RunGeneratorOptimized(generatorConfig, db); err != nil {
		log.Fatalf("Error starting generator: %v", err)
	}
}

// runGeneratorWithContextConfigAndRPSAndStatsLogger starts the generator with context, specific RPS and StatsLogger support
func runGeneratorWithContextConfigAndRPSAndStatsLogger(ctx context.Context, cfg *common.Config, stats *common.Stats, rps float64,
	statsLogger *common.StatsLogger, step, totalSteps int) {
	generatorConfig := &generator.GeneratorConfig{
		GeneratorConfig: &cfg.Generator,
		System:          cfg.System,
		Duration:        time.Duration(cfg.DurationSeconds) * time.Second,
		EnableMetrics:   cfg.Metrics,
		MetricsPort:     cfg.MetricsPort,
		GlobalVerbose:   cfg.Verbose,
		GlobalDebug:     cfg.Debug,
		GlobalError:     cfg.Error,
		RuntimeRPS:      rps, // Set specific RPS for this step
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         generatorConfig.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay(),
		Verbose:         generatorConfig.IsVerbose(),
		Debug:           cfg.Generator.IsDebug(cfg.Debug),
		Error:           cfg.Generator.IsError(cfg.Error),
		ConnectionCount: cfg.Generator.GetConnectionCount(),
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.GetURL(), options)
	if err != nil {
		log.Printf("Error creating database client: %v", err)
		return
	}

	fmt.Printf("[Generator] Starting with RPS=%.2f, system=%s\n", generatorConfig.GetRPS(), cfg.System)

	// Start generator with context and StatsLogger support
	select {
	case <-ctx.Done():
		log.Printf("Generator stopped by context cancellation before start")
		return
	default:
		// Use optimized RunGeneratorWithContextAndStatsLogger for proper context cancellation and logging
		if err := generator.RunGeneratorOptimizedWithContextAndStatsLogger(ctx, generatorConfig, db, statsLogger, step, totalSteps); err != nil {
			log.Printf("Error in generator: %v", err)
		}
	}
}

// runGeneratorWithContextConfigAndRPS starts the generator with context support and specific RPS for maxPerf mode
func runGeneratorWithContextConfigAndRPS(ctx context.Context, cfg *common.Config, stats *common.Stats, rps float64) {
	generatorConfig := &generator.GeneratorConfig{
		GeneratorConfig: &cfg.Generator,
		System:          cfg.System,
		Duration:        time.Duration(cfg.DurationSeconds) * time.Second,
		EnableMetrics:   cfg.Metrics,
		MetricsPort:     cfg.MetricsPort,
		GlobalVerbose:   cfg.Verbose,
		GlobalDebug:     cfg.Debug,
		GlobalError:     cfg.Error,
		RuntimeRPS:      rps, // Set specific RPS for this step
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         generatorConfig.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay(),
		Verbose:         generatorConfig.IsVerbose(),
		Debug:           cfg.Generator.IsDebug(cfg.Debug),
		Error:           cfg.Generator.IsError(cfg.Error),
		ConnectionCount: cfg.Generator.GetConnectionCount(),
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.GetURL(), options)
	if err != nil {
		log.Printf("Error creating database client: %v", err)
		return
	}

	fmt.Printf("[Generator] Starting with RPS=%.2f, system=%s\n", generatorConfig.GetRPS(), cfg.System)

	// Start generator with context support
	select {
	case <-ctx.Done():
		log.Printf("Generator stopped by context cancellation before start")
		return
	default:
		// Use optimized RunGeneratorWithContext for proper context cancellation
		if err := generator.RunGeneratorOptimizedWithContext(ctx, generatorConfig, db); err != nil {
			log.Printf("Error in generator: %v", err)
		}
	}
}

// runGeneratorWithContextConfig starts the generator with context support for graceful shutdown
func runGeneratorWithContextConfig(ctx context.Context, cfg *common.Config, stats *common.Stats) {
	generatorConfig := &generator.GeneratorConfig{
		GeneratorConfig: &cfg.Generator,
		System:          cfg.System,
		Duration:        time.Duration(cfg.DurationSeconds) * time.Second,
		EnableMetrics:   cfg.Metrics,
		MetricsPort:     cfg.MetricsPort,
		GlobalVerbose:   cfg.Verbose,
		GlobalDebug:     cfg.Debug,
		GlobalError:     cfg.Error,
	}

	// Create database client
	options := logdb.Options{
		BatchSize:       generatorConfig.BulkSize,
		Timeout:         generatorConfig.Timeout(), // Request timeout from config
		RetryCount:      generatorConfig.MaxRetries,
		RetryDelay:      generatorConfig.RetryDelay(),
		Verbose:         generatorConfig.IsVerbose(),
		Debug:           cfg.Generator.IsDebug(cfg.Debug),
		Error:           cfg.Generator.IsError(cfg.Error),
		ConnectionCount: cfg.Generator.GetConnectionCount(),
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.GetURL(), options)
	if err != nil {
		log.Printf("Error creating database client: %v", err)
		return
	}

	fmt.Printf("[Generator] Starting with RPS=%.2f, system=%s\n", generatorConfig.GetRPS(), cfg.System)

	// Start generator with context support
	select {
	case <-ctx.Done():
		log.Printf("Generator stopped by context cancellation before start")
		return
	default:
		// Use optimized RunGeneratorWithContext for proper context cancellation
		if err := generator.RunGeneratorOptimizedWithContext(ctx, generatorConfig, db); err != nil {
			log.Printf("Error in generator: %v", err)
		}
	}
}

// runMaxPerfTest executes maxPerf load testing mode with step-by-step RPS increase
func runMaxPerfTest(cfg *common.Config, stats *common.Stats) error {
	var loadTestConfig common.LoadTestConfig

	// Get load test configuration based on mode
	if cfg.Mode == modeCombined {
		// For combined mode, use only timing parameters from combined section
		// baseRPS/percentages will be taken from individual generator/querier sections
		loadTestConfig = cfg.Combined.GetLoadTestConfig("maxPerf")
	} else if cfg.Mode == modeGeneratorOnly {
		loadTestConfig = cfg.Generator.GetLoadTestConfig("maxPerf")
	} else if cfg.Mode == modeQuerierOnly {
		loadTestConfig = cfg.Querier.GetLoadTestConfig("maxPerf")
	} else {
		return fmt.Errorf("unsupported mode for load testing: %s", cfg.Mode)
	}

	// Initialize load test logger
	logger, err := common.NewLoadTestLogger(cfg.Mode, "maxPerf")
	if err != nil {
		return fmt.Errorf("failed to initialize load test logger: %v", err)
	}
	defer logger.Close()

	// Initialize statistics loggers for each module
	var generatorStatsLogger, querierStatsLogger *common.StatsLogger

	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		generatorStatsLogger, err = common.NewStatsLogger(cfg.Mode, "maxPerf", "generator")
		if err != nil {
			return fmt.Errorf("failed to initialize generator stats logger: %v", err)
		}
		defer generatorStatsLogger.Close()
	}

	if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
		querierStatsLogger, err = common.NewStatsLogger(cfg.Mode, "maxPerf", "querier")
		if err != nil {
			return fmt.Errorf("failed to initialize querier stats logger: %v", err)
		}
		defer querierStatsLogger.Close()
	}

	// Log test start
	logger.LogTestStart("maxPerf")

	// Execute each step
	for step := 1; step <= loadTestConfig.Steps; step++ {
		// Calculate RPS for this step with floating point precision
		var generatorRPS, querierRPS float64
		if cfg.Mode == modeCombined {
			// For combined mode, calculate RPS separately for each component
			generatorConfig := cfg.Generator.GetLoadTestConfig("maxPerf")
			querierConfig := cfg.Querier.GetLoadTestConfig("maxPerf")

			generatorPercent := generatorConfig.StartPercent + float64(step-1)*generatorConfig.IncrementPercent
			querierPercent := querierConfig.StartPercent + float64(step-1)*querierConfig.IncrementPercent

			generatorRPS = generatorConfig.BaseRPS * generatorPercent / 100.0
			querierRPS = querierConfig.BaseRPS * querierPercent / 100.0

			fmt.Printf("Step %d/%d (combined): Generator=%.1f%% (%.2f RPS), Querier=%.1f%% (%.2f RPS)\n",
				step, loadTestConfig.Steps, generatorPercent, generatorRPS, querierPercent, querierRPS)
		} else {
			// For single mode, use the same RPS calculation
			currentPercent := loadTestConfig.StartPercent + float64(step-1)*loadTestConfig.IncrementPercent
			currentRPS := loadTestConfig.BaseRPS * currentPercent / 100.0
			generatorRPS = currentRPS
			querierRPS = currentRPS

			fmt.Printf("Step %d/%d: %.1f%% of base RPS = %.2f RPS\n", step, loadTestConfig.Steps, currentPercent, currentRPS)
		}

		// Log step start with appropriate method based on mode
		if cfg.Mode == modeCombined {
			logger.LogCombinedStepStart(step, generatorRPS, querierRPS)
		} else {
			logger.LogStepStart(step, generatorRPS)
		}

		// RPS will be passed directly to the generator function

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
			go func(rps float64) {
				defer wg.Done()
				runGeneratorWithContextConfigAndRPSAndStatsLogger(ctx, cfg, stats, rps, generatorStatsLogger, step, loadTestConfig.Steps)
			}(generatorRPS)
		}

		if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
			wg.Add(1)
			go func(ctx context.Context, rps float64) {
				defer wg.Done()
				// Create fresh stats for this step to avoid accumulation between steps
				stepStats := &common.Stats{}
				// Start querier in background for this step with dynamic RPS, context and StatsLogger
				if err := runQuerierWithConfigAndRPSAndContextAndStatsLogger(ctx, cfg, stepStats, rps, querierStatsLogger, step, loadTestConfig.Steps); err != nil {
					log.Printf("Querier error in step %d: %v", step, err)
				}
			}(ctx, querierRPS)
		}

		// Wait for impact period to end
		time.Sleep(time.Duration(loadTestConfig.Impact) * time.Second)

		// Log end of impact period
		if cfg.Mode == modeCombined {
			logger.LogCombinedImpactEnd(step)
		} else {
			logger.LogImpactEnd(step)
		}
		fmt.Printf("Impact period ended, starting stable load period (%d seconds)...\n", loadTestConfig.StepDuration)

		// Wait for stable load period to complete
		time.Sleep(time.Duration(loadTestConfig.StepDuration) * time.Second)

		// Stop all load generators for this step
		cancel()
		wg.Wait()

		// Log step end
		if cfg.Mode == modeCombined {
			logger.LogCombinedStepEnd(step)
		} else {
			logger.LogStepEnd(step)
		}

		// Statistics are now automatically logged by StatsLogger integration
		// in RunGeneratorOptimizedWithContextAndStatsLogger and RunQuerierWithContext functions

		fmt.Printf("Step %d completed\n", step)
	}

	// Log test completion
	logger.LogTestEnd()

	// Stop metrics server gracefully
	common.StopMetricsServer()

	// Generate YAML report
	if cfg.Mode == modeCombined {
		// For combined mode, use separate configs
		timingConfig := cfg.Combined.GetLoadTestConfig("maxPerf")
		generatorConfig := cfg.Generator.GetLoadTestConfig("maxPerf")
		querierConfig := cfg.Querier.GetLoadTestConfig("maxPerf")
		bulkSize := cfg.Generator.BulkSize

		if err := logger.GenerateYAMLReportCombinedWithBulkSize("maxPerf", timingConfig, generatorConfig, querierConfig, bulkSize); err != nil {
			log.Printf("Warning: failed to generate combined YAML report: %v", err)
		}
	} else {
		var bulkSize int
		if cfg.Mode == modeGeneratorOnly {
			bulkSize = cfg.Generator.BulkSize
		}
		if err := logger.GenerateYAMLReportWithBulkSize("maxPerf", loadTestConfig, bulkSize); err != nil {
			log.Printf("Warning: failed to generate YAML report: %v", err)
		}
	}

	fmt.Printf("MaxPerf test completed successfully\n")
	return nil
}

// runStabilityTest executes stability load testing mode with constant load
func runStabilityTest(cfg *common.Config, stats *common.Stats) error {
	var loadTestConfig common.LoadTestConfig

	// Get load test configuration based on mode
	if cfg.Mode == modeCombined {
		// For combined mode, use only timing parameters from combined section
		// baseRPS/stepPercent will be taken from individual generator/querier sections
		loadTestConfig = cfg.Combined.GetLoadTestConfig("stability")
	} else if cfg.Mode == modeGeneratorOnly {
		loadTestConfig = cfg.Generator.GetLoadTestConfig("stability")
	} else if cfg.Mode == modeQuerierOnly {
		loadTestConfig = cfg.Querier.GetLoadTestConfig("stability")
	} else {
		return fmt.Errorf("unsupported mode for load testing: %s", cfg.Mode)
	}

	// Initialize load test logger
	logger, err := common.NewLoadTestLogger(cfg.Mode, "stability")
	if err != nil {
		return fmt.Errorf("failed to initialize load test logger: %v", err)
	}
	defer logger.Close()

	// Initialize StatsLoggers for generator and querier
	var generatorStatsLogger, querierStatsLogger *common.StatsLogger
	if cfg.Mode == modeGeneratorOnly || cfg.Mode == modeCombined {
		generatorStatsLogger, err = common.NewStatsLogger(cfg.Mode, "stability", "generator")
		if err != nil {
			return fmt.Errorf("failed to initialize generator stats logger: %v", err)
		}
		defer generatorStatsLogger.Close()
	}

	if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
		querierStatsLogger, err = common.NewStatsLogger(cfg.Mode, "stability", "querier")
		if err != nil {
			return fmt.Errorf("failed to initialize querier stats logger: %v", err)
		}
		defer querierStatsLogger.Close()
	}

	// Calculate RPS for stability test (different RPS for generator and querier in combined mode)
	var generatorRPS, querierRPS float64
	if cfg.Mode == modeCombined {
		// For combined mode, calculate RPS separately for each component
		generatorConfig := cfg.Generator.GetLoadTestConfig("stability")
		querierConfig := cfg.Querier.GetLoadTestConfig("stability")
		generatorRPS = generatorConfig.BaseRPS * generatorConfig.StepPercent / 100.0
		querierRPS = querierConfig.BaseRPS * querierConfig.StepPercent / 100.0

		// Log test start with both RPS values
		logger.LogTestStart("stability")
		fmt.Printf("Starting stability test (combined): Generator=%.2f RPS, Querier=%.2f RPS, duration=%d+%d seconds\n",
			generatorRPS, querierRPS, loadTestConfig.Impact, loadTestConfig.StepDuration)
	} else {
		// For single mode, use the same RPS
		currentRPS := loadTestConfig.BaseRPS * loadTestConfig.StepPercent / 100.0
		generatorRPS = currentRPS
		querierRPS = currentRPS

		// Log test start
		logger.LogTestStart("stability")
		fmt.Printf("Starting stability test: %.1f%% of base RPS = %.2f RPS, duration=%d+%d seconds\n",
			loadTestConfig.StepPercent, currentRPS, loadTestConfig.Impact, loadTestConfig.StepDuration)
	}

	// Log step start (stability test has only one "step")
	if cfg.Mode == modeCombined {
		logger.LogCombinedStepStart(1, generatorRPS, querierRPS)
	} else {
		logger.LogStepStart(1, generatorRPS)
	}

	// RPS will be passed directly to the generator function

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
			// Use StatsLogger version for stability test (step 1, totalSteps 1)
			runGeneratorWithContextConfigAndRPSAndStatsLogger(ctx, cfg, stats, generatorRPS, generatorStatsLogger, 1, 1)
		}()
	}

	if cfg.Mode == modeQuerierOnly || cfg.Mode == modeCombined {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			// Use StatsLogger version for stability test (step 1, totalSteps 1)
			if err := runQuerierWithConfigAndRPSAndContextAndStatsLogger(ctx, cfg, stats, querierRPS, querierStatsLogger, 1, 1); err != nil {
				log.Printf("Querier error in stability test: %v", err)
			}
		}(ctx)
	}

	// Let generators run for the full configured duration
	// Track timing phases for proper YAML reporting
	fmt.Printf("Starting impact period (%d seconds)...\n", loadTestConfig.Impact)
	fmt.Printf("Then stable load period (%d seconds)...\n", loadTestConfig.StepDuration)
	fmt.Printf("Total duration: %d seconds (generators will manage their own timing)\n",
		loadTestConfig.Impact+loadTestConfig.StepDuration)

	// Sleep for impact period duration, then log the transition to stability period
	time.Sleep(time.Duration(loadTestConfig.Impact) * time.Second)
	if cfg.Mode == modeCombined {
		logger.LogCombinedImpactEnd(1)
	} else {
		logger.LogImpactEnd(1)
	}

	// Wait for generators to complete their full duration
	// (generators have their own Duration configuration)
	wg.Wait()

	// Stop metrics server gracefully
	common.StopMetricsServer()

	// Log step and test end
	if cfg.Mode == modeCombined {
		logger.LogCombinedStepEnd(1)
	} else {
		logger.LogStepEnd(1)
	}
	logger.LogTestEnd()

	// Generate YAML report
	if cfg.Mode == modeCombined {
		// For combined mode, use separate configs
		timingConfig := cfg.Combined.GetLoadTestConfig("stability")
		generatorConfig := cfg.Generator.GetLoadTestConfig("stability")
		querierConfig := cfg.Querier.GetLoadTestConfig("stability")
		bulkSize := cfg.Generator.BulkSize

		if err := logger.GenerateYAMLReportCombinedWithBulkSize("stability", timingConfig, generatorConfig, querierConfig, bulkSize); err != nil {
			log.Printf("Warning: failed to generate combined YAML report: %v", err)
		}
	} else {
		var bulkSize int
		if cfg.Mode == modeGeneratorOnly {
			bulkSize = cfg.Generator.BulkSize
		}
		if err := logger.GenerateYAMLReportWithBulkSize("stability", loadTestConfig, bulkSize); err != nil {
			log.Printf("Warning: failed to generate YAML report: %v", err)
		}
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
		Verbose:         config.Querier.IsVerbose(config.Verbose),
		Debug:           config.Querier.IsDebug(config.Debug),
		Error:           config.Querier.IsError(config.Error),
		ConnectionCount: config.Querier.GetConnectionCount(),
	}

	// Create query executor for selected system with time configuration
	log.Printf("Debug: Creating query executor with system=%s, baseURL=%s", systemForExecutor, baseURL)
	executor, err := go_querier.CreateQueryExecutorWithTimeConfig(systemForExecutor, baseURL, options, 1, &config.Querier.Times)
	if err != nil {
		return fmt.Errorf("error creating query executor: %v", err)
	}

	// Get load test config and calculate RPS
	loadTestConfig := config.Querier.GetLoadTestConfig(config.LoadMode)
	currentRPS := loadTestConfig.BaseRPS * loadTestConfig.StepPercent / 100.0

	// Create configuration for query module
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             currentRPS,
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
		Verbose:      config.Querier.IsVerbose(config.Verbose),
	}

	log.Printf("[Queries] Starting with QPS=%.2f, system=%s\n", currentRPS, config.System)

	// Start query module
	return go_querier.RunQuerier(queryConfig, executor, stats)
}

// runQuerierWithConfigAndRPS starts the query client with specific RPS for maxPerf mode
func runQuerierWithConfigAndRPS(config *common.Config, stats *common.Stats, rps float64) error {
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
		Verbose:         config.Querier.IsVerbose(config.Verbose),
		Debug:           config.Querier.IsDebug(config.Debug),
		Error:           config.Querier.IsError(config.Error),
		ConnectionCount: config.Querier.GetConnectionCount(),
	}

	// Create query executor for selected system with time configuration
	log.Printf("Debug: Creating query executor with system=%s, baseURL=%s", systemForExecutor, baseURL)
	executor, err := go_querier.CreateQueryExecutorWithTimeConfig(systemForExecutor, baseURL, options, 1, &config.Querier.Times)
	if err != nil {
		return fmt.Errorf("error creating query executor: %v", err)
	}

	// Create configuration for query module with specific RPS
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             rps, // Use specific RPS passed as parameter
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
		Verbose:      config.Querier.IsVerbose(config.Verbose),
	}

	log.Printf("[Queries] Starting with QPS=%.2f, system=%s\n", rps, config.System)

	// Start query module
	return go_querier.RunQuerier(queryConfig, executor, stats)
}

// runQuerierWithConfigAndRPSAndContextAndStatsLogger starts the query client with context, specific RPS and StatsLogger support
func runQuerierWithConfigAndRPSAndContextAndStatsLogger(ctx context.Context, config *common.Config, stats *common.Stats, rps float64,
	statsLogger *common.StatsLogger, step, totalSteps int) error {
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
		Verbose:         config.Querier.IsVerbose(config.Verbose),
		Debug:           config.Querier.IsDebug(config.Debug),
		Error:           config.Querier.IsError(config.Error),
		ConnectionCount: config.Querier.GetConnectionCount(),
	}

	// Create query executor for selected system with time configuration
	log.Printf("Debug: Creating query executor with system=%s, baseURL=%s", systemForExecutor, baseURL)
	executor, err := go_querier.CreateQueryExecutorWithTimeConfig(systemForExecutor, baseURL, options, 1, &config.Querier.Times)
	if err != nil {
		return fmt.Errorf("error creating query executor: %v", err)
	}

	// Create configuration for query module with specific RPS
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             rps, // Use specific RPS passed as parameter
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
		Verbose:      config.Querier.IsVerbose(config.Verbose),
	}

	log.Printf("[Queries] Starting with QPS=%.2f, system=%s\n", rps, config.System)

	// Start query module with context and StatsLogger support
	return go_querier.RunQuerierWithContext(ctx, queryConfig, executor, stats, statsLogger, step, totalSteps)
}

// runQuerierWithConfigAndRPSAndContext starts the query client with context support and specific RPS for maxPerf mode
func runQuerierWithConfigAndRPSAndContext(ctx context.Context, config *common.Config, stats *common.Stats, rps float64) error {
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
		Verbose:         config.Querier.IsVerbose(config.Verbose),
		Debug:           config.Querier.IsDebug(config.Debug),
		Error:           config.Querier.IsError(config.Error),
		ConnectionCount: config.Querier.GetConnectionCount(),
	}

	// Create query executor for selected system with time configuration
	log.Printf("Debug: Creating query executor with system=%s, baseURL=%s", systemForExecutor, baseURL)
	executor, err := go_querier.CreateQueryExecutorWithTimeConfig(systemForExecutor, baseURL, options, 1, &config.Querier.Times)
	if err != nil {
		return fmt.Errorf("error creating query executor: %v", err)
	}

	// Create configuration for query module with specific RPS
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             rps, // Use specific RPS passed as parameter
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
		Verbose:      config.Querier.IsVerbose(config.Verbose),
	}

	log.Printf("[Queries] Starting with QPS=%.2f, system=%s\n", rps, config.System)

	// Start query module with context support - no StatsLogger for individual calls
	return go_querier.RunQuerierWithContext(ctx, queryConfig, executor, stats, nil, 0, 0)
}
