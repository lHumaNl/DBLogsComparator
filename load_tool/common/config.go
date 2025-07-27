package common

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"gopkg.in/yaml.v3"
)

// Config - main configuration structure
type Config struct {
	Mode            string          `yaml:"mode"`
	System          string          `yaml:"system"`                 // Added field for system selection
	LoadMode        string          `yaml:"load_mode,omitempty"`    // Load testing mode: stability, maxPerf
	DurationSeconds int             `yaml:"durationSeconds"`        // Global duration
	Metrics         bool            `yaml:"metrics"`                // Whether metrics are enabled
	MetricsPort     int             `yaml:"metrics_port,omitempty"` // Port for metrics (if not specified, 9090 is used)
	Generator       GeneratorConfig `yaml:"generator"`
	Querier         QuerierConfig   `yaml:"querier"`
}

// LoadTestConfig - configuration for specific load testing mode
type LoadTestConfig struct {
	Steps            int `yaml:"steps,omitempty"`            // Number of steps for maxPerf mode
	StepDuration     int `yaml:"stepDuration"`               // Duration of each step in seconds
	Impact           int `yaml:"impact"`                     // Stabilization time in seconds
	BaseRPS          int `yaml:"baseRPS"`                    // Base RPS for calculations
	StartPercent     int `yaml:"startPercent,omitempty"`     // Starting percentage for maxPerf mode
	IncrementPercent int `yaml:"incrementPercent,omitempty"` // Increment percentage for maxPerf mode
	StepPercent      int `yaml:"stepPercent,omitempty"`      // Fixed percentage for stability mode
}

// GeneratorConfig - log generator configuration
type GeneratorConfig struct {
	URLLoki     string         `yaml:"urlLoki"`
	URLES       string         `yaml:"urlES"`
	URLVictoria string         `yaml:"urlVictoria"`
	MaxPerf     LoadTestConfig `yaml:"maxPerf"`
	Stability   LoadTestConfig `yaml:"stability"`
	RPS         int            `yaml:"rps,omitempty"` // Legacy field, kept for compatibility
	BulkSize    int            `yaml:"bulkSize"`
	// WorkerCount removed - now using runtime.NumCPU() * 4 async processors
	// ConnectionCount removed - now using runtime.NumCPU() * 4 connections
	Distribution map[string]int `yaml:"distribution"`
	MaxRetries   int            `yaml:"maxRetries"`
	RetryDelayMs int            `yaml:"retryDelayMs"`
	TimeoutMs    int            `yaml:"timeoutMs"` // HTTP request timeout in milliseconds
	Verbose      bool           `yaml:"verbose"`
}

// TimeRangeConfig - configuration for time ranges
type TimeRangeConfig struct {
	Last5m  float64               `yaml:"last5m"`
	Last15m float64               `yaml:"last15m"`
	Last30m float64               `yaml:"last30m"`
	Last1h  float64               `yaml:"last1h"`
	Last2h  float64               `yaml:"last2h"`
	Last4h  float64               `yaml:"last4h"`
	Last8h  float64               `yaml:"last8h"`
	Last12h float64               `yaml:"last12h"`
	Last24h float64               `yaml:"last24h"`
	Last48h float64               `yaml:"last48h"`
	Last72h float64               `yaml:"last72h"`
	Custom  CustomTimeRangeConfig `yaml:"custom"`
}

// CustomTimeRangeConfig - configuration for custom time ranges
type CustomTimeRangeConfig struct {
	PercentsOffsetLeftBorder  map[string]float64 `yaml:"percents_offset_left_border"`
	PercentsOffsetRightBorder map[string]float64 `yaml:"percents_offset_right_border"`
}

// QuerierConfig - query component configuration
type QuerierConfig struct {
	URLLoki     string         `yaml:"urlLoki"`
	URLES       string         `yaml:"urlES"`
	URLVictoria string         `yaml:"urlVictoria"`
	MaxPerf     LoadTestConfig `yaml:"maxPerf"`
	Stability   LoadTestConfig `yaml:"stability"`
	RPS         int            `yaml:"rps,omitempty"` // Legacy field, kept for compatibility
	// WorkerCount removed - now using runtime.NumCPU() * 4 async processors
	MaxRetries   int             `yaml:"maxRetries"`
	RetryDelayMs int             `yaml:"retryDelayMs"`
	TimeoutMs    int             `yaml:"timeoutMs"` // HTTP request timeout in milliseconds
	Verbose      bool            `yaml:"verbose"`
	Distribution map[string]int  `yaml:"distribution"`
	Times        TimeRangeConfig `yaml:"times"`
}

// HostsConfig contains URLs of logging systems
type HostsConfig struct {
	URLLoki     string `yaml:"urlLoki"`
	URLES       string `yaml:"urlES"`
	URLVictoria string `yaml:"urlVictoria"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading configuration file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing YAML: %v", err)
	}

	// Set default metrics port if not specified
	if config.Metrics && config.MetricsPort == 0 {
		config.MetricsPort = 9090
	}

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %v", err)
	}

	return &config, nil
}

// LoadHostsConfig loads host configuration from a YAML file
func LoadHostsConfig(path string) (*HostsConfig, error) {
	// Check if the file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("hosts configuration file does not exist: %s", path)
	}

	// Read file contents
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading hosts configuration file: %v", err)
	}

	// Parse YAML
	var config HostsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing YAML in hosts configuration file: %v", err)
	}

	return &config, nil
}

// Duration returns the runtime in time.Duration format
func (c *Config) Duration() time.Duration {
	if c.DurationSeconds <= 0 {
		return 0 // infinite runtime
	}
	return time.Duration(c.DurationSeconds) * time.Second
}

// GetURL returns the URL for the selected logging system (generator)
func (g *GeneratorConfig) GetURL(system string) string {
	switch system {
	case "loki":
		return g.URLLoki
	case "es", "elasticsearch":
		return g.URLES
	case "victoria", "victorialogs":
		return g.URLVictoria
	default:
		return g.URLVictoria // Default to Victoria
	}
}

// RetryDelay returns the delay between retries in time.Duration format
func (g *GeneratorConfig) RetryDelay() time.Duration {
	return time.Duration(g.RetryDelayMs) * time.Millisecond
}

// Timeout returns the HTTP timeout in time.Duration format
func (g *GeneratorConfig) Timeout() time.Duration {
	if g.TimeoutMs <= 0 {
		return 30 * time.Second // Default 30 seconds
	}
	return time.Duration(g.TimeoutMs) * time.Millisecond
}

// GetLoadTestConfig returns the load test configuration for the specified mode
func (g *GeneratorConfig) GetLoadTestConfig(loadMode string) LoadTestConfig {
	switch loadMode {
	case "maxPerf":
		return g.MaxPerf
	case "stability":
		return g.Stability
	default:
		return g.Stability // Default to stability
	}
}

// GetConnectionCount returns the number of HTTP connections based on CPU count
func (g *GeneratorConfig) GetConnectionCount() int {
	return runtime.NumCPU() * 4
}

// GetURL returns the URL for the selected logging system (querier)
func (q *QuerierConfig) GetURL(system string) string {
	switch system {
	case "loki":
		return q.URLLoki
	case "es", "elasticsearch":
		return q.URLES
	case "victoria", "victorialogs":
		return q.URLVictoria
	default:
		return q.URLVictoria // Default to Victoria
	}
}

// RetryDelay returns the delay between retries in time.Duration format
func (q *QuerierConfig) RetryDelay() time.Duration {
	return time.Duration(q.RetryDelayMs) * time.Millisecond
}

// Timeout returns the HTTP timeout in time.Duration format
func (q *QuerierConfig) Timeout() time.Duration {
	if q.TimeoutMs <= 0 {
		return 10 * time.Second // Default 10 seconds
	}
	return time.Duration(q.TimeoutMs) * time.Millisecond
}

// GetLoadTestConfig returns the load test configuration for the specified mode
func (q *QuerierConfig) GetLoadTestConfig(loadMode string) LoadTestConfig {
	switch loadMode {
	case "maxPerf":
		return q.MaxPerf
	case "stability":
		return q.Stability
	default:
		return q.Stability // Default to stability
	}
}

// GetConnectionCount returns the number of HTTP connections based on CPU count for querier
func (q *QuerierConfig) GetConnectionCount() int {
	return runtime.NumCPU() * 4
}

// SaveConfig saves the configuration to a YAML file
func SaveConfig(config *Config, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("error serializing YAML: %v", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("error writing configuration file: %v", err)
	}

	return nil
}

// validateConfig validates the configuration for correctness
func validateConfig(config *Config) error {
	// Validate operation mode
	validModes := map[string]bool{
		"generator": true,
		"querier":   true,
		"combined":  true,
	}
	if config.Mode != "" && !validModes[config.Mode] {
		return fmt.Errorf("invalid mode '%s', must be one of: generator, querier, combined", config.Mode)
	}

	// Validate load mode
	validLoadModes := map[string]bool{
		"stability": true,
		"maxPerf":   true,
	}
	if config.LoadMode != "" && !validLoadModes[config.LoadMode] {
		return fmt.Errorf("invalid load mode '%s', must be one of: stability, maxPerf", config.LoadMode)
	}

	// Validate duration
	if config.DurationSeconds < 0 {
		return fmt.Errorf("durationSeconds cannot be negative: %d", config.DurationSeconds)
	}

	// Validate metrics port
	if config.MetricsPort < 1 || config.MetricsPort > 65535 {
		return fmt.Errorf("invalid metrics port: %d, must be between 1-65535", config.MetricsPort)
	}

	// Validate generator configuration
	if err := validateGeneratorConfig(&config.Generator); err != nil {
		return fmt.Errorf("generator config error: %v", err)
	}

	// Validate querier configuration
	if err := validateQuerierConfig(&config.Querier); err != nil {
		return fmt.Errorf("querier config error: %v", err)
	}

	return nil
}

// validateLoadTestConfig validates load test configuration
func validateLoadTestConfig(config LoadTestConfig, mode string) error {
	// Validate step duration
	if config.StepDuration <= 0 {
		return fmt.Errorf("stepDuration must be positive: %d", config.StepDuration)
	}

	// Validate impact time
	if config.Impact < 0 {
		return fmt.Errorf("impact cannot be negative: %d", config.Impact)
	}

	// Validate base RPS
	if config.BaseRPS <= 0 {
		return fmt.Errorf("baseRPS must be positive: %d", config.BaseRPS)
	}

	// Mode-specific validation
	if mode == "maxPerf" {
		if config.Steps <= 0 {
			return fmt.Errorf("steps must be positive for maxPerf mode: %d", config.Steps)
		}
		if config.StartPercent <= 0 {
			return fmt.Errorf("startPercent must be positive for maxPerf mode: %d", config.StartPercent)
		}
		if config.IncrementPercent <= 0 {
			return fmt.Errorf("incrementPercent must be positive for maxPerf mode: %d", config.IncrementPercent)
		}
	} else if mode == "stability" {
		if config.StepPercent <= 0 {
			return fmt.Errorf("stepPercent must be positive for stability mode: %d", config.StepPercent)
		}
	}

	return nil
}

// validateGeneratorConfig validates generator-specific configuration
func validateGeneratorConfig(config *GeneratorConfig) error {
	// Validate load test configurations
	if err := validateLoadTestConfig(config.MaxPerf, "maxPerf"); err != nil {
		return fmt.Errorf("maxPerf config error: %v", err)
	}
	if err := validateLoadTestConfig(config.Stability, "stability"); err != nil {
		return fmt.Errorf("stability config error: %v", err)
	}

	// Legacy RPS validation (if used)
	if config.RPS > 0 {
		if config.RPS > 10000 {
			return fmt.Errorf("RPS too high: %d, maximum recommended is 10000", config.RPS)
		}
	}

	// Validate bulk size
	if config.BulkSize <= 0 {
		return fmt.Errorf("bulkSize must be positive: %d", config.BulkSize)
	}
	if config.BulkSize > 1000 {
		return fmt.Errorf("bulkSize too high: %d, maximum recommended is 1000", config.BulkSize)
	}

	// Validate worker count
	// WorkerCount validation removed - using dynamic CPU-based allocation

	// Connection count validation removed - now using dynamic CPU-based allocation

	// Validate retry settings
	if config.MaxRetries < 0 {
		return fmt.Errorf("maxRetries cannot be negative: %d", config.MaxRetries)
	}
	if config.RetryDelayMs < 0 {
		return fmt.Errorf("retryDelayMs cannot be negative: %d", config.RetryDelayMs)
	}

	// Validate timeout
	if config.TimeoutMs < 1000 {
		return fmt.Errorf("timeoutMs too low: %d, minimum recommended is 1000ms", config.TimeoutMs)
	}

	// Validate log type distribution
	if len(config.Distribution) > 0 {
		totalWeight := 0
		for logType, weight := range config.Distribution {
			if weight < 0 {
				return fmt.Errorf("negative weight for log type '%s': %d", logType, weight)
			}
			totalWeight += weight
		}
		if totalWeight == 0 {
			return fmt.Errorf("total weight of log type distribution is zero")
		}
	}

	return nil
}

// validateQuerierConfig validates querier-specific configuration
func validateQuerierConfig(config *QuerierConfig) error {
	// Validate load test configurations
	if err := validateLoadTestConfig(config.MaxPerf, "maxPerf"); err != nil {
		return fmt.Errorf("maxPerf config error: %v", err)
	}
	if err := validateLoadTestConfig(config.Stability, "stability"); err != nil {
		return fmt.Errorf("stability config error: %v", err)
	}

	// Legacy RPS validation (if used)
	if config.RPS > 0 {
		if config.RPS > 1000 {
			return fmt.Errorf("RPS too high: %d, maximum recommended is 1000", config.RPS)
		}
	}

	// Validate worker count
	// WorkerCount validation removed - using dynamic CPU-based allocation

	// Validate retry settings
	if config.MaxRetries < 0 {
		return fmt.Errorf("maxRetries cannot be negative: %d", config.MaxRetries)
	}
	if config.RetryDelayMs < 0 {
		return fmt.Errorf("retryDelayMs cannot be negative: %d", config.RetryDelayMs)
	}

	// Validate timeout
	if config.TimeoutMs < 1000 {
		return fmt.Errorf("timeoutMs too low: %d, minimum recommended is 1000ms", config.TimeoutMs)
	}

	// Validate query type distribution
	if len(config.Distribution) > 0 {
		totalWeight := 0
		for queryType, weight := range config.Distribution {
			if weight < 0 {
				return fmt.Errorf("negative weight for query type '%s': %d", queryType, weight)
			}
			totalWeight += weight
		}
		if totalWeight == 0 {
			return fmt.Errorf("total weight of query type distribution is zero")
		}
	}

	return nil
}
