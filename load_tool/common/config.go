package common

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config - main configuration structure
type Config struct {
	Mode            string          `yaml:"mode"`
	System          string          `yaml:"system"`                 // Added field for system selection
	DurationSeconds int             `yaml:"durationSeconds"`        // Global duration
	Metrics         bool            `yaml:"metrics"`                // Whether metrics are enabled
	MetricsPort     int             `yaml:"metrics_port,omitempty"` // Port for metrics (if not specified, 9090 is used)
	Generator       GeneratorConfig `yaml:"generator"`
	Querier         QuerierConfig   `yaml:"querier"`
}

// GeneratorConfig - log generator configuration
type GeneratorConfig struct {
	URLLoki         string         `yaml:"urlLoki"`
	URLES           string         `yaml:"urlES"`
	URLVictoria     string         `yaml:"urlVictoria"`
	RPS             int            `yaml:"rps"`
	BulkSize        int            `yaml:"bulkSize"`
	WorkerCount     int            `yaml:"workerCount"`
	ConnectionCount int            `yaml:"connectionCount"`
	Distribution    map[string]int `yaml:"distribution"`
	MaxRetries      int            `yaml:"maxRetries"`
	RetryDelayMs    int            `yaml:"retryDelayMs"`
	Verbose         bool           `yaml:"verbose"`
}

// QuerierConfig - query component configuration
type QuerierConfig struct {
	URLLoki      string         `yaml:"urlLoki"`
	URLES        string         `yaml:"urlES"`
	URLVictoria  string         `yaml:"urlVictoria"`
	RPS          int            `yaml:"rps"`
	WorkerCount  int            `yaml:"workerCount"`
	MaxRetries   int            `yaml:"maxRetries"`
	RetryDelayMs int            `yaml:"retryDelayMs"`
	Verbose      bool           `yaml:"verbose"`
	Distribution map[string]int `yaml:"distribution"`
}

// HostsConfig contains URLs of logging systems
type HostsConfig struct {
	URLLoki     string `yaml:"urlLoki"`
	URLES       string `yaml:"urlES"`
	URLVictoria string `yaml:"urlVictoria"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
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

// SaveConfig saves the configuration to a YAML file
func SaveConfig(config *Config, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("error serializing YAML: %v", err)
	}

	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("error writing configuration file: %v", err)
	}

	return nil
}
