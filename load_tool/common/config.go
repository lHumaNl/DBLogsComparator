package common

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

// Config - главная конфигурационная структура
type Config struct {
	Mode            string          `yaml:"mode"`
	System          string          `yaml:"system"`          // Добавлено поле для выбора системы
	DurationSeconds int             `yaml:"durationSeconds"` // Глобальная длительность
	Generator       GeneratorConfig `yaml:"generator"`
	Querier         QuerierConfig   `yaml:"querier"`
	Metrics         MetricsConfig   `yaml:"metrics"`
}

// GeneratorConfig - конфигурация генератора логов
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

// QuerierConfig - конфигурация компонента запросов
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

// MetricsConfig - конфигурация системы метрик
type MetricsConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// LoadConfig загружает конфигурацию из YAML-файла
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения файла конфигурации: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("ошибка парсинга YAML: %v", err)
	}

	return &config, nil
}

// Duration возвращает время работы в формате time.Duration
func (c *Config) Duration() time.Duration {
	if c.DurationSeconds <= 0 {
		return 0 // бесконечная работа
	}
	return time.Duration(c.DurationSeconds) * time.Second
}

// GetURL возвращает URL для выбранной системы логирования (generator)
func (g *GeneratorConfig) GetURL(system string) string {
	switch system {
	case "loki":
		return g.URLLoki
	case "es", "elasticsearch":
		return g.URLES
	case "victoria", "victorialogs":
		return g.URLVictoria
	default:
		return g.URLVictoria // По умолчанию Victoria
	}
}

// RetryDelay возвращает задержку между повторами в формате time.Duration
func (g *GeneratorConfig) RetryDelay() time.Duration {
	return time.Duration(g.RetryDelayMs) * time.Millisecond
}

// GetURL возвращает URL для выбранной системы логирования (querier)
func (q *QuerierConfig) GetURL(system string) string {
	switch system {
	case "loki":
		return q.URLLoki
	case "es", "elasticsearch":
		return q.URLES
	case "victoria", "victorialogs":
		return q.URLVictoria
	default:
		return q.URLVictoria // По умолчанию Victoria
	}
}

// RetryDelay возвращает задержку между повторами в формате time.Duration
func (q *QuerierConfig) RetryDelay() time.Duration {
	return time.Duration(q.RetryDelayMs) * time.Millisecond
}

// SaveConfig сохраняет конфигурацию в YAML-файл
func SaveConfig(config *Config, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга YAML: %v", err)
	}

	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи в файл: %v", err)
	}
	return nil
}
