package common

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config - главная конфигурационная структура
type Config struct {
	Mode            string          `yaml:"mode"`
	System          string          `yaml:"system"`                 // Добавлено поле для выбора системы
	DurationSeconds int             `yaml:"durationSeconds"`        // Глобальная длительность
	Metrics         bool            `yaml:"metrics"`                // Включены ли метрики
	MetricsPort     int             `yaml:"metrics_port,omitempty"` // Порт для метрик (если не указан, используется 9090)
	Generator       GeneratorConfig `yaml:"generator"`
	Querier         QuerierConfig   `yaml:"querier"`
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

// HostsConfig содержит URL-адреса систем логирования
type HostsConfig struct {
	URLLoki     string `yaml:"urlLoki"`
	URLES       string `yaml:"urlES"`
	URLVictoria string `yaml:"urlVictoria"`
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

	// Установка порта метрик по умолчанию, если не указан
	if config.Metrics && config.MetricsPort == 0 {
		config.MetricsPort = 9090
	}

	return &config, nil
}

// LoadHostsConfig загружает конфигурацию хостов из YAML файла
func LoadHostsConfig(path string) (*HostsConfig, error) {
	// Проверяем существование файла
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("файл конфигурации хостов не существует: %s", path)
	}

	// Читаем содержимое файла
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения файла конфигурации хостов: %v", err)
	}

	// Разбираем YAML
	var config HostsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("ошибка разбора YAML в файле конфигурации хостов: %v", err)
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
		return fmt.Errorf("ошибка сериализации YAML: %v", err)
	}

	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи файла конфигурации: %v", err)
	}

	return nil
}
