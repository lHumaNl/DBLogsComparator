package logdb

import (
	"fmt"
	"time"
)

// LogEntry представляет собой обобщенную структуру записи лога
type LogEntry map[string]interface{}

// Options содержит параметры для инициализации баз данных логов
type Options struct {
	BatchSize  int
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
	Verbose    bool
}

// LogDB - интерфейс для всех баз данных логов
type LogDB interface {
	// Initialize инициализирует соединение с базой данных
	Initialize() error
	
	// SendLogs отправляет пакет логов в базу данных
	SendLogs(logs []LogEntry) error
	
	// Close закрывает соединение с базой данных
	Close() error
	
	// Name возвращает имя базы данных
	Name() string
	
	// Metrics возвращает метрики работы с базой данных
	Metrics() map[string]float64
	
	// FormatPayload форматирует записи логов в нужный формат для отправки
	FormatPayload(logs []LogEntry) (string, string)
}

// BaseLogDB - базовая структура с общей функциональностью для всех баз данных логов
type BaseLogDB struct {
	URL         string
	BatchSize   int
	Timeout     time.Duration
	RetryCount  int
	RetryDelay  time.Duration
	MetricsData map[string]float64
	Verbose     bool
}

// NewBaseLogDB создает новый экземпляр BaseLogDB
func NewBaseLogDB(url string, options Options) *BaseLogDB {
	return &BaseLogDB{
		URL:         url,
		BatchSize:   options.BatchSize,
		Timeout:     options.Timeout,
		RetryCount:  options.RetryCount,
		RetryDelay:  options.RetryDelay,
		Verbose:     options.Verbose,
		MetricsData: make(map[string]float64),
	}
}

// Metrics возвращает метрики работы с базой данных
func (db *BaseLogDB) Metrics() map[string]float64 {
	return db.MetricsData
}

// CreateLogDB - фабричный метод для создания экземпляров LogDB
func CreateLogDB(mode string, baseURL string, options Options) (LogDB, error) {
	switch mode {
	case "victoria":
		return NewVictoriaLogsDB(baseURL, options)
	case "es":
		return NewElasticsearchDB(baseURL, options)
	case "loki":
		return NewLokiDB(baseURL, options)
	default:
		return nil, fmt.Errorf("unsupported log DB type: %s", mode)
	}
}
