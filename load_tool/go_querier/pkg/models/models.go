package models

import (
	"context"
	"time"
)

// QueryType определяет тип запроса
type QueryType string

const (
	SimpleQuery     QueryType = "simple"     // Простой поиск по ключевому слову или полю
	ComplexQuery    QueryType = "complex"    // Сложный поиск с несколькими условиями
	AnalyticalQuery QueryType = "analytical" // Запрос с агрегациями
	TimeSeriesQuery QueryType = "timeseries" // Запрос временных рядов
)

// QueryResult представляет результат выполнения запроса
type QueryResult struct {
	Duration  time.Duration // Время выполнения
	HitCount  int           // Количество найденных документов
	BytesRead int64         // Количество прочитанных байт
	Status    string        // Статус запроса
}

// QueryExecutor интерфейс для выполнения запросов
type QueryExecutor interface {
	// ExecuteQuery выполняет запрос указанного типа и возвращает результат
	ExecuteQuery(ctx context.Context, queryType QueryType) (QueryResult, error)

	// GenerateRandomQuery создает случайный запрос указанного типа
	GenerateRandomQuery(queryType QueryType) interface{}

	// GetSystemName возвращает название системы
	GetSystemName() string
}

// Options настройки для исполнителя запросов
type Options struct {
	Timeout    time.Duration
	RetryCount int
	RetryDelay time.Duration
	Verbose    bool
}
