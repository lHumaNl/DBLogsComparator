package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"

	"github.com/dblogscomparator/log-generator/logdb"
	"github.com/dblogscomparator/log-generator/pkg"
)

func main() {
	// Установка максимального количества процессоров
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Инициализация генератора случайных чисел
	rand.Seed(time.Now().UnixNano())

	// Разбор аргументов командной строки
	mode := flag.String("mode", "victoria", "Режим работы: victoria (VictoriaLogs), es (Elasticsearch), loki (Loki)")
	baseURL := flag.String("url", "http://localhost:8428", "Базовый URL для отправки логов")
	rps := flag.Int("rps", 10, "Количество запросов в секунду")
	duration := flag.Duration("duration", 1*time.Minute, "Продолжительность теста")
	bulkSize := flag.Int("bulk-size", 100, "Количество логов в одном запросе")
	workerCount := flag.Int("worker-count", 5, "Количество рабочих горутин")
	connectionCount := flag.Int("connection-count", 10, "Количество HTTP-соединений")

	// Распределение типов логов
	webAccessWeight := flag.Int("web-access-weight", 60, "Вес для логов web_access")
	webErrorWeight := flag.Int("web-error-weight", 10, "Вес для логов web_error")
	applicationWeight := flag.Int("application-weight", 20, "Вес для логов application")
	metricWeight := flag.Int("metric-weight", 5, "Вес для логов metric")
	eventWeight := flag.Int("event-weight", 5, "Вес для логов event")

	verbose := flag.Bool("verbose", false, "Подробный вывод")
	maxRetries := flag.Int("max-retries", 3, "Максимальное количество повторных попыток")
	retryDelay := flag.Duration("retry-delay", 500*time.Millisecond, "Задержка между повторными попытками")

	enableMetrics := flag.Bool("enable-metrics", false, "Включить метрики Prometheus")
	metricsPort := flag.Int("metrics-port", 8080, "Порт для метрик Prometheus")

	flag.Parse()

	// Настройка распределения типов логов
	logTypeDistribution := map[string]int{
		"web_access":  *webAccessWeight,
		"web_error":   *webErrorWeight,
		"application": *applicationWeight,
		"metric":      *metricWeight,
		"event":       *eventWeight,
	}

	// Формирование конфигурации
	config := pkg.Config{
		Mode:                *mode,
		BaseURL:             *baseURL,
		URL:                 *baseURL,
		RPS:                 *rps,
		Duration:            *duration,
		BulkSize:            *bulkSize,
		WorkerCount:         *workerCount,
		ConnectionCount:     *connectionCount,
		LogTypeDistribution: logTypeDistribution,
		Verbose:             *verbose,
		MaxRetries:          *maxRetries,
		RetryDelay:          *retryDelay,
		EnableMetrics:       *enableMetrics,
		MetricsPort:         *metricsPort,
	}

	// Вывод информации о настройках
	fmt.Printf("=== Генератор логов ===\n")
	fmt.Printf("Режим: %s\n", config.Mode)
	fmt.Printf("URL: %s\n", config.URL)
	fmt.Printf("RPS: %d\n", config.RPS)
	fmt.Printf("Продолжительность: %s\n", config.Duration)
	fmt.Printf("Размер пакета: %d\n", config.BulkSize)
	fmt.Printf("Рабочих горутин: %d\n", config.WorkerCount)
	fmt.Printf("HTTP-соединений: %d\n", config.ConnectionCount)
	fmt.Printf("Распределение типов логов: %v\n", config.LogTypeDistribution)
	fmt.Printf("Повторных попыток: %d\n", config.MaxRetries)
	fmt.Printf("Задержка между попытками: %s\n", config.RetryDelay)
	fmt.Printf("Метрики Prometheus: %v\n", config.EnableMetrics)
	if config.EnableMetrics {
		fmt.Printf("Порт метрик: %d\n", config.MetricsPort)
	}
	fmt.Printf("=======================\n\n")

	// Создаем соответствующую базу данных логов
	db, err := logdb.CreateLogDB(config.Mode, config.BaseURL, logdb.Options{
		BatchSize:  config.BulkSize,
		Timeout:    10 * time.Second,
		RetryCount: config.MaxRetries,
		RetryDelay: config.RetryDelay,
		Verbose:    config.Verbose,
	})

	if err != nil {
		log.Fatalf("Ошибка при создании базы данных логов: %v", err)
	}

	// Запуск генератора логов
	if err := pkg.RunGenerator(config, db); err != nil {
		log.Fatalf("Ошибка при запуске генератора: %v", err)
	}
}
