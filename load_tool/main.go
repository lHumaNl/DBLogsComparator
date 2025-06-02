package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	generatorpkg "github.com/dblogscomparator/load_tool/go_generator/pkg"
	querypkg "github.com/dblogscomparator/load_tool/go_querier/pkg"
	"github.com/dblogscomparator/load_tool/common"
	"github.com/dblogscomparator/load_tool/go_generator/logdb"
)

const (
	modeGeneratorOnly = "generator"
	modeQuerierOnly   = "querier"
	modeCombined      = "combined"
)

func main() {
	// Основные флаги
	mode := flag.String("mode", "generator", "Режим работы: generator (только генерация), querier (только запросы), combined (генерация и запросы)")
	system := flag.String("system", "victoria", "Система логирования: victoria, es, loki")
	baseURL := flag.String("url", "", "Базовый URL для отправки логов/запросов")
	duration := flag.Duration("duration", 5*time.Minute, "Продолжительность теста")
	enableMetrics := flag.Bool("metrics", true, "Включить метрики Prometheus")
	metricsPort := flag.Int("metrics-port", 9090, "Порт для метрик Prometheus")
	verbose := flag.Bool("verbose", false, "Подробный вывод")

	// Флаги для генератора
	rps := flag.Int("rps", 10, "Количество запросов (на запись) в секунду")
	bulkSize := flag.Int("bulk-size", 100, "Количество логов в одном запросе")
	generatorWorkers := flag.Int("generator-workers", 0, "Количество рабочих горутин для генератора (0 = авто)")

	// Распределение типов логов
	webAccessWeight := flag.Int("web-access-weight", 60, "Вес для логов web_access")
	webErrorWeight := flag.Int("web-error-weight", 10, "Вес для логов web_error")
	applicationWeight := flag.Int("application-weight", 20, "Вес для логов application")
	metricWeight := flag.Int("metric-weight", 5, "Вес для логов metric")
	eventWeight := flag.Int("event-weight", 5, "Вес для логов event")

	// Флаги для querier
	qps := flag.Int("qps", 5, "Количество запросов (на чтение) в секунду")
	querierWorkers := flag.Int("querier-workers", 0, "Количество рабочих горутин для запросов (0 = авто)")
	queryTimeout := flag.Duration("query-timeout", 10*time.Second, "Таймаут для запросов")
	
	// Типы запросов и их распределение
	simpleQueryWeight := flag.Int("simple-query-weight", 70, "Вес для простых запросов")
	complexQueryWeight := flag.Int("complex-query-weight", 20, "Вес для сложных запросов")
	analyticalQueryWeight := flag.Int("analytical-query-weight", 10, "Вес для аналитических запросов")
	timeseriesQueryWeight := flag.Int("timeseries-query-weight", 0, "Вес для запросов временных рядов")

	// Дополнительные параметры
	maxRetries := flag.Int("max-retries", 3, "Максимальное количество повторных попыток")
	retryDelay := flag.Duration("retry-delay", 500*time.Millisecond, "Задержка между повторными попытками")

	flag.Parse()

	// Проверяем и устанавливаем URL по умолчанию если не указан
	if *baseURL == "" {
		switch *system {
		case "victoria":
			*baseURL = "http://localhost:8428"
		case "es":
			*baseURL = "http://localhost:9200"
		case "loki":
			*baseURL = "http://localhost:3100"
		default:
			log.Fatalf("Неизвестная система логирования: %s", *system)
		}
	}

	// Настройка распределения типов логов
	logTypeDistribution := map[string]int{
		"web_access":  *webAccessWeight,
		"web_error":   *webErrorWeight,
		"application": *applicationWeight,
		"metric":      *metricWeight,
		"event":       *eventWeight,
	}

	// Настройка распределения типов запросов
	queryTypeDistribution := map[querypkg.QueryType]int{
		querypkg.SimpleQuery:     *simpleQueryWeight,
		querypkg.ComplexQuery:    *complexQueryWeight,
		querypkg.AnalyticalQuery: *analyticalQueryWeight,
		querypkg.TimeSeriesQuery: *timeseriesQueryWeight,
	}

	// Вывод информации о режиме работы
	fmt.Printf("=== Инструмент нагрузочного тестирования ===\n")
	fmt.Printf("Режим: %s\n", *mode)
	fmt.Printf("Система: %s\n", *system)
	fmt.Printf("URL: %s\n", *baseURL)
	fmt.Printf("Продолжительность: %s\n", *duration)
	fmt.Printf("=======================\n\n")

	// Запускаем Prometheus сервер, если метрики включены
	if *enableMetrics {
		common.InitPrometheus()
		common.StartMetricsServer(*metricsPort)
		fmt.Printf("Prometheus метрики доступны на порту %d\n", *metricsPort)
	}

	// Создаем общую статистику
	stats := common.NewStats()

	// Запуск в зависимости от режима
	switch *mode {
	case modeGeneratorOnly:
		runGenerator(*system, *baseURL, *rps, *duration, *bulkSize, *generatorWorkers,
			logTypeDistribution, *maxRetries, *retryDelay, *verbose, stats)

	case modeQuerierOnly:
		runQuerier(*system, *baseURL, *qps, *duration, *querierWorkers,
			queryTypeDistribution, *queryTimeout, *maxRetries, *retryDelay, *verbose, stats)

	case modeCombined:
		// Запускаем оба модуля параллельно
		generatorDone := make(chan struct{})
		querierDone := make(chan struct{})

		go func() {
			runGenerator(*system, *baseURL, *rps, *duration, *bulkSize, *generatorWorkers,
				logTypeDistribution, *maxRetries, *retryDelay, *verbose, stats)
			close(generatorDone)
		}()

		// Даем генератору немного времени для создания данных перед запуском запросов
		time.Sleep(10 * time.Second)

		go func() {
			runQuerier(*system, *baseURL, *qps, *duration, *querierWorkers,
				queryTypeDistribution, *queryTimeout, *maxRetries, *retryDelay, *verbose, stats)
			close(querierDone)
		}()

		// Ждем завершения обоих модулей
		<-generatorDone
		<-querierDone

	default:
		log.Fatalf("Неизвестный режим работы: %s", *mode)
	}

	// Вывод общей статистики
	stats.PrintSummary()
	fmt.Println("Тест завершен!")
}

// Функция запуска генератора логов
func runGenerator(mode, baseURL string, rps int, duration time.Duration, bulkSize, workerCount int,
	logTypeDistribution map[string]int, maxRetries int, retryDelay time.Duration, verbose bool, stats *common.Stats) {

	// Настройка конфигурации генератора
	generatorConfig := generatorpkg.Config{
		Mode:                mode,
		BaseURL:             baseURL,
		URL:                 baseURL,
		RPS:                 rps,
		Duration:            duration,
		BulkSize:            bulkSize,
		WorkerCount:         workerCount,
		LogTypeDistribution: logTypeDistribution,
		Verbose:             verbose,
		MaxRetries:          maxRetries,
		RetryDelay:          retryDelay,
	}

	fmt.Printf("[Генератор] Запуск с RPS=%d, BulkSize=%d\n", rps, bulkSize)

	// Создаем соответствующую базу данных логов
	db, err := logdb.CreateLogDB(generatorConfig.Mode, generatorConfig.BaseURL, logdb.Options{
		BatchSize:  generatorConfig.BulkSize,
		Timeout:    10 * time.Second,
		RetryCount: generatorConfig.MaxRetries,
		RetryDelay: generatorConfig.RetryDelay,
		Verbose:    generatorConfig.Verbose,
	})

	if err != nil {
		log.Fatalf("Ошибка при создании базы данных логов: %v", err)
	}

	// Запускаем генератор
	if err := generatorpkg.RunGenerator(generatorConfig, db, stats); err != nil {
		log.Fatalf("Ошибка при запуске генератора: %v", err)
	}
}

// Функция запуска модуля запросов
func runQuerier(mode, baseURL string, qps int, duration time.Duration, workerCount int,
	queryTypeDistribution map[querypkg.QueryType]int, queryTimeout time.Duration,
	maxRetries int, retryDelay time.Duration, verbose bool, stats *common.Stats) {

	// Настройка конфигурации модуля запросов
	querierConfig := querypkg.QueryConfig{
		Mode:                mode,
		BaseURL:             baseURL,
		QPS:                 qps,
		Duration:            duration,
		WorkerCount:         workerCount,
		QueryTypeDistribution: queryTypeDistribution,
		QueryTimeout:        queryTimeout,
		MaxRetries:          maxRetries,
		RetryDelay:          retryDelay,
		Verbose:             verbose,
	}

	fmt.Printf("[Запросник] Запуск с QPS=%d\n", qps)

	// Создаем исполнитель запросов
	executor, err := querypkg.CreateQueryExecutor(mode, baseURL, querypkg.Options{
		Timeout:    queryTimeout,
		RetryCount: maxRetries,
		RetryDelay: retryDelay,
		Verbose:    verbose,
	})

	if err != nil {
		log.Fatalf("Ошибка при создании исполнителя запросов: %v", err)
	}

	// Запускаем модуль запросов
	if err := querypkg.RunQuerier(querierConfig, executor, stats); err != nil {
		log.Fatalf("Ошибка при запуске модуля запросов: %v", err)
	}
}
