package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
)

// Параметры командной строки по умолчанию
const (
	defaultConfigPath = "config.yaml"
)

func main() {
	// Параметры командной строки
	configPath := flag.String("config", defaultConfigPath, "Путь к файлу конфигурации YAML")
	legacyMode := flag.Bool("legacy", false, "Использовать режим аргументов командной строки вместо YAML")
	defaultConfig := flag.Bool("default-config", false, "Создать конфигурацию по умолчанию и выйти")

	flag.Parse()

	// Создание конфигурации по умолчанию, если запрошено
	if *defaultConfig {
		config := getDefaultConfig()
		if err := common.SaveConfig(config, *configPath); err != nil {
			log.Fatalf("Ошибка создания конфигурации по умолчанию: %v", err)
		}
		fmt.Printf("Конфигурация по умолчанию создана в файле: %s\n", *configPath)
		return
	}

	// Определение каталога с текущим исполняемым файлом
	execDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalf("Не удалось определить текущий каталог: %v", err)
	}

	// Если запрошен режим legacy, используем аргументы командной строки
	if *legacyMode {
		log.Println("Запуск в режиме legacy с использованием аргументов командной строки")
		runInLegacyMode()
		return
	}

	// Загружаем конфигурацию из YAML
	configFullPath := *configPath
	if !filepath.IsAbs(configFullPath) {
		configFullPath = filepath.Join(execDir, *configPath)
	}

	config, err := common.LoadConfig(configFullPath)
	if err != nil {
		// Если файл не существует и это стандартный путь, создаем конфигурацию по умолчанию
		if os.IsNotExist(err) && *configPath == defaultConfigPath {
			fmt.Println("Файл конфигурации не найден, создаем конфигурацию по умолчанию")
			config = getDefaultConfig()
			if err := common.SaveConfig(config, configFullPath); err != nil {
				log.Fatalf("Ошибка создания конфигурации по умолчанию: %v", err)
			}
			fmt.Printf("Конфигурация по умолчанию создана в файле: %s\n", configFullPath)
		} else {
			log.Fatalf("Ошибка загрузки конфигурации: %v", err)
		}
	}

	// Создаем структуру для статистики
	stats := &common.Stats{
		StartTime: time.Now(),
	}

	// Запускаем нужные компоненты в зависимости от режима
	switch config.Mode {
	case modeGeneratorOnly:
		fmt.Println("Запуск в режиме генератора логов")
		runGeneratorWithConfig(config, stats)
	case modeQuerierOnly:
		fmt.Println("Запуск в режиме клиента запросов")
		if err := runQuerierWithConfig(config, stats); err != nil {
			log.Fatalf("Ошибка при запуске клиента запросов: %v", err)
		}
	case modeCombined:
		fmt.Println("Запуск в комбинированном режиме (генератор и запросы)")
		go runGeneratorWithConfig(config, stats)
		if err := runQuerierWithConfig(config, stats); err != nil {
			log.Fatalf("Ошибка при запуске клиента запросов: %v", err)
		}
	default:
		log.Fatalf("Неизвестный режим работы: %s", config.Mode)
	}
}

// Создание конфигурации по умолчанию
func getDefaultConfig() *common.Config {
	return &common.Config{
		Mode:            "generator",
		System:          "victoria",
		DurationSeconds: 60,
		Generator: common.GeneratorConfig{
			URLLoki:         "http://loki:3100/loki/api/v1/push",
			URLES:           "http://elasticsearch:9200/_bulk",
			URLVictoria:     "http://victoria-logs:9428/api/v1/write",
			RPS:             100,
			BulkSize:        10,
			WorkerCount:     4,
			ConnectionCount: 10,
			Distribution: map[string]int{
				"web_access":  60,
				"web_error":   10,
				"application": 20,
				"event":       5,
				"metric":      5,
			},
			MaxRetries:   3,
			RetryDelayMs: 500,
			Verbose:      false,
		},
		Querier: common.QuerierConfig{
			URLLoki:      "http://loki:3100",
			URLES:        "http://elasticsearch:9200",
			URLVictoria:  "http://victoria-logs:9428",
			RPS:          10,
			WorkerCount:  2,
			MaxRetries:   3,
			RetryDelayMs: 500,
			Verbose:      false,
			Distribution: map[string]int{
				"exact_match":   30,
				"time_range":    40,
				"label_filter":  20,
				"complex_query": 10,
			},
		},
		Metrics: common.MetricsConfig{
			Enabled: true,
			Port:    9090,
		},
	}
}

// Запуск в режиме аргументов командной строки (legacy)
func runInLegacyMode() {
	// Основные флаги
	mode := flag.String("mode", "generator", "Режим работы: generator (только генерация), querier (только запросы), combined (генерация и запросы)")
	system := flag.String("system", "victoria", "Система логирования: loki, elasticsearch, victoria")

	// Общие параметры
	duration := flag.Int("duration", 60, "Длительность работы (в секундах, 0 = бесконечно)")

	// Параметры для режима генерации
	rps := flag.Int("rps", 100, "Запросов в секунду (для генератора)")
	bulkSize := flag.Int("bulk-size", 10, "Количество логов в одном запросе")
	workerCount := flag.Int("worker-count", 4, "Количество параллельных воркеров")
	connectionCount := flag.Int("connection-count", 10, "Количество соединений")
	maxRetries := flag.Int("max-retries", 3, "Максимальное количество повторных попыток")
	retryDelay := flag.Int("retry-delay", 500, "Задержка между повторными попытками (в миллисекундах)")
	verbose := flag.Bool("verbose", false, "Подробный вывод")

	// Веса типов логов
	webAccessWeight := flag.Int("web-access-weight", 60, "Вес логов типа web_access")
	webErrorWeight := flag.Int("web-error-weight", 10, "Вес логов типа web_error")
	applicationWeight := flag.Int("application-weight", 20, "Вес логов типа application")
	eventWeight := flag.Int("event-weight", 5, "Вес логов типа event")
	metricWeight := flag.Int("metric-weight", 5, "Вес логов типа metric")

	// Параметры для режима запросов
	qps := flag.Int("qps", 10, "Запросов в секунду (для клиента запросов)")

	// Веса типов запросов
	exactMatchWeight := flag.Int("exact-match-weight", 30, "Вес запросов типа exact_match")
	timeRangeWeight := flag.Int("time-range-weight", 40, "Вес запросов типа time_range")
	labelFilterWeight := flag.Int("label-filter-weight", 20, "Вес запросов типа label_filter")
	complexQueryWeight := flag.Int("complex-query-weight", 10, "Вес запросов типа complex_query")

	// Параметры метрик
	metricsEnabled := flag.Bool("enable-metrics", true, "Включить метрики Prometheus")
	metricsPort := flag.Int("metrics-port", 9090, "Порт для сервера метрик Prometheus")

	// Повторный разбор флагов, так как они уже были разобраны выше
	flag.CommandLine.Parse(os.Args[1:])

	// Создаем конфигурацию на основе аргументов командной строки
	config := &common.Config{
		Mode:            *mode,
		System:          *system,
		DurationSeconds: *duration,
		Generator: common.GeneratorConfig{
			URLLoki:         "http://loki:3100/loki/api/v1/push",
			URLES:           "http://elasticsearch:9200/_bulk",
			URLVictoria:     "http://victoria-logs:9428/api/v1/write",
			RPS:             *rps,
			BulkSize:        *bulkSize,
			WorkerCount:     *workerCount,
			ConnectionCount: *connectionCount,
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
			URLLoki:      "http://loki:3100",
			URLES:        "http://elasticsearch:9200",
			URLVictoria:  "http://victoria-logs:9428",
			RPS:          *qps,
			WorkerCount:  *workerCount,
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
		Metrics: common.MetricsConfig{
			Enabled: *metricsEnabled,
			Port:    *metricsPort,
		},
	}

	// Создаем структуру для статистики
	stats := &common.Stats{
		StartTime: time.Now(),
	}

	// Запускаем нужные компоненты в зависимости от режима
	switch *mode {
	case modeGeneratorOnly:
		fmt.Println("Запуск в режиме генератора логов (legacy)")
		runGeneratorWithConfig(config, stats)
	case modeQuerierOnly:
		fmt.Println("Запуск в режиме клиента запросов (legacy)")
		runQuerierWithConfig(config, stats)
	case modeCombined:
		fmt.Println("Запуск в комбинированном режиме - генератор и запросы (legacy)")
		go runGeneratorWithConfig(config, stats)
		runQuerierWithConfig(config, stats)
	default:
		log.Fatalf("Неизвестный режим работы: %s", *mode)
	}
}

// Функция запуска генератора логов с новой конфигурацией
func runGeneratorWithConfig(cfg *common.Config, stats *common.Stats) {
	generatorConfig := generator.Config{
		Mode:                "default",
		BaseURL:             cfg.Generator.GetURL(cfg.System),
		URL:                 cfg.Generator.GetURL(cfg.System),
		RPS:                 cfg.Generator.RPS,
		Duration:            time.Duration(cfg.DurationSeconds) * time.Second,
		BulkSize:            cfg.Generator.BulkSize,
		WorkerCount:         cfg.Generator.WorkerCount,
		ConnectionCount:     cfg.Generator.ConnectionCount,
		LogTypeDistribution: cfg.Generator.Distribution,
		MaxRetries:          cfg.Generator.MaxRetries,
		RetryDelay:          time.Duration(cfg.Generator.RetryDelayMs) * time.Millisecond,
		Verbose:             cfg.Generator.Verbose,
		EnableMetrics:       cfg.Metrics.Enabled,
		MetricsPort:         cfg.Metrics.Port,
	}

	// Включаем метрики, если нужно
	if cfg.Metrics.Enabled {
		generator.InitPrometheus(generatorConfig)
		generator.StartMetricsServer(cfg.Metrics.Port, generatorConfig)
	}

	// Создаем клиент базы данных логов
	options := logdb.Options{
		BatchSize:  generatorConfig.BulkSize,
		Timeout:    30 * time.Second, // Таймаут запросов
		RetryCount: generatorConfig.MaxRetries,
		RetryDelay: generatorConfig.RetryDelay,
		Verbose:    generatorConfig.Verbose,
	}

	db, err := logdb.CreateLogDB(cfg.System, generatorConfig.URL, options)
	if err != nil {
		log.Fatalf("Ошибка создания клиента базы данных: %v", err)
	}

	fmt.Printf("[Генератор] Запуск с RPS=%d, система=%s\n", cfg.Generator.RPS, cfg.System)

	// Запускаем генератор
	if err := generator.RunGenerator(generatorConfig, db); err != nil {
		log.Fatalf("Ошибка при запуске генератора: %v", err)
	}
}

// runQuerierWithConfig запускает клиент запросов с новой конфигурацией из YAML
func runQuerierWithConfig(config *common.Config, stats *common.Stats) error {
	log.Printf("Запуск клиента запросов для системы %s\n", config.System)

	// Определяем URL для выбранной системы
	var baseURL string
	switch config.System {
	case "victorialogs", "victoria":
		baseURL = config.Querier.URLVictoria
	case "elasticsearch", "es":
		baseURL = config.Querier.URLES
	case "loki":
		baseURL = config.Querier.URLLoki
	default:
		return fmt.Errorf("неизвестная система логирования: %s", config.System)
	}

	// Создаем опции для исполнителя запросов
	options := models.Options{
		Timeout:    10 * time.Second, // По умолчанию 10 секунд
		RetryCount: config.Querier.MaxRetries,
		RetryDelay: time.Duration(config.Querier.RetryDelayMs) * time.Millisecond,
		Verbose:    config.Querier.Verbose,
	}

	// Создаем исполнитель запросов для выбранной системы
	executor, err := go_querier.CreateQueryExecutor(config.System, baseURL, options)
	if err != nil {
		return fmt.Errorf("ошибка создания исполнителя запросов: %v", err)
	}

	// Создаем конфигурацию для модуля запросов
	queryConfig := go_querier.QueryConfig{
		Mode:            config.System,
		BaseURL:         baseURL,
		QPS:             config.Querier.RPS,
		DurationSeconds: config.DurationSeconds,
		WorkerCount:     config.Querier.WorkerCount,
		QueryTypeDistribution: map[models.QueryType]int{
			models.SimpleQuery:     config.Querier.Distribution["simple"],
			models.ComplexQuery:    config.Querier.Distribution["complex"],
			models.AnalyticalQuery: config.Querier.Distribution["analytical"],
			models.TimeSeriesQuery: config.Querier.Distribution["timeseries"],
		},
		QueryTimeout: 10 * time.Second, // По умолчанию 10 секунд
		MaxRetries:   config.Querier.MaxRetries,
		RetryDelayMs: config.Querier.RetryDelayMs,
		Verbose:      config.Querier.Verbose,
	}

	log.Printf("[Запросы] Запуск с QPS=%d, система=%s\n", config.Querier.RPS, config.System)

	// Запускаем модуль запросов
	return go_querier.RunQuerier(queryConfig, executor, stats)
}
