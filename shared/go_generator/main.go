package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Конфигурация нагрузки
type Config struct {
	Mode            string
	BaseURL         string
	URL             string
	RPS             int
	Duration        time.Duration
	BulkSize        int
	WorkerCount     int
	ConnectionCount int
	LogTypeDistribution map[string]int
	Verbose        bool
	MaxRetries     int
	RetryDelay     time.Duration
	EnableMetrics  bool
	MetricsPort    int
}

// Статистика выполнения
type Stats struct {
	TotalRequests      int64
	SuccessfulRequests int64
	FailedRequests     int64
	TotalLogs          int64
	RetriedRequests    int64
	StartTime          time.Time
}

// Prometheus метрики
var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_requests_total",
			Help: "Общее количество запросов, отправленных генератором логов",
		},
		[]string{"status", "destination"},
	)
	
	logsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "log_generator_logs_total",
			Help: "Общее количество сгенерированных логов по типам",
		},
		[]string{"log_type", "destination"},
	)
	
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "log_generator_request_duration_seconds",
			Help:    "Время выполнения запросов",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status", "destination"},
	)
	
	retryCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "log_generator_retry_count",
			Help: "Количество повторных попыток отправки запросов",
		},
	)
	
	rpsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_rps",
			Help: "Текущее количество запросов в секунду",
		},
	)
	
	lpsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_lps",
			Help: "Текущее количество логов в секунду",
		},
	)
	
	batchSizeGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_generator_batch_size",
			Help: "Размер пакета логов",
		},
	)
)

// Инициализация метрик Prometheus
func initPrometheus(config Config) {
	// Устанавливаем начальное значение для размера пакета
	batchSizeGauge.Set(float64(config.BulkSize))
}

// Пул HTTP-клиентов для переиспользования соединений
type ClientPool struct {
	clients []*http.Client
	mutex   sync.Mutex
	index   int
}

func NewClientPool(size int) *ClientPool {
	pool := &ClientPool{
		clients: make([]*http.Client, size),
	}

	for i := 0; i < size; i++ {
		pool.clients[i] = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 10 * time.Second,
		}
	}

	return pool
}

func (p *ClientPool) Get() *http.Client {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	client := p.clients[p.index]
	p.index = (p.index + 1) % len(p.clients)
	return client
}

// Пул буферов для минимизации аллокаций памяти
type BufferPool struct {
	pool sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *BufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func (p *BufferPool) Put(buf *bytes.Buffer) {
	p.pool.Put(buf)
}

// Структуры для различных типов логов
type BaseLog struct {
	Timestamp     string `json:"timestamp"`
	LogType       string `json:"log_type"`
	Host          string `json:"host"`
	ContainerName string `json:"container_name"`
}

type WebAccessLog struct {
	BaseLog
	RemoteAddr    string  `json:"remote_addr"`
	Request       string  `json:"request"`
	Status        int     `json:"status"`
	BytesSent     int     `json:"bytes_sent"`
	HttpReferer   string  `json:"http_referer"`
	HttpUserAgent string  `json:"http_user_agent"`
	RequestTime   float64 `json:"request_time"`
	Message       string  `json:"message"`
}

type WebErrorLog struct {
	BaseLog
	Level     string `json:"level"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code"`
	Service   string `json:"service"`
}

type ApplicationLog struct {
	BaseLog
	Level      string `json:"level"`
	Message    string `json:"message"`
	Service    string `json:"service"`
	RequestID  string `json:"request_id"`
	UserID     string `json:"user_id"`
	DurationMs int    `json:"duration_ms"`
	Context    struct {
		IP       string `json:"ip"`
		Endpoint string `json:"endpoint"`
	} `json:"context"`
}

type MetricLog struct {
	BaseLog
	MetricName string  `json:"metric_name"`
	Value      float64 `json:"value"`
	Service    string  `json:"service"`
	Region     string  `json:"region"`
	Message    string  `json:"message"`
}

type EventLog struct {
	BaseLog
	EventType  string `json:"event_type"`
	Message    string `json:"message"`
	ResourceID string `json:"resource_id"`
	Namespace  string `json:"namespace"`
}

// Генераторы случайных данных
func generateRandomIP() string {
	return fmt.Sprintf("192.168.%d.%d", rand.Intn(254)+1, rand.Intn(254)+1)
}

func generateRandomUserAgent() string {
	browsers := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
	}
	return browsers[rand.Intn(len(browsers))]
}

func generateRandomHttpStatus() int {
	statuses := []int{200, 200, 200, 200, 301, 302, 304, 400, 401, 403, 404, 500}
	return statuses[rand.Intn(len(statuses))]
}

func generateRandomHttpMethod() string {
	methods := []string{"GET", "GET", "GET", "POST", "PUT", "DELETE"}
	return methods[rand.Intn(len(methods))]
}

func generateRandomPath() string {
	paths := []string{
		"/api/users",
		"/api/products",
		"/api/orders",
		"/api/auth/login",
		"/api/auth/logout",
		"/api/settings",
		"/static/js/main.js",
		"/static/css/style.css",
		"/static/img/logo.png",
		"/",
		"/about",
		"/contact",
	}
	return paths[rand.Intn(len(paths))]
}

func generateRandomLogLevel() string {
	levels := []string{"info", "info", "info", "warn", "error", "debug"}
	return levels[rand.Intn(len(levels))]
}

func generateRandomErrorMessage() string {
	errors := []string{
		"Connection refused",
		"Timeout exceeded",
		"Invalid credentials",
		"Resource not found",
		"Permission denied",
		"Internal server error",
		"Bad request",
		"Service unavailable",
		"Gateway timeout",
		"Client timed out",
	}
	return errors[rand.Intn(len(errors))]
}

func generateRandomService() string {
	services := []string{
		"auth-service",
		"user-service",
		"product-service",
		"order-service",
		"payment-service",
		"notification-service",
		"email-service",
		"search-service",
		"recommendation-service",
		"analytics-service",
	}
	return services[rand.Intn(len(services))]
}

func generateRandomHost() string {
	hostPrefixes := []string{"app", "web", "api", "db", "cache", "worker"}
	hostNumber := rand.Intn(10) + 1
	return fmt.Sprintf("%s-%d", hostPrefixes[rand.Intn(len(hostPrefixes))], hostNumber)
}

func generateRandomContainer() string {
	containerPrefixes := []string{"nginx", "app", "redis", "postgres", "mongo", "rabbitmq", "elasticsearch"}
	containerNumber := rand.Intn(10) + 1
	return fmt.Sprintf("%s-%d", containerPrefixes[rand.Intn(len(containerPrefixes))], containerNumber)
}

// Генерация случайного лога определенного типа
func generateLog(logType string, timestamp string) interface{} {
	host := generateRandomHost()
	containerName := generateRandomContainer()

	baseLog := BaseLog{
		Timestamp:     timestamp,
		LogType:       logType,
		Host:          host,
		ContainerName: containerName,
	}

	switch logType {
	case "web_access":
		remoteAddr := generateRandomIP()
		method := generateRandomHttpMethod()
		path := generateRandomPath()
		status := generateRandomHttpStatus()
		bytesSent := rand.Intn(50000) + 100
		requestTime := float64(rand.Intn(2000)) / 1000.0
		userAgent := generateRandomUserAgent()

		return WebAccessLog{
			BaseLog:       baseLog,
			RemoteAddr:    remoteAddr,
			Request:       fmt.Sprintf("%s %s HTTP/1.1", method, path),
			Status:        status,
			BytesSent:     bytesSent,
			HttpReferer:   "-",
			HttpUserAgent: userAgent,
			RequestTime:   requestTime,
			Message:       fmt.Sprintf("%s - - [%s] \"%s %s HTTP/1.1\" %d %d \"-\" \"%s\" %.3f", remoteAddr, time.Now().UTC().Format("02/Jan/2006:15:04:05 -0700"), method, path, status, bytesSent, userAgent, requestTime),
		}

	case "web_error":
		errorMessage := generateRandomErrorMessage()
		errorCode := rand.Intn(10) + 1

		return WebErrorLog{
			BaseLog:   baseLog,
			Level:     "error",
			Message:   errorMessage,
			ErrorCode: errorCode,
			Service:   "nginx",
		}

	case "application":
		logLevel := generateRandomLogLevel()
		service := generateRandomService()
		requestID := fmt.Sprintf("req-%d", rand.Intn(90000)+10000)
		userID := fmt.Sprintf("user-%d", rand.Intn(9000)+1000)
		durationMs := rand.Intn(490) + 10

		appLog := ApplicationLog{
			BaseLog:    baseLog,
			Level:      logLevel,
			Message:    generateLogMessage(logLevel),
			Service:    service,
			RequestID:  requestID,
			UserID:     userID,
			DurationMs: durationMs,
		}
		appLog.Context.IP = generateRandomIP()
		appLog.Context.Endpoint = generateRandomPath()

		return appLog

	case "metric":
		metricNames := []string{"cpu_usage", "memory_usage", "disk_usage", "network_in", "network_out", "request_count", "error_rate", "response_time"}
		metricName := metricNames[rand.Intn(len(metricNames))]
		var value float64
		if strings.Contains(metricName, "usage") {
			value = float64(rand.Intn(10000)) / 100.0
		} else {
			value = float64(rand.Intn(10000))
		}
		regions := []string{"eu-west-1", "us-east-1", "us-west-2", "ap-southeast-1"}

		return MetricLog{
			BaseLog:    baseLog,
			MetricName: metricName,
			Value:      value,
			Service:    generateRandomService(),
			Region:     regions[rand.Intn(len(regions))],
			Message:    fmt.Sprintf("%s on %s: %.2f", metricName, host, value),
		}

	case "event":
		eventTypes := []string{"container_started", "container_stopped", "deployment_started", "deployment_completed", "backup_started", "backup_completed", "alert_triggered", "alert_resolved"}
		eventType := eventTypes[rand.Intn(len(eventTypes))]
		resourceID := fmt.Sprintf("%s-%d", strings.Split(eventType, "_")[0], rand.Intn(9000)+1000)
		namespaces := []string{"production", "staging", "development", "testing"}

		return EventLog{
			BaseLog:    baseLog,
			EventType:  eventType,
			Message:    fmt.Sprintf("%s for %s", strings.ReplaceAll(strings.Title(strings.ReplaceAll(eventType, "_", " ")), "Started", "started"), resourceID),
			ResourceID: resourceID,
			Namespace:  namespaces[rand.Intn(len(namespaces))],
		}
	}

	return baseLog
}

func generateLogMessage(logLevel string) string {
	switch logLevel {
	case "error":
		return generateRandomErrorMessage()
	case "warn":
		return "Warning: something went wrong"
	case "info":
		return "Info: operation completed successfully"
	case "debug":
		return "Debug: some debug message"
	default:
		return "Unknown log level"
	}
}

// Выбор случайного типа лога на основе распределения
func selectRandomLogType(distribution map[string]int) string {
	total := 0
	for _, weight := range distribution {
		total += weight
	}

	r := rand.Intn(total)
	cumulative := 0

	for logType, weight := range distribution {
		cumulative += weight
		if r < cumulative {
			return logType
		}
	}

	// Если что-то пошло не так, возвращаем web_access
	return "web_access"
}

// Создание пакета логов
func createBulkPayload(config Config, bufferPool *BufferPool) string {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	if config.Mode == "vector" {
		// Для Vector отправляем массив объектов
		buf.WriteString("[")
		for i := 0; i < config.BulkSize; i++ {
			logType := selectRandomLogType(config.LogTypeDistribution)
			log := generateLog(logType, timestamp)

			logJSON, err := json.Marshal(log)
			if err != nil {
				continue
			}

			buf.Write(logJSON)
			if i < config.BulkSize-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("]")
	} else {
		// Для VictoriaLogs отправляем NDJSON (каждый объект в отдельной строке)
		for i := 0; i < config.BulkSize; i++ {
			logType := selectRandomLogType(config.LogTypeDistribution)
			log := generateLog(logType, timestamp)

			logJSON, err := json.Marshal(log)
			if err != nil {
				continue
			}

			buf.Write(logJSON)
			if i < config.BulkSize-1 {
				buf.WriteString("\n")
			}
		}
	}

	return buf.String()
}

// Подсчет количества логов каждого типа в пакете
func countLogTypes(payload string) map[string]int {
	counts := make(map[string]int)
	
	// Разделение пакета на отдельные строки
	lines := strings.Split(payload, "\n")
	
	for _, line := range lines {
		if line == "" {
			continue
		}
		
		// Попытка распарсить JSON
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			continue
		}
		
		// Проверка наличия поля log_type
		if logType, ok := logEntry["log_type"].(string); ok {
			counts[logType]++
		}
	}
	
	return counts
}

// Отправка пакета логов с повторными попытками
func sendBulk(payload string, client *http.Client, config Config, stats *Stats) error {
	var url string
	var contentType string
	var destination string

	if config.Mode == "vector" {
		url = config.URL
		contentType = "application/json"
		destination = "vector"
	} else if config.Mode == "victoria" || config.Mode == "victorialogs" {
		url = config.URL
		contentType = "application/x-ndjson"
		destination = "victorialogs"
	} else if config.Mode == "es" || config.Mode == "elasticsearch" {
		url = config.URL
		contentType = "application/x-ndjson"
		destination = "elasticsearch"
	} else if config.Mode == "loki" {
		url = config.URL
		contentType = "application/json"
		destination = "loki"
	}
	
	// Установка размера пакета в метрике
	if config.EnableMetrics {
		batchSizeGauge.Set(float64(config.BulkSize))
	}

	var lastErr error
	
	// Попытки отправки с повторами при ошибках
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Увеличиваем счетчик повторных попыток
			atomic.AddInt64(&stats.RetriedRequests, 1)
			if config.EnableMetrics {
				retryCounter.Inc()
			}
			
			// Экспоненциальная задержка перед повторной попыткой
			backoff := config.RetryDelay * time.Duration(1<<uint(attempt-1))
			if config.Verbose {
				log.Printf("Повторная попытка %d/%d после ошибки: %v (задержка: %v)", 
					attempt, config.MaxRetries, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		req, err := http.NewRequest("POST", url, strings.NewReader(payload))
		if err != nil {
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", "application/json")

		requestStart := time.Now()
		resp, err := client.Do(req)
		requestEnd := time.Now()
		
		if err != nil {
			lastErr = err
			continue
		}

		defer resp.Body.Close()
		
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Успешная отправка
			if config.EnableMetrics {
				duration := requestEnd.Sub(requestStart).Seconds()
				requestDuration.WithLabelValues("success", destination).Observe(duration)
				requestsTotal.WithLabelValues("success", destination).Inc()
				
				// Увеличиваем счетчики для каждого типа лога в пакете
				for logType, count := range countLogTypes(payload) {
					logsTotal.WithLabelValues(logType, destination).Add(float64(count))
				}
			}
			return nil
		}
		
		// Неуспешный статус
		lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		
		if config.EnableMetrics {
			duration := requestEnd.Sub(requestStart).Seconds()
			requestDuration.WithLabelValues("error", destination).Observe(duration)
			requestsTotal.WithLabelValues("error", destination).Inc()
		}
		
		// Если это серверная ошибка (5xx), повторяем попытку
		// Для клиентских ошибок (4xx) нет смысла повторять
		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}

// Горутина для отправки логов
func worker(id int, jobs <-chan struct{}, stats *Stats, config Config, clientPool *ClientPool, bufferPool *BufferPool, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
		payload := createBulkPayload(config, bufferPool)
		client := clientPool.Get()

		err := sendBulk(payload, client, config, stats)
		if err != nil {
			atomic.AddInt64(&stats.FailedRequests, 1)
			if config.Verbose {
				log.Printf("Worker %d: Error sending bulk: %v", id, err)
			}
		} else {
			atomic.AddInt64(&stats.SuccessfulRequests, 1)
			atomic.AddInt64(&stats.TotalLogs, int64(config.BulkSize))
		}

		atomic.AddInt64(&stats.TotalRequests, 1)
	}
}

// Горутина для отображения статистики
func statsReporter(stats *Stats, stopChan <-chan struct{}, config Config) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	var lastTotalReqs int64
	var lastTotalLogs int64
	var lastTime time.Time = time.Now()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(stats.StartTime).Seconds()
			totalReqs := atomic.LoadInt64(&stats.TotalRequests)
			successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
			failedReqs := atomic.LoadInt64(&stats.FailedRequests)
			totalLogs := atomic.LoadInt64(&stats.TotalLogs)
			retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)
			
			// Расчет текущего RPS и LPS за последний интервал
			intervalSeconds := now.Sub(lastTime).Seconds()
			currentRPS := float64(totalReqs - lastTotalReqs) / intervalSeconds
			currentLPS := float64(totalLogs - lastTotalLogs) / intervalSeconds
			
			// Обновление метрик
			if config.EnableMetrics {
				rpsGauge.Set(currentRPS)
				lpsGauge.Set(currentLPS)
			}
			
			// Сохранение значений для следующего интервала
			lastTotalReqs = totalReqs
			lastTotalLogs = totalLogs
			lastTime = now

			// Вывод статистики
			fmt.Printf("\rElapsed: %.1fs | Requests: %d (%.1f/s) | Logs: %d (%.1f/s) | Success: %d | Failed: %d | Retried: %d",
				elapsed, totalReqs, currentRPS, totalLogs, currentLPS, successReqs, failedReqs, retriedReqs)

		case <-stopChan:
			return
		}
	}
}

// Запуск HTTP-сервера для метрик Prometheus
func startMetricsServer(metricsPort int, config Config) {
	if !config.EnableMetrics {
		return
	}

	initPrometheus(config)
	
	http.Handle("/metrics", promhttp.Handler())
	metricsAddr := fmt.Sprintf("0.0.0.0:%d", metricsPort)
	fmt.Printf("Запуск сервера метрик Prometheus на порту %d...\n", metricsPort)
	go func() {
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			fmt.Printf("Ошибка запуска сервера метрик: %v\n", err)
		}
	}()
}

func main() {
	// Парсинг аргументов командной строки
	mode := flag.String("mode", "victoria", "Режим отправки: 'victoria' для VictoriaLogs, 'es' для Elasticsearch, 'loki' для Loki")
	baseURL := flag.String("url", "http://victorialogs:9428", "Базовый URL для подключения к системе логирования (протокол://хост:порт)")
	rps := flag.Int("rps", 5, "Запросов в секунду")
	duration := flag.Duration("duration", 5*time.Minute, "Продолжительность теста")
	bulkSize := flag.Int("bulk-size", 50, "Размер пакета логов")
	workerCount := flag.Int("workers", runtime.NumCPU()*2, "Количество рабочих горутин")
	connectionCount := flag.Int("connections", 10, "Количество HTTP-соединений")
	webAccessPercent := flag.Int("log-distribution-web-access", 40, "Процент логов веб-доступа")
	webErrorPercent := flag.Int("log-distribution-web-error", 10, "Процент логов веб-ошибок")
	applicationPercent := flag.Int("log-distribution-application", 30, "Процент логов приложений")
	metricPercent := flag.Int("log-distribution-metric", 15, "Процент логов метрик")
	eventPercent := flag.Int("log-distribution-event", 5, "Процент логов событий")
	verbose := flag.Bool("verbose", false, "Подробный вывод")
	maxRetries := flag.Int("max-retries", 3, "Максимальное количество повторных попыток при ошибке")
	retryDelay := flag.Duration("retry-delay", 100*time.Millisecond, "Начальная задержка перед повторной попыткой")
	enableMetrics := flag.Bool("enable-metrics", true, "Включить сбор метрик Prometheus")
	metricsPort := flag.Int("metrics-port", 9090, "Порт для сервера метрик Prometheus")
	flag.Parse()

	// Проверка корректности параметров
	totalPercent := *webAccessPercent + *webErrorPercent + *applicationPercent + *metricPercent + *eventPercent
	if totalPercent != 100 {
		log.Fatalf("Сумма процентов типов логов должна быть равна 100, получено: %d", totalPercent)
	}

	// Формирование полного URL в зависимости от режима
	var url string
	switch strings.ToLower(*mode) {
	case "victoria", "victorialogs":
		url = *baseURL + "/insert/jsonline?_stream_fields=host,container_name,log_type&_msg_field=message&_time_field=timestamp"
	case "es", "elasticsearch":
		url = *baseURL + "/_bulk"
	case "loki":
		url = *baseURL + "/loki/api/v1/push"
	default:
		log.Fatalf("Неизвестный режим: %s", *mode)
	}

	// Инициализация конфигурации
	config := Config{
		Mode:            *mode,
		BaseURL:         *baseURL,
		URL:             url,
		RPS:             *rps,
		Duration:        *duration,
		BulkSize:        *bulkSize,
		WorkerCount:     *workerCount,
		ConnectionCount: *connectionCount,
		LogTypeDistribution: map[string]int{
			"web_access":  *webAccessPercent,
			"web_error":   *webErrorPercent,
			"application": *applicationPercent,
			"metric":      *metricPercent,
			"event":       *eventPercent,
		},
		Verbose:       *verbose,
		MaxRetries:    *maxRetries,
		RetryDelay:    *retryDelay,
		EnableMetrics: *enableMetrics,
		MetricsPort:   *metricsPort,
	}

	// Вывод информации о конфигурации
	fmt.Printf("Конфигурация:\n")
	fmt.Printf("- Режим: %s\n", config.Mode)
	fmt.Printf("- URL: %s\n", config.URL)
	fmt.Printf("- RPS: %d\n", config.RPS)
	fmt.Printf("- Продолжительность: %s\n", config.Duration)
	fmt.Printf("- Размер пакета: %d\n", config.BulkSize)
	fmt.Printf("- Рабочие горутины: %d\n", config.WorkerCount)
	fmt.Printf("- HTTP-соединения: %d\n", config.ConnectionCount)
	fmt.Printf("- Максимальное количество повторных попыток: %d\n", config.MaxRetries)
	fmt.Printf("- Начальная задержка повторных попыток: %s\n", config.RetryDelay)
	fmt.Printf("- Метрики Prometheus: %v (порт: %d)\n", config.EnableMetrics, config.MetricsPort)
	fmt.Printf("- Распределение типов логов: web_access=%d%%, web_error=%d%%, application=%d%%, metric=%d%%, event=%d%%\n",
		config.LogTypeDistribution["web_access"],
		config.LogTypeDistribution["web_error"],
		config.LogTypeDistribution["application"],
		config.LogTypeDistribution["metric"],
		config.LogTypeDistribution["event"])

	// Инициализация статистики
	stats := &Stats{
		StartTime: time.Now(),
	}

	// Инициализация пулов
	clientPool := NewClientPool(config.ConnectionCount)
	bufferPool := NewBufferPool()

	// Инициализация каналов и горутин
	jobs := make(chan struct{}, config.RPS*2)
	stopChan := make(chan struct{})

	// Запуск сервера метрик, если включено
	if config.EnableMetrics {
		startMetricsServer(config.MetricsPort, config)
	}

	// Запуск горутины для отображения статистики
	go statsReporter(stats, stopChan, config)

	// Запуск рабочих горутин
	var wg sync.WaitGroup
	for w := 1; w <= config.WorkerCount; w++ {
		wg.Add(1)
		go worker(w, jobs, stats, config, clientPool, bufferPool, &wg)
	}

	// Инициализация генератора случайных чисел
	rand.Seed(time.Now().UnixNano())

	// Основной цикл генерации нагрузки
	// Рассчитываем интервал тикера так, чтобы общий RPS был равен config.RPS,
	// независимо от количества воркеров
	tickInterval := time.Second / time.Duration(config.RPS)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	endTime := time.Now().Add(config.Duration)

	for time.Now().Before(endTime) {
		<-ticker.C
		jobs <- struct{}{}
	}

	close(jobs)
	wg.Wait()
	close(stopChan)

	// Вывод итоговой статистики
	elapsed := time.Since(stats.StartTime).Seconds()
	totalReqs := atomic.LoadInt64(&stats.TotalRequests)
	successReqs := atomic.LoadInt64(&stats.SuccessfulRequests)
	failedReqs := atomic.LoadInt64(&stats.FailedRequests)
	totalLogs := atomic.LoadInt64(&stats.TotalLogs)
	retriedReqs := atomic.LoadInt64(&stats.RetriedRequests)

	currentRPS := float64(totalReqs) / elapsed
	currentLPS := float64(totalLogs) / elapsed

	fmt.Printf("\n\nТест завершен!\n")
	fmt.Printf("Продолжительность: %.2f секунд\n", elapsed)
	fmt.Printf("Всего запросов: %d (%.2f/с)\n", totalReqs, currentRPS)
	fmt.Printf("Всего логов: %d (%.2f/с)\n", totalLogs, currentLPS)
	fmt.Printf("Успешных запросов: %d (%.2f%%)\n", successReqs, float64(successReqs)/float64(totalReqs)*100)
	fmt.Printf("Неудачных запросов: %d (%.2f%%)\n", failedReqs, float64(failedReqs)/float64(totalReqs)*100)
	fmt.Printf("Повторных попыток: %d\n", retriedReqs)
}
