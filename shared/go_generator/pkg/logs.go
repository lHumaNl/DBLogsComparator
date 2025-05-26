package pkg

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Глобальный генератор случайных чисел
var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

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
	Level          string            `json:"level"`
	Message        string            `json:"message"`
	Service        string            `json:"service"`
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	RequestMethod  string            `json:"request_method"`
	RequestPath    string            `json:"request_path"`
	RequestParams  map[string]string `json:"request_params"`
	ResponseStatus int               `json:"response_status"`
	ResponseTime   float64           `json:"response_time"`
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
	Service    string `json:"service"`
}

// Генераторы случайных данных
func GenerateRandomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rnd.Intn(256), rnd.Intn(256), rnd.Intn(256), rnd.Intn(256))
}

func GenerateRandomUserAgent() string {
	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
	}
	return userAgents[rnd.Intn(len(userAgents))]
}

func GenerateRandomHttpStatus() int {
	statuses := []int{200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
	return statuses[rnd.Intn(len(statuses))]
}

func GenerateRandomHttpMethod() string {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	return methods[rnd.Intn(len(methods))]
}

func GenerateRandomPath() string {
	// Генерация случайного пути с несколькими сегментами
	segments := []string{
		"api", "v1", "v2", "users", "products", "orders", "admin", "auth", "login", "logout",
		"register", "profile", "settings", "dashboard", "analytics", "reports", "images", "files",
		"upload", "download", "search", "categories", "tags", "comments", "reviews", "ratings",
		"cart", "checkout", "payment", "shipping", "blog", "articles", "news", "events",
	}
	
	// Случайное количество сегментов от 1 до 4
	numSegments := rnd.Intn(4) + 1
	path := ""
	
	for i := 0; i < numSegments; i++ {
		path += "/" + segments[rnd.Intn(len(segments))]
	}
	
	// Иногда добавляем параметры запроса
	if rnd.Intn(3) == 0 {
		path += fmt.Sprintf("?id=%d&limit=%d", rnd.Intn(1000), 10+rnd.Intn(90))
	}
	
	return path
}

func GenerateRandomLogLevel() string {
	levels := []string{"debug", "info", "warn", "error", "critical"}
	return levels[rnd.Intn(len(levels))]
}

func GenerateRandomErrorMessage() string {
	errorMessages := []string{
		"Failed to connect to database",
		"Connection timeout",
		"Invalid request parameters",
		"Authentication failed",
		"Permission denied",
		"Resource not found",
		"Internal server error",
		"Service unavailable",
		"Bad gateway",
		"Invalid token",
		"Session expired",
		"Rate limit exceeded",
		"Memory allocation error",
		"Disk space full",
		"Network connection lost",
	}
	return errorMessages[rnd.Intn(len(errorMessages))]
}

func GenerateRandomService() string {
	services := []string{
		"web-server",
		"api-gateway",
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
		"logging-service",
		"monitoring-service",
		"cache-service",
	}
	return services[rnd.Intn(len(services))]
}

func GenerateRandomHost() string {
	hosts := []string{
		"web-01", "web-02", "app-01", "app-02", "db-01", "db-02",
	}
	return hosts[rnd.Intn(len(hosts))]
}

func GenerateRandomContainer() string {
	containers := []string{
		"nginx", "php-fpm", "node", "redis", "postgres", "mysql",
	}
	return containers[rnd.Intn(len(containers))]
}

// GenerateLog создает случайный лог определенного типа
func GenerateLog(logType string, timestamp string) interface{} {
	switch logType {
	case "web_access":
		// Лог веб-доступа (как NGINX access log)
		method := GenerateRandomHttpMethod()
		path := GenerateRandomPath()
		status := GenerateRandomHttpStatus()
		
		// Для ошибок добавляем сообщение об ошибке
		var message string
		if status >= 400 {
			message = GenerateRandomErrorMessage()
		} else {
			message = fmt.Sprintf("Request processed successfully: %s %s", method, path)
		}
		
		return WebAccessLog{
			BaseLog: BaseLog{
				Timestamp:     timestamp,
				LogType:       "web_access",
				Host:          GenerateRandomHost(),
				ContainerName: "nginx-" + GenerateRandomContainer(),
			},
			RemoteAddr:    GenerateRandomIP(),
			Request:       fmt.Sprintf("%s %s HTTP/1.1", method, path),
			Status:        status,
			BytesSent:     rnd.Intn(10000) + 100,
			HttpReferer:   "https://example.com" + GenerateRandomPath(),
			HttpUserAgent: GenerateRandomUserAgent(),
			RequestTime:   float64(rnd.Intn(10000)) / 1000.0, // от 0 до 10 секунд
			Message:       message,
		}
		
	case "web_error":
		// Лог ошибки веб-сервера
		level := "error"
		if rnd.Intn(2) == 0 {
			level = "critical"
		}
		
		errorCode := rnd.Intn(1000) + 1000
		service := GenerateRandomService()
		
		return WebErrorLog{
			BaseLog: BaseLog{
				Timestamp:     timestamp,
				LogType:       "web_error",
				Host:          GenerateRandomHost(),
				ContainerName: "web-server-" + GenerateRandomContainer(),
			},
			Level:     level,
			Message:   GenerateRandomErrorMessage(),
			ErrorCode: errorCode,
			Service:   service,
		}
		
	case "application":
		// Лог приложения
		level := GenerateRandomLogLevel()
		service := GenerateRandomService()
		
		// Генерация случайных идентификаторов трассировки
		traceID := fmt.Sprintf("%x", rnd.Int63())
		spanID := fmt.Sprintf("%x", rnd.Int63())
		
		method := GenerateRandomHttpMethod()
		path := GenerateRandomPath()
		
		// Создание случайных параметров запроса
		params := make(map[string]string)
		numParams := rnd.Intn(5)
		for i := 0; i < numParams; i++ {
			paramKey := fmt.Sprintf("param%d", i+1)
			paramValue := fmt.Sprintf("value%d", rnd.Intn(100))
			params[paramKey] = paramValue
		}
		
		status := GenerateRandomHttpStatus()
		responseTime := float64(rnd.Intn(1000)) / 100.0 // от 0 до 10 секунд
		
		var message string
		if level == "error" || level == "critical" {
			message = GenerateRandomErrorMessage()
		} else {
			message = GenerateLogMessage(level)
		}
		
		return ApplicationLog{
			BaseLog: BaseLog{
				Timestamp:     timestamp,
				LogType:       "application",
				Host:          GenerateRandomHost(),
				ContainerName: service + "-" + GenerateRandomContainer(),
			},
			Level:          level,
			Message:        message,
			Service:        service,
			TraceID:        traceID,
			SpanID:         spanID,
			RequestMethod:  method,
			RequestPath:    path,
			RequestParams:  params,
			ResponseStatus: status,
			ResponseTime:   responseTime,
		}
		
	case "metric":
		// Лог метрики
		metricNames := []string{
			"cpu_usage", "memory_usage", "disk_usage", "network_in", "network_out",
			"requests_per_second", "response_time", "error_rate", "success_rate",
			"queue_length", "active_users", "active_sessions", "cache_hit_ratio",
		}
		
		regions := []string{
			"us-east-1", "us-west-1", "eu-west-1", "eu-central-1", "ap-northeast-1", "ap-southeast-1",
		}
		
		service := GenerateRandomService()
		metricName := metricNames[rnd.Intn(len(metricNames))]
		value := float64(rnd.Intn(10000)) / 100.0
		region := regions[rnd.Intn(len(regions))]
		
		return MetricLog{
			BaseLog: BaseLog{
				Timestamp:     timestamp,
				LogType:       "metric",
				Host:          GenerateRandomHost(),
				ContainerName: "metrics-collector-" + GenerateRandomContainer(),
			},
			MetricName: metricName,
			Value:      value,
			Service:    service,
			Region:     region,
			Message:    fmt.Sprintf("Metric %s for service %s in region %s: %.2f", metricName, service, region, value),
		}
		
	case "event":
		// Лог события
		eventTypes := []string{
			"system_start", "system_stop", "deploy", "rollback", "config_change",
			"scaling_up", "scaling_down", "backup", "restore", "maintenance",
			"alert", "notification", "scheduled_task", "migration", "sync",
		}
		
		eventType := eventTypes[rnd.Intn(len(eventTypes))]
		service := GenerateRandomService()
		resourceID := fmt.Sprintf("res-%d", rnd.Intn(10000))
		namespace := fmt.Sprintf("ns-%d", rnd.Intn(100))
		
		return EventLog{
			BaseLog: BaseLog{
				Timestamp:     timestamp,
				LogType:       logType,
				Host:          GenerateRandomHost(),
				ContainerName: GenerateRandomContainer(),
			},
			EventType:  eventType,
			Message:    fmt.Sprintf("Event %s occurred on resource %s", eventType, resourceID),
			ResourceID: resourceID,
			Namespace:  namespace,
			Service:    service,
		}
	}
	
	// Если тип лога неизвестен, возвращаем базовый лог
	return BaseLog{
		Timestamp:     timestamp,
		LogType:       "unknown",
		Host:          GenerateRandomHost(),
		ContainerName: GenerateRandomContainer(),
	}
}

func GenerateLogMessage(logLevel string) string {
	// Генерация сообщения лога в зависимости от уровня
	switch logLevel {
	case "debug":
		debugMessages := []string{
			"Debugging connection pool", "Debug: cache state dumped", "Debug trace enabled",
			"Variable values: x=42, y=13", "Debug mode active", "Memory usage: 1.2GB",
		}
		return debugMessages[rnd.Intn(len(debugMessages))]
	case "info":
		infoMessages := []string{
			"User logged in successfully", "Task completed", "Service started", "Config loaded",
			"Database connection established", "Process finished normally", "Data synchronized",
		}
		return infoMessages[rnd.Intn(len(infoMessages))]
	case "warn":
		warnMessages := []string{
			"Connection pool running low", "Slow query detected", "Deprecation warning",
			"Resource usage high", "Retry attempt 2/5", "Falling back to secondary system",
		}
		return warnMessages[rnd.Intn(len(warnMessages))]
	case "error", "critical":
		return GenerateRandomErrorMessage()
	default:
		return "Log message"
	}
}

// SelectRandomLogType выбирает случайный тип лога на основе распределения
func SelectRandomLogType(distribution map[string]int) string {
	// Вычисляем общую сумму весов
	totalWeight := 0
	for _, weight := range distribution {
		totalWeight += weight
	}
	
	if totalWeight <= 0 {
		// Если общий вес <= 0, возвращаем web_access по умолчанию
		return "web_access"
	}
	
	// Выбираем случайное число от 0 до totalWeight-1
	r := rnd.Intn(totalWeight)
	
	// Находим соответствующий тип лога
	current := 0
	for logType, weight := range distribution {
		current += weight
		if r < current {
			return logType
		}
	}
	
	// По умолчанию возвращаем web_access
	return "web_access"
}

// CountLogTypes подсчитывает количество логов каждого типа в пакете
func CountLogTypes(payload string) map[string]int {
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
