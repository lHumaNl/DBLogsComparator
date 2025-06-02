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
	Environment   string `json:"environment,omitempty"`
	DataCenter    string `json:"datacenter,omitempty"`
	Version       string `json:"version,omitempty"`
	GitCommit     string `json:"git_commit,omitempty"`
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
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	ErrorCode   int               `json:"error_code"`
	Service     string            `json:"service"`
	Exception   string            `json:"exception,omitempty"`
	Stacktrace  string            `json:"stacktrace,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
	RequestPath string            `json:"request_path,omitempty"`
	ClientIP    string            `json:"client_ip,omitempty"`
	Duration    float64           `json:"duration,omitempty"`
	RetryCount  int               `json:"retry_count,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Context     map[string]string `json:"context,omitempty"`
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
	Exception      string            `json:"exception,omitempty"`
	Stacktrace     string            `json:"stacktrace,omitempty"`
	UserID         string            `json:"user_id,omitempty"`
	SessionID      string            `json:"session_id,omitempty"`
	Dependencies   []string          `json:"dependencies,omitempty"`
	Memory         float64           `json:"memory,omitempty"`
	CPU            float64           `json:"cpu,omitempty"`
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
		"Database query failed",
		"Transaction rollback",
		"Invalid data format",
		"Missing required field",
		"Duplicate entry found",
		"SSL certificate verification failed",
		"Host unreachable",
		"Connection refused",
		"Data integrity violation",
		"Serialization error",
		"Deadlock detected",
		"Cache miss",
		"Buffer overflow",
		"Thread starvation",
		"Out of connection pool resources",
		"Dependency service timeout",
		"Invalid state transition",
		"API version mismatch",
		"Circuit breaker open",
		"Request too large",
		"Response timeout",
		"JSON parsing error",
		"XML validation error",
		"CORS policy violation",
		"Unsupported media type",
		"Index out of bounds",
		"Unexpected end of stream",
		"Concurrent modification",
		"Column not found",
		"Config file not found",
		"Invalid license key",
		"Missing environment variable",
		"Feature flag disabled",
		"Invalid redirect URL",
		"Maximum retries exceeded",
	}
	return errorMessages[rnd.Intn(len(errorMessages))]
}

func GenerateRandomException() string {
	exceptions := []string{
		"java.lang.NullPointerException",
		"java.lang.IllegalArgumentException",
		"java.lang.ArrayIndexOutOfBoundsException",
		"java.io.FileNotFoundException",
		"java.net.ConnectException",
		"java.sql.SQLException",
		"java.lang.OutOfMemoryError",
		"java.util.concurrent.TimeoutException",
		"java.net.SocketTimeoutException",
		"java.lang.SecurityException",
		"org.springframework.beans.factory.BeanCreationException",
		"org.hibernate.HibernateException",
		"com.mongodb.MongoException",
		"javax.persistence.PersistenceException",
		"redis.clients.jedis.exceptions.JedisConnectionException",
		"python.TypeError",
		"python.ValueError",
		"python.ImportError",
		"python.IOError",
		"python.KeyError",
		"python.IndexError",
		"python.NameError",
		"python.RuntimeError",
		"python.ZeroDivisionError",
		"python.AttributeError",
		"NodeJsTypeError",
		"NodeJsReferenceError",
		"NodeJsSyntaxError",
		"NodeJsRangeError",
		"PHPFatalError",
		"PHPWarning",
		"PHPNotice",
		"GoNilPointerDereference",
		"GoOutOfBoundsError",
		"GoRuntimeError",
		"GoDeadlockError",
		"RubyNoMethodError",
		"RubyArgumentError",
		"RubyRuntimeError",
	}
	return exceptions[rnd.Intn(len(exceptions))]
}

func GenerateRandomStackTrace(exception string) string {
	var parts []string
	
	if strings.HasPrefix(exception, "java") {
		// Java stacktrace
		parts = append(parts, exception+": "+GenerateRandomErrorMessage())
		packageNames := []string{"com.example", "org.service", "io.client", "net.util", "app.core"}
		methodNames := []string{"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize"}
		
		depth := 3 + rnd.Intn(7) // 3-10 frames
		for i := 0; i < depth; i++ {
			pkg := packageNames[rnd.Intn(len(packageNames))]
			cls := "Class" + string('A'+rune(rnd.Intn(26)))
			method := methodNames[rnd.Intn(len(methodNames))]
			line := rnd.Intn(500) + 1
			parts = append(parts, fmt.Sprintf("\tat %s.%s.%s(%s.java:%d)", pkg, cls, method, cls, line))
		}
		
	} else if strings.HasPrefix(exception, "python") {
		// Python traceback
		parts = append(parts, "Traceback (most recent call last):")
		fileNames := []string{"app.py", "utils.py", "models.py", "views.py", "services.py"}
		methodNames := []string{"process_request", "validate_input", "fetch_data", "update_record", "authenticate", "initialize"}
		
		depth := 3 + rnd.Intn(5) // 3-8 frames
		for i := 0; i < depth; i++ {
			file := fileNames[rnd.Intn(len(fileNames))]
			method := methodNames[rnd.Intn(len(methodNames))]
			line := rnd.Intn(300) + 1
			parts = append(parts, fmt.Sprintf("  File \"%s\", line %d, in %s", file, line, method))
			parts = append(parts, "    "+GenerateRandomCodeLine())
		}
		parts = append(parts, exception+": "+GenerateRandomErrorMessage())
	} else {
		// Generic stacktrace
		parts = append(parts, exception+": "+GenerateRandomErrorMessage())
		fileNames := []string{"app.js", "server.go", "api.rb", "client.php", "handler.cs"}
		methodNames := []string{"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize"}
		
		depth := 2 + rnd.Intn(5) // 2-7 frames
		for i := 0; i < depth; i++ {
			file := fileNames[rnd.Intn(len(fileNames))]
			method := methodNames[rnd.Intn(len(methodNames))]
			line := rnd.Intn(400) + 1
			parts = append(parts, fmt.Sprintf("    at %s (%s:%d)", method, file, line))
		}
	}
	
	return strings.Join(parts, "\n")
}

func GenerateRandomCodeLine() string {
	codeLines := []string{
		"value = data['key']",
		"result = process_item(item)",
		"return client.fetch(url)",
		"user = User.get_by_id(user_id)",
		"if not condition: raise ValueError()",
		"response = service.call(params)",
		"for item in items: process(item)",
		"connection.execute(query)",
		"return object.method()",
		"data = json.loads(response)",
	}
	return codeLines[rnd.Intn(len(codeLines))]
}

func GenerateRandomDataCenter() string {
	datacenters := []string{
		"dc-east-1", "dc-west-1", "dc-europe-1", "dc-asia-1", "dc-central-1",
	}
	return datacenters[rnd.Intn(len(datacenters))]
}

func GenerateRandomEnvironment() string {
	environments := []string{
		"production", "staging", "development", "testing", "qa",
	}
	return environments[rnd.Intn(len(environments))]
}

func GenerateRandomRequestID() string {
	chars := "abcdef0123456789"
	length := 16 + rnd.Intn(16) // 16-32 символа
	id := make([]byte, length)
	for i := range id {
		id[i] = chars[rnd.Intn(len(chars))]
	}
	return string(id)
}

func GenerateRandomVersion() string {
	major := rnd.Intn(10)
	minor := rnd.Intn(20)
	patch := rnd.Intn(50)
	return fmt.Sprintf("%d.%d.%d", major, minor, patch)
}

func GenerateRandomGitCommit() string {
	chars := "abcdef0123456789"
	id := make([]byte, 7)
	for i := range id {
		id[i] = chars[rnd.Intn(len(chars))]
	}
	return string(id)
}

func GenerateRandomTags() []string {
	allTags := []string{
		"backend", "frontend", "database", "cache", "network", "security", 
		"performance", "storage", "memory", "cpu", "disk", "timeout", 
		"connection", "authentication", "critical", "warning", "info", 
		"microservice", "gateway", "proxy", "rate-limit", "firewall",
	}
	
	numTags := 1 + rnd.Intn(5) // 1-5 тегов
	if numTags > len(allTags) {
		numTags = len(allTags)
	}
	
	// Выбираем уникальные теги
	selected := make(map[int]bool)
	tags := make([]string, 0, numTags)
	
	for len(tags) < numTags {
		idx := rnd.Intn(len(allTags))
		if !selected[idx] {
			selected[idx] = true
			tags = append(tags, allTags[idx])
		}
	}
	
	return tags
}

func GenerateRandomErrorContext() map[string]string {
	keys := []string{
		"request_id", "session_id", "user_agent", "client_ip", "server_ip", 
		"endpoint", "method", "status_code", "correlation_id", "tenant_id",
		"node_id", "cluster_id", "query", "duration_ms", "rate_limit",
		"resource_type", "operation", "transaction_id", "thread_id", "process_id",
	}
	
	values := []string{
		"d8e8fca2dc0f896fd7cb4cb0031ba249", "95f8d9ba-7f3a-4f93-8c8a-123456789abc",
		"Mozilla/5.0", "192.168.1.1", "10.0.0.123", "/api/v1/users", "GET", "404",
		"cor-123456", "tenant-007", "node-12", "cluster-east1", "SELECT * FROM users",
		"356", "100", "user", "create", "tx-987654", "thread-5", "pid-12345",
	}
	
	numFields := 2 + rnd.Intn(6) // 2-8 полей
	context := make(map[string]string)
	
	for i := 0; i < numFields; i++ {
		key := keys[rnd.Intn(len(keys))]
		value := values[rnd.Intn(len(values))]
		
		// Добавляем некоторую вариативность в значения
		if strings.Contains(key, "id") {
			value = fmt.Sprintf("%s-%d", value, rnd.Intn(1000))
		} else if strings.Contains(key, "ip") {
			value = GenerateRandomIP()
		} else if strings.Contains(key, "duration") {
			value = fmt.Sprintf("%d", rnd.Intn(10000))
		}
		
		context[key] = value
	}
	
	return context
}

func GenerateRandomDependencies() []string {
	allDeps := []string{
		"postgres:13.3", "redis:6.2", "mongodb:4.4", "nginx:1.21", 
		"rabbitmq:3.9", "kafka:2.8", "elasticsearch:7.14", "mysql:8.0",
		"memcached:1.6", "cassandra:4.0", "zookeeper:3.7", "etcd:3.5",
		"prometheus:2.30", "grafana:8.2", "influxdb:2.0", "kibana:7.14",
	}
	
	numDeps := 1 + rnd.Intn(5) // 1-5 зависимостей
	if numDeps > len(allDeps) {
		numDeps = len(allDeps)
	}
	
	// Выбираем уникальные зависимости
	selected := make(map[int]bool)
	deps := make([]string, 0, numDeps)
	
	for len(deps) < numDeps {
		idx := rnd.Intn(len(allDeps))
		if !selected[idx] {
			selected[idx] = true
			deps = append(deps, allDeps[idx])
		}
	}
	
	return deps
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

func GenerateLog(logType string, timestamp string) interface{} {
	// Общие поля для базового лога
	baseLog := BaseLog{
		Timestamp:     timestamp,
		LogType:       logType,
		Host:          GenerateRandomHost(),
		ContainerName: GenerateRandomContainer(),
	}
	
	// С 30% вероятностью добавляем дополнительные базовые поля
	if rnd.Intn(10) < 3 {
		baseLog.Environment = GenerateRandomEnvironment()
		baseLog.DataCenter = GenerateRandomDataCenter()
		baseLog.Version = GenerateRandomVersion()
		baseLog.GitCommit = GenerateRandomGitCommit()
	}
	
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
			BaseLog:       baseLog,
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
		
		// Базовый лог ошибки
		errorLog := WebErrorLog{
			BaseLog:   baseLog,
			Level:     level,
			Message:   GenerateRandomErrorMessage(),
			ErrorCode: errorCode,
			Service:   service,
			RequestID: GenerateRandomRequestID(),
		}
		
		// С 60% вероятностью добавляем дополнительные детали к логу ошибки
		if rnd.Intn(10) < 6 {
			errorLog.RequestPath = GenerateRandomPath()
			errorLog.ClientIP = GenerateRandomIP()
			errorLog.Duration = float64(rnd.Intn(5000)) / 1000.0
			errorLog.RetryCount = rnd.Intn(5)
			errorLog.Tags = GenerateRandomTags()
			errorLog.Context = GenerateRandomErrorContext()
		}
		
		// С 40% вероятностью добавляем исключение со стектрейсом
		if rnd.Intn(10) < 4 {
			exception := GenerateRandomException()
			errorLog.Exception = exception
			errorLog.Stacktrace = GenerateRandomStackTrace(exception)
		}
		
		return errorLog
		
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
		
		// Базовый лог приложения
		appLog := ApplicationLog{
			BaseLog:        baseLog,
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
		
		// С 50% вероятностью добавляем дополнительные поля для логов приложения
		if rnd.Intn(10) < 5 {
			appLog.UserID = fmt.Sprintf("user-%d", rnd.Intn(10000))
			appLog.SessionID = fmt.Sprintf("session-%x", rnd.Int63())
			appLog.Dependencies = GenerateRandomDependencies()
			appLog.Memory = float64(rnd.Intn(1024)) // MB
			appLog.CPU = float64(rnd.Intn(100)) / 100.0 // 0-100%
		}
		
		// Для уровней error/critical с 70% вероятностью добавляем исключение
		if (level == "error" || level == "critical") && rnd.Intn(10) < 7 {
			exception := GenerateRandomException()
			appLog.Exception = exception
			appLog.Stacktrace = GenerateRandomStackTrace(exception)
		}
		
		return appLog
		
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
			BaseLog:    baseLog,
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
			BaseLog:    baseLog,
			EventType:  eventType,
			Message:    fmt.Sprintf("Event %s occurred on resource %s", eventType, resourceID),
			ResourceID: resourceID,
			Namespace:  namespace,
			Service:    service,
		}
	}
	
	// Если тип лога неизвестен, возвращаем базовый лог
	return baseLog
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
