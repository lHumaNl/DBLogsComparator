package logdata

import (
	"fmt"
	"strings"
)

// Constants for log types and their fields
// This package provides common log data constants that are shared between
// the generator and querier components

// LogTypes defines the available log types
var LogTypes = []string{
	"web_access",
	"web_error",
	"application",
	"metric",
	"event",
}

// CommonLabels contains the standard labels that are present in most logs
var CommonLabels = []string{
	"log_type",
	"host",
	"container_name",
	"environment",
	"datacenter",
	"service",
}

// SearchableFieldsByLogType defines the fields that should be searchable for each log type
// This ensures all databases can perform the same searches for fair comparison
var SearchableFieldsByLogType = map[string][]string{
	"web_access": {
		// Base fields
		"log_type", "host", "container_name", "service", "level",
		// HTTP specific fields
		"status", "request_method",
		// System fields
		"environment", "datacenter",
	},
	"web_error": {
		// Base fields
		"log_type", "host", "container_name", "service", "level",
		// Error specific fields
		"error_code", "exception",
		// System fields
		"environment", "datacenter",
	},
	"application": {
		// Base fields
		"log_type", "host", "container_name", "service", "level",
		// Application specific fields
		"request_method", "response_status", "user_id",
		// System fields
		"environment", "datacenter",
	},
	"metric": {
		// Base fields
		"log_type", "host", "container_name", "service",
		// Metric specific fields
		"metric_name", "region",
		// System fields
		"environment", "datacenter",
	},
	"event": {
		// Base fields
		"log_type", "host", "container_name", "service",
		// Event specific fields
		"event_type", "namespace",
		// System fields
		"environment", "datacenter",
	},
}

// AllSearchableFields returns all unique searchable fields across all log types
func GetAllSearchableFields() []string {
	fieldSet := make(map[string]bool)
	for _, fields := range SearchableFieldsByLogType {
		for _, field := range fields {
			fieldSet[field] = true
		}
	}

	result := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		result = append(result, field)
	}
	return result
}

// GetSearchableFieldsForLogType returns searchable fields for a specific log type
func GetSearchableFieldsForLogType(logType string) []string {
	if fields, exists := SearchableFieldsByLogType[logType]; exists {
		return fields
	}
	// Return base fields if log type not found
	return []string{"log_type", "host", "container_name", "service", "environment", "datacenter"}
}

// Hosts contains a list of possible host values
var Hosts = []string{
	"app-server-01", "app-server-02", "app-server-03",
	"web-server-01", "web-server-02", "web-server-03",
	"db-server-01", "db-server-02",
	"cache-server-01", "cache-server-02",
	"worker-01", "worker-02", "worker-03", "worker-04",
}

// ContainerNames contains a list of possible container names
var ContainerNames = []string{
	"webapp", "api", "auth-service", "user-service", "product-service",
	"order-service", "payment-service", "notification-service",
	"search-service", "recommendation-engine", "analytics-service",
	"cache", "db", "queue", "scheduler",
}

// Environments contains a list of possible environment values
var Environments = []string{
	"production", "staging", "development", "testing", "qa", "preproduction",
}

// DataCenters contains a list of possible datacenter values
var DataCenters = []string{
	"us-east", "us-west", "eu-central", "eu-west", "asia-east", "asia-west", "australia-east", "australia-west",
	"south-america-east", "south-america-west", "north-america-east", "north-america-west",
}

// LogLevels contains a list of possible log levels
var LogLevels = []string{
	"debug", "info", "warn", "error", "critical",
}

// Services contains a list of possible service names
var Services = []string{
	"authentication", "user-management", "product-catalog",
	"order-processing", "payment-gateway", "shipping",
	"inventory", "recommendations", "search", "analytics",
	"notification", "email", "sms", "push", "cache",
	"database", "queue", "scheduler", "logger", "monitoring",
	"frontend", "backend", "mobile-api", "web-api", "admin-panel",
}

// HttpMethods contains a list of possible HTTP methods
var HttpMethods = []string{
	"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT",
}

// HttpStatusCodes contains a list of possible HTTP status codes
var HttpStatusCodes = []int{
	200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503,
}

// ErrorMessages contains a list of possible error messages
var ErrorMessages = []string{
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
}

// PathSegments contains possible URL path segments for generating paths
var PathSegments = []string{
	"api", "v1", "v2", "users", "products", "orders", "admin", "auth", "login", "logout",
	"register", "profile", "settings", "dashboard", "analytics", "reports", "images", "files",
	"upload", "download", "search", "categories", "tags", "comments", "reviews", "ratings",
	"cart", "checkout", "payment", "shipping", "blog", "articles", "news", "events",
}

// EventTypes contains a list of possible event types
var EventTypes = []string{
	"user_login", "user_logout", "user_registration",
	"password_reset", "profile_update", "subscription_change",
	"payment_processed", "order_placed", "order_shipped", "order_delivered",
	"product_view", "product_added_to_cart", "checkout_started",
	"email_sent", "notification_delivered", "system_startup", "system_shutdown",
	"backup_completed", "error_detected", "warning_triggered",
}

// MetricNames contains a list of possible metric names
var MetricNames = []string{
	"cpu_usage", "memory_usage", "disk_usage", "network_traffic",
	"request_count", "error_rate", "response_time", "queue_length",
	"active_connections", "active_users", "cache_hit_ratio", "gc_pause",
	"thread_count", "db_connections", "uptime", "saturation",
}

// UserAgents contains a list of possible user agent strings
var UserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
}

// ExceptionTypes contains a list of possible exception names
var ExceptionTypes = []string{
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

// CodeLines contains a list of possible code lines for stacktraces
var CodeLines = []string{
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

// Tags contains a list of possible tags for logs
var Tags = []string{
	"backend", "frontend", "database", "cache", "network", "security",
	"performance", "storage", "memory", "cpu", "disk", "timeout",
	"connection", "authentication", "critical", "warning", "info",
	"microservice", "gateway", "proxy", "rate-limit", "firewall",
}

// Dependencies contains a list of possible dependencies
var Dependencies = []string{
	"postgres:13.3", "redis:6.2", "mongodb:4.4", "nginx:1.21",
	"rabbitmq:3.9", "kafka:2.8", "elasticsearch:7.14", "mysql:8.0",
	"memcached:1.6", "cassandra:4.0", "zookeeper:3.7", "etcd:3.5",
	"prometheus:2.30", "grafana:8.2", "influxdb:2.0", "kibana:7.14",
}

// DebugMessages contains debug-level log messages
var DebugMessages = []string{
	"Debugging connection pool", "Debug: cache state dumped", "Debug trace enabled",
	"Variable values: x=42, y=13", "Debug mode active", "Memory usage: 1.2GB",
}

// InfoMessages contains info-level log messages
var InfoMessages = []string{
	"User logged in successfully", "Task completed", "Service started", "Config loaded",
	"Database connection established", "Process finished normally", "Data synchronized",
}

// WarnMessages contains warning-level log messages
var WarnMessages = []string{
	"Connection pool running low", "Slow query detected", "Deprecation warning",
	"Resource usage high", "Retry attempt 2/5", "Falling back to secondary system",
}

// ContextKeys contains possible context keys for error logs
var ContextKeys = []string{
	"request_id", "session_id", "user_agent", "client_ip", "server_ip",
	"endpoint", "method", "status_code", "correlation_id", "tenant_id",
	"node_id", "cluster_id", "query", "duration_ms", "rate_limit",
	"resource_type", "operation", "transaction_id", "thread_id", "process_id",
}

// ContextValues contains possible context values for error logs
var ContextValues = []string{
	"d8e8fca2dc0f896fd7cb4cb0031ba249", "95f8d9ba-7f3a-4f93-8c8a-123456789abc",
	"Mozilla/5.0", "192.168.1.1", "10.0.0.123", "/api/v1/users", "GET", "404",
	"cor-123456", "tenant-007", "node-12", "cluster-east1", "SELECT * FROM users",
	"356", "100", "user", "create", "tx-987654", "thread-5", "pid-12345",
}

// Stack trace constants for different languages

// JavaPackageNames contains Java package names for stack traces
var JavaPackageNames = []string{
	"com.example", "org.service", "io.client", "net.util", "app.core",
}

// JavaMethodNames contains Java method names for stack traces
var JavaMethodNames = []string{
	"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize",
}

// PythonFileNames contains Python file names for stack traces
var PythonFileNames = []string{
	"app.py", "utils.py", "models.py", "views.py", "services.py",
}

// PythonMethodNames contains Python method names for stack traces
var PythonMethodNames = []string{
	"process_request", "validate_input", "fetch_data", "update_record", "authenticate", "initialize",
}

// GenericFileNames contains generic file names for stack traces
var GenericFileNames = []string{
	"app.js", "server.go", "api.rb", "client.php", "handler.cs",
}

// GenericMethodNames contains generic method names for stack traces
var GenericMethodNames = []string{
	"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize",
}

// Query-related constants for fair comparison across databases

// TimeIntervals contains temporal intervals used by all database systems for TimeSeriesQuery
var TimeIntervals = []string{
	"1m", "5m", "10m", "30m", "1h",
}

// FilterLabels contains field labels used for base filtering in TimeSeriesQuery across all systems
var FilterLabels = []string{
	"log_type", "service", "environment", "datacenter",
}

// GroupByFields contains fields used for grouping in TimeSeriesQuery across all systems
var GroupByFields = []string{
	"service", "level", "host", "container_name",
}

// TopKValues contains possible K values for TopKQuery across all systems
// Full range supported by all systems (Loki has no technical K limit according to documentation)
var TopKValues = []int{
	5, 10, 15, 20, 50, 100, 500, 1000,
}

// TopKFields contains fields available for TopK aggregation across all systems
var TopKFields = []string{
	"service", "level", "method", "status", "log_type", "http_method",
}

// TimeSeriesLimits contains possible limit values for TimeSeriesQuery across all systems
// These limits control the number of time series buckets returned to ensure fair comparison
var TimeSeriesLimits = []int{
	50, 100, 200, 500, 1000, 2000,
}

// NumericFields contains numeric fields used for statistical aggregations across all systems
var NumericFields = []string{
	"response_time", "latency", "duration", "bytes", "cpu", "memory",
}

// ========================= UNIFIED SIMPLE QUERY CONSTANTS =========================

// FieldTypes defines the types of fields for generating appropriate operators
type FieldType int

const (
	StringField FieldType = iota
	IntField
	FloatField
	IPField
	TimestampField
)

// FieldTypeMap maps field names to their types for generating appropriate operators
var FieldTypeMap = map[string]FieldType{
	// String fields
	"log_type":       StringField,
	"host":           StringField,
	"container_name": StringField,
	"service":        StringField,
	"level":          StringField,
	"environment":    StringField,
	"datacenter":     StringField,
	"request_method": StringField,
	"event_type":     StringField,
	"metric_name":    StringField,
	"exception":      StringField,
	"user_id":        StringField,
	"region":         StringField,
	"namespace":      StringField,

	// Integer fields
	"status":          IntField,
	"error_code":      IntField,
	"response_status": IntField,
	"bytes_sent":      IntField,
	"thread_count":    IntField,
	"port":            IntField,

	// Float fields
	"response_time": FloatField,
	"latency":       FloatField,
	"duration":      FloatField,
	"cpu":           FloatField,
	"memory":        FloatField,
	"value":         FloatField,

	// IP fields
	"client_ip":   IPField,
	"server_ip":   IPField,
	"remote_addr": IPField,
}

// SimpleQueryOperators defines operators available for each field type
// UNIFIED: Only operators supported by all systems (removed !~ for Loki compatibility)
var SimpleQueryOperators = map[FieldType][]string{
	StringField:    {"=", "!=", "=~"}, // equals, not equals, regex (negative regex handled in pattern)
	IntField:       {"=", "!=", "=~"}, // equals, not equals, regex (negative regex handled in pattern)
	FloatField:     {"=", "!=", "=~"}, // equals, not equals, regex (negative regex handled in pattern)
	IPField:        {"=", "!=", "=~"}, // equals, not equals, regex (negative regex handled in pattern)
	TimestampField: {"=", "!=", "=~"}, // equals, not equals, regex (negative regex handled in pattern)
}

// SimpleQueryKeywords contains keywords for text searches (message field, etc.)
var SimpleQueryKeywords = []string{
	"error", "warning", "failed", "exception", "timeout", "success",
	"completed", "running", "critical", "forbidden", "GET", "POST",
	"PUT", "DELETE", "authenticated", "authorized", "denied", "started",
	"finished", "processing", "connecting", "connected", "disconnected",
}

// IPRegexPatterns contains regex patterns for IP field searches
var IPRegexPatterns = []string{
	`192\.168\.[0-9]+\.[0-9]+`,                    // private network 192.168.x.x
	`10\.[0-9]+\.[0-9]+\.[0-9]+`,                  // private network 10.x.x.x
	`172\.(1[6-9]|2[0-9]|3[0-1])\.[0-9]+\.[0-9]+`, // private network 172.16-31.x.x
	`[0-9]+\.[0-9]+\.[0-9]+\.[1-9][0-9]*`,         // any IP ending with non-zero
	`127\.0\.0\.[0-9]+`,                           // localhost variations
}

// ========================= AGGREGATION FUNCTIONS FOR ALL SYSTEMS =========================

// AggregationFunctions contains aggregation functions supported across all systems
var AggregationFunctions = []string{
	"sum", "min", "max", "avg", "count", // basic functions available in all systems
	"stddev", "stdvar", // standard deviation functions
}

// ElasticsearchAggregationFunctions contains ES-specific aggregation functions
var ElasticsearchAggregationFunctions = []string{
	"avg", "min", "max", "sum", "cardinality", "value_count", "percentiles",
}

// LokiAggregationFunctions contains Loki-specific aggregation functions
var LokiAggregationFunctions = []string{
	"sum", "min", "max", "avg", "count", "stddev", "stdvar",
}

// LokiTimeWindowFunctions contains Loki time window functions
var LokiTimeWindowFunctions = []string{
	"rate", "count_over_time", "bytes_over_time", "bytes_rate",
	"min_over_time", "max_over_time", "avg_over_time",
	"sum_over_time", "stddev_over_time", "stdvar_over_time",
	"quantile_over_time", "first_over_time", "last_over_time",
}

// VictoriaLogsStatFunctions contains VictoriaLogs statistical functions
var VictoriaLogsStatFunctions = []string{
	"count", "quantile", "avg", "min", "max", "sum",
}

// ========================= TIME INTERVALS STANDARDIZATION =========================

// StandardTimeIntervals contains time intervals used by all systems
var StandardTimeIntervals = []string{
	"1m", "5m", "10m", "30m", "1h",
}

// ExtendedTimeIntervals contains additional time intervals for variety
var ExtendedTimeIntervals = []string{
	"30s", "2m", "15m", "45m", "2h", "3h",
}

// UniversalTimeWindows contains time windows used across all systems for fair comparison
// This ensures Loki's required time windows are matched in Elasticsearch and VictoriaLogs
var UniversalTimeWindows = []string{
	"10s", "15s", "30s", "1m", "5m", "10m", "30m", "1h",
}

// ========================= ELASTICSEARCH CONSTANTS =========================

// ElasticsearchQueryPhrases contains keywords for ES message searches
var ElasticsearchQueryPhrases = []string{
	"error", "warning", "exception", "failed", "timeout", "success",
	"completed", "running", "critical", "forbidden", "GET", "POST", "PUT", "DELETE",
}

// ElasticsearchLimits contains possible limit values for ES queries
var ElasticsearchLimits = []int{
	10, 50, 100, 200, 500, 1000,
}

// ========================= LOKI CONSTANTS =========================

// LokiKeywords contains keywords for Loki text filters
var LokiKeywords = []string{
	"error", "warning", "failed", "exception", "timeout",
	"success", "completed", "started", "authenticated", "authorized",
	"INFO", "WARN", "ERROR", "FATAL", "DEBUG",
	"GET", "POST", "PUT", "DELETE", "PATCH",
}

// LokiUnwrapFields contains fields that can be unwrapped in Loki queries
var LokiUnwrapFields = []string{
	"bytes", "duration", "latency", "size", "count",
	"status_code", "response_time", "cpu", "memory",
}

// ========================= VICTORIALOGS CONSTANTS =========================

// VictoriaLogsLimits contains possible limit values for VL queries
var VictoriaLogsLimits = []string{
	"10", "50", "100", "200", "500", "1000",
}

// VictoriaLogsSteps contains step values for VL time series queries
var VictoriaLogsSteps = []string{
	"10s", "30s", "60s", "5m", "10m", "30m",
}

// ========================= UNIFIED SIMPLE QUERY STRATEGY =========================

// UnifiedSimpleQueryStrategy defines the unified strategy for field selection
// to ensure maximum fairness and diversity across all database systems
type SimpleQueryStrategy struct {
	CoreFields      []string            // High-priority fields (40% probability)
	TypeFields      map[string][]string // Log-type specific fields (35% probability)
	UniversalFields []string            // All other available fields (20% probability)
	MessageEnabled  bool                // Whether message search is enabled (5% probability)
}

// UnifiedStrategy contains the unified field selection strategy
var UnifiedStrategy = SimpleQueryStrategy{
	// Core fields: guaranteed to exist and be searchable in all systems
	CoreFields: []string{
		"log_type", "host", "container_name", "service", "environment", "datacenter",
	},

	// Type-specific fields: fields that are commonly found in specific log types
	TypeFields: map[string][]string{
		"web_access":  {"status", "request_method", "remote_addr"},
		"web_error":   {"level", "error_code", "exception"},
		"application": {"level", "user_id", "response_status", "request_method"},
		"metric":      {"metric_name", "region"},
		"event":       {"event_type", "namespace"},
	},

	// Universal fields: all other searchable fields available across systems
	UniversalFields: []string{
		"level", "status", "error_code", "user_id", "metric_name",
		"event_type", "exception", "response_status", "request_method",
		"remote_addr", "region", "namespace",
	},

	MessageEnabled: true,
}

// GetAllUnifiedFields returns all unique fields from the unified strategy
func GetAllUnifiedFields() []string {
	fieldSet := make(map[string]bool)

	// Add core fields
	for _, field := range UnifiedStrategy.CoreFields {
		fieldSet[field] = true
	}

	// Add type-specific fields
	for _, fields := range UnifiedStrategy.TypeFields {
		for _, field := range fields {
			fieldSet[field] = true
		}
	}

	// Add universal fields
	for _, field := range UnifiedStrategy.UniversalFields {
		fieldSet[field] = true
	}

	result := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		result = append(result, field)
	}
	return result
}

// ========================= UNIFIED SIMPLE QUERY GENERATION FUNCTIONS =========================

// SimpleQueryParams represents parameters for generating a unified simple query
type SimpleQueryParams struct {
	Field     string      // field name
	Value     interface{} // field value
	Operator  string      // comparison operator
	FieldType FieldType   // type of the field
	IsRegex   bool        // whether value is a regex pattern
}

// GenerateUnifiedSimpleQuery generates parameters using the unified strategy
func GenerateUnifiedSimpleQuery() SimpleQueryParams {
	probability := RandomIntn(100)

	var field string

	switch {
	case probability < 40:
		// 40% - Core fields (high success rate)
		field = UnifiedStrategy.CoreFields[RandomIntn(len(UnifiedStrategy.CoreFields))]

	case probability < 75:
		// 35% - Type-specific fields (medium success rate)
		logType := GetRandomLogType()
		typeFields, exists := UnifiedStrategy.TypeFields[logType]
		if exists && len(typeFields) > 0 {
			field = typeFields[RandomIntn(len(typeFields))]
		} else {
			// Fallback to core fields if type fields not available
			field = UnifiedStrategy.CoreFields[RandomIntn(len(UnifiedStrategy.CoreFields))]
		}

	case probability < 95:
		// 20% - Universal fields (lower success rate, but more diversity)
		field = UnifiedStrategy.UniversalFields[RandomIntn(len(UnifiedStrategy.UniversalFields))]

	default:
		// 5% - Message search (special case)
		if UnifiedStrategy.MessageEnabled {
			return GenerateMessageSearchParams()
		} else {
			// Fallback to core field if message search disabled
			field = UnifiedStrategy.CoreFields[RandomIntn(len(UnifiedStrategy.CoreFields))]
		}
	}

	return GetRandomSimpleQueryParamsForField(field)
}

// GenerateUnifiedSimpleQueryForLoki generates Loki-compatible simple query parameters
// This ensures compatibility with Loki requirements
func GenerateUnifiedSimpleQueryForLoki() SimpleQueryParams {
	// Start with standard simple query
	params := GenerateUnifiedSimpleQuery()

	// If it's a negative condition, try to convert it to positive
	if params.Operator == "!=" || params.Operator == "!~" {
		_, newOp, newValue := ConvertNegativeToPositive(params.Field, params.Operator, params.Value)
		if newOp == "=" || newOp == "=~" {
			// Successfully converted to positive
			params.Operator = newOp
			params.Value = newValue
			params.IsRegex = (newOp == "=~")
		}
		// If conversion failed, the query will have a fallback positive matcher added in the executor
	}

	return params
}

// GetRandomSimpleQueryParamsForField generates parameters for a specific field
func GetRandomSimpleQueryParamsForField(field string) SimpleQueryParams {
	// Determine field type
	fieldType, exists := FieldTypeMap[field]
	if !exists {
		fieldType = StringField // default to string
	}

	// Choose appropriate operator for this field type
	operators := SimpleQueryOperators[fieldType]
	operator := operators[RandomIntn(len(operators))]

	// Generate appropriate value based on field type and operator
	var value interface{}
	var isRegex bool

	switch fieldType {
	case StringField:
		if operator == "=~" {
			// Use regex pattern for string fields
			isRegex = true
			labelValues := GetLabelValuesMap()[field]
			if len(labelValues) > 0 {
				// Create regex from actual values (e.g., "(value1|value2)")
				numValues := RandomIntn(3) + 1 // 1-3 values
				if numValues > len(labelValues) {
					numValues = len(labelValues)
				}

				selectedValues := make([]string, numValues)
				for i := 0; i < numValues; i++ {
					selectedValues[i] = labelValues[RandomIntn(len(labelValues))]
				}

				if len(selectedValues) == 1 {
					value = selectedValues[0]
				} else {
					// Create OR regex pattern
					value = fmt.Sprintf("(%s)", strings.Join(selectedValues, "|"))
				}
			} else {
				value = ".+error.+" // fallback regex - Loki compatible (no empty matches)
			}
		} else {
			// Use exact value for string fields
			labelValues := GetLabelValuesMap()[field]
			if len(labelValues) > 0 {
				value = labelValues[RandomIntn(len(labelValues))]
			} else {
				value = "default_value"
			}
		}

	case IntField:
		if operator == "=~" {
			// Use regex pattern for integer fields (e.g., "4[0-9][0-9]" for 400-499)
			isRegex = true
			switch field {
			case "status", "response_status":
				// Common HTTP status patterns - use [0-9] for Loki compatibility
				patterns := []string{"2[0-9][0-9]", "4[0-9][0-9]", "5[0-9][0-9]", "20[0-9]", "40[0-9]", "50[0-9]"}
				value = patterns[RandomIntn(len(patterns))]
			case "error_code":
				// Error code patterns - use [0-9] for Loki compatibility
				patterns := []string{"4[0-9][0-9]", "5[0-9][0-9]", "40[0-9]", "50[0-9]", "[4-5]0[0-9]"}
				value = patterns[RandomIntn(len(patterns))]
			default:
				// Generic number patterns
				patterns := []string{"[0-9]+", "1[0-9]+", "[1-9][0-9]*", "[0-9]{2,4}"}
				value = patterns[RandomIntn(len(patterns))]
			}
		} else {
			// Use exact values for integer fields
			switch field {
			case "status", "response_status":
				value = HttpStatusCodes[RandomIntn(len(HttpStatusCodes))]
			case "error_code":
				errorCodes := []int{400, 401, 403, 404, 500, 502, 503, 504}
				value = errorCodes[RandomIntn(len(errorCodes))]
			default:
				// Common exact values
				commonValues := []int{1, 10, 100, 200, 500, 1000}
				value = commonValues[RandomIntn(len(commonValues))]
			}
		}

	case FloatField:
		if operator == "=~" {
			// Use regex pattern for float fields
			isRegex = true
			patterns := []string{"[0-9]+\\.[0-9]+", "[0-9]+\\.[0-9]", "[0-9]{1,2}\\.[0-9]", "9[0-9]\\.[0-9]"}
			value = patterns[RandomIntn(len(patterns))]
		} else {
			// Use exact float values
			commonFloats := []float64{0.0, 1.5, 10.0, 25.5, 50.0, 75.5, 90.0, 95.5, 100.0}
			value = commonFloats[RandomIntn(len(commonFloats))]
		}

	case IPField:
		if operator == "=~" {
			// Use IP regex patterns
			isRegex = true
			value = IPRegexPatterns[RandomIntn(len(IPRegexPatterns))]
		} else {
			// Generate a simple IP
			value = fmt.Sprintf("192.168.%d.%d", RandomIntn(255), RandomIntn(255))
		}
	}

	return SimpleQueryParams{
		Field:     field,
		Value:     value,
		Operator:  operator,
		FieldType: fieldType,
		IsRegex:   isRegex,
	}
}

// GetRandomSimpleQueryParams generates random parameters for a simple query (legacy function)
func GetRandomSimpleQueryParams() SimpleQueryParams {
	// 1. Choose a random log type to get appropriate searchable fields
	logType := GetRandomLogType()
	searchableFields := GetSearchableFieldsForLogType(logType)

	// 2. Choose a random field from searchable fields
	field := searchableFields[RandomIntn(len(searchableFields))]

	// 3. Determine field type
	fieldType, exists := FieldTypeMap[field]
	if !exists {
		fieldType = StringField // default to string
	}

	// 4. Choose appropriate operator for this field type
	operators := SimpleQueryOperators[fieldType]
	operator := operators[RandomIntn(len(operators))]

	// 5. Generate appropriate value based on field type and operator
	var value interface{}
	var isRegex bool

	switch fieldType {
	case StringField:
		if operator == "=~" {
			// Use regex pattern for string fields
			isRegex = true
			labelValues := GetLabelValuesMap()[field]
			if len(labelValues) > 0 {
				// Create regex from actual values (e.g., "(value1|value2)")
				numValues := RandomIntn(3) + 1 // 1-3 values
				if numValues > len(labelValues) {
					numValues = len(labelValues)
				}

				selectedValues := make([]string, numValues)
				for i := 0; i < numValues; i++ {
					selectedValues[i] = labelValues[RandomIntn(len(labelValues))]
				}

				if len(selectedValues) == 1 {
					value = selectedValues[0]
				} else {
					// Create OR regex pattern
					value = fmt.Sprintf("(%s)", strings.Join(selectedValues, "|"))
				}
			} else {
				value = ".+error.+" // fallback regex - Loki compatible (no empty matches)
			}
		} else {
			// Use exact value for string fields
			labelValues := GetLabelValuesMap()[field]
			if len(labelValues) > 0 {
				value = labelValues[RandomIntn(len(labelValues))]
			} else {
				value = "default_value"
			}
		}

	case IntField:
		if operator == "=~" {
			// Use regex pattern for integer fields (e.g., "4[0-9][0-9]" for 400-499)
			isRegex = true
			switch field {
			case "status", "response_status":
				// Common HTTP status patterns - use [0-9] for Loki compatibility
				patterns := []string{"2[0-9][0-9]", "4[0-9][0-9]", "5[0-9][0-9]", "20[0-9]", "40[0-9]", "50[0-9]"}
				value = patterns[RandomIntn(len(patterns))]
			case "error_code":
				// Error code patterns - use [0-9] for Loki compatibility
				patterns := []string{"4[0-9][0-9]", "5[0-9][0-9]", "40[0-9]", "50[0-9]", "[4-5]0[0-9]"}
				value = patterns[RandomIntn(len(patterns))]
			default:
				// Generic number patterns
				patterns := []string{"[0-9]+", "1[0-9]+", "[1-9][0-9]*", "[0-9]{2,4}"}
				value = patterns[RandomIntn(len(patterns))]
			}
		} else {
			// Use exact values for integer fields
			switch field {
			case "status", "response_status":
				value = HttpStatusCodes[RandomIntn(len(HttpStatusCodes))]
			case "error_code":
				errorCodes := []int{400, 401, 403, 404, 500, 502, 503, 504}
				value = errorCodes[RandomIntn(len(errorCodes))]
			default:
				// Common exact values
				commonValues := []int{1, 10, 100, 200, 500, 1000}
				value = commonValues[RandomIntn(len(commonValues))]
			}
		}

	case FloatField:
		if operator == "=~" {
			// Use regex pattern for float fields
			isRegex = true
			patterns := []string{"[0-9]+\\.[0-9]+", "[0-9]+\\.[0-9]", "[0-9]{1,2}\\.[0-9]", "9[0-9]\\.[0-9]"}
			value = patterns[RandomIntn(len(patterns))]
		} else {
			// Use exact float values
			commonFloats := []float64{0.0, 1.5, 10.0, 25.5, 50.0, 75.5, 90.0, 95.5, 100.0}
			value = commonFloats[RandomIntn(len(commonFloats))]
		}

	case IPField:
		if operator == "=~" {
			// Use IP regex patterns
			isRegex = true
			value = IPRegexPatterns[RandomIntn(len(IPRegexPatterns))]
		} else {
			// Generate a simple IP
			value = fmt.Sprintf("192.168.%d.%d", RandomIntn(255), RandomIntn(255))
		}
	}

	return SimpleQueryParams{
		Field:     field,
		Value:     value,
		Operator:  operator,
		FieldType: fieldType,
		IsRegex:   isRegex,
	}
}

// GenerateMessageSearchParams generates parameters for message field searches
func GenerateMessageSearchParams() SimpleQueryParams {
	keyword := SimpleQueryKeywords[RandomIntn(len(SimpleQueryKeywords))]

	// 50% chance for case-insensitive search
	isRegex := RandomIntn(2) == 0
	operator := "="
	if isRegex {
		operator = "=~"
		keyword = "(?i)" + keyword // case insensitive
	}

	return SimpleQueryParams{
		Field:     "message",
		Value:     keyword,
		Operator:  operator,
		FieldType: StringField,
		IsRegex:   isRegex,
	}
}

// ComplexQueryParams represents parameters for generating a unified complex query
type ComplexQueryParams struct {
	Fields        []string           // field names (2-4 fields)
	Values        []interface{}      // field values
	Operators     []string           // comparison operators per field
	FieldTypes    []FieldType        // types of the fields
	IsRegex       []bool             // whether each value is a regex pattern
	LogicOperator string             // logic operator between terms (AND/OR)
	NegationMask  []bool             // whether each term should be negated
	MessageSearch *SimpleQueryParams // optional message search component
}

// GenerateUnifiedComplexQuery generates parameters for a complex query using unified strategy
// Complex query = 2-4 fields simultaneously with logical operators
func GenerateUnifiedComplexQuery() ComplexQueryParams {
	// Choose number of fields: 2-4 fields per query
	fieldCount := RandomIntn(3) + 2 // 2-4 fields

	// Select fields using the same unified strategy as SimpleQuery
	fields := make([]string, fieldCount)
	selectedFields := make(map[string]bool)

	for i := 0; i < fieldCount; i++ {
		attempts := 0
		for {
			// Use same probability distribution as SimpleQuery
			probability := RandomIntn(100)
			var field string

			switch {
			case probability < 40:
				// 40% - Core fields (high success rate)
				field = UnifiedStrategy.CoreFields[RandomIntn(len(UnifiedStrategy.CoreFields))]

			case probability < 75:
				// 35% - Type-specific fields (medium success rate)
				logType := GetRandomLogType()
				typeFields, exists := UnifiedStrategy.TypeFields[logType]
				if exists && len(typeFields) > 0 {
					field = typeFields[RandomIntn(len(typeFields))]
				} else {
					// Fallback to core fields if type fields not available
					field = UnifiedStrategy.CoreFields[RandomIntn(len(UnifiedStrategy.CoreFields))]
				}

			default:
				// 25% - Universal fields (lower success rate, but more diversity)
				field = UnifiedStrategy.UniversalFields[RandomIntn(len(UnifiedStrategy.UniversalFields))]
			}

			// Ensure no duplicate fields
			if !selectedFields[field] {
				fields[i] = field
				selectedFields[field] = true
				break
			}

			attempts++
			if attempts > 10 {
				// Fallback to core field with index to ensure uniqueness
				field = UnifiedStrategy.CoreFields[i%len(UnifiedStrategy.CoreFields)]
				if !selectedFields[field] {
					fields[i] = field
					selectedFields[field] = true
					break
				}
			}
		}
	}

	// Generate parameters for each field using the same logic as SimpleQuery
	values := make([]interface{}, fieldCount)
	operators := make([]string, fieldCount)
	fieldTypes := make([]FieldType, fieldCount)
	isRegex := make([]bool, fieldCount)
	negationMask := make([]bool, fieldCount)

	for i, field := range fields {
		params := GetRandomSimpleQueryParamsForField(field)
		values[i] = params.Value
		operators[i] = params.Operator
		fieldTypes[i] = params.FieldType
		isRegex[i] = params.IsRegex

		// 20% chance for negation per term, BUT ensure at least one positive matcher
		negationMask[i] = RandomIntn(5) == 0
	}

	// CRITICAL FIX: Ensure at least one positive matcher for fair comparison across all systems
	// This prevents "empty-compatible" errors in Loki and ensures consistent queries
	hasPositive := false
	for i, operator := range operators {
		if !negationMask[i] && (operator == "=" || operator == "=~") {
			hasPositive = true
			break
		}
	}

	// If no positive matcher exists, convert the first field to positive
	if !hasPositive {
		negationMask[0] = false // Ensure first field is not negated
		// If the first field operator is negative by nature, convert it to positive
		if operators[0] == "!=" || operators[0] == "!~" {
			switch operators[0] {
			case "!=":
				operators[0] = "="
			case "!~":
				operators[0] = "=~"
			}
		}
	}

	// FAIRNESS: Only AND logic for fair comparison across all systems
	// Loki doesn't support OR in label selectors, so we use only AND everywhere
	logicOperator := "AND"

	// 15% chance to add message search component (similar to SimpleQuery 5% + ComplexQuery boost)
	var messageSearch *SimpleQueryParams
	if RandomIntn(100) < 15 && UnifiedStrategy.MessageEnabled {
		msgParams := GenerateMessageSearchParams()
		messageSearch = &msgParams
	}

	return ComplexQueryParams{
		Fields:        fields,
		Values:        values,
		Operators:     operators,
		FieldTypes:    fieldTypes,
		IsRegex:       isRegex,
		LogicOperator: logicOperator,
		NegationMask:  negationMask,
		MessageSearch: messageSearch,
	}
}

// GenerateUnifiedComplexQueryForLoki generates Loki-compatible complex query parameters
// This ensures at least one positive matcher exists to satisfy Loki requirements
func GenerateUnifiedComplexQueryForLoki() ComplexQueryParams {
	// Start with standard complex query
	params := GenerateUnifiedComplexQuery()

	// Extract non-message fields for processing
	fields := make([]string, 0)
	operators := make([]string, 0)
	values := make([]interface{}, 0)

	for i, field := range params.Fields {
		// Skip message fields - they'll be handled separately
		if field == "message" {
			continue
		}

		operator := params.Operators[i]
		value := params.Values[i]
		negated := params.NegationMask[i]

		// Apply negation mask to operator
		if negated {
			switch operator {
			case "=":
				operator = "!="
			case "!=":
				operator = "="
			case "=~":
				// Convert to positive regex with negative pattern instead of !~
				_, newOp, newValue := ConvertNegativeToPositive(field, "!~", value)
				if newOp == "=~" {
					operator = newOp
					value = newValue
				} else {
					operator = "!=" // fallback to != if regex conversion fails
				}
			}
		}

		fields = append(fields, field)
		operators = append(operators, operator)
		values = append(values, value)
	}

	// Ensure Loki compatibility
	fields, operators, values = EnsureLokiCompatibility(fields, operators, values)

	// Update the params with compatible fields
	newFieldTypes := make([]FieldType, len(fields))
	newIsRegex := make([]bool, len(fields))
	newNegationMask := make([]bool, len(fields))

	for i, field := range fields {
		// Determine field type
		if fieldType, exists := FieldTypeMap[field]; exists {
			newFieldTypes[i] = fieldType
		} else {
			newFieldTypes[i] = StringField
		}

		// Check if it's a regex operator
		newIsRegex[i] = (operators[i] == "=~" || operators[i] == "!~")

		// Reset negation mask since we've already applied it
		newNegationMask[i] = false
	}

	return ComplexQueryParams{
		Fields:        fields,
		Values:        values,
		Operators:     operators,
		FieldTypes:    newFieldTypes,
		IsRegex:       newIsRegex,
		LogicOperator: params.LogicOperator,
		NegationMask:  newNegationMask,
		MessageSearch: params.MessageSearch,
	}
}

// ========================= UNIFIED ANALYTICAL QUERY GENERATION =========================

// AnalyticalQueryParams represents parameters for generating a unified analytical query
type AnalyticalQueryParams struct {
	FilterField     string      // Field for base filtering
	FilterValue     interface{} // Value for base filtering
	GroupByField    string      // Field for grouping results
	AggregationType string      // "count", "avg", "min", "max", "quantile"
	NumericField    string      // Field for numeric aggregations (avg, min, max, quantile)
	Quantile        float64     // Quantile value for quantile aggregation (0.0-1.0)
	Limit           int         // Result limit
}

// GetRandomAnalyticalLimit returns a random limit for analytical queries
func GetRandomAnalyticalLimit() int {
	return ElasticsearchLimits[RandomIntn(len(ElasticsearchLimits))]
}

// GetRandomTimeSeriesLimit returns a random limit for time series queries
func GetRandomTimeSeriesLimit() int {
	return TimeSeriesLimits[RandomIntn(len(TimeSeriesLimits))]
}

// GetRandomQuantile returns a random quantile value for analytical queries
func GetRandomQuantile() float64 {
	quantiles := []float64{0.50, 0.75, 0.90, 0.95, 0.99}
	return quantiles[RandomIntn(len(quantiles))]
}

// GenerateUnifiedAnalyticalQuery generates parameters for unified analytical queries
// Distribution: Count(40%), Avg(20%), Min(10%), Max(10%), Quantile(20%)
func GenerateUnifiedAnalyticalQuery() AnalyticalQueryParams {
	// 1. Generate filter field and value (same approach as SimpleQuery)
	simpleParams := GenerateUnifiedSimpleQuery()

	// 2. Select group by field from standard fields
	groupByField := GroupByFields[RandomIntn(len(GroupByFields))]

	// 3. Get limit from existing constants
	limit := GetRandomAnalyticalLimit()

	// 4. Determine aggregation type based on probability
	probability := RandomIntn(100)

	switch {
	case probability < 40:
		// 40% - Count aggregation
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "count",
			Limit:           limit,
		}

	case probability < 60:
		// 20% - Average aggregation
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "avg",
			NumericField:    numericField,
			Limit:           limit,
		}

	case probability < 70:
		// 10% - Minimum aggregation
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "min",
			NumericField:    numericField,
			Limit:           limit,
		}

	case probability < 80:
		// 10% - Maximum aggregation
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "max",
			NumericField:    numericField,
			Limit:           limit,
		}

	case probability < 88:
		// 8% - Sum aggregation
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "sum",
			NumericField:    numericField,
			Limit:           limit,
		}

	default:
		// 12% - Quantile aggregation
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		quantile := GetRandomQuantile()
		return AnalyticalQueryParams{
			FilterField:     simpleParams.Field,
			FilterValue:     simpleParams.Value,
			GroupByField:    groupByField,
			AggregationType: "quantile",
			NumericField:    numericField,
			Quantile:        quantile,
			Limit:           limit, // Not used for quantile but included for consistency
		}
	}
}

// StatQueryParams represents parameters for generating a unified statistical query
// StatQuery differs from AnalyticalQuery: returns single global value instead of grouped results
type StatQueryParams struct {
	FilterField  string      // Field for base filtering (from unified strategy)
	FilterValue  interface{} // Value for base filtering
	StatType     string      // "count", "avg", "sum", "min", "max", "quantile"
	NumericField string      // Field for numeric operations (only used for non-count stats)
	Quantile     float64     // Quantile value for quantile aggregation (0.0-1.0)
}

// GenerateUnifiedStatQuery generates parameters for unified statistical queries
// Distribution: count(30%), avg(20%), sum(15%), quantile(15%), min(10%), max(10%) - only functions available in all systems
func GenerateUnifiedStatQuery() StatQueryParams {
	// 1. Generate filter field and value (same approach as SimpleQuery)
	simpleParams := GenerateUnifiedSimpleQuery()

	// 2. Determine stat type based on probability distribution
	probability := RandomIntn(100)

	switch {
	case probability < 30:
		// 30% - Count statistic (most common, available in all systems)
		return StatQueryParams{
			FilterField: simpleParams.Field,
			FilterValue: simpleParams.Value,
			StatType:    "count",
		}

	case probability < 50:
		// 20% - Average statistic (available in all systems)
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return StatQueryParams{
			FilterField:  simpleParams.Field,
			FilterValue:  simpleParams.Value,
			StatType:     "avg",
			NumericField: numericField,
		}

	case probability < 65:
		// 15% - Sum statistic (available in all systems)
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return StatQueryParams{
			FilterField:  simpleParams.Field,
			FilterValue:  simpleParams.Value,
			StatType:     "sum",
			NumericField: numericField,
		}

	case probability < 80:
		// 15% - Quantile statistic (available in all systems)
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		quantile := GetRandomQuantile()
		return StatQueryParams{
			FilterField:  simpleParams.Field,
			FilterValue:  simpleParams.Value,
			StatType:     "quantile",
			NumericField: numericField,
			Quantile:     quantile,
		}

	case probability < 90:
		// 10% - Minimum statistic (available in all systems)
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return StatQueryParams{
			FilterField:  simpleParams.Field,
			FilterValue:  simpleParams.Value,
			StatType:     "min",
			NumericField: numericField,
		}

	default:
		// 10% - Maximum statistic (available in all systems)
		numericField := NumericFields[RandomIntn(len(NumericFields))]
		return StatQueryParams{
			FilterField:  simpleParams.Field,
			FilterValue:  simpleParams.Value,
			StatType:     "max",
			NumericField: numericField,
		}
	}
}

// TopKQueryParams represents parameters for generating a unified TopK query
// TopK queries find the top N most frequent values for a specific field
type TopKQueryParams struct {
	FilterField    string      // Field for base filtering (from unified strategy)
	FilterValue    interface{} // Value for base filtering
	FilterOperator string      // Operator for base filtering (=, !=, =~, !~)
	FilterIsRegex  bool        // Whether filter value is a regex pattern
	GroupByField   string      // Field for top-K ranking (from all searchable fields)
	K              int         // Number of top results to return (from TopKValues)
	TimeWindow     string      // Time window for Loki queries (from TimeIntervals)
	HasBaseFilter  bool        // Whether to include base filtering (50% chance)
}

// TimeSeriesQueryParams represents parameters for generating a unified TimeSeries query
// TimeSeries queries create temporal aggregations with field grouping
type TimeSeriesQueryParams struct {
	FilterField     string      // Field for base filtering (from FilterLabels)
	FilterValue     interface{} // Value for base filtering
	GroupByField    string      // Field for grouping results (from GroupByFields)
	TimeInterval    string      // Time interval for temporal aggregation (from TimeIntervals)
	AggregationType string      // Aggregation function: count, avg, min, max, sum, quantile
	NumericField    string      // Numeric field for avg/min/max/sum/quantile aggregations
	Quantile        float64     // Quantile value for percentile calculations (0.0-1.0)
	Limit           int         // Result limit (from TimeSeriesLimits)
}

// GenerateUnifiedTopKQuery generates parameters for unified TopK queries
// Distribution: fair field selection across all 18 searchable fields, K values [5,10,15,20,50,100,500,1000] supported by all systems
func GenerateUnifiedTopKQuery() TopKQueryParams {
	// 1. Select K value from unified range (full range supported by all systems)
	k := TopKValues[RandomIntn(len(TopKValues))]

	// 2. Select time window for time-based queries (Loki requirement)
	timeWindow := TimeIntervals[RandomIntn(len(TimeIntervals))]

	// 3. Select field for top-K ranking from all searchable fields for maximum fairness
	allFields := GetAllSearchableFields()
	groupByField := allFields[RandomIntn(len(allFields))]

	// 4. Determine if we should add base filtering (50% chance for fair comparison)
	hasBaseFilter := RandomIntn(2) == 0

	var filterField string
	var filterValue interface{}

	var filterOperator string
	var filterIsRegex bool

	if hasBaseFilter {
		// Generate base filter using same approach as SimpleQuery but different field
		simpleParams := GenerateUnifiedSimpleQuery()

		// Ensure filter field is different from groupBy field
		for simpleParams.Field == groupByField {
			simpleParams = GenerateUnifiedSimpleQuery()
		}

		filterField = simpleParams.Field
		filterValue = simpleParams.Value
		filterOperator = simpleParams.Operator
		filterIsRegex = simpleParams.IsRegex
	}

	return TopKQueryParams{
		FilterField:    filterField,
		FilterValue:    filterValue,
		FilterOperator: filterOperator,
		FilterIsRegex:  filterIsRegex,
		GroupByField:   groupByField,
		K:              k,
		TimeWindow:     timeWindow,
		HasBaseFilter:  hasBaseFilter,
	}
}

// GenerateUnifiedTimeSeriesQuery generates parameters for unified TimeSeries queries
// Uses shared constants for maximum fairness: FilterLabels, GroupByFields, TimeIntervals, TimeSeriesLimits
func GenerateUnifiedTimeSeriesQuery() TimeSeriesQueryParams {
	// 1. Select filter field from unified filter labels for fair comparison across all systems
	filterField := FilterLabels[RandomIntn(len(FilterLabels))]

	// 2. Get filter value from unified field values
	labelValuesMap := GetLabelValuesMap()
	var filterValue interface{}
	if values, exists := labelValuesMap[filterField]; exists && len(values) > 0 {
		filterValue = values[RandomIntn(len(values))]
	} else {
		filterValue = "default_value"
	}

	// 3. Select group by field from unified group by fields for fair comparison across all systems
	groupByField := GroupByFields[RandomIntn(len(GroupByFields))]

	// 4. Select time interval from unified time intervals for fair comparison across all systems
	timeInterval := TimeIntervals[RandomIntn(len(TimeIntervals))]

	// 5. Select aggregation type from the same 6 functions available in all systems
	aggregationTypes := []string{"count", "avg", "min", "max", "sum", "quantile"}
	aggregationType := aggregationTypes[RandomIntn(len(aggregationTypes))]

	// 6. Select numeric field for numeric aggregations (avg, min, max, sum, quantile)
	var numericField string
	var quantile float64
	if aggregationType != "count" {
		numericField = NumericFields[RandomIntn(len(NumericFields))]
		if aggregationType == "quantile" {
			// Generate quantile value between 0.50 and 0.99
			quantile = 0.50 + float64(RandomIntn(50))/100.0 // 0.50-0.99
		}
	}

	// 7. Select result limit from unified time series limits for fair comparison across all systems
	limit := GetRandomTimeSeriesLimit()

	return TimeSeriesQueryParams{
		FilterField:     filterField,
		FilterValue:     filterValue,
		GroupByField:    groupByField,
		TimeInterval:    timeInterval,
		AggregationType: aggregationType,
		NumericField:    numericField,
		Quantile:        quantile,
		Limit:           limit,
	}
}

// =============================================================================
// LOKI COMPATIBILITY FUNCTIONS
// =============================================================================

// SemanticNegationMap defines semantic opposites for common field values
// This allows converting negative conditions to positive ones using domain knowledge
var SemanticNegationMap = map[string]map[string][]string{
	"level": {
		"error":    {"info", "warn", "debug", "trace"},
		"info":     {"error", "warn", "critical", "fatal"},
		"warn":     {"info", "error", "debug", "trace"},
		"debug":    {"info", "warn", "error", "critical"},
		"critical": {"info", "warn", "debug", "trace"},
		"fatal":    {"info", "warn", "debug", "trace"},
	},
	"status": {
		"404": {"200", "201", "301", "302", "500"},
		"500": {"200", "201", "400", "404", "301"},
		"200": {"400", "404", "500", "503", "301"},
		"301": {"200", "404", "500", "400", "201"},
	},
	"response_status": {
		"404": {"200", "201", "301", "302", "500"},
		"500": {"200", "201", "400", "404", "301"},
		"200": {"400", "404", "500", "503", "301"},
		"301": {"200", "404", "500", "400", "201"},
	},
	"environment": {
		"production":  {"development", "staging", "testing"},
		"development": {"production", "staging", "testing"},
		"staging":     {"production", "development", "testing"},
		"testing":     {"production", "development", "staging"},
	},
	"error_code": {
		"404": {"200", "201", "301", "500"},
		"500": {"200", "400", "404", "301"},
		"400": {"200", "404", "500", "301"},
	},
}

// ConvertNegativeToPositive converts a negative condition to positive using semantic mapping or regex
func ConvertNegativeToPositive(field, operator string, value interface{}) (string, string, interface{}) {
	if operator != "!=" && operator != "!~" {
		return field, operator, value // Already positive
	}

	valueStr := fmt.Sprintf("%v", value)

	// Try semantic negation first
	if fieldMap, exists := SemanticNegationMap[field]; exists {
		if opposites, exists := fieldMap[valueStr]; exists && len(opposites) > 0 {
			// Convert to positive condition with alternatives
			if operator == "!=" {
				// Convert != to =~ with alternatives
				alternativePattern := fmt.Sprintf("(%s)", strings.Join(opposites, "|"))
				return field, "=~", alternativePattern
			} else { // operator == "!~"
				// Convert !~ to =~ with alternatives (more complex regex)
				alternativePattern := fmt.Sprintf("(%s)", strings.Join(opposites, "|"))
				return field, "=~", alternativePattern
			}
		}
	}

	// Use simple regex negation for specific patterns
	if operator == "!~" {
		// Convert complex negative regex to positive alternatives
		switch {
		case strings.Contains(valueStr, "[0-9]"):
			// For HTTP status codes like "5[0-9][0-9]" -> convert to "1[0-9][0-9]|2[0-9][0-9]|3[0-9][0-9]|4[0-9][0-9]"
			if strings.HasPrefix(valueStr, "5[0-9][0-9]") {
				return field, "=~", "[1-4][0-9][0-9]"
			}
			if strings.HasPrefix(valueStr, "4[0-9][0-9]") {
				return field, "=~", "[1-3][0-9][0-9]|5[0-9][0-9]"
			}
		}
	}

	// Fallback: add a universal positive matcher and keep the negative condition
	// This ensures Loki compatibility by having at least one positive matcher
	return field, operator, value
}

// EnsureLokiCompatibility ensures that query parameters are compatible with Loki requirements
// Loki requires at least one positive matcher (=, =~) in every query
func EnsureLokiCompatibility(fields []string, operators []string, values []interface{}) ([]string, []string, []interface{}) {
	hasPositive := false

	// Check if we already have a positive matcher
	for _, op := range operators {
		if op == "=" || op == "=~" {
			hasPositive = true
			break
		}
	}

	if !hasPositive {
		// Strategy 1: Try to convert the first negative to positive using semantic mapping
		for i, op := range operators {
			if op == "!=" || op == "!~" {
				_, newOp, newValue := ConvertNegativeToPositive(fields[i], op, values[i])
				if newOp == "=" || newOp == "=~" {
					// Successfully converted
					operators[i] = newOp
					values[i] = newValue
					hasPositive = true
					break
				}
			}
		}

		// Strategy 2: If conversion failed, add a universal positive matcher
		if !hasPositive {
			// Prepend a universal positive matcher
			fields = append([]string{"log_type"}, fields...)
			operators = append([]string{"=~"}, operators...)
			values = append([]interface{}{".+"}, values...)
		}
	}

	return fields, operators, values
}

// FixLokiRegexEscaping fixes regex patterns for Loki compatibility
func FixLokiRegexEscaping(pattern string) string {
	// CRITICAL FIX: The previous implementation was causing double-escaping issues
	// Loki uses RE2 regex engine and expects proper JSON-escaped strings

	// The key insight from the errors:
	// - "127\.0\.0\.[0-9]+" causes "invalid char escape" because Loki expects "127\\.0\\.0\\.[0-9]+"
	// - We need to ensure proper JSON string escaping for literal dots in IP addresses

	// For IP address patterns like "192\.168\.[0-9]+\.[0-9]+", we need proper escaping
	if strings.Contains(pattern, `\.`) {
		// Convert single-escaped dots to double-escaped for JSON compatibility
		// This is the fix for the "invalid char escape" errors we saw
		pattern = strings.ReplaceAll(pattern, `\.`, `\\.`)
	}

	// Additional fixes for common problematic patterns
	// Handle bracket patterns that might need escaping
	// Note: [0-9] patterns are correct and don't need additional escaping

	return pattern
}

// GenerateNegativeRegexPattern creates a negative regex pattern from a positive one
// This replaces the need for !~ operator which causes issues in Loki
func GenerateNegativeRegexPattern(positivePattern string) string {
	// For simple patterns, use negative lookahead
	// ^(?!pattern).* means "match anything that doesn't start with pattern"

	// Handle specific common patterns
	switch {
	case strings.Contains(positivePattern, "error"):
		// Instead of !~".*error.*", use =~"^(?!.*error).*"
		return "^(?!.*error).*"

	case strings.HasPrefix(positivePattern, "5[0-9][0-9]"):
		// Instead of !~"5[0-9][0-9]", use =~"[1-4][0-9][0-9]"
		return "[1-4][0-9][0-9]"

	case strings.HasPrefix(positivePattern, "4[0-9][0-9]"):
		// Instead of !~"4[0-9][0-9]", use =~"[1-35][0-9][0-9]"
		return "[1-35][0-9][0-9]"

	case strings.Contains(positivePattern, "192\\.168"):
		// Instead of !~"192\.168\.[0-9]+\.[0-9]+", use =~"^(?!192\.168).*"
		return "^(?!192\\.168).*"

	case strings.Contains(positivePattern, "10\\.[0-9]"):
		// Instead of !~"10\.[0-9]+\.[0-9]+\.[0-9]+", use =~"^(?!10\.).*"
		return "^(?!10\\.).*"

	default:
		// Generic negative lookahead pattern
		// Remove outer .* if present to avoid nested .*
		pattern := positivePattern
		if strings.HasPrefix(pattern, ".*") && strings.HasSuffix(pattern, ".*") {
			// Remove outer .* wrapper
			pattern = strings.TrimPrefix(pattern, ".*")
			pattern = strings.TrimSuffix(pattern, ".*")
		}

		return fmt.Sprintf("^(?!.*%s).*", pattern)
	}
}

// ConvertNegativeRegexToPositive converts old !~ operations to =~ with negative patterns
func ConvertNegativeRegexToPositive(field, operator string, value interface{}) (string, string, interface{}) {
	if operator != "!~" {
		return field, operator, value // Not a negative regex
	}

	// Convert !~ to =~ with negative pattern
	positivePattern := fmt.Sprintf("%v", value)
	negativePattern := GenerateNegativeRegexPattern(positivePattern)

	return field, "=~", negativePattern
}

// FixVictoriaLogsRegexEscaping fixes regex patterns for VictoriaLogs LogsQL compatibility
func FixVictoriaLogsRegexEscaping(pattern string) string {
	// VictoriaLogs LogsQL specific regex escaping requirements
	// Based on error analysis: "improperly quoted string" errors occur with complex patterns

	// For complex IP patterns that cause parsing errors, we need to simplify them
	// The problematic pattern: 172\.(1[6-9]|2[0-9]|3[0-1])\.[0-9]+\.[0-9]+
	// VictoriaLogs has issues with complex character classes and alternation groups in quoted strings

	// Handle complex IP patterns specifically
	if strings.Contains(pattern, `172\.(1[6-9]|2[0-9]|3[0-1])`) {
		// Replace with a simpler pattern that VictoriaLogs can handle
		pattern = `172\.1[6-9]\.[0-9]+\.[0-9]+|172\.2[0-9]\.[0-9]+\.[0-9]+|172\.3[0-1]\.[0-9]+\.[0-9]+`
		// Simplify further - remove backslash escaping for dots
		pattern = strings.ReplaceAll(pattern, `\.`, `.`)
		return pattern
	}

	// For other patterns, remove backslash escaping since VictoriaLogs handles dots naturally in regex context
	if strings.Contains(pattern, `\.`) {
		// For IP patterns, simplify to avoid escaping issues
		// Replace complex regex patterns with simpler alternatives
		pattern = strings.ReplaceAll(pattern, `\.`, `.`)
	}

	// Handle bracket patterns - simplify complex ranges
	pattern = strings.ReplaceAll(pattern, `\(`, `(`)
	pattern = strings.ReplaceAll(pattern, `\)`, `)`)
	pattern = strings.ReplaceAll(pattern, `\[`, `[`)
	pattern = strings.ReplaceAll(pattern, `\]`, `]`)

	return pattern
}

// ========================= LOKI CARDINALITY OPTIMIZATION FUNCTIONS =========================

// IsLabelField checks if a field should be used as a Loki label (from CommonLabels)
func IsLabelField(field string) bool {
	for _, label := range CommonLabels {
		if field == label {
			return true
		}
	}
	return false
}

// GetNonLabelSearchableFields returns searchable fields that are NOT in CommonLabels
func GetNonLabelSearchableFields() []string {
	allFields := GetAllSearchableFields()
	nonLabelFields := []string{}
	for _, field := range allFields {
		if !IsLabelField(field) {
			nonLabelFields = append(nonLabelFields, field)
		}
	}
	return nonLabelFields
}

// FieldsSplit represents separated fields for Loki hybrid queries (labels + JSON)
type FieldsSplit struct {
	LabelFields    []string
	LabelValues    []interface{}
	LabelOperators []string
	JsonFields     []string
	JsonValues     []interface{}
	JsonOperators  []string
}

// SplitComplexQueryFields splits query fields into label vs JSON fields for Loki optimization
func SplitComplexQueryFields(fields []string, values []interface{}, operators []string) FieldsSplit {
	result := FieldsSplit{
		LabelFields:    []string{},
		LabelValues:    []interface{}{},
		LabelOperators: []string{},
		JsonFields:     []string{},
		JsonValues:     []interface{}{},
		JsonOperators:  []string{},
	}

	for i, field := range fields {
		if IsLabelField(field) {
			result.LabelFields = append(result.LabelFields, field)
			result.LabelValues = append(result.LabelValues, values[i])
			result.LabelOperators = append(result.LabelOperators, operators[i])
		} else {
			result.JsonFields = append(result.JsonFields, field)
			result.JsonValues = append(result.JsonValues, values[i])
			result.JsonOperators = append(result.JsonOperators, operators[i])
		}
	}

	return result
}
