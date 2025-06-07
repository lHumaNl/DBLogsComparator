package logdata

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
