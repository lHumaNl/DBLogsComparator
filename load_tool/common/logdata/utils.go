package logdata

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// RandomSource provides a common random source for consistent randomization
var RandomSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// GetRandomLogType returns a random log type
func GetRandomLogType() string {
	return LogTypes[RandomSource.Intn(len(LogTypes))]
}

// GetRandomHost returns a random host name
func GetRandomHost() string {
	return Hosts[RandomSource.Intn(len(Hosts))]
}

// GetRandomContainer returns a random container name
func GetRandomContainer() string {
	return ContainerNames[RandomSource.Intn(len(ContainerNames))]
}

// GetRandomEnvironment returns a random environment name
func GetRandomEnvironment() string {
	return Environments[RandomSource.Intn(len(Environments))]
}

// GetRandomDataCenter returns a random data center name
func GetRandomDataCenter() string {
	return DataCenters[RandomSource.Intn(len(DataCenters))]
}

// GetRandomService returns a random service name
func GetRandomService() string {
	return Services[RandomSource.Intn(len(Services))]
}

// GetRandomLogLevel returns a random log level
func GetRandomLogLevel() string {
	return LogLevels[RandomSource.Intn(len(LogLevels))]
}

// GetRandomHttpMethod returns a random HTTP method
func GetRandomHttpMethod() string {
	return HttpMethods[RandomSource.Intn(len(HttpMethods))]
}

// GetRandomHttpStatusCode returns a random HTTP status code
func GetRandomHttpStatusCode() int {
	return HttpStatusCodes[RandomSource.Intn(len(HttpStatusCodes))]
}

// GetRandomErrorMessage returns a random error message
func GetRandomErrorMessage() string {
	return ErrorMessages[RandomSource.Intn(len(ErrorMessages))]
}

// GetRandomPath generates a random URL path
func GetRandomPath() string {
	// Random number of segments from 1 to 4
	numSegments := RandomSource.Intn(4) + 1
	path := ""

	for i := 0; i < numSegments; i++ {
		path += "/" + PathSegments[RandomSource.Intn(len(PathSegments))]
	}

	// Sometimes add query parameters
	if RandomSource.Intn(3) == 0 {
		path += fmt.Sprintf("?id=%d&limit=%d", RandomSource.Intn(1000), 10+RandomSource.Intn(90))
	}

	return path
}

// GetRandomEventType returns a random event type
func GetRandomEventType() string {
	return EventTypes[RandomSource.Intn(len(EventTypes))]
}

// GetRandomMetricName returns a random metric name
func GetRandomMetricName() string {
	return MetricNames[RandomSource.Intn(len(MetricNames))]
}

// GetLabelValuesMap returns a map of label names to their possible values
func GetLabelValuesMap() map[string][]string {
	return map[string][]string{
		"log_type":       LogTypes,
		"host":           Hosts,
		"container_name": ContainerNames,
		"environment":    Environments,
		"datacenter":     DataCenters,
		"service":        Services,
		"level":          LogLevels,
	}
}

// GetRandomValueForLabel returns a random value for the specified label
func GetRandomValueForLabel(label string) string {
	labelValuesMap := GetLabelValuesMap()

	if values, exists := labelValuesMap[label]; exists {
		return values[RandomSource.Intn(len(values))]
	}

	// Default case for unknown labels
	return fmt.Sprintf("value-%d", RandomSource.Intn(10))
}

// GetMultipleRandomValuesForLabel returns multiple random values for a label
func GetMultipleRandomValuesForLabel(label string, count int) []string {
	labelValuesMap := GetLabelValuesMap()
	var values []string

	if labelValues, exists := labelValuesMap[label]; exists {
		// Ensure we don't try to get more values than exist
		if count > len(labelValues) {
			count = len(labelValues)
		}

		// Make a copy to avoid modifying the original slice
		valuesCopy := make([]string, len(labelValues))
		copy(valuesCopy, labelValues)

		// Shuffle the values
		rand.Shuffle(len(valuesCopy), func(i, j int) {
			valuesCopy[i], valuesCopy[j] = valuesCopy[j], valuesCopy[i]
		})

		// Take the first 'count' values
		values = valuesCopy[:count]
	} else {
		// Generate random values if the label doesn't exist in our map
		values = make([]string, count)
		for i := 0; i < count; i++ {
			values[i] = fmt.Sprintf("value-%d", RandomSource.Intn(10))
		}
	}

	return values
}

// BuildLokiLabelFilterExpression creates a Loki label expression with randomly selected values
func BuildLokiLabelFilterExpression(label string, valueCount int, useRegex bool) string {
	values := GetMultipleRandomValuesForLabel(label, valueCount)

	if len(values) == 0 {
		return fmt.Sprintf(`%s=~".+"`, label)
	}

	if len(values) == 1 && !useRegex {
		return fmt.Sprintf(`%s="%s"`, label, values[0])
	}

	// Use regex for multiple values
	joinedValues := strings.Join(values, "|")
	return fmt.Sprintf(`%s=~"(%s)"`, label, joinedValues)
}

// GetRandomLabels returns a list of random labels from the common labels
func GetRandomLabels(count int) []string {
	if count >= len(CommonLabels) {
		return CommonLabels
	}

	// Make a copy to avoid modifying the original slice
	labelsCopy := make([]string, len(CommonLabels))
	copy(labelsCopy, CommonLabels)

	// Shuffle the labels
	rand.Shuffle(len(labelsCopy), func(i, j int) {
		labelsCopy[i], labelsCopy[j] = labelsCopy[j], labelsCopy[i]
	})

	// Take the first 'count' labels
	return labelsCopy[:count]
}

// Contains checks if a string is in a slice
func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// GetRandomIP returns a random IP address
func GetRandomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d",
		RandomSource.Intn(256),
		RandomSource.Intn(256),
		RandomSource.Intn(256),
		RandomSource.Intn(256))
}

// GetRandomUserAgent returns a random user agent string
func GetRandomUserAgent() string {
	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
	}
	return userAgents[RandomSource.Intn(len(userAgents))]
}

// GetRandomVersion returns a random version string
func GetRandomVersion() string {
	major := RandomSource.Intn(10)
	minor := RandomSource.Intn(20)
	patch := RandomSource.Intn(50)
	return fmt.Sprintf("%d.%d.%d", major, minor, patch)
}

// GetRandomGitCommit returns a random git commit hash
func GetRandomGitCommit() string {
	chars := "abcdef0123456789"
	id := make([]byte, 7)
	for i := range id {
		id[i] = chars[RandomSource.Intn(len(chars))]
	}
	return string(id)
}

// GetRandomRequestID returns a random request ID
func GetRandomRequestID() string {
	chars := "abcdef0123456789"
	length := 16 + RandomSource.Intn(16) // 16-32 characters
	id := make([]byte, length)
	for i := range id {
		id[i] = chars[RandomSource.Intn(len(chars))]
	}
	return string(id)
}

// GetRandomException returns a random exception name
func GetRandomException() string {
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
	return exceptions[RandomSource.Intn(len(exceptions))]
}

// GetRandomCodeLine returns a random line of code for stacktraces
func GetRandomCodeLine() string {
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
	return codeLines[RandomSource.Intn(len(codeLines))]
}

// GetRandomStackTrace generates a random stack trace for an exception
func GetRandomStackTrace(exception string) string {
	var parts []string

	if strings.HasPrefix(exception, "java") {
		// Java stacktrace
		parts = append(parts, exception+": "+GetRandomErrorMessage())
		packageNames := []string{"com.example", "org.service", "io.client", "net.util", "app.core"}
		methodNames := []string{"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize"}

		depth := 3 + RandomSource.Intn(7) // 3-10 frames
		for i := 0; i < depth; i++ {
			pkg := packageNames[RandomSource.Intn(len(packageNames))]
			cls := "Class" + string('A'+rune(RandomSource.Intn(26)))
			method := methodNames[RandomSource.Intn(len(methodNames))]
			line := RandomSource.Intn(500) + 1
			parts = append(parts, fmt.Sprintf("\tat %s.%s.%s(%s.java:%d)", pkg, cls, method, cls, line))
		}

	} else if strings.HasPrefix(exception, "python") {
		// Python traceback
		parts = append(parts, "Traceback (most recent call last):")
		fileNames := []string{"app.py", "utils.py", "models.py", "views.py", "services.py"}
		methodNames := []string{"process_request", "validate_input", "fetch_data", "update_record", "authenticate", "initialize"}

		depth := 3 + RandomSource.Intn(5) // 3-8 frames
		for i := 0; i < depth; i++ {
			file := fileNames[RandomSource.Intn(len(fileNames))]
			method := methodNames[RandomSource.Intn(len(methodNames))]
			line := RandomSource.Intn(300) + 1
			parts = append(parts, fmt.Sprintf("  File \"%s\", line %d, in %s", file, line, method))
			parts = append(parts, "    "+GetRandomCodeLine())
		}
		parts = append(parts, exception+": "+GetRandomErrorMessage())
	} else {
		// Generic stacktrace
		parts = append(parts, exception+": "+GetRandomErrorMessage())
		fileNames := []string{"app.js", "server.go", "api.rb", "client.php", "handler.cs"}
		methodNames := []string{"processRequest", "validateInput", "fetchData", "updateRecord", "authenticate", "initialize"}

		depth := 2 + RandomSource.Intn(5) // 2-7 frames
		for i := 0; i < depth; i++ {
			file := fileNames[RandomSource.Intn(len(fileNames))]
			method := methodNames[RandomSource.Intn(len(methodNames))]
			line := RandomSource.Intn(400) + 1
			parts = append(parts, fmt.Sprintf("    at %s (%s:%d)", method, file, line))
		}
	}

	return strings.Join(parts, "\n")
}

// GetRandomTags returns a random list of tags
func GetRandomTags() []string {
	allTags := []string{
		"backend", "frontend", "database", "cache", "network", "security",
		"performance", "storage", "memory", "cpu", "disk", "timeout",
		"connection", "authentication", "critical", "warning", "info",
		"microservice", "gateway", "proxy", "rate-limit", "firewall",
	}

	numTags := 1 + RandomSource.Intn(5) // 1-5 tags
	if numTags > len(allTags) {
		numTags = len(allTags)
	}

	// Select unique tags
	selected := make(map[int]bool)
	tags := make([]string, 0, numTags)

	for len(tags) < numTags {
		idx := RandomSource.Intn(len(allTags))
		if !selected[idx] {
			selected[idx] = true
			tags = append(tags, allTags[idx])
		}
	}

	return tags
}

// GetRandomErrorContext returns a map of random context values for errors
func GetRandomErrorContext() map[string]string {
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

	numFields := 2 + RandomSource.Intn(6) // 2-8 fields
	context := make(map[string]string)

	for i := 0; i < numFields; i++ {
		key := keys[RandomSource.Intn(len(keys))]
		value := values[RandomSource.Intn(len(values))]

		// Add some variability to values
		if strings.Contains(key, "id") {
			value = fmt.Sprintf("%s-%d", value, RandomSource.Intn(1000))
		} else if strings.Contains(key, "ip") {
			value = GetRandomIP()
		} else if strings.Contains(key, "duration") {
			value = fmt.Sprintf("%d", RandomSource.Intn(10000))
		}

		context[key] = value
	}

	return context
}

// GetRandomDependencies returns a list of random dependencies
func GetRandomDependencies() []string {
	allDeps := []string{
		"postgres:13.3", "redis:6.2", "mongodb:4.4", "nginx:1.21",
		"rabbitmq:3.9", "kafka:2.8", "elasticsearch:7.14", "mysql:8.0",
		"memcached:1.6", "cassandra:4.0", "zookeeper:3.7", "etcd:3.5",
		"prometheus:2.30", "grafana:8.2", "influxdb:2.0", "kibana:7.14",
	}

	numDeps := 1 + RandomSource.Intn(5) // 1-5 dependencies
	if numDeps > len(allDeps) {
		numDeps = len(allDeps)
	}

	// Select unique dependencies
	selected := make(map[int]bool)
	deps := make([]string, 0, numDeps)

	for len(deps) < numDeps {
		idx := RandomSource.Intn(len(allDeps))
		if !selected[idx] {
			selected[idx] = true
			deps = append(deps, allDeps[idx])
		}
	}

	return deps
}

// GetLogMessage generates a log message based on the log level
func GetLogMessage(logLevel string) string {
	// Generate log message based on log level
	switch logLevel {
	case "debug":
		debugMessages := []string{
			"Debugging connection pool", "Debug: cache state dumped", "Debug trace enabled",
			"Variable values: x=42, y=13", "Debug mode active", "Memory usage: 1.2GB",
		}
		return debugMessages[RandomSource.Intn(len(debugMessages))]
	case "info":
		infoMessages := []string{
			"User logged in successfully", "Task completed", "Service started", "Config loaded",
			"Database connection established", "Process finished normally", "Data synchronized",
		}
		return infoMessages[RandomSource.Intn(len(infoMessages))]
	case "warn":
		warnMessages := []string{
			"Connection pool running low", "Slow query detected", "Deprecation warning",
			"Resource usage high", "Retry attempt 2/5", "Falling back to secondary system",
		}
		return warnMessages[RandomSource.Intn(len(warnMessages))]
	case "error", "critical":
		return GetRandomErrorMessage()
	default:
		return "Log message"
	}
}

// SelectRandomLogType selects a random log type based on distribution weights
func SelectRandomLogType(distribution map[string]int) string {
	// Calculate total weight
	totalWeight := 0
	for _, weight := range distribution {
		totalWeight += weight
	}

	if totalWeight <= 0 {
		// If total weight <= 0, return web_access by default
		return "web_access"
	}

	// Select random number from 0 to totalWeight-1
	r := RandomSource.Intn(totalWeight)

	// Find corresponding log type
	current := 0
	for logType, weight := range distribution {
		current += weight
		if r < current {
			return logType
		}
	}

	// Return web_access by default
	return "web_access"
}

// CountLogTypes counts the occurrences of different log types in a payload
func CountLogTypes(payload string) map[string]int {
	counts := make(map[string]int)

	// Split payload into individual lines
	lines := strings.Split(payload, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		// Attempt to parse JSON
		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			continue
		}

		// Check for log_type field
		if logType, ok := logEntry["log_type"].(string); ok {
			counts[logType]++
		}
	}

	return counts
}

// GetRandomLabel returns a random label from the CommonLabels array
func GetRandomLabel() string {
	return CommonLabels[RandomSource.Intn(len(CommonLabels))]
}
