package logdata

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Random functions now use math/rand/v2 for better thread-safe performance

// GetRandomLogType returns a random log type
func GetRandomLogType() string {
	return LogTypes[RandomIntn(len(LogTypes))]
}

// GetRandomHost returns a random host name
func GetRandomHost() string {
	return Hosts[RandomIntn(len(Hosts))]
}

// GetRandomContainer returns a random container name
func GetRandomContainer() string {
	return ContainerNames[RandomIntn(len(ContainerNames))]
}

// GetRandomEnvironment returns a random environment name
func GetRandomEnvironment() string {
	return Environments[RandomIntn(len(Environments))]
}

// GetRandomDataCenter returns a random data center name
func GetRandomDataCenter() string {
	return DataCenters[RandomIntn(len(DataCenters))]
}

// GetRandomService returns a random service name
func GetRandomService() string {
	return Services[RandomIntn(len(Services))]
}

// GetRandomLogLevel returns a random log level
func GetRandomLogLevel() string {
	return LogLevels[RandomIntn(len(LogLevels))]
}

// GetRandomHttpMethod returns a random HTTP method
func GetRandomHttpMethod() string {
	return HttpMethods[RandomIntn(len(HttpMethods))]
}

// GetRandomHttpStatusCode returns a random HTTP status code
func GetRandomHttpStatusCode() int {
	return HttpStatusCodes[RandomIntn(len(HttpStatusCodes))]
}

// GetRandomErrorMessage returns a random error message
func GetRandomErrorMessage() string {
	return ErrorMessages[RandomIntn(len(ErrorMessages))]
}

// GetRandomPath generates a random URL path
func GetRandomPath() string {
	// Random number of segments from 1 to 4
	numSegments := RandomIntn(4) + 1
	path := ""

	for i := 0; i < numSegments; i++ {
		path += "/" + PathSegments[RandomIntn(len(PathSegments))]
	}

	// Sometimes add query parameters
	if RandomIntn(3) == 0 {
		path += fmt.Sprintf("?id=%d&limit=%d", RandomIntn(1000), 10+RandomIntn(90))
	}

	return path
}

// GetRandomEventType returns a random event type
func GetRandomEventType() string {
	return EventTypes[RandomIntn(len(EventTypes))]
}

// GetRandomMetricName returns a random metric name
func GetRandomMetricName() string {
	return MetricNames[RandomIntn(len(MetricNames))]
}

// GetLabelValuesMap returns a map of label names to their possible values
func GetLabelValuesMap() map[string][]string {
	// Helper function to convert int slice to string slice
	convertIntsToStrings := func(ints []int) []string {
		stringSlice := make([]string, len(ints))
		for i, v := range ints {
			stringSlice[i] = fmt.Sprintf("%d", v)
		}
		return stringSlice
	}

	return map[string][]string{
		// CommonLabels (keyword fields)
		"log_type":       LogTypes,
		"host":           Hosts,
		"container_name": ContainerNames,
		"environment":    Environments,
		"datacenter":     DataCenters,
		"service":        Services,

		// Other searchable fields (text fields) - using existing constants
		"level":           LogLevels,
		"status":          convertIntsToStrings(HttpStatusCodes),
		"request_method":  HttpMethods,
		"error_code":      convertIntsToStrings(HttpStatusCodes),
		"metric_name":     MetricNames,
		"event_type":      EventTypes,
		"exception":       ExceptionTypes,
		"response_status": convertIntsToStrings(HttpStatusCodes),
		"region":          DataCenters, // Reuse existing DataCenters

		// Simple generated values for high-cardinality fields
		"user_id":   {"user-1", "user-2", "user-3", "user-4", "user-5"},
		"namespace": {"default", "kube-system", "monitoring", "auth", "payments"},
	}
}

// GetRandomValueForLabel returns a random value for the specified label
func GetRandomValueForLabel(label string) string {
	labelValuesMap := GetLabelValuesMap()

	if values, exists := labelValuesMap[label]; exists {
		return values[RandomIntn(len(values))]
	}

	// Default case for unknown labels
	return fmt.Sprintf("value-%d", RandomIntn(10))
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

		// Shuffle the values using our thread-safe random
		for i := len(valuesCopy) - 1; i > 0; i-- {
			j := RandomIntn(i + 1)
			valuesCopy[i], valuesCopy[j] = valuesCopy[j], valuesCopy[i]
		}

		// Take the first 'count' values
		values = valuesCopy[:count]
	} else {
		// Generate random values if the label doesn't exist in our map
		values = make([]string, count)
		for i := 0; i < count; i++ {
			values[i] = fmt.Sprintf("value-%d", RandomIntn(10))
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

	// Shuffle the labels using our thread-safe random
	for i := len(labelsCopy) - 1; i > 0; i-- {
		j := RandomIntn(i + 1)
		labelsCopy[i], labelsCopy[j] = labelsCopy[j], labelsCopy[i]
	}

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
		RandomIntn(256),
		RandomIntn(256),
		RandomIntn(256),
		RandomIntn(256))
}

// GetRandomUserAgent returns a random user agent string
func GetRandomUserAgent() string {
	return UserAgents[RandomIntn(len(UserAgents))]
}

// GetRandomVersion returns a random version string
func GetRandomVersion() string {
	major := RandomIntn(10)
	minor := RandomIntn(20)
	patch := RandomIntn(50)
	return fmt.Sprintf("%d.%d.%d", major, minor, patch)
}

// GetRandomGitCommit returns a random git commit hash
func GetRandomGitCommit() string {
	chars := "abcdef0123456789"
	id := make([]byte, 7)
	for i := range id {
		id[i] = chars[RandomIntn(len(chars))]
	}
	return string(id)
}

// GetRandomRequestID returns a random request ID
func GetRandomRequestID() string {
	chars := "abcdef0123456789"
	length := 16 + RandomIntn(16) // 16-32 characters
	id := make([]byte, length)
	for i := range id {
		id[i] = chars[RandomIntn(len(chars))]
	}
	return string(id)
}

// GetRandomException returns a random exception name
func GetRandomException() string {
	return ExceptionTypes[RandomIntn(len(ExceptionTypes))]
}

// GetRandomCodeLine returns a random line of code for stacktraces
func GetRandomCodeLine() string {
	return CodeLines[RandomIntn(len(CodeLines))]
}

// GetRandomStackTrace generates a random stack trace for an exception
func GetRandomStackTrace(exception string) string {
	var parts []string

	if strings.HasPrefix(exception, "java") {
		// Java stacktrace
		parts = append(parts, exception+": "+GetRandomErrorMessage())

		depth := 3 + RandomIntn(7) // 3-10 frames
		for i := 0; i < depth; i++ {
			pkg := JavaPackageNames[RandomIntn(len(JavaPackageNames))]
			cls := "Class" + string('A'+rune(RandomIntn(26)))
			method := JavaMethodNames[RandomIntn(len(JavaMethodNames))]
			line := RandomIntn(500) + 1
			parts = append(parts, fmt.Sprintf("\tat %s.%s.%s(%s.java:%d)", pkg, cls, method, cls, line))
		}

	} else if strings.HasPrefix(exception, "python") {
		// Python traceback
		parts = append(parts, "Traceback (most recent call last):")

		depth := 3 + RandomIntn(5) // 3-8 frames
		for i := 0; i < depth; i++ {
			file := PythonFileNames[RandomIntn(len(PythonFileNames))]
			method := PythonMethodNames[RandomIntn(len(PythonMethodNames))]
			line := RandomIntn(300) + 1
			parts = append(parts, fmt.Sprintf("  File \"%s\", line %d, in %s", file, line, method))
			parts = append(parts, "    "+GetRandomCodeLine())
		}
		parts = append(parts, exception+": "+GetRandomErrorMessage())
	} else {
		// Generic stacktrace
		parts = append(parts, exception+": "+GetRandomErrorMessage())

		depth := 2 + RandomIntn(5) // 2-7 frames
		for i := 0; i < depth; i++ {
			file := GenericFileNames[RandomIntn(len(GenericFileNames))]
			method := GenericMethodNames[RandomIntn(len(GenericMethodNames))]
			line := RandomIntn(400) + 1
			parts = append(parts, fmt.Sprintf("    at %s (%s:%d)", method, file, line))
		}
	}

	return strings.Join(parts, "\n")
}

// GetRandomTags returns a random list of tags
func GetRandomTags() []string {
	numTags := 1 + RandomIntn(5) // 1-5 tags
	if numTags > len(Tags) {
		numTags = len(Tags)
	}

	// Select unique tags
	selected := make(map[int]bool)
	tags := make([]string, 0, numTags)

	for len(tags) < numTags {
		idx := RandomIntn(len(Tags))
		if !selected[idx] {
			selected[idx] = true
			tags = append(tags, Tags[idx])
		}
	}

	return tags
}

// GetRandomErrorContext returns a map of random context values for errors
func GetRandomErrorContext() map[string]string {
	numFields := 2 + RandomIntn(6) // 2-8 fields
	context := make(map[string]string)

	for i := 0; i < numFields; i++ {
		key := ContextKeys[RandomIntn(len(ContextKeys))]
		value := ContextValues[RandomIntn(len(ContextValues))]

		// Add some variability to values
		if strings.Contains(key, "id") {
			value = fmt.Sprintf("%s-%d", value, RandomIntn(1000))
		} else if strings.Contains(key, "ip") {
			value = GetRandomIP()
		} else if strings.Contains(key, "duration") {
			value = fmt.Sprintf("%d", RandomIntn(10000))
		}

		context[key] = value
	}

	return context
}

// GetRandomDependencies returns a list of random dependencies
func GetRandomDependencies() []string {
	numDeps := 1 + RandomIntn(5) // 1-5 dependencies
	if numDeps > len(Dependencies) {
		numDeps = len(Dependencies)
	}

	// Select unique dependencies
	selected := make(map[int]bool)
	deps := make([]string, 0, numDeps)

	for len(deps) < numDeps {
		idx := RandomIntn(len(Dependencies))
		if !selected[idx] {
			selected[idx] = true
			deps = append(deps, Dependencies[idx])
		}
	}

	return deps
}

// GetLogMessage generates a log message based on the log level
func GetLogMessage(logLevel string) string {
	// Generate log message based on log level
	switch logLevel {
	case "debug":
		return DebugMessages[RandomIntn(len(DebugMessages))]
	case "info":
		return InfoMessages[RandomIntn(len(InfoMessages))]
	case "warn":
		return WarnMessages[RandomIntn(len(WarnMessages))]
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
	r := RandomIntn(totalWeight)

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
	return CommonLabels[RandomIntn(len(CommonLabels))]
}
