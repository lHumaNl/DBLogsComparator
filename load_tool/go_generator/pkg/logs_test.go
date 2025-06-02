package pkg

import (
	"encoding/json"
	"testing"
)

func TestGenerateRandomData(t *testing.T) {
	// Testing random IP address generation function
	ip := GenerateRandomIP()
	if ip == "" {
		t.Error("GenerateRandomIP should return a non-empty string")
	}

	// Testing User-Agent generation function
	ua := GenerateRandomUserAgent()
	if ua == "" {
		t.Error("GenerateRandomUserAgent should return a non-empty string")
	}

	// Testing HTTP status generation function
	status := GenerateRandomHttpStatus()
	validStatuses := []int{200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
	validStatus := false
	for _, s := range validStatuses {
		if status == s {
			validStatus = true
			break
		}
	}
	if !validStatus {
		t.Errorf("GenerateRandomHttpStatus should return one of the valid HTTP statuses, got: %d", status)
	}

	// Testing HTTP method generation function
	method := GenerateRandomHttpMethod()
	validMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	validMethod := false
	for _, m := range validMethods {
		if method == m {
			validMethod = true
			break
		}
	}
	if !validMethod {
		t.Errorf("GenerateRandomHttpMethod should return one of the valid HTTP methods, got: %s", method)
	}
}

func TestSelectRandomLogType(t *testing.T) {
	// Testing log type selection function based on distribution
	distribution := map[string]int{
		"web_access":  60,
		"web_error":   10,
		"application": 20,
		"metric":      5,
		"event":       5,
	}

	// Run log type selection multiple times
	// and verify that all types are possible
	types := make(map[string]bool)
	iterations := 1000

	for i := 0; i < iterations; i++ {
		logType := SelectRandomLogType(distribution)
		types[logType] = true
	}

	// Check that all log types were selected at least once
	for logType := range distribution {
		if !types[logType] {
			t.Errorf("Log type '%s' was not selected even once in %d iterations", logType, iterations)
		}
	}
}

func TestGenerateLog(t *testing.T) {
	timestamp := "2023-01-01T12:00:00Z"

	// Test for web_access type
	webAccessLog := GenerateLog("web_access", timestamp)

	// Convert to JSON and back to map to check fields
	jsonData, err := json.Marshal(webAccessLog)
	if err != nil {
		t.Fatalf("Error serializing web_access log: %v", err)
	}

	var webAccessMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &webAccessMap); err != nil {
		t.Fatalf("Error deserializing web_access log: %v", err)
	}

	checkBaseLogFields(t, webAccessMap, "web_access", timestamp)
	if _, ok := webAccessMap["remote_addr"]; !ok {
		t.Error("web_access log must have 'remote_addr' field")
	}
	if _, ok := webAccessMap["status"]; !ok {
		t.Error("web_access log must have 'status' field")
	}

	// Test for web_error type
	webErrorLog := GenerateLog("web_error", timestamp)
	jsonData, err = json.Marshal(webErrorLog)
	if err != nil {
		t.Fatalf("Error serializing web_error log: %v", err)
	}

	var webErrorMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &webErrorMap); err != nil {
		t.Fatalf("Error deserializing web_error log: %v", err)
	}

	checkBaseLogFields(t, webErrorMap, "web_error", timestamp)
	if _, ok := webErrorMap["level"]; !ok {
		t.Error("web_error log must have 'level' field")
	}
	if _, ok := webErrorMap["error_code"]; !ok {
		t.Error("web_error log must have 'error_code' field")
	}

	// Test for application type
	appLog := GenerateLog("application", timestamp)
	jsonData, err = json.Marshal(appLog)
	if err != nil {
		t.Fatalf("Error serializing application log: %v", err)
	}

	var appLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &appLogMap); err != nil {
		t.Fatalf("Error deserializing application log: %v", err)
	}

	checkBaseLogFields(t, appLogMap, "application", timestamp)
	if _, ok := appLogMap["level"]; !ok {
		t.Error("application log must have 'level' field")
	}
	if _, ok := appLogMap["trace_id"]; !ok {
		t.Error("application log must have 'trace_id' field")
	}

	// Test for metric type
	metricLog := GenerateLog("metric", timestamp)
	jsonData, err = json.Marshal(metricLog)
	if err != nil {
		t.Fatalf("Error serializing metric log: %v", err)
	}

	var metricLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &metricLogMap); err != nil {
		t.Fatalf("Error deserializing metric log: %v", err)
	}

	checkBaseLogFields(t, metricLogMap, "metric", timestamp)
	if _, ok := metricLogMap["metric_name"]; !ok {
		t.Error("metric log must have 'metric_name' field")
	}
	if _, ok := metricLogMap["value"]; !ok {
		t.Error("metric log must have 'value' field")
	}

	// Test for event type
	eventLog := GenerateLog("event", timestamp)
	jsonData, err = json.Marshal(eventLog)
	if err != nil {
		t.Fatalf("Error serializing event log: %v", err)
	}

	var eventLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &eventLogMap); err != nil {
		t.Fatalf("Error deserializing event log: %v", err)
	}

	checkBaseLogFields(t, eventLogMap, "event", timestamp)
	if _, ok := eventLogMap["event_type"]; !ok {
		t.Error("event log must have 'event_type' field")
	}
	if _, ok := eventLogMap["resource_id"]; !ok {
		t.Error("event log must have 'resource_id' field")
	}

	// Test for unknown type (should return BaseLog)
	unknownLog := GenerateLog("unknown", timestamp)
	jsonData, err = json.Marshal(unknownLog)
	if err != nil {
		t.Fatalf("Error serializing unknown log: %v", err)
	}

	var unknownLogMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &unknownLogMap); err != nil {
		t.Fatalf("Error deserializing unknown log: %v", err)
	}

	checkBaseLogFields(t, unknownLogMap, "unknown", timestamp)
}

// Helper function to check base log fields
func checkBaseLogFields(t *testing.T, log map[string]interface{}, expectedType string, expectedTimestamp string) {
	if log["log_type"] != expectedType {
		t.Errorf("Incorrect 'log_type' field: expected '%s', got '%s'", expectedType, log["log_type"])
	}

	if log["timestamp"] != expectedTimestamp {
		t.Errorf("Incorrect 'timestamp' field: expected '%s', got '%s'", expectedTimestamp, log["timestamp"])
	}

	if _, ok := log["host"]; !ok {
		t.Error("Log must have 'host' field")
	}

	if _, ok := log["container_name"]; !ok {
		t.Error("Log must have 'container_name' field")
	}
}

// TestLogSerialization checks the correctness of log serialization and deserialization
func TestLogSerialization(t *testing.T) {
	timestamp := "2023-01-01T12:00:00Z"
	logTypes := []string{"web_access", "web_error", "application", "metric", "event"}

	for _, logType := range logTypes {
		log := GenerateLog(logType, timestamp)

		// Serialization to JSON
		jsonData, err := json.Marshal(log)
		if err != nil {
			t.Errorf("Error serializing log of type '%s': %v", logType, err)
			continue
		}

		// Check that the serialized JSON can be unmarshaled back
		var result map[string]interface{}
		if err := json.Unmarshal(jsonData, &result); err != nil {
			t.Errorf("Error deserializing log of type '%s': %v", logType, err)
			continue
		}
	}
}
