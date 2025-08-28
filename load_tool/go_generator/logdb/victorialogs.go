package logdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
)

// VictoriaLogsDB - implementation of LogDB for VictoriaLogs
type VictoriaLogsDB struct {
	*BaseLogDB
	TimeField   string // Field with timestamp (for _time_field)
	ExtraParams string // Additional query parameters
	httpClient  *http.Client
}

// NewVictoriaLogsDB creates a new instance of VictoriaLogsDB
func NewVictoriaLogsDB(baseURL string, options Options) (*VictoriaLogsDB, error) {
	// Check and add the path /insert/jsonline to the URL if it's not already there
	if !strings.Contains(baseURL, "/insert/jsonline") {
		baseURL = strings.TrimRight(baseURL, "/") + "/insert/jsonline"
	}

	// Add necessary parameters for correct log indexing
	if !strings.Contains(baseURL, "?") {
		baseURL += "?"
	} else {
		baseURL += "&"
	}
	// Specify all necessary parameters for correct log indexing
	baseURL += "_time_field=timestamp&_msg_field=message&_stream_fields=log_type,service,host"

	base := NewBaseLogDB(baseURL, options)

	db := &VictoriaLogsDB{
		BaseLogDB: base,
		TimeField: "timestamp", // Default value
	}

	// Use shared connection pool manager for generator
	db.httpClient = GetGeneratorClient("victorialogs", baseURL, options)

	// Add _time_field parameter if it's not already added
	if db.TimeField != "" && !strings.Contains(db.URL, "_time_field=") {
		if strings.Contains(db.URL, "?") {
			db.URL += fmt.Sprintf("&_time_field=%s", db.TimeField)
		} else {
			db.URL += fmt.Sprintf("?_time_field=%s", db.TimeField)
		}
	}

	return db, nil
}

// Initialize initializes the connection to VictoriaLogs
func (db *VictoriaLogsDB) Initialize() error {
	// No additional initialization is required for VictoriaLogs
	return nil
}

// Close closes the connection to VictoriaLogs
func (db *VictoriaLogsDB) Close() error {
	// No explicit closing is required for the HTTP client
	return nil
}

// Name returns the name of the database
func (db *VictoriaLogsDB) Name() string {
	return "victorialogs"
}

// getStreamStrategy returns the stream generation strategy based on environment variable
func (db *VictoriaLogsDB) getStreamStrategy() string {
	strategy := os.Getenv("VICTORIALOGS_STREAM_STRATEGY")
	switch strategy {
	case "full":
		return "full_commonlabels"
	case "selective":
		return "selective_commonlabels"
	case "max":
		return "max_cardinality"
	default:
		return "max_cardinality" // Changed default to use all 18 fields for maximum cardinality
	}
}

// escapeStreamValue escapes special characters for VictoriaLogs stream syntax
func escapeStreamValue(value string) string {
	// Escape special characters for VictoriaLogs stream syntax
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\t", "\\t")
	return value
}

// generateStreamLabels generates optimized stream labels using CommonLabels for fair comparison
func (db *VictoriaLogsDB) generateStreamLabels(logEntry LogEntry) string {
	strategy := db.getStreamStrategy()

	var streamLabels []string

	switch strategy {
	case "full_commonlabels":
		// Use all CommonLabels for stream generation (6 fields)
		for _, label := range logdata.CommonLabels {
			if value, exists := logEntry[label]; exists {
				escapedValue := escapeStreamValue(fmt.Sprintf("%v", value))
				streamLabels = append(streamLabels, fmt.Sprintf(`%s="%s"`, label, escapedValue))
			}
		}

	case "selective_commonlabels":
		// Use selective high-impact CommonLabels for balanced cardinality (4 fields)
		essentialLabels := []string{"log_type", "environment", "service", "datacenter"}
		for _, label := range essentialLabels {
			if value, exists := logEntry[label]; exists {
				escapedValue := escapeStreamValue(fmt.Sprintf("%v", value))
				streamLabels = append(streamLabels, fmt.Sprintf(`%s="%s"`, label, escapedValue))
			}
		}

	case "max_cardinality":
		// Use all 18 searchable fields for maximum cardinality and fair comparison
		allFields := logdata.GetAllSearchableFields()
		for _, field := range allFields {
			if value, exists := logEntry[field]; exists {
				escapedValue := escapeStreamValue(fmt.Sprintf("%v", value))
				streamLabels = append(streamLabels, fmt.Sprintf(`%s="%s"`, field, escapedValue))
			}
		}

	default:
		// Fallback to minimal legacy approach
		if logType, exists := logEntry["log_type"]; exists {
			escapedValue := escapeStreamValue(fmt.Sprintf("%v", logType))
			streamLabels = append(streamLabels, fmt.Sprintf(`log_type="%s"`, escapedValue))
		}
		if env, exists := logEntry["environment"]; exists {
			escapedValue := escapeStreamValue(fmt.Sprintf("%v", env))
			streamLabels = append(streamLabels, fmt.Sprintf(`environment="%s"`, escapedValue))
		}
	}

	// Return formatted stream identifier
	if len(streamLabels) > 0 {
		return "{" + strings.Join(streamLabels, ",") + "}"
	}

	// Fallback stream if no labels available
	return `{log_type="unknown"}`
}

// isStreamLabel checks if a field should be used as a stream label
func (db *VictoriaLogsDB) isStreamLabel(field string) bool {
	strategy := db.getStreamStrategy()

	switch strategy {
	case "full_commonlabels":
		// Check against all CommonLabels (6 fields)
		for _, label := range logdata.CommonLabels {
			if field == label {
				return true
			}
		}

	case "selective_commonlabels":
		// Check against selective labels (4 fields)
		essentialLabels := []string{"log_type", "environment", "service", "datacenter"}
		for _, label := range essentialLabels {
			if field == label {
				return true
			}
		}

	case "max_cardinality":
		// Check against all 18 searchable fields for maximum cardinality
		allFields := logdata.GetAllSearchableFields()
		for _, searchableField := range allFields {
			if field == searchableField {
				return true
			}
		}

	default:
		// Legacy minimal labels
		return field == "log_type" || field == "environment"
	}

	return false
}

// FormatPayload formats log entries in NDJSON format for VictoriaLogs
func (db *VictoriaLogsDB) FormatPayload(logs []LogEntry) (string, string) {
	var buf bytes.Buffer
	return db.formatPayloadInternal(logs, &buf)
}

// FormatPayloadWithBuffer formats logs using provided buffer for efficiency
func (db *VictoriaLogsDB) FormatPayloadWithBuffer(logs []LogEntry, buf *bytes.Buffer) (string, string) {
	buf.Reset() // Clear the buffer
	return db.formatPayloadInternal(logs, buf)
}

// formatPayloadInternal contains the shared formatting logic with optimized JSON encoding
func (db *VictoriaLogsDB) formatPayloadInternal(logs []LogEntry, buf *bytes.Buffer) (string, string) {

	// Create optimized JSON encoder for streaming NDJSON
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false) // Disable HTML escaping for performance

	// For VictoriaLogs, each log should be on a separate line (NDJSON)
	for _, originalLog := range logs {
		// Create a copy to avoid modifying the original (thread safety improvement)
		log := make(LogEntry, len(originalLog))
		for k, v := range originalLog {
			log[k] = v
		}

		// Ensure timestamp is in the correct ISO8601 format
		if _, ok := log["timestamp"]; !ok {
			log["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		} else if ts, ok := log["timestamp"].(string); ok {
			// Convert timestamp to the correct format if necessary
			if ts == "0" || ts == "" {
				log["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
			}
		}

		// Ensure message exists
		if _, ok := log["message"]; !ok {
			log["message"] = fmt.Sprintf("Log message for %s", log["log_type"])
		}

		// Format log with optimized stream configuration
		vlDoc := db.formatLogWithOptimizedStream(log)

		// Use streaming encoder (optimization: no intermediate []byte allocation)
		if err := encoder.Encode(vlDoc); err != nil {
			continue
		}

		// Remove trailing newline from last entry if needed for specific format requirements
		// Note: json.Encoder automatically adds newlines, which is what we want for NDJSON
	}

	// For VictoriaLogs, use content-type application/stream+json
	return buf.String(), "application/stream+json"
}

// formatLogWithOptimizedStream formats a single log entry with optimized stream configuration
func (db *VictoriaLogsDB) formatLogWithOptimizedStream(logEntry LogEntry) map[string]interface{} {
	// Generate optimized stream labels using CommonLabels
	streamLabels := db.generateStreamLabels(logEntry)

	// Create VictoriaLogs document format
	vlDoc := map[string]interface{}{
		"_stream": streamLabels,
		"_time":   logEntry["timestamp"],
		"_msg":    logEntry["message"],
	}

	// Add non-stream fields as regular fields
	for key, value := range logEntry {
		if !db.isStreamLabel(key) && key != "timestamp" && key != "message" {
			vlDoc[key] = value
		}
	}

	return vlDoc
}

// SendLogs sends a batch of logs to VictoriaLogs
func (db *VictoriaLogsDB) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayload(logs)

	// Debug mode: output request body
	if db.Debug {
		fmt.Printf("DEBUG [VictoriaLogs Request]: %s\n", payload)
	}

	var lastErr error

	// Attempt to send with retries on errors
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			// Exponential backoff before retrying
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("VictoriaLogs: Retrying %d/%d after error: %v (backoff: %v)\n",
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		// Create request
		req, err := http.NewRequest("POST", db.URL, strings.NewReader(payload))
		if err != nil {
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", "application/json")

		// Send request
		requestStart := time.Now()
		resp, err := db.httpClient.Do(req)
		requestEnd := time.Now()

		// Update metrics
		db.UpdateMetric("request_duration", requestEnd.Sub(requestStart).Seconds())

		// CRITICAL: Close response body even on error to prevent memory leaks
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}

		if err != nil {
			lastErr = err
			db.IncrementMetric("failed_requests", 1)
			continue
		}

		// Check response status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Successful send
			db.IncrementMetric("successful_requests", 1)
			db.IncrementMetric("total_logs", float64(len(logs)))
			return nil
		}

		// Read response body to get error information
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		db.IncrementMetric("failed_requests", 1)

		// If it's a server error (5xx), retry
		// For client errors (4xx), there's no point in retrying
		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}

// SendLogsWithBuffer sends logs using buffer pool for better performance
func (db *VictoriaLogsDB) SendLogsWithBuffer(logs []LogEntry, buf *bytes.Buffer) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayloadWithBuffer(logs, buf)

	// Debug mode: output request body
	if db.Debug {
		fmt.Printf("DEBUG [VictoriaLogs Request]: %s\n", payload)
	}

	var lastErr error

	// Retry mechanism with exponential backoff
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("VictoriaLogs: Retrying %d/%d after error: %v (backoff: %v)\n",
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		// Create the request (URL already includes /insert/jsonline path)
		req, err := http.NewRequest("POST", db.URL, strings.NewReader(payload))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		req.Header.Set("Content-Type", contentType)

		// Send the request using the shared HTTP client (NOT creating new client every time)
		resp, err := db.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}

		// CRITICAL: Close response body to prevent memory leaks
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}

		// Check response status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Successful send
			return nil
		}

		// Read response body to get error information
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)

		// If it's a server error (5xx), retry
		// For client errors (4xx), there's no point in retrying
		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}
