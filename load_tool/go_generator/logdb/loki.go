package logdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
)

// LokiDB - implementation of LogDB for Loki
type LokiDB struct {
	*BaseLogDB
	Labels      map[string]string // Labels for logs
	LabelFields []string          // Fields from logs to be used as labels
	httpClient  *http.Client
}

// NewLokiDB creates a new instance of LokiDB
func NewLokiDB(baseURL string, options Options) (*LokiDB, error) {
	base := NewBaseLogDB(baseURL, options)

	db := &LokiDB{
		BaseLogDB:   base,
		Labels:      make(map[string]string),
		LabelFields: logdata.LokiCommonLabels,
	}

	// Use shared connection pool manager for generator
	db.httpClient = GetGeneratorClient("loki", baseURL, options)

	// Check and correct URL for Loki API
	// In Loki 3.5.1+ API path: /loki/api/v1/push
	if !strings.HasSuffix(db.URL, "/loki/api/v1/push") && !strings.HasSuffix(db.URL, "/api/v1/push") {
		if strings.HasSuffix(db.URL, "/") {
			db.URL += "loki/api/v1/push"
		} else {
			db.URL += "/loki/api/v1/push"
		}
	}

	// For debugging, output the final URL
	if db.Verbose {
		fmt.Printf("Loki API URL: %s\n", db.URL)
	}

	return db, nil
}

// Initialize initializes the connection to Loki
func (db *LokiDB) Initialize() error {
	// No additional initialization required for Loki
	return nil
}

// Close closes the connection to Loki
func (db *LokiDB) Close() error {
	// No explicit closing required for HTTP client
	return nil
}

// Name returns the database name
func (db *LokiDB) Name() string {
	return "loki"
}

// extractLabels extracts labels from the log
func (db *LokiDB) extractLabels(log LogEntry) map[string]string {
	labels := make(map[string]string)

	// Copy base labels
	for k, v := range db.Labels {
		labels[k] = v
	}

	// Add labels from log fields
	for _, field := range db.LabelFields {
		if value, ok := log[field]; ok && value != nil {
			// Convert different types to string for labels
			switch v := value.(type) {
			case string:
				if v != "" {
					labels[field] = v
				}
			case int:
				labels[field] = fmt.Sprintf("%d", v)
			case float64:
				// Convert float to int if it's a whole number (like status codes)
				if v == float64(int(v)) {
					labels[field] = fmt.Sprintf("%d", int(v))
				} else {
					labels[field] = fmt.Sprintf("%.2f", v)
				}
			case bool:
				labels[field] = fmt.Sprintf("%t", v)
			default:
				// For other types, use fmt.Sprintf
				labels[field] = fmt.Sprintf("%v", v)
			}
		}
	}

	return labels
}

// formatLabelsString formats labels into a string for Loki
func (db *LokiDB) formatLabelsString(labels map[string]string) string {
	var pairs []string
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

// FormatPayload formats log entries into Loki Push API format
func (db *LokiDB) FormatPayload(logs []LogEntry) (string, string) {
	var buf bytes.Buffer
	return db.formatPayloadInternal(logs, &buf)
}

// FormatPayloadWithBuffer formats logs using provided buffer for efficiency
func (db *LokiDB) FormatPayloadWithBuffer(logs []LogEntry, buf *bytes.Buffer) (string, string) {
	buf.Reset() // Clear the buffer
	return db.formatPayloadInternal(logs, buf)
}

// formatPayloadInternal contains the shared formatting logic with optimized JSON encoding
func (db *LokiDB) formatPayloadInternal(logs []LogEntry, buf *bytes.Buffer) (string, string) {
	// Group logs by labels - use map[string][]interface{} for direct storage
	streamsByLabels := make(map[string][]interface{})
	labelsByString := make(map[string]map[string]string) // Cache parsed labels

	// Create a temporary buffer for individual log JSON marshaling
	var tempBuf bytes.Buffer
	tempEncoder := json.NewEncoder(&tempBuf)
	tempEncoder.SetEscapeHTML(false)

	for _, log := range logs {
		// Make sure timestamp is in the correct format (Loki needs Unix timestamp in nanoseconds)
		var timestamp int64
		if ts, ok := log["timestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				timestamp = t.UnixNano()
			} else {
				timestamp = time.Now().UnixNano()
			}
		} else {
			timestamp = time.Now().UnixNano()
		}

		// Extract labels from log
		labels := db.extractLabels(log)
		labelsStr := db.formatLabelsString(labels)

		// Store parsed labels to avoid re-parsing later
		if _, exists := labelsByString[labelsStr]; !exists {
			labelsByString[labelsStr] = labels
		}

		// Use temporary buffer for single log JSON encoding (avoids allocation)
		tempBuf.Reset()
		if err := tempEncoder.Encode(log); err != nil {
			continue
		}

		// Get JSON string and remove trailing newline added by encoder
		logJSON := strings.TrimRight(tempBuf.String(), "\n")

		// Add to the appropriate stream - store as []interface{} directly for Loki format
		if _, ok := streamsByLabels[labelsStr]; !ok {
			streamsByLabels[labelsStr] = []interface{}{}
		}

		streamsByLabels[labelsStr] = append(streamsByLabels[labelsStr], []interface{}{
			fmt.Sprintf("%d", timestamp),
			logJSON,
		})
	}

	// Build Loki request structure directly without intermediate maps
	lokiStreams := make([]interface{}, 0, len(streamsByLabels))

	for labelsStr, values := range streamsByLabels {
		stream := map[string]interface{}{
			"stream": labelsByString[labelsStr], // Use cached parsed labels
			"values": values,                    // Direct assignment, no copying
		}
		lokiStreams = append(lokiStreams, stream)
	}

	lokiRequest := map[string]interface{}{
		"streams": lokiStreams,
	}

	// Single JSON encoding using streaming encoder
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false) // Disable HTML escaping for performance

	if err := encoder.Encode(lokiRequest); err != nil {
		// Simplified fallback - reuse the same encoder with a fresh buffer
		buf.Reset()
		encoder = json.NewEncoder(buf)
		encoder.SetEscapeHTML(false)

		// If this also fails, return error indication
		if err := encoder.Encode(lokiRequest); err != nil {
			return `{"streams":[]}`, "application/json"
		}
	}

	return buf.String(), "application/json"
}

// SendLogs sends a batch of logs to Loki
func (db *LokiDB) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayload(logs)

	// Debug mode: output request body
	if db.Debug {
		fmt.Printf("DEBUG [Loki Request]: %s\n", payload)
	}

	var lastErr error

	// Retry attempts for sending with error handling
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			// Exponential backoff delay before retry
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("Loki: Retry attempt %d/%d after error: %v (delay: %v)\n",
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		// Create request
		req, err := http.NewRequest("POST", db.URL, bytes.NewReader([]byte(payload)))
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

		// Read response body for error information
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		db.IncrementMetric("failed_requests", 1)

		// If it's a server error (5xx), retry
		// For client errors (4xx), no point in retrying
		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}

// SendLogsWithBuffer sends logs using buffer pool for better performance
func (db *LokiDB) SendLogsWithBuffer(logs []LogEntry, buf *bytes.Buffer) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayloadWithBuffer(logs, buf)

	// Debug mode: output request body
	if db.Debug {
		fmt.Printf("DEBUG [Loki Request]: %s\n", payload)
	}

	var lastErr error

	// Retry with exponential backoff on server errors
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("Loki: Retrying %d/%d after error: %v (backoff: %v)\n",
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		// Create request (use URL as-is, like the original implementation)
		req, err := http.NewRequest("POST", db.URL, strings.NewReader(payload))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		req.Header.Set("Content-Type", contentType)

		// Execute request using the shared HTTP client (NOT creating new client every time)
		resp, err := db.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}

		// CRITICAL: Close response body to prevent memory leaks
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Success - update metrics just like SendLogs does
			db.IncrementMetric("successful_requests", 1)
			db.IncrementMetric("total_logs", float64(len(logs)))
			return nil
		}

		// Read response body for error information
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)

		// Update failed requests metric
		db.IncrementMetric("failed_requests", 1)

		// For 4xx errors, don't retry
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}
