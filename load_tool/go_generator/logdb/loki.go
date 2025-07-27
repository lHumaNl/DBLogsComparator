package logdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
		LabelFields: []string{"log_type", "host", "container_name"},
	}

	// Creating HTTP client with dynamic connection count
	maxConns := options.ConnectionCount
	if maxConns <= 0 {
		maxConns = 100 // fallback default
	}
	db.httpClient = &http.Client{
		Timeout: db.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     maxConns,
			IdleConnTimeout:     90 * time.Second,
		},
	}

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
		if value, ok := log[field].(string); ok {
			labels[field] = value
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
	// Group logs by labels
	streams := make(map[string][]map[string]interface{})

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

		// Convert log to string
		logJSON, err := json.Marshal(log)
		if err != nil {
			continue
		}

		// Add to the appropriate stream
		if _, ok := streams[labelsStr]; !ok {
			streams[labelsStr] = []map[string]interface{}{}
		}

		streams[labelsStr] = append(streams[labelsStr], map[string]interface{}{
			"ts":   timestamp,
			"line": string(logJSON),
		})
	}

	// Format into Loki Push API format
	lokiRequest := map[string]interface{}{
		"streams": []map[string]interface{}{},
	}

	for labelsStr, values := range streams {
		stream := map[string]interface{}{
			"stream": map[string]interface{}{},
			"values": [][]interface{}{},
		}

		// Parse labels from string back to object
		labelsObj := make(map[string]string)
		labelsStr = strings.TrimPrefix(labelsStr, "{")
		labelsStr = strings.TrimSuffix(labelsStr, "}")
		labelPairs := strings.Split(labelsStr, ",")

		for _, pair := range labelPairs {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				key := parts[0]
				value := strings.Trim(parts[1], "\"")
				labelsObj[key] = value
			}
		}

		stream["stream"] = labelsObj

		// Add log values
		for _, v := range values {
			stream["values"] = append(stream["values"].([][]interface{}), []interface{}{
				fmt.Sprintf("%d", v["ts"]),
				v["line"],
			})
		}

		lokiRequest["streams"] = append(lokiRequest["streams"].([]map[string]interface{}), stream)
	}

	payload, _ := json.Marshal(lokiRequest)
	return string(payload), "application/json"
}

// SendLogs sends a batch of logs to Loki
func (db *LokiDB) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayload(logs)

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

		if err != nil {
			lastErr = err
			db.IncrementMetric("failed_requests", 1)
			continue
		}

		defer resp.Body.Close()

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
