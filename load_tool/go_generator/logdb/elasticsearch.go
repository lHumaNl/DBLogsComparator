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

// ElasticsearchDB - LogDB implementation for Elasticsearch
type ElasticsearchDB struct {
	*BaseLogDB
	IndexPrefix  string // Index name prefix
	IndexPattern string // Index name pattern (e.g., "logs-YYYY.MM.DD")
	httpClient   *http.Client
}

// NewElasticsearchDB creates a new ElasticsearchDB instance
func NewElasticsearchDB(baseURL string, options Options) (*ElasticsearchDB, error) {
	base := NewBaseLogDB(baseURL, options)

	db := &ElasticsearchDB{
		BaseLogDB:    base,
		IndexPrefix:  "logs",            // Default value
		IndexPattern: "logs-2006.01.02", // Default value - uses Go time format
	}

	// Creating HTTP client
	db.httpClient = &http.Client{
		Timeout: db.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// If URL doesn't end with /_bulk, add it
	if !strings.HasSuffix(db.URL, "/_bulk") {
		if strings.HasSuffix(db.URL, "/") {
			db.URL = db.URL + "_bulk"
		} else {
			db.URL = db.URL + "/_bulk"
		}
	}

	// Create index with proper mapping if it doesn't exist yet
	err := db.createIndexWithMapping()
	if err != nil && db.Verbose {
		fmt.Printf("Warning: failed to create index with mapping: %v\n", err)
	}

	return db, nil
}

// createIndexWithMapping creates index with needed field mapping
func (db *ElasticsearchDB) createIndexWithMapping() error {
	currentIndex := db.getCurrentIndex()

	// Check if index exists
	checkURL := strings.TrimSuffix(db.URL, "/_bulk") + "/" + currentIndex
	req, err := http.NewRequest("HEAD", checkURL, nil)
	if err != nil {
		return err
	}

	resp, err := db.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If index already exists, do nothing
	if resp.StatusCode == 200 {
		return nil
	}

	// Create index with needed mapping
	createURL := strings.TrimSuffix(db.URL, "/_bulk") + "/" + currentIndex

	// Define mapping for fields
	mapping := `{
		"mappings": {
			"properties": {
				"@timestamp": { "type": "date" },
				"timestamp": { "type": "date" },
				"message": { "type": "text" },
				"level": { "type": "keyword" },
				"log_type": { "type": "keyword" },
				"host": { "type": "keyword" },
				"service": { "type": "keyword" },
				"container_name": { "type": "keyword" },
				"metric_name": { "type": "keyword" },
				"event_type": { "type": "keyword" },
				"status": { "type": "keyword" },
				"request_method": { "type": "keyword" },
				"error_code": { "type": "keyword" },
				
				"environment": { "type": "keyword" },
				"datacenter": { "type": "keyword" },
				"version": { "type": "keyword" },
				"git_commit": { "type": "keyword" },
				
				"request_id": { "type": "keyword" },
				"request_path": { 
					"type": "text",
					"fields": {
						"keyword": { "type": "keyword", "ignore_above": 256 }
					}
				},
				"client_ip": { "type": "ip" },
				"duration": { "type": "float" },
				"retry_count": { "type": "integer" },
				"tags": { "type": "keyword" },
				"context": { "type": "object" },
				
				"exception": { "type": "keyword" },
				"stacktrace": { "type": "text" },
				"user_id": { "type": "keyword" },
				"session_id": { "type": "keyword" },
				"dependencies": { "type": "keyword" },
				"memory": { "type": "float" },
				"cpu": { "type": "float" }
			}
		}
	}`

	createReq, err := http.NewRequest("PUT", createURL, strings.NewReader(mapping))
	if err != nil {
		return err
	}

	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := db.httpClient.Do(createReq)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()

	if createResp.StatusCode >= 400 {
		body, _ := io.ReadAll(createResp.Body)
		return fmt.Errorf("error creating index: code %d, response: %s", createResp.StatusCode, body)
	}

	return nil
}

// Initialize initializes connection to Elasticsearch
func (db *ElasticsearchDB) Initialize() error {
	// Check Elasticsearch availability
	req, err := http.NewRequest("GET", strings.TrimSuffix(db.URL, "/_bulk"), nil)
	if err != nil {
		return err
	}

	resp, err := db.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Elasticsearch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Elasticsearch returned error status: %d, body: %s", resp.StatusCode, body)
	}

	return nil
}

// Close closes connection to Elasticsearch
func (db *ElasticsearchDB) Close() error {
	// No need to close HTTP client explicitly
	// Для HTTP-клиента не требуется явное закрытие
	return nil
}

// Name returns database name
func (db *ElasticsearchDB) Name() string {
	return "Elasticsearch"
}

// getCurrentIndex returns current index name based on pattern
func (db *ElasticsearchDB) getCurrentIndex() string {
	// If index already contains time, use it as is
	if strings.Contains(db.IndexPattern, "2006") || strings.Contains(db.IndexPattern, "01") ||
		strings.Contains(db.IndexPattern, "02") {
		return time.Now().UTC().Format(db.IndexPattern)
	}

	// Otherwise, use prefix only
	return db.IndexPrefix
}

// FormatPayload formats log entries into Bulk API format for Elasticsearch
func (db *ElasticsearchDB) FormatPayload(logs []LogEntry) (string, string) {
	var buf bytes.Buffer
	currentIndex := db.getCurrentIndex()

	// For Elasticsearch Bulk API, each request consists of two lines:
	// 1. Operation metadata (create, index, update, delete)
	// 2. Document data
	for _, log := range logs {
		// Ensure timestamp is in correct format
		var timestampStr string
		if ts, ok := log["timestamp"]; ok {
			if tsStr, ok := ts.(string); ok {
				timestampStr = tsStr
				if tsStr == "0" || tsStr == "" {
					timestampStr = time.Now().UTC().Format(time.RFC3339Nano)
				}
			} else {
				timestampStr = time.Now().UTC().Format(time.RFC3339Nano)
			}
		} else {
			timestampStr = time.Now().UTC().Format(time.RFC3339Nano)
		}

		// Convert timestamp to Kibana expected format (strict_date_optional_time)
		if t, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
			// Use "yyyy-MM-dd'T'HH:mm:ss.SSSZ" format for Elasticsearch
			timestampStr = t.Format("2006-01-02T15:04:05.000Z07:00")
		}

		// Set timestamp and @timestamp fields
		log["timestamp"] = timestampStr
		log["@timestamp"] = timestampStr

		// Add level field for all log types if missing
		if _, ok := log["level"]; !ok {
			// Choose level based on log type
			logType, _ := log["log_type"].(string)
			switch logType {
			case "web_access":
				// For web_access, use info for normal requests and warn/error for errors
				if status, ok := log["status"].(float64); ok {
					if status >= 500 {
						log["level"] = "error"
					} else if status >= 400 {
						log["level"] = "warn"
					} else {
						log["level"] = "info"
					}
				} else {
					log["level"] = "info"
				}
			case "metric":
				// For metrics, use info
				log["level"] = "info"
			case "event":
				// For events, use info or warn based on event type
				if eventType, ok := log["event_type"].(string); ok && (strings.Contains(eventType, "error") || strings.Contains(eventType, "fail")) {
					log["level"] = "warn"
				} else {
					log["level"] = "info"
				}
			default:
				// Default to info
				log["level"] = "info"
			}
		}

		// Ensure all string fields are represented as keyword for filtering
		// Convert level to string if not already
		if level, ok := log["level"]; ok {
			if _, ok := level.(string); !ok {
				log["level"] = fmt.Sprintf("%v", level)
			}
		}

		// Metadata - operation index in specified index
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": currentIndex,
			},
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			continue
		}

		buf.Write(metaJSON)
		buf.WriteString("\n")

		// Document data
		docJSON, err := json.Marshal(log)
		if err != nil {
			continue
		}

		buf.Write(docJSON)
		buf.WriteString("\n")
	}

	// For Elasticsearch Bulk API, use content-type application/x-ndjson
	return buf.String(), "application/x-ndjson"
}

// SendLogs sends a batch of logs to Elasticsearch
func (db *ElasticsearchDB) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	payload, contentType := db.FormatPayload(logs)

	var lastErr error

	// Retry sending with exponential backoff on errors
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			// Exponential backoff before retrying
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("Elasticsearch: Retrying %d/%d after error: %v (backoff: %v)\n",
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
		db.MetricsData["request_duration"] = requestEnd.Sub(requestStart).Seconds()

		if err != nil {
			lastErr = err
			db.MetricsData["failed_requests"]++
			continue
		}

		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Successful send
			db.MetricsData["successful_requests"]++
			db.MetricsData["total_logs"] += float64(len(logs))
			return nil
		}

		// Read response body for error information
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		db.MetricsData["failed_requests"]++

		// If server error (5xx), retry
		// For client errors (4xx), no need to retry
		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return lastErr
}
