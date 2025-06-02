package logdb

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestNewElasticsearchDB checks the creation of an ElasticsearchDB instance
func TestNewElasticsearchDB(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		options Options
		wantURL string
		wantErr bool
	}{
		{
			name:    "Simple URL",
			baseURL: "http://localhost:9200",
			options: Options{
				BatchSize:  100,
				Timeout:    time.Second * 10,
				RetryCount: 3,
				RetryDelay: time.Second,
			},
			wantURL: "http://localhost:9200/_bulk",
			wantErr: false,
		},
		{
			name:    "URL with trailing slash",
			baseURL: "http://localhost:9200/",
			options: Options{},
			wantURL: "http://localhost:9200/_bulk",
			wantErr: false,
		},
		{
			name:    "URL with existing path",
			baseURL: "http://localhost:9200/logs",
			options: Options{},
			wantURL: "http://localhost:9200/logs/_bulk",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := NewElasticsearchDB(tt.baseURL, tt.options)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewElasticsearchDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if db.URL != tt.wantURL {
					t.Errorf("URL = %v, want %v", db.URL, tt.wantURL)
				}

				// Check that the HTTP client is created
				if db.httpClient == nil {
					t.Errorf("httpClient is not initialized")
				}

				// Check fields from Options
				if db.BatchSize != tt.options.BatchSize {
					t.Errorf("BatchSize = %v, want %v", db.BatchSize, tt.options.BatchSize)
				}
			}
		})
	}
}

// TestElasticsearchDBFormatPayload checks the formatting of data for sending
func TestElasticsearchDBFormatPayload(t *testing.T) {
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{})

	logs := []LogEntry{
		{
			"timestamp": "2023-01-01T12:00:00Z",
			"message":   "Test message 1",
			"level":     "info",
			"service":   "test-service",
		},
		{
			"timestamp": "2023-01-01T12:01:00Z",
			"message":   "Test message 2",
			"level":     "error",
			"host":      "test-host",
		},
	}

	payload, contentType := db.FormatPayload(logs)

	// Check Content-Type
	if contentType != "application/x-ndjson" {
		t.Errorf("contentType = %v, want %v", contentType, "application/x-ndjson")
	}

	// Check that for each log there are two objects (index and the log itself)
	// Each log in Elasticsearch format should have 2 lines: index and data
	lines := strings.Split(strings.TrimSpace(payload), "\n")
	if len(lines) != len(logs)*2 {
		t.Errorf("payload contains %v lines, expected %v", len(lines), len(logs)*2)
	}

	// Check that each second line contains data from the logs
	for i, log := range logs {
		dataLineIndex := i*2 + 1 // Second line for each log

		for key := range log {
			if !strings.Contains(lines[dataLineIndex], key) {
				t.Errorf("Line %v does not contain key %v", lines[dataLineIndex], key)
			}
		}

		// Check that the first line contains index
		indexLine := lines[i*2]
		if !strings.Contains(indexLine, "index") {
			t.Errorf("Line %v does not contain index directive", indexLine)
		}
	}
}

// TestElasticsearchDBSendLogs checks log sending
func TestElasticsearchDBSendLogs(t *testing.T) {
	// Create an ElasticsearchDB instance with a mock client
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{
		RetryCount: 1,
		RetryDelay: time.Millisecond * 10,
	})

	// Test 1: Successful sending
	t.Run("Successful Send", func(t *testing.T) {
		// Replace HTTP client with a mock
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					// Check request URL
					if req.URL.String() != db.URL {
						t.Errorf("Request URL = %v, expected %v", req.URL.String(), db.URL)
					}

					// Check request method
					if req.Method != "POST" {
						t.Errorf("Request method = %v, expected %v", req.Method, "POST")
					}

					// Check Content-Type
					if req.Header.Get("Content-Type") != "application/x-ndjson" {
						t.Errorf("Content-Type = %v, expected %v", req.Header.Get("Content-Type"), "application/x-ndjson")
					}

					// Return successful response
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(bytes.NewBufferString(`{"errors":false,"took":5}`)),
					}, nil
				},
			},
		}
		db.httpClient = mockClient

		// Prepare test data
		logs := []LogEntry{
			{
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err != nil {
			t.Errorf("SendLogs() error = %v, expected nil", err)
		}
	})

	// Test 2: Sending error
	t.Run("Failed Send", func(t *testing.T) {
		// Replace HTTP client with an error mock
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("connection error")
				},
			},
		}
		db.httpClient = mockClient

		// Prepare test data
		logs := []LogEntry{
			{
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err == nil {
			t.Errorf("SendLogs() error = nil, expected an error")
		}
	})

	// Test 3: Error in Elasticsearch response
	t.Run("Elasticsearch Error", func(t *testing.T) {
		// Replace HTTP client with an Elasticsearch error mock
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: 400, // Change status to 400 to ensure we get an error
						Body:       io.NopCloser(bytes.NewBufferString(`{"errors":true,"items":[{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"mapping error"}}}]}`)),
					}, nil
				},
			},
		}
		db.httpClient = mockClient

		// Prepare test data
		logs := []LogEntry{
			{
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err == nil {
			t.Errorf("SendLogs() error = nil, expected an error")
		}
	})
}

// TestElasticsearchDBName checks the Name method
func TestElasticsearchDBName(t *testing.T) {
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{})

	name := db.Name()
	expected := "Elasticsearch"

	if name != expected {
		t.Errorf("Name() = %v, want %v", name, expected)
	}
}

// TestElasticsearchDBInitializeAndClose checks the Initialize and Close methods
func TestElasticsearchDBInitializeAndClose(t *testing.T) {
	// Create a test client that doesn't make real requests
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{})

	// Replace the client to avoid real requests
	db.httpClient = &http.Client{
		Transport: &MockHTTPTransport{
			RoundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString(`{"version":{"number":"7.10.0"}}`)),
				}, nil
			},
		},
	}

	// Initialize should simply return nil
	err := db.Initialize()
	if err != nil {
		t.Errorf("Initialize() error = %v, expected nil", err)
	}

	// Close should simply return nil
	err = db.Close()
	if err != nil {
		t.Errorf("Close() error = %v, expected nil", err)
	}
}
