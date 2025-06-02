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

// TestNewLokiDB checks the creation of a LokiDB instance
func TestNewLokiDB(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		options Options
		wantURL string
		wantErr bool
	}{
		{
			name:    "Simple URL",
			baseURL: "http://localhost:3100",
			options: Options{
				BatchSize:  100,
				Timeout:    time.Second * 10,
				RetryCount: 3,
				RetryDelay: time.Second,
			},
			wantURL: "http://localhost:3100/loki/api/v1/push",
			wantErr: false,
		},
		{
			name:    "URL with trailing slash",
			baseURL: "http://localhost:3100/",
			options: Options{},
			wantURL: "http://localhost:3100/loki/api/v1/push",
			wantErr: false,
		},
		{
			name:    "URL with existing path",
			baseURL: "http://localhost:3100/loki/api/v1/push",
			options: Options{},
			wantURL: "http://localhost:3100/loki/api/v1/push",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := NewLokiDB(tt.baseURL, tt.options)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewLokiDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if db.URL != tt.wantURL {
					t.Errorf("URL = %v, want %v", db.URL, tt.wantURL)
				}

				// Check that HTTP client is initialized
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

// TestLokiDBFormatPayload checks the formatting of data for sending
func TestLokiDBFormatPayload(t *testing.T) {
	db, _ := NewLokiDB("http://localhost:3100", Options{})

	timestamp := time.Now()
	timestampStr := timestamp.Format(time.RFC3339)

	logs := []LogEntry{
		{
			"timestamp": timestampStr,
			"message":   "Test message 1",
			"level":     "info",
			"service":   "test-service",
			"log_type":  "web_access",
		},
		{
			"timestamp": timestampStr,
			"message":   "Test message 2",
			"level":     "error",
			"host":      "test-host",
			"log_type":  "web_error",
		},
	}

	payload, contentType := db.FormatPayload(logs)

	// Check Content-Type
	if contentType != "application/json" {
		t.Errorf("contentType = %v, want %v", contentType, "application/json")
	}

	// Check that payload contains Loki structure
	expectedParts := []string{
		"streams",
		"values",
		"log_type",
		"web_access",
		"web_error",
		"Test message 1",
		"Test message 2",
	}

	for _, part := range expectedParts {
		if !strings.Contains(payload, part) {
			t.Errorf("payload does not contain expected part: %v", part)
		}
	}
}

// TestLokiDBSendLogs checks sending logs
func TestLokiDBSendLogs(t *testing.T) {
	// Create LokiDB instance with mock client
	db, _ := NewLokiDB("http://localhost:3100", Options{
		RetryCount: 1,
		RetryDelay: time.Millisecond * 10,
	})

	// Test 1: Successful sending
	t.Run("Successful Send", func(t *testing.T) {
		// Replace HTTP client with mock
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
					if req.Header.Get("Content-Type") != "application/json" {
						t.Errorf("Content-Type = %v, expected %v", req.Header.Get("Content-Type"), "application/json")
					}

					// Return successful response
					return &http.Response{
						StatusCode: 204,
						Body:       io.NopCloser(bytes.NewBufferString("")),
					}, nil
				},
			},
		}
		db.httpClient = mockClient

		// Prepare test data
		logs := []LogEntry{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err != nil {
			t.Errorf("SendLogs() error = %v, expected nil", err)
		}
	})

	// Test 2: Send error
	t.Run("Failed Send", func(t *testing.T) {
		// Replace HTTP client with mock with error
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
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err == nil {
			t.Errorf("SendLogs() error = nil, expected error")
		}
	})

	// Test 3: Failed status response
	t.Run("Failed Status", func(t *testing.T) {
		// Replace HTTP client with mock with failed status
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: 400,
						Body:       io.NopCloser(bytes.NewBufferString(`{"error":"invalid payload"}`)),
					}, nil
				},
			},
		}
		db.httpClient = mockClient

		// Prepare test data
		logs := []LogEntry{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}

		// Send logs
		err := db.SendLogs(logs)

		// Check result
		if err == nil {
			t.Errorf("SendLogs() error = nil, expected error")
		}
	})
}

// TestLokiDBName checks the Name method
func TestLokiDBName(t *testing.T) {
	db, _ := NewLokiDB("http://localhost:3100", Options{})

	name := db.Name()
	expected := "Loki"

	if name != expected {
		t.Errorf("Name() = %v, want %v", name, expected)
	}
}

// TestLokiDBInitializeAndClose checks the Initialize and Close methods
func TestLokiDBInitializeAndClose(t *testing.T) {
	db, _ := NewLokiDB("http://localhost:3100", Options{})

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
