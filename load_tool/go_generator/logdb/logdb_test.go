package logdb

import (
	"net/http"
	"testing"
	"time"
)

// MockHTTPClient - mock for HTTP client used in tests
type MockHTTPClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.DoFunc(req)
}

// MockHTTPTransport - mock for HTTP client transport
type MockHTTPTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

// TestCreateLogDB checks the creation of LogDB instances through the factory method
func TestCreateLogDB(t *testing.T) {
	options := Options{
		BatchSize:  100,
		Timeout:    time.Second * 10,
		RetryCount: 3,
		RetryDelay: time.Second,
		Verbose:    true,
	}

	tests := []struct {
		name     string
		mode     string
		baseURL  string
		options  Options
		wantType string
		wantErr  bool
	}{
		{
			name:     "VictoriaLogs",
			mode:     "victoria",
			baseURL:  "http://localhost:8428",
			options:  options,
			wantType: "*logdb.VictoriaLogsDB",
			wantErr:  false,
		},
		{
			name:     "Elasticsearch",
			mode:     "es",
			baseURL:  "http://localhost:9200",
			options:  options,
			wantType: "*logdb.ElasticsearchDB",
			wantErr:  false,
		},
		{
			name:     "Loki",
			mode:     "loki",
			baseURL:  "http://localhost:3100",
			options:  options,
			wantType: "*logdb.LokiDB",
			wantErr:  false,
		},
		{
			name:     "UnsupportedMode",
			mode:     "unknown",
			baseURL:  "http://localhost:8080",
			options:  options,
			wantType: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := CreateLogDB(tt.mode, tt.baseURL, tt.options)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateLogDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				dbType := getDBType(db)
				if dbType != tt.wantType {
					t.Errorf("CreateLogDB() dbType = %v, want %v", dbType, tt.wantType)
				}

				// Check basic methods
				if db.Name() == "" {
					t.Errorf("Name() returned an empty string")
				}

				metrics := db.Metrics()
				if metrics == nil {
					t.Errorf("Metrics() returned nil")
				}
			}
		})
	}
}

// TestBaseLogDB checks the functionality of the BaseLogDB base class
func TestBaseLogDB(t *testing.T) {
	options := Options{
		BatchSize:  200,
		Timeout:    time.Second * 5,
		RetryCount: 2,
		RetryDelay: time.Millisecond * 500,
		Verbose:    true,
	}

	url := "http://example.com/api"

	base := NewBaseLogDB(url, options)

	// Check that all fields are set correctly
	if base.URL != url {
		t.Errorf("URL = %v, want %v", base.URL, url)
	}

	if base.BatchSize != options.BatchSize {
		t.Errorf("BatchSize = %v, want %v", base.BatchSize, options.BatchSize)
	}

	if base.Timeout != options.Timeout {
		t.Errorf("Timeout = %v, want %v", base.Timeout, options.Timeout)
	}

	if base.RetryCount != options.RetryCount {
		t.Errorf("RetryCount = %v, want %v", base.RetryCount, options.RetryCount)
	}

	if base.RetryDelay != options.RetryDelay {
		t.Errorf("RetryDelay = %v, want %v", base.RetryDelay, options.RetryDelay)
	}

	if base.Verbose != options.Verbose {
		t.Errorf("Verbose = %v, want %v", base.Verbose, options.Verbose)
	}

	// Check that MetricsData is initialized
	if base.MetricsData == nil {
		t.Errorf("MetricsData is not initialized")
	}

	// Check the Metrics() method
	metrics := base.Metrics()
	if metrics == nil {
		t.Errorf("Metrics() returned nil")
	}

	// Change a metric and check that changes are reflected
	base.MetricsData["test_metric"] = 42
	if base.Metrics()["test_metric"] != 42 {
		t.Errorf("Metrics() does not reflect changes in MetricsData")
	}
}

// getDBType returns a string representation of the object type
func getDBType(db interface{}) string {
	if db == nil {
		return ""
	}

	switch db.(type) {
	case *VictoriaLogsDB:
		return "*logdb.VictoriaLogsDB"
	case *ElasticsearchDB:
		return "*logdb.ElasticsearchDB"
	case *LokiDB:
		return "*logdb.LokiDB"
	default:
		return "unknown"
	}
}
