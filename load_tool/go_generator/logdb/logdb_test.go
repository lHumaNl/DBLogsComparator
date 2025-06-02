package logdb

import (
	"net/http"
	"testing"
	"time"
)

// MockHTTPClient - мок для HTTP-клиента, используемый в тестах
type MockHTTPClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.DoFunc(req)
}

// MockHTTPTransport - мок для транспорта HTTP-клиента
type MockHTTPTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

// TestCreateLogDB проверяет создание экземпляров LogDB через фабричный метод
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
				
				// Проверяем базовые методы
				if db.Name() == "" {
					t.Errorf("Name() вернул пустую строку")
				}
				
				metrics := db.Metrics()
				if metrics == nil {
					t.Errorf("Metrics() вернул nil")
				}
			}
		})
	}
}

// TestBaseLogDB проверяет функциональность базового класса BaseLogDB
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
	
	// Проверяем, что все поля установлены правильно
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
	
	// Проверяем, что MetricsData инициализирован
	if base.MetricsData == nil {
		t.Errorf("MetricsData не инициализирован")
	}
	
	// Проверяем метод Metrics()
	metrics := base.Metrics()
	if metrics == nil {
		t.Errorf("Metrics() вернул nil")
	}
	
	// Изменяем метрику и проверяем, что изменения отражаются
	base.MetricsData["test_metric"] = 42
	if base.Metrics()["test_metric"] != 42 {
		t.Errorf("Metrics() не отражает изменения в MetricsData")
	}
}

// getDBType возвращает строковое представление типа объекта
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
