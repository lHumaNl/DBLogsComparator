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

// TestNewLokiDB проверяет создание экземпляра LokiDB
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
				
				// Проверяем, что HTTP-клиент создан
				if db.httpClient == nil {
					t.Errorf("httpClient не инициализирован")
				}
				
				// Проверяем поля из Options
				if db.BatchSize != tt.options.BatchSize {
					t.Errorf("BatchSize = %v, want %v", db.BatchSize, tt.options.BatchSize)
				}
			}
		})
	}
}

// TestLokiDBFormatPayload проверяет форматирование данных для отправки
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
	
	// Проверяем Content-Type
	if contentType != "application/json" {
		t.Errorf("contentType = %v, want %v", contentType, "application/json")
	}
	
	// Проверяем, что payload содержит структуру Loki
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
			t.Errorf("payload не содержит ожидаемую часть: %v", part)
		}
	}
}

// TestLokiDBSendLogs проверяет отправку логов
func TestLokiDBSendLogs(t *testing.T) {
	// Создаем экземпляр LokiDB с мок-клиентом
	db, _ := NewLokiDB("http://localhost:3100", Options{
		RetryCount: 1,
		RetryDelay: time.Millisecond * 10,
	})
	
	// Тест 1: Успешная отправка
	t.Run("Successful Send", func(t *testing.T) {
		// Подменяем HTTP-клиент на мок
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					// Проверяем URL запроса
					if req.URL.String() != db.URL {
						t.Errorf("URL запроса = %v, ожидался %v", req.URL.String(), db.URL)
					}
					
					// Проверяем метод запроса
					if req.Method != "POST" {
						t.Errorf("Метод запроса = %v, ожидался %v", req.Method, "POST")
					}
					
					// Проверяем Content-Type
					if req.Header.Get("Content-Type") != "application/json" {
						t.Errorf("Content-Type = %v, ожидался %v", req.Header.Get("Content-Type"), "application/json")
					}
					
					// Возвращаем успешный ответ
					return &http.Response{
						StatusCode: 204,
						Body:       io.NopCloser(bytes.NewBufferString("")),
					}, nil
				},
			},
		}
		db.httpClient = mockClient
		
		// Подготавливаем тестовые данные
		logs := []LogEntry{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}
		
		// Отправляем логи
		err := db.SendLogs(logs)
		
		// Проверяем результат
		if err != nil {
			t.Errorf("SendLogs() error = %v, ожидался nil", err)
		}
	})
	
	// Тест 2: Ошибка отправки
	t.Run("Failed Send", func(t *testing.T) {
		// Подменяем HTTP-клиент на мок с ошибкой
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("connection error")
				},
			},
		}
		db.httpClient = mockClient
		
		// Подготавливаем тестовые данные
		logs := []LogEntry{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}
		
		// Отправляем логи
		err := db.SendLogs(logs)
		
		// Проверяем результат
		if err == nil {
			t.Errorf("SendLogs() error = nil, ожидалась ошибка")
		}
	})
	
	// Тест 3: Неуспешный статус ответа
	t.Run("Failed Status", func(t *testing.T) {
		// Подменяем HTTP-клиент на мок с ошибочным статусом
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
		
		// Подготавливаем тестовые данные
		logs := []LogEntry{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"message":   "Test message",
				"level":     "info",
				"log_type":  "web_access",
			},
		}
		
		// Отправляем логи
		err := db.SendLogs(logs)
		
		// Проверяем результат
		if err == nil {
			t.Errorf("SendLogs() error = nil, ожидалась ошибка")
		}
	})
}

// TestLokiDBName проверяет метод Name
func TestLokiDBName(t *testing.T) {
	db, _ := NewLokiDB("http://localhost:3100", Options{})
	
	name := db.Name()
	expected := "Loki"
	
	if name != expected {
		t.Errorf("Name() = %v, want %v", name, expected)
	}
}

// TestLokiDBInitializeAndClose проверяет методы Initialize и Close
func TestLokiDBInitializeAndClose(t *testing.T) {
	db, _ := NewLokiDB("http://localhost:3100", Options{})
	
	// Initialize должен просто возвращать nil
	err := db.Initialize()
	if err != nil {
		t.Errorf("Initialize() error = %v, ожидался nil", err)
	}
	
	// Close должен просто возвращать nil
	err = db.Close()
	if err != nil {
		t.Errorf("Close() error = %v, ожидался nil", err)
	}
}
