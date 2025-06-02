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

// TestNewElasticsearchDB проверяет создание экземпляра ElasticsearchDB
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

// TestElasticsearchDBFormatPayload проверяет форматирование данных для отправки
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
	
	// Проверяем Content-Type
	if contentType != "application/x-ndjson" {
		t.Errorf("contentType = %v, want %v", contentType, "application/x-ndjson")
	}
	
	// Проверяем, что для каждого лога есть два объекта (индекс и сам лог)
	// Каждый лог в Elasticsearch формате должен иметь 2 строки: index и data
	lines := strings.Split(strings.TrimSpace(payload), "\n")
	if len(lines) != len(logs)*2 {
		t.Errorf("payload содержит %v строк, ожидалось %v", len(lines), len(logs)*2)
	}
	
	// Проверяем, что каждая вторая строка содержит данные из логов
	for i, log := range logs {
		dataLineIndex := i*2 + 1 // Вторая строка для каждого лога
		
		for key := range log {
			if !strings.Contains(lines[dataLineIndex], key) {
				t.Errorf("Строка %v не содержит ключ %v", lines[dataLineIndex], key)
			}
		}
		
		// Проверяем, что первая строка содержит index
		indexLine := lines[i*2]
		if !strings.Contains(indexLine, "index") {
			t.Errorf("Строка %v не содержит директиву index", indexLine)
		}
	}
}

// TestElasticsearchDBSendLogs проверяет отправку логов
func TestElasticsearchDBSendLogs(t *testing.T) {
	// Создаем экземпляр ElasticsearchDB с мок-клиентом
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{
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
					if req.Header.Get("Content-Type") != "application/x-ndjson" {
						t.Errorf("Content-Type = %v, ожидался %v", req.Header.Get("Content-Type"), "application/x-ndjson")
					}
					
					// Возвращаем успешный ответ
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(bytes.NewBufferString(`{"errors":false,"took":5}`)),
					}, nil
				},
			},
		}
		db.httpClient = mockClient
		
		// Подготавливаем тестовые данные
		logs := []LogEntry{
			{
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
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
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
			},
		}
		
		// Отправляем логи
		err := db.SendLogs(logs)
		
		// Проверяем результат
		if err == nil {
			t.Errorf("SendLogs() error = nil, ожидалась ошибка")
		}
	})
	
	// Тест 3: Ошибка в ответе Elasticsearch
	t.Run("Elasticsearch Error", func(t *testing.T) {
		// Подменяем HTTP-клиент на мок с ошибкой Elasticsearch
		mockClient := &http.Client{
			Transport: &MockHTTPTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: 400,  // Изменяем статус на 400, чтобы точно получить ошибку
						Body:       io.NopCloser(bytes.NewBufferString(`{"errors":true,"items":[{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"mapping error"}}}]}`)),
					}, nil
				},
			},
		}
		db.httpClient = mockClient
		
		// Подготавливаем тестовые данные
		logs := []LogEntry{
			{
				"timestamp": "2023-01-01T12:00:00Z",
				"message":   "Test message",
				"level":     "info",
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

// TestElasticsearchDBName проверяет метод Name
func TestElasticsearchDBName(t *testing.T) {
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{})
	
	name := db.Name()
	expected := "Elasticsearch"
	
	if name != expected {
		t.Errorf("Name() = %v, want %v", name, expected)
	}
}

// TestElasticsearchDBInitializeAndClose проверяет методы Initialize и Close
func TestElasticsearchDBInitializeAndClose(t *testing.T) {
	// Создаем тестовый клиент, который не делает реальных запросов
	db, _ := NewElasticsearchDB("http://localhost:9200", Options{})
	
	// Подменяем клиент, чтобы не было реальных запросов
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
