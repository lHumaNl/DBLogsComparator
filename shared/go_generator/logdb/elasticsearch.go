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

// ElasticsearchDB - реализация LogDB для Elasticsearch
type ElasticsearchDB struct {
	*BaseLogDB
	IndexPrefix  string // Префикс для имени индекса
	IndexPattern string // Паттерн для имени индекса (например, "logs-YYYY.MM.DD")
	httpClient   *http.Client
}

// NewElasticsearchDB создает новый экземпляр ElasticsearchDB
func NewElasticsearchDB(baseURL string, options Options) (*ElasticsearchDB, error) {
	base := NewBaseLogDB(baseURL, options)
	
	db := &ElasticsearchDB{
		BaseLogDB:    base,
		IndexPrefix:  "logs",          // Значение по умолчанию
		IndexPattern: "logs-2006.01.02", // Значение по умолчанию - использует формат Go для времени
	}
	
	// Создание HTTP-клиента
	db.httpClient = &http.Client{
		Timeout: db.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	
	// Если URL не заканчивается на /_bulk, добавляем его
	if !strings.HasSuffix(db.URL, "/_bulk") {
		if strings.HasSuffix(db.URL, "/") {
			db.URL += "_bulk"
		} else {
			db.URL += "/_bulk"
		}
	}
	
	return db, nil
}

// Initialize инициализирует соединение с Elasticsearch
func (db *ElasticsearchDB) Initialize() error {
	// Проверяем доступность Elasticsearch
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

// Close закрывает соединение с Elasticsearch
func (db *ElasticsearchDB) Close() error {
	// Для HTTP-клиента не требуется явное закрытие
	return nil
}

// Name возвращает имя базы данных
func (db *ElasticsearchDB) Name() string {
	return "Elasticsearch"
}

// getCurrentIndex возвращает текущее имя индекса на основе паттерна
func (db *ElasticsearchDB) getCurrentIndex() string {
	// Если индекс уже содержит время, используем его как есть
	if strings.Contains(db.IndexPattern, "2006") || strings.Contains(db.IndexPattern, "01") || 
	   strings.Contains(db.IndexPattern, "02") {
		return time.Now().UTC().Format(db.IndexPattern)
	}
	
	// Иначе просто используем префикс
	return db.IndexPrefix
}

// FormatPayload форматирует записи логов в формат Bulk API для Elasticsearch
func (db *ElasticsearchDB) FormatPayload(logs []LogEntry) (string, string) {
	var buf bytes.Buffer
	currentIndex := db.getCurrentIndex()
	
	// Для Elasticsearch Bulk API каждый запрос состоит из пары строк:
	// 1. Метаданные операции (create, index, update, delete)
	// 2. Данные документа
	for _, log := range logs {
		// Убедимся, что timestamp в правильном формате
		if _, ok := log["timestamp"]; !ok {
			log["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		
		// Метаданные - операция index в указанный индекс
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
		
		// Данные документа
		docJSON, err := json.Marshal(log)
		if err != nil {
			continue
		}
		
		buf.Write(docJSON)
		buf.WriteString("\n")
	}
	
	// Для Elasticsearch Bulk API используем content-type application/x-ndjson
	return buf.String(), "application/x-ndjson"
}

// SendLogs отправляет пакет логов в Elasticsearch
func (db *ElasticsearchDB) SendLogs(logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	
	payload, contentType := db.FormatPayload(logs)
	
	var lastErr error
	
	// Попытки отправки с повторами при ошибках
	for attempt := 0; attempt <= db.RetryCount; attempt++ {
		if attempt > 0 {
			// Экспоненциальная задержка перед повторной попыткой
			backoff := db.RetryDelay * time.Duration(1<<uint(attempt-1))
			if db.Verbose {
				fmt.Printf("Elasticsearch: Повторная попытка %d/%d после ошибки: %v (задержка: %v)\n", 
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}
		
		// Создание запроса
		req, err := http.NewRequest("POST", db.URL, strings.NewReader(payload))
		if err != nil {
			lastErr = err
			continue
		}
		
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", "application/json")
		
		// Отправка запроса
		requestStart := time.Now()
		resp, err := db.httpClient.Do(req)
		requestEnd := time.Now()
		
		// Обновление метрик
		db.MetricsData["request_duration"] = requestEnd.Sub(requestStart).Seconds()
		
		if err != nil {
			lastErr = err
			db.MetricsData["failed_requests"]++
			continue
		}
		
		defer resp.Body.Close()
		
		// Проверка статуса ответа
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Успешная отправка
			db.MetricsData["successful_requests"]++
			db.MetricsData["total_logs"] += float64(len(logs))
			return nil
		}
		
		// Чтение тела ответа для получения информации об ошибке
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		db.MetricsData["failed_requests"]++
		
		// Если это серверная ошибка (5xx), повторяем попытку
		// Для клиентских ошибок (4xx) нет смысла повторять
		if resp.StatusCode < 500 {
			return lastErr
		}
	}
	
	return lastErr
}
