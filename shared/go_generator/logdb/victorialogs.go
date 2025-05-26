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

// VictoriaLogsDB - реализация LogDB для VictoriaLogs
type VictoriaLogsDB struct {
	*BaseLogDB
	TimeField     string // Поле с временной меткой (для _time_field)
	ExtraParams   string // Дополнительные параметры запроса
	httpClient    *http.Client
}

// NewVictoriaLogsDB создает новый экземпляр VictoriaLogsDB
func NewVictoriaLogsDB(baseURL string, options Options) (*VictoriaLogsDB, error) {
	base := NewBaseLogDB(baseURL, options)
	
	db := &VictoriaLogsDB{
		BaseLogDB: base,
		TimeField: "timestamp", // Значение по умолчанию
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
	
	// Добавление параметра _time_field, если он еще не добавлен
	if db.TimeField != "" && !strings.Contains(db.URL, "_time_field=") {
		if strings.Contains(db.URL, "?") {
			db.URL += fmt.Sprintf("&_time_field=%s", db.TimeField)
		} else {
			db.URL += fmt.Sprintf("?_time_field=%s", db.TimeField)
		}
	}
	
	return db, nil
}

// Initialize инициализирует соединение с VictoriaLogs
func (db *VictoriaLogsDB) Initialize() error {
	// Для VictoriaLogs не требуется дополнительная инициализация
	return nil
}

// Close закрывает соединение с VictoriaLogs
func (db *VictoriaLogsDB) Close() error {
	// Для HTTP-клиента не требуется явное закрытие
	return nil
}

// Name возвращает имя базы данных
func (db *VictoriaLogsDB) Name() string {
	return "VictoriaLogs"
}

// FormatPayload форматирует записи логов в NDJSON формат для VictoriaLogs
func (db *VictoriaLogsDB) FormatPayload(logs []LogEntry) (string, string) {
	var buf bytes.Buffer
	
	// Для VictoriaLogs каждый лог должен быть на отдельной строке (NDJSON)
	for i, log := range logs {
		// Убедимся, что timestamp в правильном формате
		if _, ok := log["timestamp"]; !ok {
			log["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		
		logJSON, err := json.Marshal(log)
		if err != nil {
			continue
		}
		
		buf.Write(logJSON)
		if i < len(logs)-1 {
			buf.WriteString("\n")
		}
	}
	
	// Для VictoriaLogs используем content-type application/x-ndjson
	return buf.String(), "application/x-ndjson"
}

// SendLogs отправляет пакет логов в VictoriaLogs
func (db *VictoriaLogsDB) SendLogs(logs []LogEntry) error {
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
				fmt.Printf("VictoriaLogs: Повторная попытка %d/%d после ошибки: %v (задержка: %v)\n", 
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
