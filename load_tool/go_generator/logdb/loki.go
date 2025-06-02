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

// LokiDB - реализация LogDB для Loki
type LokiDB struct {
	*BaseLogDB
	Labels      map[string]string // Метки для логов
	LabelFields []string          // Поля из логов, которые нужно использовать как метки
	httpClient  *http.Client
}

// NewLokiDB создает новый экземпляр LokiDB
func NewLokiDB(baseURL string, options Options) (*LokiDB, error) {
	base := NewBaseLogDB(baseURL, options)

	db := &LokiDB{
		BaseLogDB:   base,
		Labels:      make(map[string]string),
		LabelFields: []string{"log_type", "host", "container_name"},
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

	// Проверяем и корректируем URL для API Loki
	// В Loki 3.5.1+ API путь: /loki/api/v1/push
	if !strings.HasSuffix(db.URL, "/loki/api/v1/push") && !strings.HasSuffix(db.URL, "/api/v1/push") {
		if strings.HasSuffix(db.URL, "/") {
			db.URL += "loki/api/v1/push"
		} else {
			db.URL += "/loki/api/v1/push"
		}
	}

	// Для отладки выводим финальный URL
	fmt.Printf("Loki API URL: %s\n", db.URL)

	return db, nil
}

// Initialize инициализирует соединение с Loki
func (db *LokiDB) Initialize() error {
	// Для Loki не требуется дополнительная инициализация
	return nil
}

// Close закрывает соединение с Loki
func (db *LokiDB) Close() error {
	// Для HTTP-клиента не требуется явное закрытие
	return nil
}

// Name возвращает имя базы данных
func (db *LokiDB) Name() string {
	return "Loki"
}

// extractLabels извлекает метки из лога
func (db *LokiDB) extractLabels(log LogEntry) map[string]string {
	labels := make(map[string]string)

	// Копируем базовые метки
	for k, v := range db.Labels {
		labels[k] = v
	}

	// Добавляем метки из полей лога
	for _, field := range db.LabelFields {
		if value, ok := log[field].(string); ok {
			labels[field] = value
		}
	}

	return labels
}

// formatLabelsString форматирует метки в строку для Loki
func (db *LokiDB) formatLabelsString(labels map[string]string) string {
	var pairs []string
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

// FormatPayload форматирует записи логов в формат Loki Push API
func (db *LokiDB) FormatPayload(logs []LogEntry) (string, string) {
	// Группировка логов по меткам
	streams := make(map[string][]map[string]interface{})

	for _, log := range logs {
		// Убедимся, что timestamp в правильном формате (для Loki нужен Unix timestamp в наносекундах)
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

		// Извлекаем метки из лога
		labels := db.extractLabels(log)
		labelsStr := db.formatLabelsString(labels)

		// Преобразуем лог в строку
		logJSON, err := json.Marshal(log)
		if err != nil {
			continue
		}

		// Добавляем в соответствующий поток
		if _, ok := streams[labelsStr]; !ok {
			streams[labelsStr] = []map[string]interface{}{}
		}

		streams[labelsStr] = append(streams[labelsStr], map[string]interface{}{
			"ts":   timestamp,
			"line": string(logJSON),
		})
	}

	// Форматируем в формат Loki Push API
	lokiRequest := map[string]interface{}{
		"streams": []map[string]interface{}{},
	}

	for labelsStr, values := range streams {
		stream := map[string]interface{}{
			"stream": map[string]interface{}{},
			"values": [][]interface{}{},
		}

		// Парсим метки из строки обратно в объект
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

		// Добавляем значения логов
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

// SendLogs отправляет пакет логов в Loki
func (db *LokiDB) SendLogs(logs []LogEntry) error {
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
				fmt.Printf("Loki: Повторная попытка %d/%d после ошибки: %v (задержка: %v)\n",
					attempt, db.RetryCount, lastErr, backoff)
			}
			time.Sleep(backoff)
		}

		// Создание запроса
		req, err := http.NewRequest("POST", db.URL, bytes.NewReader([]byte(payload)))
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
