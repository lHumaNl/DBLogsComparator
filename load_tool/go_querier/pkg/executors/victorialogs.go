package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dblogscomparator/load_tool/go_querier/pkg"
)

// VictoriaLogsExecutor исполнитель запросов для VictoriaLogs
type VictoriaLogsExecutor struct {
	BaseURL    string
	Client     *http.Client
	Options    pkg.Options
	SearchPath string
}

// VictoriaLogsResponse представляет ответ от VictoriaLogs
type VictoriaLogsResponse struct {
	Status    string        `json:"status"`
	Data      VictoriaData  `json:"data"`
	ErrorType string        `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
}

// VictoriaData представляет данные ответа VictoriaLogs
type VictoriaData struct {
	Result     []VictoriaResult `json:"result"`
	ResultType string           `json:"resultType"`
}

// VictoriaResult представляет результат запроса VictoriaLogs
type VictoriaResult struct {
	Metric map[string]string   `json:"metric"`
	Values [][]interface{}     `json:"values,omitempty"`
	Value  []interface{}       `json:"value,omitempty"`
}

// Фиксированные ключевые слова для поиска в логах
var victoriaPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Фиксированные имена полей для поиска
var victoriaLogFields = []string{
	"log_type", "host", "container_name", "environment", "datacenter", 
	"version", "level", "message", "service", "remote_addr",
	"request", "status", "http_referer", "error_code",
}

// NewVictoriaLogsExecutor создает новый исполнитель запросов для VictoriaLogs
func NewVictoriaLogsExecutor(baseURL string, options pkg.Options) *VictoriaLogsExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}
	
	return &VictoriaLogsExecutor{
		BaseURL:    baseURL,
		Client:     client,
		Options:    options,
		SearchPath: "/select/logsql",
	}
}

// GetSystemName возвращает название системы
func (e *VictoriaLogsExecutor) GetSystemName() string {
	return "victorialogs"
}

// ExecuteQuery выполняет запрос указанного типа в VictoriaLogs
func (e *VictoriaLogsExecutor) ExecuteQuery(ctx context.Context, queryType pkg.QueryType) (pkg.QueryResult, error) {
	// Создаем случайный запрос указанного типа
	query := e.GenerateRandomQuery(queryType).(string)
	
	// Выполняем запрос к VictoriaLogs
	result, err := e.executeVictoriaLogsQuery(ctx, query)
	if err != nil {
		return pkg.QueryResult{}, err
	}
	
	return result, nil
}

// GenerateRandomQuery создает случайный запрос указанного типа для VictoriaLogs
func (e *VictoriaLogsExecutor) GenerateRandomQuery(queryType pkg.QueryType) interface{} {
	// Общий промежуток времени - последние 24 часа
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)
	
	// Для некоторых запросов берем более короткий промежуток времени
	var query string
	
	switch queryType {
	case pkg.SimpleQuery:
		// Простой поиск по одному полю или ключевому слову
		if rand.Intn(2) == 0 {
			// Поиск по ключевому слову
			keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
			query = fmt.Sprintf("message=~%q", keyword)
		} else {
			// Поиск по конкретному полю
			field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]
			fieldValue := fmt.Sprintf("value%d", rand.Intn(100))
			query = fmt.Sprintf("%s=%q", field, fieldValue)
		}
		
	case pkg.ComplexQuery:
		// Сложный поиск с несколькими условиями
		conditions := []string{}
		
		// Случайное количество условий от 2 до 4
		numConditions := 2 + rand.Intn(3)
		for i := 0; i < numConditions; i++ {
			// Выбор случайного поля
			field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]
			
			// Случайное условие в зависимости от типа поля
			switch field {
			case "status", "error_code":
				// Для числовых полей используем операторы сравнения
				op := []string{"=", "!=", ">", "<"}[rand.Intn(4)]
				value := 100 + rand.Intn(500)
				conditions = append(conditions, fmt.Sprintf("%s%s%d", field, op, value))
			default:
				// Для строковых полей используем оператор соответствия
				if rand.Intn(2) == 0 {
					// Точное совпадение
					value := fmt.Sprintf("value%d", rand.Intn(100))
					conditions = append(conditions, fmt.Sprintf("%s=%q", field, value))
				} else {
					// Регулярное выражение
					keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
					conditions = append(conditions, fmt.Sprintf("%s=~%q", field, keyword))
				}
			}
		}
		
		// Объединяем условия с помощью операторов AND и OR
		for i := 0; i < len(conditions)-1; i++ {
			if rand.Intn(3) < 2 {
				// В большинстве случаев используем AND
				conditions[i] = conditions[i] + " AND "
			} else {
				// Иногда используем OR
				conditions[i] = conditions[i] + " OR "
			}
		}
		
		query = strings.Join(conditions, "")
		
	case pkg.AnalyticalQuery:
		// Аналитический запрос с агрегацией
		// Для VictoriaLogs используем возможности LogsQL
		
		// Выбираем случайное поле для агрегации
		field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]
		
		// Выбираем случайную функцию агрегации
		aggregation := []string{"count", "count_distinct"}[rand.Intn(2)]
		
		// Формируем условие фильтрации
		condition := ""
		if rand.Intn(2) == 0 {
			// Добавляем условие по уровню лога
			levels := []string{"info", "warn", "error", "debug", "critical"}
			level := levels[rand.Intn(len(levels))]
			condition = fmt.Sprintf("level=%q", level)
		} else {
			// Добавляем условие по типу лога
			logTypes := []string{"web_access", "web_error", "application", "metric", "event"}
			logType := logTypes[rand.Intn(len(logTypes))]
			condition = fmt.Sprintf("log_type=%q", logType)
		}
		
		// Формируем запрос в зависимости от типа агрегации
		if aggregation == "count" {
			query = fmt.Sprintf("count() by (%s) where %s", field, condition)
		} else {
			query = fmt.Sprintf("count_distinct(%s) where %s", field, condition)
		}
		
	case pkg.TimeSeriesQuery:
		// Запрос временных рядов
		// Для VictoriaLogs используем интервалы времени
		
		// Используем более короткий промежуток времени - последние 6 часов
		startTime = now.Add(-6 * time.Hour)
		
		// Выбираем случайное поле для анализа временных рядов
		field := []string{"status", "error_code"}[rand.Intn(2)]
		
		// Формируем условие фильтрации
		condition := ""
		if rand.Intn(2) == 0 {
			// Добавляем условие по сервису
			services := []string{"api", "auth", "frontend", "backend", "database"}
			service := services[rand.Intn(len(services))]
			condition = fmt.Sprintf("service=%q", service)
		} else {
			// Добавляем условие по типу лога
			logTypes := []string{"web_access", "web_error", "application"}
			logType := logTypes[rand.Intn(len(logTypes))]
			condition = fmt.Sprintf("log_type=%q", logType)
		}
		
		// Определяем интервал времени (в минутах)
		intervals := []int{5, 10, 15, 30, 60}
		interval := intervals[rand.Intn(len(intervals))]
		
		// Формируем запрос с интервалом
		query = fmt.Sprintf("count() by (%s) where %s __interval=%dm", field, condition, interval)
	}
	
	// Добавляем временной диапазон, если запрос не аналитический (там уже есть условие)
	if queryType != pkg.AnalyticalQuery {
		timeRange := fmt.Sprintf(" __timeFilter(time, %d, %d)", startTime.Unix(), now.Unix())
		query += timeRange
	}
	
	return query
}

// executeVictoriaLogsQuery выполняет запрос к VictoriaLogs
func (e *VictoriaLogsExecutor) executeVictoriaLogsQuery(ctx context.Context, query string) (pkg.QueryResult, error) {
	// Формируем URL запроса
	queryURL := fmt.Sprintf("%s%s", e.BaseURL, e.SearchPath)
	
	// Создаем параметры запроса
	params := url.Values{}
	params.Set("query", query)
	
	// Создаем HTTP-запрос
	req, err := http.NewRequestWithContext(ctx, "GET", queryURL, nil)
	if err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка создания запроса: %v", err)
	}
	
	// Устанавливаем параметры запроса
	req.URL.RawQuery = params.Encode()
	
	// Устанавливаем заголовки
	req.Header.Set("Accept", "application/json")
	
	// Выполняем запрос
	startTime := time.Now()
	resp, err := e.Client.Do(req)
	duration := time.Since(startTime)
	
	if err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка выполнения запроса: %v", err)
	}
	defer resp.Body.Close()
	
	// Читаем ответ
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка чтения ответа: %v", err)
	}
	
	// Проверяем код ответа
	if resp.StatusCode != http.StatusOK {
		return pkg.QueryResult{}, fmt.Errorf("ошибка запроса: код %d, тело: %s", resp.StatusCode, string(body))
	}
	
	// Декодируем ответ
	var response VictoriaLogsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка декодирования ответа: %v", err)
	}
	
	// Проверяем статус ответа
	if response.Status != "success" {
		return pkg.QueryResult{}, fmt.Errorf("ошибка запроса: %s", response.Error)
	}
	
	// Подсчитываем количество результатов
	hitCount := 0
	for _, result := range response.Data.Result {
		if len(result.Values) > 0 {
			hitCount += len(result.Values)
		} else if len(result.Value) > 0 {
			hitCount++
		}
	}
	
	// Создаем результат
	result := pkg.QueryResult{
		Duration:  duration,
		HitCount:  hitCount,
		BytesRead: int64(len(body)),
		Status:    response.Status,
	}
	
	return result, nil
}
