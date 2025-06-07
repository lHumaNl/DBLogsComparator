package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// VictoriaLogsExecutor implements the QueryExecutor interface for VictoriaLogs
type VictoriaLogsExecutor struct {
	BaseURL            string
	SearchPath         string
	Client             *http.Client
	Options            models.Options
	availableLabels    []string
	timeRangeGenerator *common.TimeRangeGenerator
	// labelCache for caching label values
	labelCache     map[string][]string
	labelCacheLock sync.RWMutex
}

// VictoriaLogsResponse represents the response from VictoriaLogs
type VictoriaLogsResponse struct {
	Status    string       `json:"status"`
	Data      VictoriaData `json:"data"`
	ErrorType string       `json:"errorType,omitempty"`
	Error     string       `json:"error,omitempty"`
}

// VictoriaData represents the data in VictoriaLogs response
type VictoriaData struct {
	Result     []VictoriaResult `json:"result"`
	ResultType string           `json:"resultType"`
}

// VictoriaResult represents the result of a VictoriaLogs query
type VictoriaResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values,omitempty"`
	Value  []interface{}     `json:"value,omitempty"`
}

// NewVictoriaLogsExecutor creates a new query executor for VictoriaLogs
func NewVictoriaLogsExecutor(baseURL string, options models.Options, workerID int) *VictoriaLogsExecutor {
	// Ensure the base URL ends with a slash
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}

	// Define the search path for VictoriaLogs API
	// This is the endpoint for querying logs
	searchPath := "select/logsql"

	// Initialize with common labels from our shared data package
	availableLabels := logdata.CommonLabels

	// Initialize the label cache with static values from common data
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}

	return &VictoriaLogsExecutor{
		BaseURL:            baseURL,
		SearchPath:         searchPath,
		Client:             &http.Client{Timeout: options.Timeout},
		Options:            options,
		availableLabels:    availableLabels,
		timeRangeGenerator: common.NewTimeRangeGenerator(),
		labelCache:         labelCache,
		labelCacheLock:     sync.RWMutex{},
	}
}

// GetSystemName returns the system name
func (e *VictoriaLogsExecutor) GetSystemName() string {
	return "victorialogs"
}

// ExecuteQuery executes a query of the given type against VictoriaLogs
func (e *VictoriaLogsExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	var query string

	// Generate the query based on the specified query type
	switch queryType {
	case models.SimpleQuery:
		query = e.generateSimpleQuery(e.availableLabels)
	case models.ComplexQuery:
		query = e.generateComplexQuery(e.availableLabels)
	case models.AnalyticalQuery:
		query = e.generateAnalyticalQuery()
	case models.TimeSeriesQuery:
		query = e.generateTimeSeriesQuery()
	case models.StatQuery:
		query = e.generateStatQuery(e.availableLabels)
	case models.TopKQuery:
		query = e.generateTopKQuery(e.availableLabels)
	default:
		return models.QueryResult{}, fmt.Errorf("unsupported query type: %s", string(queryType))
	}

	// Generate a time range using our time range generator
	timeRange := e.timeRangeGenerator.GenerateRandomTimeRange()

	// Convert time range to RFC3339 format
	startTime := timeRange.Start.Format(time.RFC3339)
	endTime := timeRange.End.Format(time.RFC3339)

	// Prepare the query parameters
	queryInfo := map[string]string{
		"query": query,
		"start": startTime,
		"end":   endTime,
	}

	// Add limit parameter
	limit := strconv.Itoa(rand.Intn(100) + 1)
	queryInfo["limit"] = limit

	// VictoriaLogs may need a step parameter for some queries
	queryInfo["step"] = "60s"

	// Execute the query with the prepared parameters
	result, err := e.executeVictoriaLogsQuery(queryInfo)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("%s query error: %v", string(queryType), err)
	}

	return result, nil
}

// GetLabelValues retrieves label values from static data or cache
func (e *VictoriaLogsExecutor) GetLabelValues(ctx context.Context, label string) ([]string, error) {
	// Check if we have this label in our cache
	e.labelCacheLock.RLock()
	values, exists := e.labelCache[label]
	e.labelCacheLock.RUnlock()

	if exists {
		return values, nil
	}

	// For any label we don't have in our static data, return some generic values
	genericValues := []string{"value1", "value2", "value3", "value-" + strconv.Itoa(rand.Intn(100))}

	// Cache these values for future use
	e.labelCacheLock.Lock()
	e.labelCache[label] = genericValues
	e.labelCacheLock.Unlock()

	return genericValues, nil
}

// GenerateRandomQuery generates a random query of the specific type
func (e *VictoriaLogsExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Generate a random time range based on the configuration
	timeRange := e.timeRangeGenerator.GenerateRandomTimeRange()
	startTime := timeRange.Start
	endTime := timeRange.End

	// Format timestamps for VictoriaLogs in ISO 8601 format
	startTimeStr := startTime.Format(time.RFC3339)
	endTimeStr := endTime.Format(time.RFC3339)

	// Generate random limit
	limit := strconv.Itoa(rand.Intn(100) + 1)

	// Generate random step for time series queries
	step := "60s" // Default
	steps := []string{"10s", "30s", "60s", "5m", "10m", "30m"}
	if queryType == models.TimeSeriesQuery {
		step = steps[rand.Intn(len(steps))]
	}

	// Generate the query string based on query type
	var queryString string
	switch queryType {
	case models.SimpleQuery:
		queryString = e.generateSimpleQuery(e.availableLabels)
	case models.ComplexQuery:
		queryString = e.generateComplexQuery(e.availableLabels)
	case models.AnalyticalQuery:
		queryString = e.generateAnalyticalQuery()
	case models.TimeSeriesQuery:
		queryString = e.generateTimeSeriesQuery()
	case models.StatQuery:
		queryString = e.generateStatQuery(e.availableLabels)
	case models.TopKQuery:
		queryString = e.generateTopKQuery(e.availableLabels)
	}

	// Return parameters as map for request body
	return map[string]string{
		"query": queryString,
		"start": startTimeStr,
		"end":   endTimeStr,
		"limit": limit,
		"step":  step,
	}
}

// generateSimpleQuery creates a simple query with label equality filter
func (e *VictoriaLogsExecutor) generateSimpleQuery(availableLabels []string) string {
	// Choose a random label from available labels
	var chosenLabel string

	// Try to use commonly populated labels first
	commonLabels := []string{"log_type", "host", "container_name", "service", "level", "environment"}

	// Check if any of the common labels are available
	for _, label := range commonLabels {
		if logdata.Contains(availableLabels, label) {
			chosenLabel = label
			break
		}
	}

	// If no common label found, choose any random label
	if chosenLabel == "" && len(availableLabels) > 0 {
		chosenLabel = availableLabels[rand.Intn(len(availableLabels))]
	} else if chosenLabel == "" {
		chosenLabel = "log_type"
	}

	// Get 1-2 random values for this label using the common function from logdata
	count := rand.Intn(2) + 1
	selectedValues := logdata.GetMultipleRandomValuesForLabel(chosenLabel, count)

	// Create the query string in VictoriaLogs LogsQL syntax
	var queryString string
	if len(selectedValues) > 0 {
		// Most queries will use exact matching
		if rand.Intn(10) < 8 {
			if len(selectedValues) == 1 {
				queryString = fmt.Sprintf("%s=%q", chosenLabel, selectedValues[0])
			} else {
				// For multiple values, VictoriaLogs uses OR pattern
				values := make([]string, 0, len(selectedValues))
				for _, v := range selectedValues {
					values = append(values, fmt.Sprintf("%s=%q", chosenLabel, v))
				}
				queryString = strings.Join(values, " OR ")
			}
		} else {
			// Some queries will use regex matching
			pattern := strings.Join(selectedValues, "|")
			queryString = fmt.Sprintf("%s=~\"(%s)\"", chosenLabel, pattern)
		}
	} else {
		// Fallback for empty values
		queryString = fmt.Sprintf("%s!=\"\"", chosenLabel)
	}

	// Add text search with 50% probability
	if rand.Intn(2) == 0 {
		// Common keywords to search for in logs
		keywords := []string{
			"error", "warning", "failed", "exception", "timeout",
			"success", "completed", "started", "INFO", "DEBUG",
		}
		keyword := keywords[rand.Intn(len(keywords))]

		// In LogsQL, full-text search is done with simple words
		queryString = fmt.Sprintf("%s %s", queryString, keyword)
	}

	return queryString
}

// generateComplexQuery creates a complex query with multiple filters
func (e *VictoriaLogsExecutor) generateComplexQuery(availableLabels []string) string {
	// Select multiple random labels
	labelCount := rand.Intn(3) + 2 // 2-4 labels

	// Make sure we don't try to select more labels than available
	if labelCount > len(availableLabels) {
		labelCount = len(availableLabels)
	}

	// Get random labels without duplicates
	selectedLabels := logdata.GetRandomLabels(labelCount)

	// Make sure log_type is included for better querying
	hasLogType := logdata.Contains(selectedLabels, "log_type")
	if !hasLogType && len(selectedLabels) < labelCount {
		selectedLabels = append(selectedLabels, "log_type")
	}

	// Build conditions for each selected label
	conditions := make([]string, 0, len(selectedLabels))

	for _, label := range selectedLabels {
		// Get values for this label from logdata package
		var selectedValues []string

		// Choose 1-2 random values for most conditions
		valueCount := 1
		if rand.Intn(10) > 6 { // 30% chance of multiple values
			valueCount = 2
		}

		selectedValues = logdata.GetMultipleRandomValuesForLabel(label, valueCount)

		if len(selectedValues) == 0 {
			continue
		}

		// For some labels use regex matching, for others exact matching
		if rand.Intn(10) < 7 { // 70% chance of exact matching
			if len(selectedValues) == 1 {
				// Single value exact match
				conditions = append(conditions, fmt.Sprintf("%s=%q", label, selectedValues[0]))
			} else {
				// Multiple values with OR
				labelConditions := make([]string, 0, len(selectedValues))
				for _, value := range selectedValues {
					labelConditions = append(labelConditions, fmt.Sprintf("%s=%q", label, value))
				}
				conditions = append(conditions, "("+strings.Join(labelConditions, " OR ")+")")
			}
		} else { // 30% chance of regex matching
			pattern := strings.Join(selectedValues, "|")
			conditions = append(conditions, fmt.Sprintf("%s=~\"(%s)\"", label, pattern))
		}
	}

	// Add text search with 50% probability
	if rand.Intn(2) == 0 {
		// Keywords to search for
		keywords := []string{
			"error", "warning", "failed", "exception", "timeout",
			"success", "completed", "started", "INFO", "DEBUG",
		}
		keyword := keywords[rand.Intn(len(keywords))]

		// In LogsQL, full-text search is done with simple words
		conditions = append(conditions, keyword)
	}

	// Build the final query by joining conditions with AND
	query := strings.Join(conditions, " AND ")

	return query
}

// generateStatQuery generates a statistical query for VictoriaLogs
func (e *VictoriaLogsExecutor) generateStatQuery(availableLabels []string) string {
	// Выбираем случайную метку для фильтрации
	labelIdx := rand.Intn(len(availableLabels))
	chosenLabel := availableLabels[labelIdx]
	chosenValue := logdata.GetRandomValueForLabel(chosenLabel)

	// Правильный синтаксис VictoriaLogs для статистики
	return fmt.Sprintf(`%s="%s" | stats count() as count_value`,
		chosenLabel, chosenValue)
}

// generateTopKQuery генерирует top-K запрос для VictoriaLogs
func (e *VictoriaLogsExecutor) generateTopKQuery(availableLabels []string) string {
	// Выбираем случайную метку для фильтрации
	labelIdx := rand.Intn(len(availableLabels))
	chosenLabel := availableLabels[labelIdx]
	chosenValue := logdata.GetRandomValueForLabel(chosenLabel)

	// Выбираем случайную метку для группировки
	groupByLabelIdx := rand.Intn(len(availableLabels))
	groupByLabel := availableLabels[groupByLabelIdx]

	// Случайный лимит для top-K
	limit := rand.Intn(20) + 5

	// Используем правильный синтаксис для TopK запроса
	return fmt.Sprintf(`%s="%s" | stats by (%s) count() as count_value | sort by (count_value desc) | limit %d`,
		chosenLabel, chosenValue, groupByLabel, limit)
}

// generateAnalyticalQuery генерирует аналитический запрос для VictoriaLogs
func (e *VictoriaLogsExecutor) generateAnalyticalQuery() string {
	// Выбираем случайный тип логов
	logType := logdata.LogTypes[rand.Intn(len(logdata.LogTypes))]

	// Выбираем метку для группировки
	groupByLabels := []string{"service", "container_name", "host", "level", "environment"}
	groupByLabel := groupByLabels[rand.Intn(len(groupByLabels))]

	// Используем правильный синтаксис для аналитических запросов
	return fmt.Sprintf(`log_type="%s" | stats by (%s) count() as count_value`,
		logType, groupByLabel)
}

// generateTimeSeriesQuery генерирует запрос временного ряда для VictoriaLogs
func (e *VictoriaLogsExecutor) generateTimeSeriesQuery() string {
	// Выбираем случайные типы логов и уровни
	logType := logdata.LogTypes[rand.Intn(len(logdata.LogTypes))]

	// VictoriaLogs не поддерживает sample_over, используем более простые запросы
	// для временных рядов (фильтрация по времени обрабатывается в ExecuteQuery)
	return fmt.Sprintf(`log_type="%s" | limit 100`, logType)
}

// executeVictoriaLogsQuery executes a query against VictoriaLogs
func (e *VictoriaLogsExecutor) executeVictoriaLogsQuery(queryInfo map[string]string) (models.QueryResult, error) {
	startTime := time.Now()

	// Правильный эндпоинт для запросов к VictoriaLogs с проверкой на слэш в конце
	var queryURL string
	if strings.HasSuffix(e.BaseURL, "/") {
		queryURL = e.BaseURL + "select/logsql/query"
	} else {
		queryURL = e.BaseURL + "/select/logsql/query"
	}

	// Подготовка параметров запроса
	params := url.Values{}
	for key, value := range queryInfo {
		params.Add(key, value)
	}

	// Создаем POST запрос с параметрами в теле
	req, err := http.NewRequestWithContext(context.Background(), "POST", queryURL, strings.NewReader(params.Encode()))
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %v", err)
	}

	// Устанавливаем правильные заголовки
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	// Выполняем запрос
	resp, err := e.Client.Do(req)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Засекаем время выполнения запроса
	duration := time.Since(startTime)

	// Проверяем статус ответа
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return models.QueryResult{}, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Читаем ответ
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error reading response: %v", err)
	}

	// Проверка на пустой ответ
	if len(bodyBytes) == 0 {
		return models.QueryResult{
			Duration:    duration,
			HitCount:    0,
			ResultCount: 0,
			BytesRead:   0,
			Status:      resp.Status,
			RawResponse: bodyBytes,
			QueryString: queryInfo["query"],
		}, nil
	}

	// Декодируем JSON-ответ
	var response VictoriaLogsResponse

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		// Если не удалось распарсить как JSON, но статус OK, возвращаем результат с данными о размере
		return models.QueryResult{
			Duration:    duration,
			HitCount:    0,
			ResultCount: 0,
			BytesRead:   int64(len(bodyBytes)),
			Status:      resp.Status,
			RawResponse: bodyBytes,
			QueryString: queryInfo["query"],
		}, nil
	}

	// Подсчитываем общее количество возвращенных логов
	hitCount := 0
	if response.Data.Result != nil {
		for _, result := range response.Data.Result {
			if result.Values != nil {
				hitCount += len(result.Values)
			}
		}
	}

	// Возвращаем структурированный результат
	return models.QueryResult{
		Duration:    duration,
		HitCount:    hitCount,
		ResultCount: hitCount,
		BytesRead:   int64(len(bodyBytes)),
		Status:      resp.Status,
		RawResponse: bodyBytes,
		QueryString: queryInfo["query"],
	}, nil
}
