package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// LokiExecutor исполнитель запросов для Loki
type LokiExecutor struct {
	BaseURL string
	Client  *http.Client
	Options models.Options
}

// LokiResponse представляет ответ API Loki для запроса Query
type LokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string        `json:"resultType"`
		Result     []interface{} `json:"result"`
		Stats      struct {
			Summary struct {
				BytesProcessedPerSecond int64   `json:"bytesProcessedPerSecond"`
				LinesProcessedPerSecond int64   `json:"linesProcessedPerSecond"`
				TotalBytesProcessed     int64   `json:"totalBytesProcessed"`
				TotalLinesProcessed     int64   `json:"totalLinesProcessed"`
				ExecTime                float64 `json:"execTime"`
			} `json:"summary"`
		} `json:"stats,omitempty"`
	} `json:"data"`
}

// Фиксированные ключевые слова для поиска в логах
var lokiQueryPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Фиксированные метки для фильтрации в Loki
var lokiLabels = []string{
	"log_type", "host", "container_name", "environment", "datacenter",
	"version", "level", "service", "job", "instance",
}

// NewLokiExecutor создает новый исполнитель запросов для Loki
func NewLokiExecutor(baseURL string, options models.Options) *LokiExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}

	return &LokiExecutor{
		BaseURL: baseURL,
		Client:  client,
		Options: options,
	}
}

// GetSystemName возвращает название системы
func (e *LokiExecutor) GetSystemName() string {
	return "loki"
}

// ExecuteQuery выполняет запрос указанного типа в Loki
func (e *LokiExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Создаем случайный запрос указанного типа
	queryInfo := e.GenerateRandomQuery(queryType).(map[string]string)

	// Выполняем запрос к Loki
	result, err := e.executeLokiQuery(ctx, queryInfo)
	if err != nil {
		return models.QueryResult{}, err
	}

	return result, nil
}

// GenerateRandomQuery создает случайный запрос указанного типа для Loki
func (e *LokiExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Общий промежуток времени - последние 24 часа
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	// Для некоторых запросов берем более короткий промежуток времени
	var queryString string
	var start string
	var end string
	var limit string
	var queryPath string

	// Преобразуем время в формат, понятный Loki (UNIX наносекунды)
	start = strconv.FormatInt(startTime.UnixNano(), 10)
	end = strconv.FormatInt(now.UnixNano(), 10)

	switch queryType {
	case models.SimpleQuery:
		// Простой поиск по логу с одним фильтром
		if rand.Intn(2) == 0 {
			// Поиск по ключевому слову в логе
			keyword := lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))]
			queryString = fmt.Sprintf("{} |= \"%s\"", keyword)
		} else {
			// Поиск по метке
			label := lokiLabels[rand.Intn(len(lokiLabels))]
			labelValue := fmt.Sprintf("value%d", rand.Intn(100))
			queryString = fmt.Sprintf("{%s=\"%s\"}", label, labelValue)
		}

		limit = strconv.Itoa(100 + rand.Intn(100)) // Лимит от 100 до 199
		queryPath = "query_range"

	case models.ComplexQuery:
		// Сложный поиск с несколькими условиями

		// Начнем с выбора нескольких меток для фильтрации
		labelFilters := []string{}

		// Случайное количество меток от 1 до 3
		numLabels := 1 + rand.Intn(3)
		usedLabels := map[string]bool{}

		for i := 0; i < numLabels; i++ {
			// Выберем случайную метку, которую еще не использовали
			var label string
			for {
				label = lokiLabels[rand.Intn(len(lokiLabels))]
				if !usedLabels[label] {
					usedLabels[label] = true
					break
				}
				// Если все метки уже использованы, просто выходим из цикла
				if len(usedLabels) == len(lokiLabels) {
					break
				}
			}

			labelValue := fmt.Sprintf("value%d", rand.Intn(100))
			labelFilters = append(labelFilters, fmt.Sprintf("%s=\"%s\"", label, labelValue))
		}

		// Создаем селектор логов с метками
		labelSelector := "{" + strings.Join(labelFilters, ", ") + "}"

		// Добавляем фильтры по содержимому
		contentFilters := []string{}

		// Случайное количество фильтров по содержимому от 0 до 2
		numContentFilters := rand.Intn(3)
		for i := 0; i < numContentFilters; i++ {
			// Выбираем случайное ключевое слово
			keyword := lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))]

			// Случайно выбираем оператор фильтрации
			operators := []string{"|=", "!=", "|~", "!~"}
			operator := operators[rand.Intn(len(operators))]

			contentFilters = append(contentFilters, fmt.Sprintf("%s \"%s\"", operator, keyword))
		}

		// Собираем полный запрос
		queryString = labelSelector
		if len(contentFilters) > 0 {
			queryString += " " + strings.Join(contentFilters, " ")
		}

		limit = strconv.Itoa(100 + rand.Intn(150)) // Лимит от 100 до 249
		queryPath = "query_range"

	case models.AnalyticalQuery:
		// Аналитический запрос с логическими операциями

		// Выбираем более короткий промежуток времени - последние 12 часов
		startTime = now.Add(-12 * time.Hour)
		start = strconv.FormatInt(startTime.UnixNano(), 10)

		// Создаем базовый селектор логов с одной меткой
		label := lokiLabels[rand.Intn(len(lokiLabels))]
		labelValue := fmt.Sprintf("value%d", rand.Intn(100))
		labelSelector := fmt.Sprintf("{%s=\"%s\"}", label, labelValue)

		// Случайно выбираем тип аналитической операции
		operations := []string{
			"| json",
			"| logfmt",
			"| pattern",
			"| line_format",
		}
		operation := operations[rand.Intn(len(operations))]

		// Формируем запрос в зависимости от операции
		switch operation {
		case "| json":
			// Извлечение полей из JSON
			fields := []string{"msg", "level", "status", "error"}
			field := fields[rand.Intn(len(fields))]
			queryString = fmt.Sprintf("%s | json | %s=\"%s\"",
				labelSelector,
				field,
				lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))])

		case "| logfmt":
			// Парсинг logfmt формата
			fields := []string{"level", "status", "method", "path"}
			field := fields[rand.Intn(len(fields))]
			queryString = fmt.Sprintf("%s | logfmt | %s=\"%s\"",
				labelSelector,
				field,
				lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))])

		case "| pattern":
			// Поиск по шаблону
			patterns := []string{
				"<* *>",
				"* - - *",
				"* * *: *",
			}
			pattern := patterns[rand.Intn(len(patterns))]
			queryString = fmt.Sprintf("%s | pattern `%s`", labelSelector, pattern)

		case "| line_format":
			// Форматирование вывода
			queryString = fmt.Sprintf("%s | json | line_format \"{{.level}} - {{.msg}}\"", labelSelector)
		}

		limit = strconv.Itoa(50 + rand.Intn(50)) // Лимит от 50 до 99
		queryPath = "query_range"

	case models.TimeSeriesQuery:
		// Запрос временных рядов

		// Используем более короткий промежуток времени - последние 6 часов
		startTime = now.Add(-6 * time.Hour)
		start = strconv.FormatInt(startTime.UnixNano(), 10)

		// Выбираем случайный тип метрики
		metrics := []string{
			"rate", "count_over_time", "sum_over_time", "avg_over_time",
			"min_over_time", "max_over_time", "stddev_over_time",
		}
		metric := metrics[rand.Intn(len(metrics))]

		// Создаем селектор логов
		label := lokiLabels[rand.Intn(len(lokiLabels))]
		labelValue := fmt.Sprintf("value%d", rand.Intn(100))
		labelSelector := fmt.Sprintf("{%s=\"%s\"}", label, labelValue)

		// Случайно выбираем временное окно для агрегации
		windows := []string{"5m", "10m", "15m", "30m", "1h"}
		window := windows[rand.Intn(len(windows))]

		queryString = fmt.Sprintf("%s(%s[%s])", metric, labelSelector, window)

		// Для некоторых запросов добавляем дополнительную обработку
		if rand.Intn(3) == 0 {
			// Группировка по метке
			byLabels := []string{"job", "instance", "host", "service"}
			byLabel := byLabels[rand.Intn(len(byLabels))]
			queryString = fmt.Sprintf("sum by(%s) (%s)", byLabel, queryString)
		}

		limit = strconv.Itoa(1000)
		queryPath = "query_range"
	}

	// Формируем итоговый запрос
	return map[string]string{
		"query": queryString,
		"start": start,
		"end":   end,
		"limit": limit,
		"path":  queryPath,
	}
}

// executeLokiQuery выполняет запрос к Loki
func (e *LokiExecutor) executeLokiQuery(ctx context.Context, queryInfo map[string]string) (models.QueryResult, error) {
	// Формируем URL запроса
	apiPath := fmt.Sprintf("/loki/api/v1/%s", queryInfo["path"])
	queryURL, err := url.Parse(e.BaseURL + apiPath)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("ошибка формирования URL: %v", err)
	}

	// Добавляем параметры запроса
	q := queryURL.Query()
	q.Set("query", queryInfo["query"])
	q.Set("start", queryInfo["start"])
	q.Set("end", queryInfo["end"])

	// Добавляем лимит для запросов логов
	if queryInfo["limit"] != "" {
		q.Set("limit", queryInfo["limit"])
	}

	// Для временных рядов добавляем параметр step
	if queryInfo["path"] == "query_range" {
		// Устанавливаем step в зависимости от интервала запроса
		startUnix, _ := strconv.ParseInt(queryInfo["start"], 10, 64)
		endUnix, _ := strconv.ParseInt(queryInfo["end"], 10, 64)

		// Вычисляем интервал в секундах
		intervalSec := (endUnix - startUnix) / 1e9

		// Определяем шаг как 1/100 от интервала, но не менее 15 секунд и не более 5 минут
		stepSec := intervalSec / 100
		if stepSec < 15 {
			stepSec = 15
		} else if stepSec > 300 {
			stepSec = 300
		}

		q.Set("step", fmt.Sprintf("%ds", stepSec))
	}

	queryURL.RawQuery = q.Encode()

	// Создаем HTTP-запрос
	req, err := http.NewRequestWithContext(ctx, "GET", queryURL.String(), nil)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("ошибка создания запроса: %v", err)
	}

	// Устанавливаем заголовки
	req.Header.Set("Accept", "application/json")

	// Выполняем запрос
	startTime := time.Now()
	resp, err := e.Client.Do(req)
	duration := time.Since(startTime)

	if err != nil {
		return models.QueryResult{}, fmt.Errorf("ошибка выполнения запроса: %v", err)
	}
	defer resp.Body.Close()

	// Читаем ответ
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("ошибка чтения ответа: %v", err)
	}

	// Проверяем код ответа
	if resp.StatusCode != http.StatusOK {
		return models.QueryResult{}, fmt.Errorf("ошибка запроса: код %d, тело: %s", resp.StatusCode, string(body))
	}

	// Декодируем ответ
	var response LokiResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return models.QueryResult{}, fmt.Errorf("ошибка декодирования ответа: %v", err)
	}

	// Подсчитываем количество результатов
	hitCount := 0
	if response.Data.Result != nil {
		hitCount = len(response.Data.Result)
	}

	// Собираем информацию о статистике запроса
	status := "success"
	if response.Data.Stats.Summary.ExecTime > 0 {
		status = fmt.Sprintf("success (%.2f ms, %d bytes processed)",
			response.Data.Stats.Summary.ExecTime*1000,
			response.Data.Stats.Summary.TotalBytesProcessed)
	}

	// Создаем результат
	result := models.QueryResult{
		Duration:  duration,
		HitCount:  hitCount,
		BytesRead: int64(len(body)),
		Status:    status,
	}

	return result, nil
}
