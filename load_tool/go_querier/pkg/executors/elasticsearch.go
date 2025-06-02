package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/dblogscomparator/load_tool/go_querier/pkg"
)

// ElasticsearchExecutor исполнитель запросов для Elasticsearch
type ElasticsearchExecutor struct {
	BaseURL    string
	Client     *http.Client
	Options    pkg.Options
	IndexName  string
}

// ESSearchResponse представляет ответ от Elasticsearch
type ESSearchResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64       `json:"max_score"`
		Hits     []interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations map[string]interface{} `json:"aggregations,omitempty"`
}

// ESErrorResponse представляет ошибку от Elasticsearch
type ESErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
	Status int `json:"status"`
}

// Фиксированные ключевые слова для поиска в логах
var esQueryPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Фиксированные имена полей для поиска
var esLogFields = []string{
	"log_type", "host", "container_name", "environment", "datacenter", 
	"version", "level", "message", "service", "remote_addr",
	"request", "status", "http_referer", "error_code",
}

// NewElasticsearchExecutor создает новый исполнитель запросов для Elasticsearch
func NewElasticsearchExecutor(baseURL string, options pkg.Options) *ElasticsearchExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}
	
	return &ElasticsearchExecutor{
		BaseURL:    baseURL,
		Client:     client,
		Options:    options,
		IndexName:  "logs-*", // Используем маску для всех индексов логов
	}
}

// GetSystemName возвращает название системы
func (e *ElasticsearchExecutor) GetSystemName() string {
	return "elasticsearch"
}

// ExecuteQuery выполняет запрос указанного типа в Elasticsearch
func (e *ElasticsearchExecutor) ExecuteQuery(ctx context.Context, queryType pkg.QueryType) (pkg.QueryResult, error) {
	// Создаем случайный запрос указанного типа
	query := e.GenerateRandomQuery(queryType).(map[string]interface{})
	
	// Выполняем запрос к Elasticsearch
	result, err := e.executeElasticsearchQuery(ctx, query)
	if err != nil {
		return pkg.QueryResult{}, err
	}
	
	return result, nil
}

// GenerateRandomQuery создает случайный запрос указанного типа для Elasticsearch
func (e *ElasticsearchExecutor) GenerateRandomQuery(queryType pkg.QueryType) interface{} {
	// Общий промежуток времени - последние 24 часа
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)
	
	// Для некоторых запросов берем более короткий промежуток времени
	var query map[string]interface{}
	
	// Базовый фильтр по времени
	timeRange := map[string]interface{}{
		"range": map[string]interface{}{
			"timestamp": map[string]interface{}{
				"gte": startTime.Format(time.RFC3339),
				"lte": now.Format(time.RFC3339),
			},
		},
	}
	
	switch queryType {
	case pkg.SimpleQuery:
		// Простой поиск по одному полю или ключевому слову
		if rand.Intn(2) == 0 {
			// Поиск по ключевому слову в сообщении
			keyword := esQueryPhrases[rand.Intn(len(esQueryPhrases))]
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []interface{}{
							timeRange,
							map[string]interface{}{
								"match": map[string]interface{}{
									"message": keyword,
								},
							},
						},
					},
				},
			}
		} else {
			// Поиск по конкретному полю
			field := esLogFields[rand.Intn(len(esLogFields))]
			fieldValue := fmt.Sprintf("value%d", rand.Intn(100))
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []interface{}{
							timeRange,
							map[string]interface{}{
								"term": map[string]interface{}{
									field: fieldValue,
								},
							},
						},
					},
				},
			}
		}
		
	case pkg.ComplexQuery:
		// Сложный поиск с несколькими условиями
		conditions := []interface{}{timeRange}
		
		// Случайное количество условий от 2 до 4
		numConditions := 2 + rand.Intn(3)
		for i := 0; i < numConditions; i++ {
			// Выбор случайного поля
			field := esLogFields[rand.Intn(len(esLogFields))]
			
			// Случайное условие в зависимости от типа поля
			switch field {
			case "status", "error_code":
				// Для числовых полей используем диапазон
				min := 100 + rand.Intn(200)
				max := min + rand.Intn(300)
				
				conditions = append(conditions, map[string]interface{}{
					"range": map[string]interface{}{
						field: map[string]interface{}{
							"gte": min,
							"lte": max,
						},
					},
				})
			default:
				// Для строковых полей используем разные типы матчеров
				if rand.Intn(3) == 0 {
					// Точное совпадение
					value := fmt.Sprintf("value%d", rand.Intn(100))
					conditions = append(conditions, map[string]interface{}{
						"term": map[string]interface{}{
							field: value,
						},
					})
				} else if rand.Intn(2) == 0 {
					// Полнотекстовый поиск
					keyword := esQueryPhrases[rand.Intn(len(esQueryPhrases))]
					conditions = append(conditions, map[string]interface{}{
						"match": map[string]interface{}{
							field: keyword,
						},
					})
				} else {
					// Поиск по префиксу
					prefix := fmt.Sprintf("pref%d", rand.Intn(10))
					conditions = append(conditions, map[string]interface{}{
						"prefix": map[string]interface{}{
							field: prefix,
						},
					})
				}
			}
		}
		
		// Объединяем условия с помощью должен/может
		if rand.Intn(3) < 2 {
			// В большинстве случаев используем must (AND)
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": conditions,
					},
				},
			}
		} else {
			// Иногда используем should с minimum_should_match (OR)
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must":               []interface{}{timeRange},
						"should":             conditions[1:], // Первый элемент - timeRange, уже добавлен в must
						"minimum_should_match": 1,
					},
				},
			}
		}
		
	case pkg.AnalyticalQuery:
		// Аналитический запрос с агрегацией
		
		// Выбираем случайное поле для агрегации
		field := esLogFields[rand.Intn(len(esLogFields))]
		
		// Выбираем случайную функцию агрегации
		aggregationType := []string{"terms", "histogram"}[rand.Intn(2)]
		
		// Формируем фильтр
		filter := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{timeRange},
			},
		}
		
		// Добавляем дополнительное условие к фильтру
		if rand.Intn(2) == 0 {
			// Добавляем условие по уровню лога
			levels := []string{"info", "warn", "error", "debug", "critical"}
			level := levels[rand.Intn(len(levels))]
			
			filter["bool"].(map[string]interface{})["must"] = append(
				filter["bool"].(map[string]interface{})["must"].([]interface{}),
				map[string]interface{}{
					"term": map[string]interface{}{
						"level": level,
					},
				},
			)
		} else {
			// Добавляем условие по типу лога
			logTypes := []string{"web_access", "web_error", "application", "metric", "event"}
			logType := logTypes[rand.Intn(len(logTypes))]
			
			filter["bool"].(map[string]interface{})["must"] = append(
				filter["bool"].(map[string]interface{})["must"].([]interface{}),
				map[string]interface{}{
					"term": map[string]interface{}{
						"log_type": logType,
					},
				},
			)
		}
		
		// Формируем запрос в зависимости от типа агрегации
		var aggregation map[string]interface{}
		
		if aggregationType == "terms" {
			aggregation = map[string]interface{}{
				"field_stats": map[string]interface{}{
					"terms": map[string]interface{}{
						"field": field,
						"size":  20,
					},
				},
			}
		} else {
			// Для числовых полей
			if field == "status" || field == "error_code" {
				aggregation = map[string]interface{}{
					"field_stats": map[string]interface{}{
						"histogram": map[string]interface{}{
							"field":    field,
							"interval": 50,
						},
					},
				}
			} else {
				// Для других полей используем date_histogram если не можем использовать histogram
				aggregation = map[string]interface{}{
					"time_stats": map[string]interface{}{
						"date_histogram": map[string]interface{}{
							"field":    "timestamp",
							"interval": "1h",
						},
					},
				}
			}
		}
		
		query = map[string]interface{}{
			"query":       filter,
			"aggregations": aggregation,
			"size":        0, // Только агрегации, без документов
		}
		
	case pkg.TimeSeriesQuery:
		// Запрос временных рядов
		
		// Используем более короткий промежуток времени - последние 6 часов
		startTime = now.Add(-6 * time.Hour)
		
		// Обновляем фильтр по времени
		timeRange = map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": map[string]interface{}{
					"gte": startTime.Format(time.RFC3339),
					"lte": now.Format(time.RFC3339),
				},
			},
		}
		
		// Формируем фильтр
		filter := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{timeRange},
			},
		}
		
		// Добавляем дополнительное условие к фильтру
		if rand.Intn(2) == 0 {
			// Добавляем условие по сервису
			services := []string{"api", "auth", "frontend", "backend", "database"}
			service := services[rand.Intn(len(services))]
			
			filter["bool"].(map[string]interface{})["must"] = append(
				filter["bool"].(map[string]interface{})["must"].([]interface{}),
				map[string]interface{}{
					"term": map[string]interface{}{
						"service": service,
					},
				},
			)
		} else {
			// Добавляем условие по типу лога
			logTypes := []string{"web_access", "web_error", "application"}
			logType := logTypes[rand.Intn(len(logTypes))]
			
			filter["bool"].(map[string]interface{})["must"] = append(
				filter["bool"].(map[string]interface{})["must"].([]interface{}),
				map[string]interface{}{
					"term": map[string]interface{}{
						"log_type": logType,
					},
				},
			)
		}
		
		// Определяем интервал времени
		intervals := []string{"5m", "10m", "15m", "30m", "1h"}
		interval := intervals[rand.Intn(len(intervals))]
		
		// Выбираем поле для анализа
		fields := []string{"status", "error_code"}
		field := fields[rand.Intn(len(fields))]
		
		// Формируем запрос с date_histogram и вложенной агрегацией
		query = map[string]interface{}{
			"query": filter,
			"aggregations": map[string]interface{}{
				"time_buckets": map[string]interface{}{
					"date_histogram": map[string]interface{}{
						"field":    "timestamp",
						"interval": interval,
					},
					"aggregations": map[string]interface{}{
						"status_stats": map[string]interface{}{
							"terms": map[string]interface{}{
								"field": field,
								"size":  10,
							},
						},
					},
				},
			},
			"size": 0, // Только агрегации, без документов
		}
	}
	
	// Добавляем размер выборки для неаналитических запросов
	if queryType == pkg.SimpleQuery || queryType == pkg.ComplexQuery {
		query["size"] = 100
		query["track_total_hits"] = true
	}
	
	return query
}

// executeElasticsearchQuery выполняет запрос к Elasticsearch
func (e *ElasticsearchExecutor) executeElasticsearchQuery(ctx context.Context, query map[string]interface{}) (pkg.QueryResult, error) {
	// Сериализуем запрос в JSON
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка сериализации запроса: %v", err)
	}
	
	// Формируем URL запроса
	queryURL := fmt.Sprintf("%s/%s/_search", e.BaseURL, e.IndexName)
	
	// Создаем HTTP-запрос
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(queryJSON))
	if err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка создания запроса: %v", err)
	}
	
	// Устанавливаем заголовки
	req.Header.Set("Content-Type", "application/json")
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
		var errorResp ESErrorResponse
		if err := json.Unmarshal(body, &errorResp); err == nil {
			return pkg.QueryResult{}, fmt.Errorf("ошибка запроса: %s: %s", errorResp.Error.Type, errorResp.Error.Reason)
		}
		return pkg.QueryResult{}, fmt.Errorf("ошибка запроса: код %d, тело: %s", resp.StatusCode, string(body))
	}
	
	// Декодируем ответ
	var response ESSearchResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return pkg.QueryResult{}, fmt.Errorf("ошибка декодирования ответа: %v", err)
	}
	
	// Подсчитываем количество результатов
	hitCount := response.Hits.Total.Value
	
	// Создаем результат
	result := pkg.QueryResult{
		Duration:  duration,
		HitCount:  hitCount,
		BytesRead: int64(len(body)),
		Status:    fmt.Sprintf("success (%d ms)", response.Took),
	}
	
	return result, nil
}
