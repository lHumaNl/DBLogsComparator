package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// ElasticsearchExecutor query executor for Elasticsearch
type ElasticsearchExecutor struct {
	BaseURL   string
	Client    *http.Client
	Options   models.Options
	IndexName string
}

// ESSearchResponse represents the response from Elasticsearch
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

// ESErrorResponse represents an error from Elasticsearch
type ESErrorResponse struct {
	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
	Status int `json:"status"`
}

// Fixed keywords for searching in logs
var esQueryPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Fixed field names for searching
var esLogFields = []string{
	"log_type", "host", "container_name", "environment", "datacenter",
	"version", "level", "message", "service", "remote_addr",
	"request", "status", "http_referer", "error_code",
}

// NewElasticsearchExecutor creates a new query executor for Elasticsearch
func NewElasticsearchExecutor(baseURL string, options models.Options) *ElasticsearchExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}

	return &ElasticsearchExecutor{
		BaseURL:   baseURL,
		Client:    client,
		Options:   options,
		IndexName: "logs-*", // Using a mask for all log indices
	}
}

// GetSystemName returns the system name
func (e *ElasticsearchExecutor) GetSystemName() string {
	return "elasticsearch"
}

// ExecuteQuery executes a query of the specified type in Elasticsearch
func (e *ElasticsearchExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Create a random query of the specified type
	query := e.GenerateRandomQuery(queryType).(map[string]interface{})

	// Execute the query to Elasticsearch
	result, err := e.executeElasticsearchQuery(ctx, query)
	if err != nil {
		return models.QueryResult{}, err
	}

	return result, nil
}

// GenerateRandomQuery creates a random query of the specified type for Elasticsearch
func (e *ElasticsearchExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Common time range - last 24 hours
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	// For some queries, we take a shorter time range
	var query map[string]interface{}

	// Basic time filter
	timeRange := map[string]interface{}{
		"range": map[string]interface{}{
			"timestamp": map[string]interface{}{
				"gte": startTime.Format(time.RFC3339),
				"lte": now.Format(time.RFC3339),
			},
		},
	}

	switch queryType {
	case models.SimpleQuery:
		// Simple search by one field or keyword
		if rand.Intn(2) == 0 {
			// Search by keyword in message
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
			// Search by specific field
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

	case models.ComplexQuery:
		// Complex search with multiple conditions
		conditions := []interface{}{timeRange}

		// Random number of conditions from 2 to 4
		numConditions := 2 + rand.Intn(3)
		for i := 0; i < numConditions; i++ {
			// Select a random field
			field := esLogFields[rand.Intn(len(esLogFields))]

			// Random condition depending on the field type
			switch field {
			case "status", "error_code":
				// For numeric fields, use a range
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
				// For string fields, use different types of matchers
				if rand.Intn(3) == 0 {
					// Exact match
					value := fmt.Sprintf("value%d", rand.Intn(100))
					conditions = append(conditions, map[string]interface{}{
						"term": map[string]interface{}{
							field: value,
						},
					})
				} else if rand.Intn(2) == 0 {
					// Full-text search
					keyword := esQueryPhrases[rand.Intn(len(esQueryPhrases))]
					conditions = append(conditions, map[string]interface{}{
						"match": map[string]interface{}{
							field: keyword,
						},
					})
				} else {
					// Prefix search
					prefix := fmt.Sprintf("pref%d", rand.Intn(10))
					conditions = append(conditions, map[string]interface{}{
						"prefix": map[string]interface{}{
							field: prefix,
						},
					})
				}
			}
		}

		// Combine conditions using must/should
		if rand.Intn(10) < 7 {
			// In most cases, use must (AND)
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": conditions,
					},
				},
			}
		} else {
			// Sometimes use should with minimum_should_match (OR)
			query = map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []interface{}{
							timeRange,
						},
						"should":               conditions[1:], // First element - timeRange, already added to must
						"minimum_should_match": 1,
					},
				},
			}
		}

	case models.AnalyticalQuery:
		// Analytical query with aggregation

		// Select a random field for aggregation
		field := esLogFields[rand.Intn(len(esLogFields))]

		// Select a random aggregation function
		aggType := []string{"terms", "histogram", "date_histogram"}[rand.Intn(3)]

		// Form the filter
		filter := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					timeRange,
				},
			},
		}

		// Add additional condition to the filter
		if rand.Intn(2) == 0 {
			// Add condition by log level
			levels := []string{"info", "warn", "error", "debug", "trace"}
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
			// Add condition by log type
			logTypes := []string{"application", "system", "access", "error", "audit"}
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

		// Form the query depending on the aggregation type
		aggs := map[string]interface{}{}

		switch aggType {
		case "terms":
			aggs["agg1"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": field,
					"size":  10,
				},
			}
		case "histogram":
			if field == "status" || field == "error_code" {
				// For numeric fields
				aggs["agg1"] = map[string]interface{}{
					"histogram": map[string]interface{}{
						"field":    field,
						"interval": 50,
					},
				}
			} else {
				// For other fields use date_histogram if we can't use histogram
				aggs["agg1"] = map[string]interface{}{
					"date_histogram": map[string]interface{}{
						"field":    "timestamp",
						"interval": "1h",
					},
				}
			}
		case "date_histogram":
			aggs["agg1"] = map[string]interface{}{
				"date_histogram": map[string]interface{}{
					"field":    "timestamp",
					"interval": "1h",
				},
			}
		}

		query = map[string]interface{}{
			"query":        map[string]interface{}{"bool": filter},
			"aggregations": aggs,
			"size":         0, // Only aggregations, no documents
		}

	case models.TimeSeriesQuery:
		// Time series query

		// Use a shorter time range - last 6 hours
		startTime = now.Add(-6 * time.Hour)

		// Update the time filter
		timeRange = map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": map[string]interface{}{
					"gte": startTime.Format(time.RFC3339),
					"lte": now.Format(time.RFC3339),
				},
			},
		}

		// Form the filter
		filter := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					timeRange,
				},
			},
		}

		// Add additional condition to the filter
		if rand.Intn(2) == 0 {
			// Add condition by service
			services := []string{"api", "web", "auth", "db", "cache", "queue"}
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
			// Add condition by log type
			logTypes := []string{"application", "system", "access", "error", "audit"}
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

		// Define the time interval
		intervals := []string{"1m", "5m", "10m", "30m", "1h"}
		interval := intervals[rand.Intn(len(intervals))]

		// Define metrics to collect
		metrics := []string{"count", "avg", "max", "min"}
		metric := metrics[rand.Intn(len(metrics))]

		var metricAgg map[string]interface{}

		if metric == "count" {
			metricAgg = map[string]interface{}{
				"value_count": map[string]interface{}{
					"field": "_index",
				},
			}
		} else {
			numericFields := []string{"status", "response_time", "error_code", "cpu_usage", "memory_usage"}
			field := numericFields[rand.Intn(len(numericFields))]

			metricAgg = map[string]interface{}{
				metric: map[string]interface{}{
					"field": field,
				},
			}
		}

		query = map[string]interface{}{
			"query": map[string]interface{}{"bool": filter},
			"aggregations": map[string]interface{}{
				"time_buckets": map[string]interface{}{
					"date_histogram": map[string]interface{}{
						"field":    "timestamp",
						"interval": interval,
					},
					"aggregations": map[string]interface{}{
						"metric": metricAgg,
					},
				},
			},
			"size": 0,
		}
	}

	return query
}

// executeElasticsearchQuery executes a query to Elasticsearch
func (e *ElasticsearchExecutor) executeElasticsearchQuery(ctx context.Context, query map[string]interface{}) (models.QueryResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error marshaling query: %v", err)
	}

	url := fmt.Sprintf("%s/%s/_search", e.BaseURL, e.IndexName)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(queryJSON))
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	startTime := time.Now()
	resp, err := e.Client.Do(req)
	duration := time.Since(startTime)

	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode >= 400 {
		var errorResp ESErrorResponse
		if err := json.Unmarshal(body, &errorResp); err != nil {
			return models.QueryResult{}, fmt.Errorf("error parsing error response: %v", err)
		}
		return models.QueryResult{}, fmt.Errorf("elasticsearch error: %s - %s", errorResp.Error.Type, errorResp.Error.Reason)
	}

	var esResp ESSearchResponse
	if err := json.Unmarshal(body, &esResp); err != nil {
		return models.QueryResult{}, fmt.Errorf("error parsing response: %v", err)
	}

	result := models.QueryResult{
		Duration:    duration,
		ResultCount: esResp.Hits.Total.Value,
		RawResponse: body,
	}

	return result, nil
}
