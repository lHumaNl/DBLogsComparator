package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg"
	queriercommon "github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// ElasticsearchExecutor query executor for Elasticsearch
type ElasticsearchExecutor struct {
	BaseURL    string
	ClientPool *pkg.ClientPool // Use pool instead of single client
	Options    models.Options
	IndexName  string

	// Cache for label values
	labelCache map[string][]string

	// Available labels from common data
	availableLabels []string

	// Time range generator for realistic queries
	timeRangeGenerator *queriercommon.TimeRangeGenerator
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

// Constants for ES query generation
var esQueryPhrases = []string{
	"error", "warning", "exception", "failed", "timeout", "success",
	"completed", "running", "critical", "forbidden", "GET", "POST", "PUT", "DELETE",
}

// NewElasticsearchExecutor creates a new query executor for Elasticsearch
func NewElasticsearchExecutor(baseURL string, options models.Options) *ElasticsearchExecutor {
	// Create HTTP client pool with dynamic connection count
	clientPool := pkg.NewClientPool(options.ConnectionCount, options.Timeout)

	// Initialize with static labels from our common data package
	availableLabels := logdata.CommonLabels

	// Initialize label cache with static values
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}

	return &ElasticsearchExecutor{
		BaseURL:            baseURL,
		ClientPool:         clientPool,
		Options:            options,
		IndexName:          "logs-*", // Using a mask for all log indices
		labelCache:         labelCache,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGenerator(),
	}
}

// NewElasticsearchExecutorWithTimeConfig creates a new Elasticsearch executor with time configuration
func NewElasticsearchExecutorWithTimeConfig(baseURL string, options models.Options, timeConfig *common.TimeRangeConfig) *ElasticsearchExecutor {
	// Create HTTP client pool with dynamic connection count
	clientPool := pkg.NewClientPool(options.ConnectionCount, options.Timeout)
	// Initialize with static labels from our common data package
	availableLabels := logdata.CommonLabels
	// Initialize label cache with static values
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}
	return &ElasticsearchExecutor{
		BaseURL:            baseURL,
		ClientPool:         clientPool,
		Options:            options,
		IndexName:          "logs-*", // Using a mask for all log indices
		labelCache:         labelCache,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGeneratorWithConfig(timeConfig),
	}
}

// GetSystemName returns the system name
func (e *ElasticsearchExecutor) GetSystemName() string {
	return "elasticsearch"
}

// GenerateTimeRange generates a time range for queries (for error handling)
func (e *ElasticsearchExecutor) GenerateTimeRange() interface{} {
	return e.timeRangeGenerator.GenerateConfiguredTimeRange()
}

// ExecuteQuery executes a query of the specified type in Elasticsearch
func (e *ElasticsearchExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Generate time range for the query using the configured TimeRangeGenerator
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()

	// Create a random query of the specified type with the time range
	query := e.GenerateRandomQueryWithTimeRange(queryType, timeRange).(map[string]interface{})

	// Execute the query to Elasticsearch
	result, err := e.executeElasticsearchQuery(ctx, query)
	if err != nil {
		return models.QueryResult{}, err
	}

	// Populate time information in the result
	result.StartTime = timeRange.Start
	result.EndTime = timeRange.End
	result.TimeStringRepr = timeRange.StringRepr

	return result, nil
}

// GenerateRandomQuery generates a random Elasticsearch query based on the query type
// For compatibility, generates a time range internally
func (e *ElasticsearchExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Generate time range for the query using the configured time range generator
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()
	return e.GenerateRandomQueryWithTimeRange(queryType, timeRange)
}

// GenerateRandomQueryWithTimeRange generates a random Elasticsearch query with a specific time range
func (e *ElasticsearchExecutor) GenerateRandomQueryWithTimeRange(queryType models.QueryType, timeRange queriercommon.TimeRange) interface{} {
	startTime := timeRange.Start
	endTime := timeRange.End

	// Format times for Elasticsearch (ISO format)
	startTimeStr := startTime.Format(time.RFC3339)
	endTimeStr := endTime.Format(time.RFC3339)

	// Set reasonable limits for the query - make limit random
	limits := []string{"10", "50", "100", "200", "500", "1000"}
	limit := limits[rand.Intn(len(limits))] // Random limit for log queries

	// Convert limit to int
	limitInt, _ := strconv.Atoi(limit)

	// Create base query structure with time range filter
	baseQuery := map[string]interface{}{
		"size": limitInt,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte": startTimeStr,
								"lte": endTimeStr,
							},
						},
					},
				},
			},
		},
	}

	// Reference to the bool query's filters for easier access
	filters := baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"].([]interface{})

	// Depending on query type, add different filters and structure
	switch queryType {
	case models.SimpleQuery:
		baseQuery = e.generateSimpleQuery(baseQuery, filters)
	case models.ComplexQuery:
		baseQuery = e.generateComplexQuery(baseQuery, filters)
	case models.AnalyticalQuery:
		baseQuery = e.generateAnalyticalQuery(baseQuery, filters, limitInt)
	case models.TimeSeriesQuery:
		baseQuery = e.generateTimeSeriesQuery(baseQuery, filters)
	case models.StatQuery:
		baseQuery = e.generateStatisticalQuery(baseQuery, filters)
	case models.TopKQuery:
		baseQuery = e.generateTopKQuery(baseQuery, filters, limitInt)
	}

	return baseQuery
}

// generateSimpleQuery creates a simple query with a single condition
func (e *ElasticsearchExecutor) generateSimpleQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Simple query type - select from different simple scenarios
	querySubtype := rand.Intn(5)

	switch querySubtype {
	case 0:
		// Filter by log_type only
		logType := logdata.GetRandomLogType()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"log_type": logType,
			},
		})

	case 1:
		// Search by keyword in message field
		keyword := esQueryPhrases[rand.Intn(len(esQueryPhrases))]
		filters = append(filters, map[string]interface{}{
			"match": map[string]interface{}{
				"message": keyword,
			},
		})

	case 2:
		// Filter by log level
		level := logdata.GetRandomLogLevel()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"level": level,
			},
		})

	case 3:
		// Filter web_access logs by status code
		status := logdata.GetRandomHttpStatusCode()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"status": status,
			},
		})

	default:
		// Filter by container name (most likely to find logs)
		container := logdata.GetRandomContainer()
		filters = append(filters, map[string]interface{}{
			"wildcard": map[string]interface{}{
				"container_name": "*" + container + "*",
			},
		})
	}

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	// Ensure size parameter is included if not already present
	if _, ok := baseQuery["size"]; !ok {
		// Set reasonable limits for the query - make limit random
		limits := []int{10, 50, 100, 200, 500, 1000}
		limit := limits[rand.Intn(len(limits))] // Random limit for log queries
		baseQuery["size"] = limit
	}

	return baseQuery
}

// generateComplexQuery creates a complex query with multiple conditions
func (e *ElasticsearchExecutor) generateComplexQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Complex query with multiple filters based on log type
	logType := logdata.GetRandomLogType()

	// Add base log type filter
	filters = append(filters, map[string]interface{}{
		"term": map[string]interface{}{
			"log_type": logType,
		},
	})

	// Add type-specific conditions
	switch logType {
	case "web_access":
		// For web access logs, add filters based on HTTP status and methods
		if rand.Intn(2) == 0 {
			// Status range filter (2xx, 4xx, or 5xx)
			statuses := []int{200, 400, 500}
			baseStatus := statuses[rand.Intn(len(statuses))]
			filters = append(filters, map[string]interface{}{
				"range": map[string]interface{}{
					"status": map[string]interface{}{
						"gte": baseStatus,
						"lt":  baseStatus + 100,
					},
				},
			})
		} else {
			// HTTP method filter
			method := logdata.GetRandomHttpMethod()
			filters = append(filters, map[string]interface{}{
				"prefix": map[string]interface{}{
					"request": method + " ",
				},
			})
		}

		// Maybe add bytes_sent filter
		if rand.Intn(2) == 0 {
			minBytes := 1000 * rand.Intn(10)
			filters = append(filters, map[string]interface{}{
				"range": map[string]interface{}{
					"bytes_sent": map[string]interface{}{
						"gt": minBytes,
					},
				},
			})
		}

	case "web_error":
		// For web_error logs, add filters based on error levels and codes
		// Add level filter
		level := logdata.LogLevels[rand.Intn(2)+3] // "error" or "critical"
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"level": level,
			},
		})

		// Maybe add error_code filter
		if rand.Intn(2) == 0 {
			minErrorCode := 1000 + rand.Intn(1000)
			filters = append(filters, map[string]interface{}{
				"range": map[string]interface{}{
					"error_code": map[string]interface{}{
						"gt": minErrorCode,
					},
				},
			})
		}

	case "application":
		// For application logs, add filters based on service name, level, etc.
		// Add service filter
		service := logdata.GetRandomService()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"service": service,
			},
		})

		// Add level filter
		level := logdata.GetRandomLogLevel()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"level": level,
			},
		})

		// Maybe add response_status filter for service operations
		if rand.Intn(2) == 0 {
			status := logdata.GetRandomHttpStatusCode()
			filters = append(filters, map[string]interface{}{
				"term": map[string]interface{}{
					"response_status": status,
				},
			})
		}

	case "metric":
		// For metric logs, filter on metric name and values
		metricName := logdata.GetRandomMetricName()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"metric_name": metricName,
			},
		})

		// Maybe add value range filter
		if rand.Intn(2) == 0 {
			minValue := rand.Float64() * 100
			filters = append(filters, map[string]interface{}{
				"range": map[string]interface{}{
					"value": map[string]interface{}{
						"gt": minValue,
					},
				},
			})
		}

	case "event":
		// For event logs, filter on event type
		eventType := logdata.GetRandomEventType()
		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				"event_type": eventType,
			},
		})
	}

	// Maybe add keyword search to message
	if rand.Intn(2) == 0 {
		keyword := esQueryPhrases[rand.Intn(len(esQueryPhrases))]
		filters = append(filters, map[string]interface{}{
			"match": map[string]interface{}{
				"message": keyword,
			},
		})
	}

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	// Ensure size parameter is included if not already present
	if _, ok := baseQuery["size"]; !ok {
		// Set reasonable limits for the query - make limit random
		limits := []int{10, 50, 100, 200, 500, 1000}
		limit := limits[rand.Intn(len(limits))] // Random limit for log queries
		baseQuery["size"] = limit
	}

	return baseQuery
}

// executeElasticsearchQuery executes a query to Elasticsearch
func (e *ElasticsearchExecutor) executeElasticsearchQuery(ctx context.Context, query map[string]interface{}) (models.QueryResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return models.QueryResult{}, err
	}

	// Extract time range information from the query for logging
	timeRange := e.extractTimeRangeFromQuery(query)

	// Extract query limit if present
	var limit string
	if size, ok := query["size"].(float64); ok {
		limit = fmt.Sprintf("%d", int(size))
	} else if size, ok := query["size"].(int); ok {
		limit = fmt.Sprintf("%d", size)
	}

	// Extract query string for logging
	var queryString string

	// First handle the main query part
	if qObj, ok := query["query"].(map[string]interface{}); ok {
		if boolObj, ok := qObj["bool"].(map[string]interface{}); ok {
			if filters, ok := boolObj["filter"].([]interface{}); ok {
				// Extract only relevant filters (excluding range/timestamp)
				// Group similar filters by type (term, match, etc.)
				filterGroups := make(map[string]map[string]interface{})

				for _, filter := range filters {
					filterMap, ok := filter.(map[string]interface{})
					if !ok {
						continue
					}

					// Skip range filters which are typically for timestamp
					if _, hasRange := filterMap["range"]; hasRange {
						rangeMap := filterMap["range"].(map[string]interface{})
						// Keep non-timestamp ranges (like status ranges)
						if _, hasTimestamp := rangeMap["timestamp"]; hasTimestamp {
							continue
						}
					}

					// Process each filter type (term, match, etc.)
					for filterType, conditions := range filterMap {
						condMap, ok := conditions.(map[string]interface{})
						if !ok {
							continue
						}

						// Initialize the filter group if it doesn't exist
						if _, exists := filterGroups[filterType]; !exists {
							filterGroups[filterType] = make(map[string]interface{})
						}

						// Add all conditions to the appropriate filter group
						for field, value := range condMap {
							filterGroups[filterType][field] = value
						}
					}
				}

				// If we found any filters, format them as a compact JSON
				if len(filterGroups) > 0 {
					filtersJSON, err := json.Marshal(filterGroups)
					if err == nil {
						queryString = string(filtersJSON)
					}
				}
			}
		}
		if queryString == "" {
			if qJSON, err := json.Marshal(qObj); err == nil {
				queryString = string(qJSON)
			}
		}
	}

	// Check if there are aggregations and include them in the query string
	if aggs, ok := query["aggs"].(map[string]interface{}); ok && len(aggs) > 0 {
		// Create a composite query representation that includes both the filter and aggregation
		compositeQuery := make(map[string]interface{})

		// Add the filter part if we have it
		if queryString != "" {
			var filterObj map[string]interface{}
			if err := json.Unmarshal([]byte(queryString), &filterObj); err == nil {
				// Copy all keys except "bool" to the composite query
				for k, v := range filterObj {
					if k != "bool" {
						compositeQuery[k] = v
					}
				}
			}
		}

		// Add the aggregation part
		compositeQuery["aggs"] = aggs

		// If there's a size parameter and this is a top-k query, include it
		if size, ok := query["size"]; ok {
			compositeQuery["size"] = size
		}

		// Marshal it back to a string
		if compositeJSON, err := json.Marshal(compositeQuery); err == nil {
			queryString = string(compositeJSON)
		}
	} else if queryString != "" {
		// Even if there are no aggregations, still filter out the "bool" key
		var queryObj map[string]interface{}
		if err := json.Unmarshal([]byte(queryString), &queryObj); err == nil {
			// Remove the "bool" key if it exists
			delete(queryObj, "bool")

			// For simple and complex queries, make sure to include the size
			if size, ok := query["size"]; ok {
				queryObj["size"] = size
			}

			// Marshal it back to a string
			if cleanJSON, err := json.Marshal(queryObj); err == nil && len(queryObj) > 0 {
				queryString = string(cleanJSON)
			}
		}
	} else {
		// If we don't have a query string yet but we have a size, create a basic query representation
		if size, ok := query["size"]; ok {
			basicQuery := map[string]interface{}{
				"size": size,
			}
			if basicJSON, err := json.Marshal(basicQuery); err == nil {
				queryString = string(basicJSON)
			}
		}
	}

	// Extract step for time series queries if available
	var step string
	if aggs, ok := query["aggs"].(map[string]interface{}); ok {
		if dateHist, ok := aggs["date_histogram"].(map[string]interface{}); ok {
			if dateHistAgg, ok := dateHist["date_histogram"].(map[string]interface{}); ok {
				if interval, ok := dateHistAgg["interval"].(string); ok {
					step = interval
				} else if fixedInterval, ok := dateHistAgg["fixed_interval"].(string); ok {
					step = fixedInterval
				}
			}
		}
	}

	indexPattern := e.buildIndexPattern(timeRange.Start, timeRange.End)

	// Prepare request to Elasticsearch
	url := fmt.Sprintf("%s/%s/_search", e.BaseURL, indexPattern)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(queryJSON))
	if err != nil {
		return models.QueryResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Get execution start time
	startTime := time.Now()

	// Execute the request
	client := e.ClientPool.Get()
	resp, err := client.Do(req)
	if err != nil {
		return models.QueryResult{}, err
	}
	defer resp.Body.Close()

	// Calculate query duration
	duration := time.Since(startTime)

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, err
	}

	// Check for non-200 status
	if resp.StatusCode != http.StatusOK {
		// Check if it's an "index_not_found" error, which is common and can be handled gracefully
		if resp.StatusCode == http.StatusNotFound {
			var errorResp map[string]interface{}
			if err := json.Unmarshal(body, &errorResp); err == nil {
				if errObj, ok := errorResp["error"].(map[string]interface{}); ok {
					if errType, ok := errObj["type"].(string); ok && errType == "index_not_found_exception" {
						// This is a common case - the index doesn't exist for this time range
						// Just return an empty result instead of an error
						log.Printf("DEBUG: Index not found for pattern %s, returning empty result. Query: %s", indexPattern, queryString)
						return models.QueryResult{
							HitCount:       0,
							ResultCount:    0,
							Duration:       duration,
							RawResponse:    body,
							QueryString:    queryString,
							StartTime:      timeRange.Start,
							EndTime:        timeRange.End,
							Limit:          limit,
							Step:           step,
							TimeStringRepr: "", // Will be updated in querier.go
						}, nil
					}
				}
			}
		}
		return models.QueryResult{}, fmt.Errorf("elasticsearch error: %d %s. Query: %s", resp.StatusCode, string(body), queryString)
	}

	// Parse response
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return models.QueryResult{}, err
	}

	// Check for errors in response
	if errObj, ok := response["error"]; ok {
		errJSON, _ := json.Marshal(errObj)
		return models.QueryResult{}, fmt.Errorf("elasticsearch error: %s. Query: %s", string(errJSON), queryString)
	}

	// Extract found and total
	var hitCount int
	var resultCount int

	// Check if there are hits in the response
	if hits, ok := response["hits"].(map[string]interface{}); ok {
		// Extract total count
		if totalObj, ok := hits["total"].(map[string]interface{}); ok {
			if value, ok := totalObj["value"].(float64); ok {
				resultCount = int(value)
			}
		} else if value, ok := hits["total"].(float64); ok {
			resultCount = int(value)
		}

		// Extract documents found
		if hitsArr, ok := hits["hits"].([]interface{}); ok {
			hitCount = len(hitsArr)
		}
	}

	// Process aggregations if present
	if aggs, ok := response["aggregations"].(map[string]interface{}); ok && len(aggs) > 0 {
		aggCount := e.processAggregations(aggs)
		hitCount = int(aggCount)
		// For aggregation queries, total may not be reported properly
		if resultCount == 0 {
			resultCount = hitCount
		}
	}

	byteRead := int64(len(body))

	return models.QueryResult{
		HitCount:       hitCount,
		ResultCount:    resultCount,
		Duration:       duration,
		RawResponse:    body,
		QueryString:    queryString,
		StartTime:      timeRange.Start,
		EndTime:        timeRange.End,
		Limit:          limit,
		Step:           step,
		BytesRead:      byteRead,
		TimeStringRepr: "", // Will be updated in querier.go
	}, nil
}

// extractTimeRangeFromQuery extracts the time range from a query
func (e *ElasticsearchExecutor) extractTimeRangeFromQuery(query map[string]interface{}) queriercommon.TimeRange {
	var startTime, endTime time.Time

	// Default to now - 7 days to now if we can't extract
	endTime = time.Now()
	startTime = endTime.AddDate(0, 0, -7)

	// Try to extract time range from query
	if queryObj, ok := query["query"].(map[string]interface{}); ok {
		if boolObj, ok := queryObj["bool"].(map[string]interface{}); ok {
			if filters, ok := boolObj["filter"].([]interface{}); ok {
				for _, filter := range filters {
					filterMap, ok := filter.(map[string]interface{})
					if !ok {
						continue
					}

					if rangeObj, ok := filterMap["range"].(map[string]interface{}); ok {
						if timestampObj, ok := rangeObj["timestamp"].(map[string]interface{}); ok {
							if gteStr, ok := timestampObj["gte"].(string); ok {
								if t, err := time.Parse(time.RFC3339, gteStr); err == nil {
									startTime = t
								}
							}
							if lteStr, ok := timestampObj["lte"].(string); ok {
								if t, err := time.Parse(time.RFC3339, lteStr); err == nil {
									endTime = t
								}
							}
						}
					}
				}
			}
		}
	}

	return queriercommon.TimeRange{Start: startTime, End: endTime}
}

// buildIndexPattern builds Elasticsearch index pattern based on time range
// This supports both static patterns (logs-*) and time-based patterns (logs-YYYY.MM.DD)
func (e *ElasticsearchExecutor) buildIndexPattern(startTime, endTime time.Time) string {
	// For simplicity, we'll use a fixed pattern for all queries
	// In production, you might want to use time-based patterns for efficiency
	return "logs-*"
}

// generateAnalyticalQuery creates an analytical query with aggregations
func (e *ElasticsearchExecutor) generateAnalyticalQuery(baseQuery map[string]interface{}, filters []interface{}, limit int) map[string]interface{} {
	// Analytical queries with aggregations based on log type
	logType := logdata.GetRandomLogType()

	// Add base log type filter
	filters = append(filters, map[string]interface{}{
		"term": map[string]interface{}{
			"log_type": logType,
		},
	})

	// Add aggregations based on log type
	aggregations := make(map[string]interface{})

	switch logType {
	case "web_access":
		if rand.Intn(2) == 0 {
			// Count by status
			aggregations["status_counts"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "status",
					"size":  limit,
				},
			}
		} else {
			// Average bytes sent by status
			aggregations["bytes_by_status"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "status",
					"size":  limit,
				},
				"aggs": map[string]interface{}{
					"avg_bytes": map[string]interface{}{
						"avg": map[string]interface{}{"field": "bytes_sent"},
					},
				},
			}
		}

	case "web_error":
		if rand.Intn(2) == 0 {
			// Count by error_code
			aggregations["error_code_counts"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "error_code",
					"size":  limit,
				},
			}
		} else {
			// Count by level
			aggregations["level_counts"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "level",
					"size":  limit,
				},
			}
		}

	case "application":
		if rand.Intn(2) == 0 {
			// Count by service
			aggregations["service_counts"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "service",
					"size":  limit,
				},
			}
		} else {
			// Count by level
			aggregations["level_counts"] = map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "level",
					"size":  limit,
				},
			}
		}

	case "metric":
		// Average value by metric_name
		aggregations["metric_values"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": "metric_name",
				"size":  limit,
			},
			"aggs": map[string]interface{}{
				"avg_value": map[string]interface{}{
					"avg": map[string]interface{}{"field": "value"},
				},
			},
		}

	case "event":
		// Count by event_type
		aggregations["event_type_counts"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": "event_type",
				"size":  limit,
			},
		}

	default:
		// Generic count by log_type
		aggregations["log_type_counts"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": "log_type",
				"size":  limit,
			},
		}
	}

	// Add percentiles aggregation for numerical fields (40% chance)
	if rand.Intn(10) < 4 {
		numericFields := []string{"response_time", "latency", "bytes_sent", "duration", "cpu", "memory"}
		field := numericFields[rand.Intn(len(numericFields))]

		// Generate unique random percentiles
		numPercentiles := rand.Intn(3) + 2 // 2-4 percentiles
		uniqueQuantiles := queriercommon.GetUniqueRandomQuantiles(numPercentiles)
		percentiles := make([]float64, len(uniqueQuantiles))
		for i, q := range uniqueQuantiles {
			percentiles[i] = q * 100 // Convert to percentage for ES
		}

		aggregations[field+"_percentiles"] = map[string]interface{}{
			"percentiles": map[string]interface{}{
				"field":    field,
				"percents": percentiles,
			},
		}
	}

	// Add aggregations to query
	baseQuery["size"] = 0 // Don't need documents, just aggregations
	baseQuery["aggs"] = aggregations

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters
	return baseQuery
}

// generateTimeSeriesQuery creates a time series query with date histogram aggregation
func (e *ElasticsearchExecutor) generateTimeSeriesQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Date histogram aggregation by minute for one metric
	metricName := logdata.GetRandomMetricName()

	filters = append(filters, map[string]interface{}{
		"term": map[string]interface{}{"metric_name": metricName},
	})

	baseQuery["aggs"] = map[string]interface{}{
		"per_min": map[string]interface{}{
			"date_histogram": map[string]interface{}{
				"field":          "timestamp",
				"fixed_interval": "1m",
			},
			"aggs": map[string]interface{}{
				"avg_value": map[string]interface{}{
					"avg": map[string]interface{}{"field": "value"},
				},
			},
		},
	}

	baseQuery["size"] = 0

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters
	return baseQuery
}

// generateStatisticalQuery creates a statistical query with aggregations
func (e *ElasticsearchExecutor) generateStatisticalQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Define available aggregation functions for Elasticsearch
	aggFunctions := []string{
		"avg", "min", "max", "sum", "cardinality", "value_count", "percentiles",
	}

	// Choose a random aggregation function
	aggFunction := aggFunctions[rand.Intn(len(aggFunctions))]

	// Fields that we can use for metrics
	metricFields := []string{
		"bytes", "duration", "latency", "size", "status_code",
		"response_time", "timestamp", "http.response.body.bytes",
	}

	// Choose a random field to aggregate on
	metricField := metricFields[rand.Intn(len(metricFields))]

	// Add a filter based on log_type to increase chance of matches
	logType := logdata.GetRandomLogType()
	filters = append(filters, map[string]interface{}{
		"term": map[string]interface{}{"log_type": logType},
	})

	// Add another filter based on a different label to make it more complex
	selectedLabel := logdata.GetRandomLabel()
	if selectedLabel != "log_type" {
		labelValues := e.labelCache[selectedLabel]
		if len(labelValues) > 0 {
			selectedValue := labelValues[rand.Intn(len(labelValues))]
			filters = append(filters, map[string]interface{}{
				"term": map[string]interface{}{selectedLabel: selectedValue},
			})
		}
	}

	// Set the query size to 0 because we only need aggregation results
	baseQuery["size"] = 0

	// Create the aggregation structure
	var aggregation map[string]interface{}

	// 30% chance to use a date histogram with a statistical aggregation
	if rand.Intn(10) < 3 {
		// Date histogram with statistics
		intervals := []string{"1m", "5m", "10m", "30m", "1h"}
		interval := intervals[rand.Intn(len(intervals))]

		aggregation = map[string]interface{}{
			"time_buckets": map[string]interface{}{
				"date_histogram": map[string]interface{}{
					"field":          "timestamp",
					"fixed_interval": interval,
				},
				"aggs": map[string]interface{}{
					"stat_value": func() map[string]interface{} {
						if aggFunction == "percentiles" {
							// Use unique random percentiles for date histogram too
							numPercentiles := rand.Intn(3) + 1 // 1-3 percentiles
							uniqueQuantiles := queriercommon.GetUniqueRandomQuantiles(numPercentiles)
							percentiles := make([]float64, len(uniqueQuantiles))
							for i, q := range uniqueQuantiles {
								percentiles[i] = q * 100 // Convert to percentage for ES
							}
							return map[string]interface{}{
								"percentiles": map[string]interface{}{
									"field":    metricField,
									"percents": percentiles,
								},
							}
						}
						return map[string]interface{}{
							aggFunction: map[string]interface{}{
								"field": metricField,
							},
						}
					}(),
				},
			},
		}
	} else {
		// Simple statistical aggregation
		if aggFunction == "percentiles" {
			// Special handling for percentiles aggregation - use unique random percentiles
			numPercentiles := rand.Intn(3) + 1 // 1-3 percentiles
			uniqueQuantiles := queriercommon.GetUniqueRandomQuantiles(numPercentiles)
			percentiles := make([]float64, len(uniqueQuantiles))
			for i, q := range uniqueQuantiles {
				percentiles[i] = q * 100 // Convert to percentage for ES
			}

			aggregation = map[string]interface{}{
				"stat_result": map[string]interface{}{
					"percentiles": map[string]interface{}{
						"field":    metricField,
						"percents": percentiles,
					},
				},
			}
		} else {
			aggregation = map[string]interface{}{
				"stat_result": map[string]interface{}{
					aggFunction: map[string]interface{}{
						"field": metricField,
					},
				},
			}
		}
	}

	// Add aggregation to the query
	baseQuery["aggs"] = aggregation

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	return baseQuery
}

// generateTopKQuery creates a top K query with terms aggregation
func (e *ElasticsearchExecutor) generateTopKQuery(baseQuery map[string]interface{}, filters []interface{}, limit int) map[string]interface{} {
	// Define possible values for k (how many top results) - use the limit parameter instead of hardcoded values
	topKValues := []int{5, 10, 15, 20, 50, 100, 500, 1000}
	k := topKValues[rand.Intn(len(topKValues))]

	// Use the larger of the random k value or the limit parameter if it's > 0
	if limit > 0 && limit > k {
		k = limit
	}

	// Available fields for top-K operation - use common logdata definitions
	availableFields := []string{
		"service",
		"level",
		"method",
		"status",
		"log_type",
		"http_method",
	}

	// Select a field to get top values for
	field := availableFields[rand.Intn(len(availableFields))]

	// Make sure we use the keyword type for text fields in Elasticsearch
	esField := field
	if field != "level" && field != "status" && field != "log_type" {
		esField += ".keyword"
	}

	// Randomly decide if we should add a filter (50% chance)
	if len(filters) > 0 && rand.Intn(2) == 0 {
		// Sometimes add an additional filter on a different field (50% chance)
		filterField := availableFields[rand.Intn(len(availableFields))]

		// Avoid using the same field for filtering and aggregation
		for filterField == field {
			filterField = availableFields[rand.Intn(len(availableFields))]
		}

		// Get filter values based on the field
		var filterValue string
		switch filterField {
		case "service":
			services := logdata.Services
			if len(services) > 0 {
				filterValue = services[rand.Intn(len(services))]
			}
		case "level":
			levels := logdata.LogLevels
			if len(levels) > 0 {
				filterValue = levels[rand.Intn(len(levels))]
			}
		case "log_type":
			logTypes := logdata.LogTypes
			if len(logTypes) > 0 {
				filterValue = logTypes[rand.Intn(len(logTypes))]
			}
		case "status":
			statuses := logdata.HttpStatusCodes
			if len(statuses) > 0 {
				filterValue = fmt.Sprintf("%d", statuses[rand.Intn(len(statuses))])
			}
		case "http_method":
			methods := logdata.HttpMethods
			if len(methods) > 0 {
				filterValue = methods[rand.Intn(len(methods))]
			}
		}

		if filterValue != "" {
			// Add field filter to the existing filters
			esFilterField := filterField
			if filterField != "level" && filterField != "status" && filterField != "log_type" {
				esFilterField += ".keyword"
			}

			termFilter := map[string]interface{}{
				"term": map[string]interface{}{
					esFilterField: filterValue,
				},
			}
			filters = append(filters, termFilter)
		}
	}

	// Create aggregation for top-K values
	aggregations := map[string]interface{}{
		"top_values": map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esField,
				"order": map[string]interface{}{
					"_count": "desc",
				},
				"size": k,
			},
		},
	}

	// Add aggregation to the query
	baseQuery["aggs"] = aggregations

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	return baseQuery
}

// processAggregations extracts counts from Elasticsearch aggregation results
func (e *ElasticsearchExecutor) processAggregations(aggs map[string]interface{}) int64 {
	var count int64

	for _, v := range aggs {
		aggObj, ok := v.(map[string]interface{})
		if !ok {
			continue
		}

		// Process buckets from terms aggregation
		if buckets, ok := aggObj["buckets"].([]interface{}); ok {
			count += int64(len(buckets))
		}

		// Process date histogram aggregation
		if perMinBuckets, ok := aggObj["buckets"].([]interface{}); ok {
			count += int64(len(perMinBuckets))
		}

		// Process sub-aggregations recursively
		if subAggs, ok := aggObj["aggs"].(map[string]interface{}); ok {
			count += e.processAggregations(subAggs)
		}

		// Look for other aggregations like avg, sum, stats, etc.
		for _, field := range []string{"value", "avg", "sum", "min", "max"} {
			if val, ok := aggObj[field]; ok && val != nil {
				count++ // Count the statistic itself
				break
			}
		}
	}

	return count
}
