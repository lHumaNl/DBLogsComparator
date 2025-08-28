package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// Constants for ES query generation - moved to logdata.ElasticsearchQueryPhrases

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
		return result, err
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

	// Set reasonable limits for the query - use unified limits
	limits := logdata.ElasticsearchLimits
	limit := strconv.Itoa(limits[logdata.RandomIntn(len(limits))]) // Random limit for log queries

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

// generateSimpleQuery creates a simple query using unified strategy
func (e *ElasticsearchExecutor) generateSimpleQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Use unified simple query generation
	params := logdata.GenerateUnifiedSimpleQuery()

	// UNIFIED STRATEGY: SimpleQuery must only use positive operators (= or =~)
	// If we get a negative operator, retry to get a positive one
	maxRetries := 5
	for i := 0; i < maxRetries && (params.Operator == "!=" || params.Operator == "!~"); i++ {
		params = logdata.GenerateUnifiedSimpleQuery()
	}

	// If still negative after retries, convert to positive
	if params.Operator == "!=" {
		params.Operator = "="
	} else if params.Operator == "!~" {
		params.Operator = "=~"
	}

	// Handle message search specially
	if params.Field == "message" {
		filters = append(filters, map[string]interface{}{
			"match": map[string]interface{}{
				"message": params.Value,
			},
		})
	} else {
		// Handle regular field searches - only positive operators now
		fieldType, fieldExists := logdata.FieldTypeMap[params.Field]

		// Check if this is a regex query on an integer field
		if (params.IsRegex || params.Operator == "=~") && fieldExists && fieldType == logdata.IntField {
			// Convert regex pattern to range query for integer fields
			if regexStr, ok := params.Value.(string); ok {
				rangeCondition := e.convertRegexToRange(params.Field, regexStr)
				filter := map[string]interface{}{
					"range": map[string]interface{}{
						params.Field: rangeCondition,
					},
				}
				filters = append(filters, filter)
			} else {
				// Fallback to exact term if conversion fails
				filter := map[string]interface{}{
					"term": map[string]interface{}{
						params.Field: params.Value,
					},
				}
				filters = append(filters, filter)
			}
		} else {
			// Handle string fields - use different query types based on field type
			var queryType string
			var esField string

			if fieldExists && fieldType == logdata.StringField {
				if logdata.IsElasticsearchKeywordField(params.Field) {
					// CommonLabels use keyword field with term/regexp queries
					esField = params.Field + ".keyword"
					if params.IsRegex || params.Operator == "=~" {
						queryType = "regexp"
					} else {
						queryType = "term"
					}
				} else {
					// Non-CommonLabels use text field with match queries
					esField = params.Field
					queryType = "match"
				}
			} else {
				// Non-string fields use term/regexp as before
				esField = params.Field
				if params.IsRegex || params.Operator == "=~" {
					queryType = "regexp"
				} else {
					queryType = "term"
				}
			}

			filter := map[string]interface{}{}
			filter[queryType] = map[string]interface{}{
				esField: params.Value,
			}
			filters = append(filters, filter)
		}
	}

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	// Ensure size parameter is included if not already present
	if _, ok := baseQuery["size"]; !ok {
		// Set reasonable limits for the query - use unified limits
		limits := logdata.ElasticsearchLimits
		limit := limits[logdata.RandomIntn(len(limits))] // Random limit for log queries
		baseQuery["size"] = limit
	}

	return baseQuery
}

// generateComplexQuery creates a complex query using unified strategy
func (e *ElasticsearchExecutor) generateComplexQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Use unified complex query generation
	params := logdata.GenerateUnifiedComplexQuery()

	// UNIFIED STRATEGY: Validate that we have minimum requirements
	// ComplexQuery must have 2+ conditions with at least 1 positive
	if len(params.Fields) < 2 {
		// Regenerate if we don't have enough fields
		params = logdata.GenerateUnifiedComplexQuery()
		for len(params.Fields) < 2 {
			params = logdata.GenerateUnifiedComplexQuery()
		}
	}

	// Ensure at least one positive condition exists
	hasPositive := false
	for i := range params.Operators {
		if params.Operators[i] == "=" || params.Operators[i] == "=~" {
			hasPositive = true
			break
		}
	}

	// If no positive conditions, convert the first one to positive
	if !hasPositive {
		if params.Operators[0] == "!=" {
			params.Operators[0] = "="
		} else if params.Operators[0] == "!~" {
			params.Operators[0] = "=~"
		}
	}

	// Create main bool query structure based on logic operator
	var mainBool map[string]interface{}
	var filtersList []interface{}
	var mustNotList []interface{}

	// Process each field in the complex query
	for i, field := range params.Fields {
		value := params.Values[i]
		operator := params.Operators[i]
		isRegex := params.IsRegex[i]
		isNegated := params.NegationMask[i]

		// Create filter for this field (same logic as SimpleQuery)
		var filter map[string]interface{}

		// Handle message search specially
		if field == "message" {
			filter = map[string]interface{}{
				"match": map[string]interface{}{
					"message": value,
				},
			}
		} else {
			// Handle regular field searches
			fieldType, fieldExists := logdata.FieldTypeMap[field]

			// Check if this is a regex query on an integer field
			if (isRegex || operator == "=~" || operator == "!~") && fieldExists && fieldType == logdata.IntField {
				// Convert regex pattern to range query for integer fields
				if regexStr, ok := value.(string); ok {
					rangeCondition := e.convertRegexToRange(field, regexStr)
					filter = map[string]interface{}{
						"range": map[string]interface{}{
							field: rangeCondition,
						},
					}
				} else {
					// Fallback to exact term if conversion fails
					filter = map[string]interface{}{
						"term": map[string]interface{}{
							field: value,
						},
					}
				}
			} else {
				// Handle string fields - use different query types based on field type
				var queryType string
				var esField string

				if fieldExists && fieldType == logdata.StringField {
					if logdata.IsElasticsearchKeywordField(field) {
						// CommonLabels use keyword field with term/regexp queries
						esField = field + ".keyword"
						if isRegex || operator == "=~" || operator == "!~" {
							queryType = "regexp"
						} else {
							queryType = "term"
						}
					} else {
						// Non-CommonLabels use text field with match queries
						esField = field
						queryType = "match"
					}
				} else {
					// Non-string fields use term/regexp as before
					esField = field
					if isRegex || operator == "=~" || operator == "!~" {
						queryType = "regexp"
					} else {
						queryType = "term"
					}
				}

				filter = map[string]interface{}{
					queryType: map[string]interface{}{
						esField: value,
					},
				}
			}
		}

		// Apply negation - simplified for fair comparison (only AND logic)
		if isNegated || operator == "!=" || operator == "!~" {
			// Add to must_not
			mustNotList = append(mustNotList, filter)
		} else {
			// FAIRNESS: Only AND logic for fair comparison across all systems
			// Loki doesn't support OR in label selectors, so we use only AND everywhere
			filtersList = append(filtersList, filter)
		}
	}

	// Add message search component if present
	if params.MessageSearch != nil {
		msgFilter := map[string]interface{}{
			"match": map[string]interface{}{
				"message": params.MessageSearch.Value,
			},
		}

		if params.MessageSearch.Operator == "!=" || params.MessageSearch.Operator == "!~" {
			mustNotList = append(mustNotList, msgFilter)
		} else {
			filtersList = append(filtersList, msgFilter) // Always AND for message search
		}
	}

	// Build the final bool query structure - simplified for fair comparison
	mainBool = map[string]interface{}{}

	if len(filtersList) > 0 {
		mainBool["filter"] = append(filters, filtersList...)
	} else {
		mainBool["filter"] = filters
	}

	// FAIRNESS: No OR logic for fair comparison - only AND and must_not
	if len(mustNotList) > 0 {
		mainBool["must_not"] = mustNotList
	}

	// Update the bool query in base query
	baseQuery["query"].(map[string]interface{})["bool"] = mainBool

	// Ensure size parameter is included if not already present
	if _, ok := baseQuery["size"]; !ok {
		// Set reasonable limits for the query - use unified limits
		limits := logdata.ElasticsearchLimits
		limit := limits[logdata.RandomIntn(len(limits))]
		baseQuery["size"] = limit
	}

	return baseQuery
}

// executeElasticsearchQuery executes a query to Elasticsearch
func (e *ElasticsearchExecutor) executeElasticsearchQuery(ctx context.Context, query map[string]interface{}) (models.QueryResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return models.QueryResult{
			QueryString: fmt.Sprintf("%v", query),
		}, err
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

	// Extract query string for logging - reuse already marshaled JSON
	queryString := string(queryJSON)

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
		return models.QueryResult{
			QueryString: string(queryJSON),
		}, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Get execution start time
	startTime := time.Now()

	// Execute the request
	client := e.ClientPool.Get()
	resp, err := client.Do(req)
	if err != nil {
		return models.QueryResult{
			QueryString: string(queryJSON),
		}, err
	}
	defer resp.Body.Close()

	// Calculate query duration
	duration := time.Since(startTime)

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{
			QueryString: string(queryJSON),
		}, err
	}

	// Debug mode: output response body
	if e.Options.Debug {
		fmt.Printf("DEBUG [Elasticsearch Response]: %s\n", string(body))
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
		return models.QueryResult{
			QueryString: queryString,
		}, fmt.Errorf("elasticsearch error: %d %s. Query: %s", resp.StatusCode, string(body), queryString)
	}

	// Parse response
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return models.QueryResult{
			QueryString: string(queryJSON),
		}, err
	}

	// Check for errors in response
	if errObj, ok := response["error"]; ok {
		errJSON, _ := json.Marshal(errObj)
		return models.QueryResult{
			QueryString: queryString,
		}, fmt.Errorf("elasticsearch error: %s. Query: %s", string(errJSON), queryString)
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
	// Use unified analytical query generation for maximum fairness
	params := logdata.GenerateUnifiedAnalyticalQuery()

	// Add base filter using unified parameters
	// Prepare filter field for Elasticsearch - use keyword field only for CommonLabels
	esFilterField := params.FilterField
	if fieldType, exists := logdata.FieldTypeMap[params.FilterField]; exists && fieldType == logdata.StringField {
		if logdata.IsElasticsearchKeywordField(params.FilterField) {
			esFilterField += ".keyword"
		}
		// For other string fields, use the field as-is (text type)
	}

	// Use appropriate query type based on field classification
	var filterQuery map[string]interface{}
	if fieldType, exists := logdata.FieldTypeMap[params.FilterField]; exists && fieldType == logdata.StringField {
		if logdata.IsElasticsearchKeywordField(params.FilterField) {
			// CommonLabels use term query with .keyword field
			filterQuery = map[string]interface{}{
				"term": map[string]interface{}{
					esFilterField: params.FilterValue,
				},
			}
		} else {
			// Non-CommonLabels use match query with text field
			filterQuery = map[string]interface{}{
				"match": map[string]interface{}{
					params.FilterField: params.FilterValue,
				},
			}
		}
	} else {
		// Non-string fields use term query
		filterQuery = map[string]interface{}{
			"term": map[string]interface{}{
				params.FilterField: params.FilterValue,
			},
		}
	}
	filters = append(filters, filterQuery)

	// Add time window filter for fair comparison with Loki's required time windows
	// This ensures all systems use same time scope for aggregations
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
	filters = append(filters, map[string]interface{}{
		"range": map[string]interface{}{
			"timestamp": map[string]interface{}{
				"gte": fmt.Sprintf("now-%s", timeWindow),
				"lte": "now",
			},
		},
	})

	// Build aggregations based on unified parameters
	aggregations := make(map[string]interface{})

	// For Elasticsearch aggregations, use only CommonLabels (keyword fields)
	// If the selected field is not a CommonLabel, pick a random CommonLabel instead
	esGroupByField := params.GroupByField
	if !logdata.IsElasticsearchKeywordField(params.GroupByField) {
		// Replace with a random CommonLabel for Elasticsearch compatibility
		commonLabels := logdata.GetElasticsearchKeywordFields()
		esGroupByField = commonLabels[logdata.RandomIntn(len(commonLabels))]
	}
	esGroupByField += ".keyword"

	switch params.AggregationType {
	case "count":
		// Count aggregation with grouping and limit
		aggregations["grouped_counts"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
		}

	case "avg":
		// Average aggregation with grouping and limit
		aggregations["grouped_avg"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"avg_value": map[string]interface{}{
					"avg": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "min":
		// Minimum aggregation with grouping and limit
		aggregations["grouped_min"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"min_value": map[string]interface{}{
					"min": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "max":
		// Maximum aggregation with grouping and limit
		aggregations["grouped_max"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"max_value": map[string]interface{}{
					"max": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "sum":
		// Sum aggregation with grouping and limit
		aggregations["grouped_sum"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"sum_value": map[string]interface{}{
					"sum": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "quantile":
		// Quantile aggregation with grouping for consistency with other systems
		aggregations["grouped_quantile"] = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"quantile_value": map[string]interface{}{
					"percentiles": map[string]interface{}{
						"field":    params.NumericField,
						"percents": []float64{params.Quantile * 100}, // Convert to percentage for ES
					},
				},
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
	// Use unified time series query generation for maximum fairness across all systems
	params := logdata.GenerateUnifiedTimeSeriesQuery()

	// Add base filter using unified parameters
	// Prepare filter field for Elasticsearch - use keyword field only for CommonLabels
	esFilterField := params.FilterField
	if fieldType, exists := logdata.FieldTypeMap[params.FilterField]; exists && fieldType == logdata.StringField {
		if logdata.IsElasticsearchKeywordField(params.FilterField) {
			esFilterField += ".keyword"
		}
		// For other string fields, use the field as-is (text type)
	}

	filters = append(filters, map[string]interface{}{
		"term": map[string]interface{}{esFilterField: params.FilterValue},
	})

	// Create time series aggregation using unified parameters
	// For Elasticsearch aggregations, use only CommonLabels (keyword fields)
	// If the selected field is not a CommonLabel, pick a random CommonLabel instead
	esGroupByField := params.GroupByField
	if !logdata.IsElasticsearchKeywordField(params.GroupByField) {
		// Replace with a random CommonLabel for Elasticsearch compatibility
		commonLabels := logdata.GetElasticsearchKeywordFields()
		esGroupByField = commonLabels[logdata.RandomIntn(len(commonLabels))]
	}
	esGroupByField += ".keyword"

	// Build aggregations based on unified aggregation type
	var subAggregation map[string]interface{}

	switch params.AggregationType {
	case "count":
		// Count aggregation with terms grouping
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
		}

	case "avg":
		// Average aggregation with terms grouping and numeric field
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"avg_value": map[string]interface{}{
					"avg": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "min":
		// Minimum aggregation with terms grouping and numeric field
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"min_value": map[string]interface{}{
					"min": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "max":
		// Maximum aggregation with terms grouping and numeric field
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"max_value": map[string]interface{}{
					"max": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "sum":
		// Sum aggregation with terms grouping and numeric field
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"sum_value": map[string]interface{}{
					"sum": map[string]interface{}{
						"field": params.NumericField,
					},
				},
			},
		}

	case "quantile":
		// Quantile aggregation with terms grouping and numeric field
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
			"aggs": map[string]interface{}{
				"quantile_value": map[string]interface{}{
					"percentiles": map[string]interface{}{
						"field":    params.NumericField,
						"percents": []float64{params.Quantile * 100},
					},
				},
			},
		}

	default:
		// Fallback to count
		subAggregation = map[string]interface{}{
			"terms": map[string]interface{}{
				"field": esGroupByField,
				"size":  params.Limit,
			},
		}
	}

	baseQuery["aggs"] = map[string]interface{}{
		"per_time": map[string]interface{}{
			"date_histogram": map[string]interface{}{
				"field":          "timestamp",
				"fixed_interval": params.TimeInterval,
			},
			"aggs": map[string]interface{}{
				"by_field": subAggregation,
			},
		},
	}

	baseQuery["size"] = 0

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters
	return baseQuery
}

// generateStatisticalQuery creates a statistical query using unified approach
// Returns single global statistic (no grouping) for fair comparison
func (e *ElasticsearchExecutor) generateStatisticalQuery(baseQuery map[string]interface{}, filters []interface{}) map[string]interface{} {
	// Use unified stat query generation
	params := logdata.GenerateUnifiedStatQuery()

	// Add base filter using unified field and value
	if params.FilterField == "message" {
		// Handle message search specially
		filters = append(filters, map[string]interface{}{
			"match": map[string]interface{}{
				"message": params.FilterValue,
			},
		})
	} else {
		// Handle regular field filters
		// Prepare filter field for Elasticsearch (add .keyword for string fields)
		esFilterField := params.FilterField
		if fieldType, exists := logdata.FieldTypeMap[params.FilterField]; exists && fieldType == logdata.StringField {
			esFilterField += ".keyword"
		}

		filters = append(filters, map[string]interface{}{
			"term": map[string]interface{}{
				esFilterField: params.FilterValue,
			},
		})
	}

	// Add time window filter for fair comparison with Loki's required time windows
	// This ensures all systems use same time scope for statistical operations
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
	filters = append(filters, map[string]interface{}{
		"range": map[string]interface{}{
			"timestamp": map[string]interface{}{
				"gte": fmt.Sprintf("now-%s", timeWindow),
				"lte": "now",
			},
		},
	})

	// Set the query size to 0 because we only need aggregation results
	baseQuery["size"] = 0

	// Create the aggregation structure based on stat type
	var aggregation map[string]interface{}

	switch params.StatType {
	case "count":
		// No aggregation needed for count - just filter and count
		aggregation = nil

	case "avg":
		aggregation = map[string]interface{}{
			"global_stat": map[string]interface{}{
				"avg": map[string]interface{}{
					"field": params.NumericField,
				},
			},
		}

	case "sum":
		aggregation = map[string]interface{}{
			"global_stat": map[string]interface{}{
				"sum": map[string]interface{}{
					"field": params.NumericField,
				},
			},
		}

	case "min":
		aggregation = map[string]interface{}{
			"global_stat": map[string]interface{}{
				"min": map[string]interface{}{
					"field": params.NumericField,
				},
			},
		}

	case "max":
		aggregation = map[string]interface{}{
			"global_stat": map[string]interface{}{
				"max": map[string]interface{}{
					"field": params.NumericField,
				},
			},
		}

	case "quantile":
		// Quantile aggregation using percentiles
		aggregation = map[string]interface{}{
			"global_stat": map[string]interface{}{
				"percentiles": map[string]interface{}{
					"field":    params.NumericField,
					"percents": []float64{params.Quantile * 100}, // Convert to percentage for ES
				},
			},
		}
	}

	// Add aggregation to the query if needed
	if aggregation != nil {
		baseQuery["aggs"] = aggregation
	}

	// Update the filters in the base query
	baseQuery["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = filters

	return baseQuery
}

// generateTopKQuery creates a top K query with terms aggregation using unified parameters
func (e *ElasticsearchExecutor) generateTopKQuery(baseQuery map[string]interface{}, filters []interface{}, limit int) map[string]interface{} {
	// Generate unified TopK parameters for maximum fairness
	params := logdata.GenerateUnifiedTopKQuery()

	// Use the larger of unified K value or limit parameter if it's > 0
	k := params.K
	if limit > 0 && limit > k {
		k = limit
	}

	// For Elasticsearch aggregations, use only CommonLabels (keyword fields)
	// If the selected field is not a CommonLabel, pick a random CommonLabel instead
	esField := params.GroupByField
	if !logdata.IsElasticsearchKeywordField(params.GroupByField) {
		// Replace with a random CommonLabel for Elasticsearch compatibility
		commonLabels := logdata.GetElasticsearchKeywordFields()
		esField = commonLabels[logdata.RandomIntn(len(commonLabels))]
	}
	esField += ".keyword"

	// Add base filter if specified in unified parameters
	if params.HasBaseFilter {
		// Prepare filter field for Elasticsearch - use keyword field only for CommonLabels
		esFilterField := params.FilterField
		filterFieldType, filterFieldExists := logdata.FieldTypeMap[params.FilterField]
		if filterFieldExists && filterFieldType == logdata.StringField {
			if logdata.IsElasticsearchKeywordField(params.FilterField) {
				esFilterField += ".keyword"
			}
			// For other string fields, use the field as-is (text type)
		}

		// Check if this is a regex pattern on integer field (same logic as other query types)
		var termFilter map[string]interface{}
		if filterFieldExists && filterFieldType == logdata.IntField {
			// Check if FilterValue looks like a regex pattern
			if regexStr, ok := params.FilterValue.(string); ok && (regexStr == "50[0-9]" || regexStr == "40[0-9]" || regexStr == "2[0-9][0-9]" || regexStr == "4[0-9][0-9]" || regexStr == "5[0-9][0-9]" || regexStr == "[4-5]0[0-9]") {
				// Convert regex to range query
				rangeCondition := e.convertRegexToRange(params.FilterField, regexStr)
				termFilter = map[string]interface{}{
					"range": map[string]interface{}{
						params.FilterField: rangeCondition,
					},
				}
			} else {
				// Use exact term for non-regex integer values
				termFilter = map[string]interface{}{
					"term": map[string]interface{}{
						params.FilterField: params.FilterValue,
					},
				}
			}
		} else {
			// Use appropriate query type for string fields
			if logdata.IsElasticsearchKeywordField(params.FilterField) {
				// CommonLabels use term query with .keyword field
				termFilter = map[string]interface{}{
					"term": map[string]interface{}{
						esFilterField: params.FilterValue,
					},
				}
			} else {
				// Non-CommonLabels use match query with text field
				termFilter = map[string]interface{}{
					"match": map[string]interface{}{
						params.FilterField: params.FilterValue,
					},
				}
			}
		}
		filters = append(filters, termFilter)
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

// convertRegexToRange converts regex patterns for integer fields to Elasticsearch range queries
func (e *ElasticsearchExecutor) convertRegexToRange(field string, regexPattern string) map[string]interface{} {
	switch regexPattern {
	// HTTP status code patterns
	case "2[0-9][0-9]":
		return map[string]interface{}{"gte": 200, "lt": 300}
	case "4[0-9][0-9]":
		return map[string]interface{}{"gte": 400, "lt": 500}
	case "5[0-9][0-9]":
		return map[string]interface{}{"gte": 500, "lt": 600}
	case "20[0-9]":
		return map[string]interface{}{"gte": 200, "lte": 209}
	case "40[0-9]":
		return map[string]interface{}{"gte": 400, "lte": 409}
	case "50[0-9]":
		return map[string]interface{}{"gte": 500, "lte": 509}

	// Error code patterns
	case "[4-5]0[0-9]":
		return map[string]interface{}{"gte": 400, "lte": 509}

	// Generic number patterns
	case "[0-9]+":
		return map[string]interface{}{"gte": 0, "lt": 1000}
	case "1[0-9]+":
		return map[string]interface{}{"gte": 10, "lt": 200}
	case "[1-9][0-9]*":
		return map[string]interface{}{"gte": 1, "lt": 1000}
	case "[0-9]{2,4}":
		return map[string]interface{}{"gte": 10, "lte": 9999}

	default:
		// Fallback for unknown patterns - return a reasonable range
		return map[string]interface{}{"gte": 0, "lt": 1000}
	}
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
