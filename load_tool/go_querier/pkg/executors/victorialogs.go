package executors

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg"
	queriercommon "github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// victoriaLogFieldValues stores predefined field values for generating VictoriaLogs queries.
// It is populated from the logdata package.
var victoriaLogFieldValues map[string][]string

func init() {
	victoriaLogFieldValues = make(map[string][]string)
	victoriaLogFieldValues["log_type"] = logdata.LogTypes
	victoriaLogFieldValues["host"] = logdata.Hosts
	victoriaLogFieldValues["container_name"] = logdata.ContainerNames
	victoriaLogFieldValues["environment"] = logdata.Environments
	victoriaLogFieldValues["datacenter"] = logdata.DataCenters
	victoriaLogFieldValues["service"] = logdata.Services
	victoriaLogFieldValues["level"] = logdata.LogLevels
	victoriaLogFieldValues["request_method"] = logdata.HttpMethods // Fixed: was http_method
	victoriaLogFieldValues["metric_name"] = logdata.MetricNames
	victoriaLogFieldValues["event_type"] = logdata.EventTypes
	victoriaLogFieldValues["exception"] = logdata.ExceptionTypes

	// Convert HttpStatusCodes from int to string
	statusStrings := make([]string, len(logdata.HttpStatusCodes))
	for i, code := range logdata.HttpStatusCodes {
		statusStrings[i] = strconv.Itoa(code)
	}
	victoriaLogFieldValues["status"] = statusStrings

	// Add error_code field - generate error codes similar to what generator creates
	errorCodes := make([]string, 100)
	for i := 0; i < 100; i++ {
		errorCodes[i] = strconv.Itoa(400 + i) // 400-499 error codes
	}
	victoriaLogFieldValues["error_code"] = errorCodes

	// Add response_status field for application logs
	victoriaLogFieldValues["response_status"] = statusStrings

	// Add other missing fields
	victoriaLogFieldValues["user_id"] = []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
	victoriaLogFieldValues["region"] = logdata.DataCenters
	victoriaLogFieldValues["namespace"] = []string{"default", "kube-system", "monitoring", "logging", "app"}
}

// VictoriaLogsStatsResponse represents the response structure from VictoriaLogs stats endpoints
type VictoriaLogsStatsResponse struct {
	Status string `json:"status"`
	Data   *struct {
		ResultType string      `json:"resultType"`
		Result     []StatsItem `json:"result"`
	} `json:"data"`
}

// StatsItem represents a single result item from VictoriaLogs stats response
type StatsItem struct {
	Metric map[string]interface{} `json:"metric"`
	Value  []interface{}          `json:"value,omitempty"`  // For vector results
	Values [][]interface{}        `json:"values,omitempty"` // For matrix results
}

// parseStatsResponse parses the new VictoriaLogs stats response format
func (e *VictoriaLogsExecutor) parseStatsResponse(response VictoriaLogsStatsResponse, query string, queryType models.QueryType) models.QueryResult {
	result := models.QueryResult{QueryString: query}

	if response.Data == nil || response.Data.Result == nil {
		result.HitCount = 0
		return result
	}

	// Handle empty result array
	if len(response.Data.Result) == 0 {
		result.HitCount = 0
		return result
	}

	// Count results based on query type
	switch queryType {
	case models.AnalyticalQuery, models.TimeSeriesQuery, models.TopKQuery:
		// For aggregation queries, count number of result items (groups)
		result.HitCount = len(response.Data.Result)
	case models.StatQuery:
		// For stat queries, check if we got actual value (not NaN)
		validResults := 0
		for _, item := range response.Data.Result {
			hasValidValue := false

			if len(item.Value) > 0 {
				// Vector result - check if value is not NaN/empty
				if len(item.Value) > 1 {
					if valueStr := fmt.Sprintf("%v", item.Value[1]); valueStr != "NaN" && valueStr != "" && valueStr != "<nil>" {
						hasValidValue = true
					}
				}
			} else if len(item.Values) > 0 {
				// Matrix result - check if any value is not NaN/empty
				for _, valuePoint := range item.Values {
					if len(valuePoint) > 1 {
						if valueStr := fmt.Sprintf("%v", valuePoint[1]); valueStr != "NaN" && valueStr != "" && valueStr != "<nil>" {
							hasValidValue = true
							break
						}
					}
				}
			}

			if hasValidValue {
				validResults++
			}
		}
		result.HitCount = validResults
	default:
		// Default: count all result items
		result.HitCount = len(response.Data.Result)
	}
	return result
}

// extractCountAlias extracts the alias for any aggregation function from a LogQL-like query string.
// Example: "status:\"200\" | stats by (service) count() as service_count" -> "service_count"
// Example: "level:error | stats by (app) count(*) as error_count" -> "error_count"
// Example: "service:\"api\" | stats by (time:1h, service) quantile(0.51, latency) as p51_latency" -> "p51_latency"
func extractCountAlias(query string) string {
	// Simple string parsing without regex
	asIndex := strings.Index(query, " as ")
	if asIndex == -1 {
		return "" // Default if " as " not found
	}

	// Find the start of the alias after " as "
	aliasStart := asIndex + 4 // Skip " as "
	if aliasStart >= len(query) {
		return ""
	}

	// Find the end of the alias (next space or end of string)
	aliasEnd := len(query)
	for i := aliasStart; i < len(query); i++ {
		if query[i] == ' ' || query[i] == '|' {
			aliasEnd = i
			break
		}
	}

	alias := strings.TrimSpace(query[aliasStart:aliasEnd])
	return alias
}

// VictoriaLogsExecutor implements the QueryExecutor interface for VictoriaLogs
type VictoriaLogsExecutor struct {
	BaseURL            string
	SearchPath         string
	ClientPool         *pkg.ClientPool // Use pool instead of single client
	Options            models.Options
	availableLabels    []string
	timeRangeGenerator *queriercommon.TimeRangeGenerator
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
	Streams    []VictoriaStream `json:"streams,omitempty"` // Added for logs response
}

// VictoriaStream represents a stream of logs in VictoriaLogs response
type VictoriaStream struct {
	Stream map[string]string   `json:"stream"`
	Values [][]string          `json:"values"`
	Logs   []map[string]string `json:"logs,omitempty"`
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
	// This is the endpoint base for querying logs
	searchPath := "select/logsql/query"

	// Initialize with common labels from our shared data package
	availableLabels := logdata.CommonLabels

	// Initialize the label cache with static values from common data
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}

	// Create HTTP client pool with dynamic connection count
	clientPool := pkg.NewClientPool(options.ConnectionCount, options.Timeout)

	return &VictoriaLogsExecutor{
		BaseURL:            baseURL,
		SearchPath:         searchPath,
		ClientPool:         clientPool,
		Options:            options,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGenerator(),
		labelCache:         labelCache,
		labelCacheLock:     sync.RWMutex{},
	}
}

// NewVictoriaLogsExecutorWithTimeConfig creates a new VictoriaLogs executor with time configuration
func NewVictoriaLogsExecutorWithTimeConfig(baseURL string, options models.Options, workerID int, timeConfig *common.TimeRangeConfig) *VictoriaLogsExecutor {
	// Ensure the base URL ends with a slash
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}
	// Define the search path for VictoriaLogs API
	searchPath := "select/logsql/query"
	// Initialize with common labels from our shared data package
	availableLabels := logdata.CommonLabels
	// Initialize the label cache with static values from common data
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}
	// Create HTTP client pool with dynamic connection count
	clientPool := pkg.NewClientPool(options.ConnectionCount, options.Timeout)
	return &VictoriaLogsExecutor{
		BaseURL:            baseURL,
		SearchPath:         searchPath,
		ClientPool:         clientPool,
		Options:            options,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGeneratorWithConfig(timeConfig),
		labelCache:         labelCache,
		labelCacheLock:     sync.RWMutex{},
	}
}

// GetSystemName returns the system name
func (e *VictoriaLogsExecutor) GetSystemName() string {
	return "victorialogs"
}

// GenerateTimeRange generates a time range for queries (for error handling)
func (e *VictoriaLogsExecutor) GenerateTimeRange() interface{} {
	return e.timeRangeGenerator.GenerateConfiguredTimeRange()
}

// ExecuteQuery executes a query of the given type against VictoriaLogs
func (e *VictoriaLogsExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	var query string
	var err error

	// Add limit parameter from unified constants
	possibleLimits := logdata.VictoriaLogsLimits
	limit := possibleLimits[logdata.RandomIntn(len(possibleLimits))]
	intLimit, _ := strconv.Atoi(limit)

	// Generate the query based on query type
	switch queryType {
	case models.SimpleQuery:
		query = e.generateSimpleQuery()
	case models.ComplexQuery:
		query = e.generateComplexQuery()
	case models.AnalyticalQuery:
		query = e.generateAnalyticalQuery()
	case models.TimeSeriesQuery:
		query = e.generateTimeSeriesQuery()
	case models.StatQuery:
		query = e.generateStatQuery()
	case models.TopKQuery:
		query = e.generateTopKQuery(intLimit) // Default top K value
	default:
		return models.QueryResult{}, fmt.Errorf("unsupported query type: %s", string(queryType))
	}

	// Generate time range for the query using the configured TimeRangeGenerator
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()
	startTime := queriercommon.FormatTimeRFC3339(timeRange.Start)
	endTime := queriercommon.FormatTimeRFC3339(timeRange.End)

	// Determine step based on time range duration, similar to Loki executor
	var step string
	rangeDuration := timeRange.End.Sub(timeRange.Start)
	if rangeDuration <= time.Minute*5 {
		step = "5s"
	} else if rangeDuration <= time.Minute*30 {
		step = "30s"
	} else if rangeDuration <= time.Hour {
		step = "60s"
	} else if rangeDuration <= time.Hour*3 {
		step = "120s"
	} else {
		step = "180s"
	}

	// Prepare the query parameters
	queryInfo := map[string]string{
		"query": query,
		"start": startTime,
		"end":   endTime,
		"step":  step,
		"limit": limit,
	}

	// Execute the query with the prepared parameters
	result, err := e.executeVictoriaLogsQuery(queryInfo, queryType) // Pass queryType
	if err != nil {
		return models.QueryResult{
			QueryString: query,
		}, err
	}

	// Populate all necessary fields in QueryResult, including the step
	result.QueryString = query // Ensure QueryString is populated
	result.StartTime = timeRange.Start
	result.EndTime = timeRange.End
	result.Limit = limit
	result.Step = step                           // Populate Step in the result
	result.TimeStringRepr = timeRange.StringRepr // Use string representation from time range

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
	genericValues := []string{"value1", "value2", "value3", "value-" + strconv.Itoa(logdata.RandomIntn(100))}

	// Cache these values for future use
	e.labelCacheLock.Lock()
	e.labelCache[label] = genericValues
	e.labelCacheLock.Unlock()

	return genericValues, nil
}

// GenerateRandomQuery generates a random query of the specific type
func (e *VictoriaLogsExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Generate a time range based on the configuration
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()
	startTime := timeRange.Start
	endTime := timeRange.End

	// Format timestamps for VictoriaLogs in ISO 8601 format
	startTimeStr := startTime.Format(time.RFC3339)
	endTimeStr := endTime.Format(time.RFC3339)

	// Generate random limit - use unified constants
	possibleLimits := logdata.VictoriaLogsLimits
	limit := possibleLimits[logdata.RandomIntn(len(possibleLimits))]
	intLimit, _ := strconv.Atoi(limit)

	// Generate random step for time series queries - use unified constants
	step := "60s" // Default
	steps := logdata.VictoriaLogsSteps
	if queryType == models.TimeSeriesQuery {
		step = steps[logdata.RandomIntn(len(steps))]
	}

	// Generate the query string based on query type
	var queryString string
	switch queryType {
	case models.SimpleQuery:
		queryString = e.generateSimpleQuery()
	case models.ComplexQuery:
		queryString = e.generateComplexQuery()
	case models.AnalyticalQuery:
		queryString = e.generateAnalyticalQuery()
	case models.TimeSeriesQuery:
		queryString = e.generateTimeSeriesQuery()
	case models.StatQuery:
		queryString = e.generateStatQuery()
	case models.TopKQuery:
		queryString = e.generateTopKQuery(intLimit) // Default top K value
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

// executeVictoriaLogsQuery executes a query against VictoriaLogs
func (e *VictoriaLogsExecutor) executeVictoriaLogsQuery(queryInfo map[string]string, queryType models.QueryType) (models.QueryResult, error) {
	// Use correct VictoriaLogs endpoint based on query type
	var requestURL string

	switch queryType {
	case models.TimeSeriesQuery:
		// TimeSeries queries with time-based grouping use stats_query_range
		requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/stats_query_range"
	case models.AnalyticalQuery, models.TopKQuery:
		// Aggregation queries without time series use stats_query
		requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/stats_query"
	case models.StatQuery:
		// StatQuery can be either stats query or simple search - check if query contains | stats
		if queryInfo != nil {
			if queryStr, exists := queryInfo["query"]; exists && strings.Contains(queryStr, "| stats") {
				// Contains stats pipe - use stats_query endpoint
				requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/stats_query"
			} else {
				// Simple filter query without stats - use standard query endpoint
				requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/query"
			}
		} else {
			// Fallback to standard query endpoint
			requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/query"
		}
	default:
		// Simple and Complex queries (log retrieval) use standard query endpoint
		requestURL = strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/query"
	}

	// Set endpoint-specific parameters
	switch queryType {
	case models.TimeSeriesQuery:
		// stats_query_range requires 'step' parameter for time intervals
		if _, exists := queryInfo["step"]; !exists {
			// Use time interval from query or default to 5m
			if timeInterval, exists := queryInfo["time_interval"]; exists {
				queryInfo["step"] = timeInterval
			} else {
				queryInfo["step"] = "5m" // Default step
			}
		}
		// Keep limit for time series queries (will be placed before stats in query)
		if _, exists := queryInfo["limit"]; !exists {
			queryInfo["limit"] = "1000"
		}
	case models.StatQuery:
		// stats_query uses 'time' instead of time range for instant queries
		if _, exists := queryInfo["time"]; !exists {
			// Use end time or current time
			if endTime, exists := queryInfo["end"]; exists {
				queryInfo["time"] = endTime
			}
			// Remove start/end for instant queries
			delete(queryInfo, "start")
			delete(queryInfo, "end")
		}
		// Keep limit for stat queries
		if _, exists := queryInfo["limit"]; !exists {
			queryInfo["limit"] = "1000"
		}
	default:
		// Standard query endpoint - ensure limit parameter is present
		if _, exists := queryInfo["limit"]; !exists {
			queryInfo["limit"] = "1000" // Default limit
		}
	}

	// Create form data for POST request body
	data := url.Values{}
	for k, v := range queryInfo {
		data.Set(k, v)
	}

	req, err := http.NewRequest("POST", requestURL, strings.NewReader(data.Encode()))
	if err != nil {
		return models.QueryResult{
			QueryString: queryInfo["query"],
		}, fmt.Errorf("error creating request: %w", err)
	}

	// Set required headers for form-encoded POST request
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := e.ClientPool.Get()
	resp, err := client.Do(req)
	if err != nil {
		return models.QueryResult{
			QueryString: queryInfo["query"],
		}, fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{
			QueryString: queryInfo["query"],
		}, fmt.Errorf("failed to read VictoriaLogs response body: %w", err)
	}
	queryResult := models.QueryResult{BytesRead: int64(len(bodyBytes))}

	if resp.StatusCode != http.StatusOK {
		queryResult.QueryString = queryInfo["query"]
		// Extract only the meaningful error without technical details
		errorMsg := string(bodyBytes)
		if strings.Contains(errorMsg, "cannot parse query") {
			// Find the part after "cannot parse query"
			if idx := strings.Index(errorMsg, "cannot parse query"); idx != -1 {
				errorMsg = "cannot parse query" + errorMsg[idx+17:]
			}
			// Remove context part at the end
			if idx := strings.Index(errorMsg, "; context:"); idx != -1 {
				errorMsg = errorMsg[:idx]
			}
		}
		return queryResult, fmt.Errorf("Status %d: %s", resp.StatusCode, errorMsg)
	}

	// If the query is an aggregation query, the response might be a single JSON object, an array of objects, or sometimes non-JSON for no results.
	isAggregationQuery := strings.Contains(queryInfo["query"], "stats") || strings.Contains(queryInfo["query"], "count(") || strings.Contains(queryInfo["query"], "topk")

	var determinedFoundCount int

	// Pre-check for aggregation queries that might return non-JSON or specific placeholders for no data.
	if isAggregationQuery {
		bodyStr := string(bodyBytes)
		// VictoriaLogs sometimes returns a single dot "." for an aggregation query with no results.
		if len(bodyBytes) == 0 || bodyStr == "." {
			queryResult.HitCount = 0
			if bodyStr == "." {
				log.Printf("DEBUG: VictoriaLogs aggregation query returned non-JSON placeholder '%s' for query: %s. Treating as 0 results.", bodyStr, queryInfo["query"])
			}
			return queryResult, nil
		}
	}

	if isAggregationQuery {
		// Attempt 1: Try to parse as VictoriaLogs stats response (new format from stats endpoints)
		var statsResp VictoriaLogsStatsResponse
		if err := json.Unmarshal(bodyBytes, &statsResp); err == nil && statsResp.Status == "success" && statsResp.Data != nil {
			// Handle stats endpoint response format
			result := e.parseStatsResponse(statsResp, queryInfo["query"], queryType)
			if result.HitCount >= 0 { // Valid result found
				return result, nil
			}
			// Fall through to try other parsing methods
		}

		// Attempt 2: Try to parse as a simple map[string]interface{} (e.g., for aggregation results or single group)
		var parsedSimple map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &parsedSimple); err == nil {
			aggAlias := extractCountAlias(queryInfo["query"]) // Get alias like "log_count", "p51_latency", etc.

			if aggAlias != "" { // If an alias was found in the query string
				if val, ok := parsedSimple[aggAlias]; ok { // Check if the alias exists as a key in the map
					var countStr string
					switch v := val.(type) {
					case string:
						countStr = v
					case float64: // JSON numbers are often float64
						// Format float to string, trying to preserve integer form if applicable
						if v == float64(int64(v)) { // Check if it's a whole number
							countStr = strconv.FormatInt(int64(v), 10)
						} else {
							countStr = strconv.FormatFloat(v, 'f', -1, 64)
						}
					case int:
						countStr = strconv.Itoa(v)
					case int32:
						countStr = strconv.Itoa(int(v))
					case int64:
						countStr = strconv.FormatInt(v, 10)
					default:
						log.Printf("WARN: Alias field '%s' in simple map has unexpected type %T. Value: %v. Query: %s", aggAlias, val, val, queryInfo["query"])
					}

					if countStr != "" {
						// Handle special cases like NaN, Inf, etc.
						if countStr == "NaN" {
							// NaN means no data available for aggregation, so 0 results
							determinedFoundCount = 0
						} else if countStr == "+Inf" || countStr == "-Inf" {
							// Infinity values mean there is 1 aggregated result with infinite value
							determinedFoundCount = 1
						} else if c, errConv := strconv.Atoi(countStr); errConv == nil {
							// If it's a TopK or Analytical query and we got a simple map, it's one result item.
							// Otherwise (e.g. StatQuery), the count is the value itself.
							if queryType == models.TopKQuery || queryType == models.AnalyticalQuery {
								determinedFoundCount = 1 // This map represents a single (aggregated) result item
							} else {
								determinedFoundCount = c
							}
						} else {
							// Try to parse as float for statistical aggregations
							if _, errFloat := strconv.ParseFloat(countStr, 64); errFloat == nil {
								// It's a valid float value from statistical aggregation, count as 1 result
								determinedFoundCount = 1
							} else {
								log.Printf("WARN: Could not convert aggregation result from alias field '%s' (value: '%s') to number: %v. Query: %s", aggAlias, countStr, errConv, queryInfo["query"])
							}
						}
					} else {
						// Alias was in query, but not in the response map.
						log.Printf("DEBUG: Alias '%s' extracted from query, but not found in the simple map response: %v. Query: %s", aggAlias, parsedSimple, queryInfo["query"])
						// determinedFoundCount remains 0, allowing other parsing methods to try.
					}
				} else if len(parsedSimple) > 0 && determinedFoundCount == 0 {
					// No "function() as alias" pattern found in the query, and the response is a simple map.
					log.Printf("DEBUG: Aggregation response is a simple map, but no aggregation alias pattern found in query. Map: %v. Query: %s", parsedSimple, queryInfo["query"])
					// determinedFoundCount remains 0, relying on subsequent parsing or indicating a potential issue.
				}
			}
		} else {
			// Attempt 2: Parse as full VictoriaLogsResponse (expects specific structure, often array for grouped results)
			var vlResp VictoriaLogsResponse
			originalVLErr := json.Unmarshal(bodyBytes, &vlResp) // Save this error

			if originalVLErr == nil && vlResp.Status == "success" {
				if len(vlResp.Data.Result) > 0 {
					determinedFoundCount = len(vlResp.Data.Result) // Number of groups/series
				} else if vlResp.Data.ResultType == "scalar" && len(vlResp.Data.Result) == 0 {
					// Handle scalar results that might be empty in Result but have stats
					// This is a guess; actual scalar results might be in simpleResp or need specific handling
					determinedFoundCount = 0 // Default for scalar if not in simpleResp
				} else {
					determinedFoundCount = 0 // Status success, but no data.Result or relevant scalar info
				}
			} else {
				// Attempt 3: Parse as newline-delimited JSON (NDJSON)
				scanner := bufio.NewScanner(bytes.NewReader(bodyBytes))
				count := 0
				foundValidJSONLine := false
				for scanner.Scan() {
					line := scanner.Bytes()
					trimmedLine := bytes.TrimSpace(line)
					if len(trimmedLine) > 0 && json.Valid(trimmedLine) {
						count++
						foundValidJSONLine = true
					} else if len(trimmedLine) > 0 { // Non-empty line that isn't valid JSON
						foundValidJSONLine = false // Invalidate if any line is not proper JSON
						count = 0                  // Reset count as the stream is not clean NDJSON
						break
					}
				}

				if scanErr := scanner.Err(); scanErr != nil {
					log.Printf("WARN: Scanner error during NDJSON parsing for aggregation: %v. Query: %s", scanErr, queryInfo["query"])
					determinedFoundCount = 0 // Error during scan
				} else if foundValidJSONLine && count > 0 {
					determinedFoundCount = count
				} else {
					// All parsing attempts failed for aggregation query. Log the most relevant earlier error.
					// If originalVLErr is nil, it means vlResp parsing didn't error but didn't yield results.
					// In that case, the body might not be JSON at all (e.g. plain text error from VL).
					finalErrorToLog := originalVLErr
					if finalErrorToLog == nil { // If parsing vlResp was 'successful' but didn't find data, and NDJSON also failed.
						// This implies the body was not the expected VictoriaLogsResponse struct, nor NDJSON.
						// It might be a plain text error from VictoriaLogs if the query itself was bad.
						log.Printf("WARN: Aggregation query response was not a known JSON structure (VictoriaLogsResponse, simple map, or NDJSON). Raw: %s. Query: %s", string(bodyBytes), queryInfo["query"])
					} else {
						log.Printf("WARN: All JSON parsing attempts failed for aggregation query. Last error (VictoriaLogsResponse struct parse): %v. Raw: %s. Query: %s", finalErrorToLog, string(bodyBytes), queryInfo["query"])
					}
					determinedFoundCount = 0
				}
			}
		}
		queryResult.HitCount = determinedFoundCount
	} else {
		// For non-aggregation (raw log fetching), count lines
		scanner := bufio.NewScanner(bytes.NewReader(bodyBytes))
		lines := 0
		for scanner.Scan() {
			line := scanner.Bytes()
			// Ensure line is not empty and is a valid JSON object before counting
			if len(bytes.TrimSpace(line)) > 0 && json.Valid(line) {
				lines++
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("WARN: Error scanning VictoriaLogs response for raw logs: %v. Query: %s", err, queryInfo["query"])
		}
		determinedFoundCount = lines
		// For raw logs, ProcessedEntries = FoundCount (num log lines)
		// Assuming HitCount/ResultCount covers this.
	}

	queryResult.HitCount = determinedFoundCount
	queryResult.ResultCount = determinedFoundCount // Set ResultCount as well
	return queryResult, nil
}

// generateSimpleQuery creates a simple query using unified strategy
func (e *VictoriaLogsExecutor) generateSimpleQuery() string {
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
		// VictoriaLogs supports message field searches
		keyword := fmt.Sprintf("%v", params.Value)

		// Apply regex escaping for regex operators
		if params.Operator == "=~" || params.Operator == "!~" {
			keyword = logdata.FixVictoriaLogsRegexEscaping(keyword)
		}

		// Use appropriate operator - only positive operators now
		switch params.Operator {
		case "=":
			return fmt.Sprintf(`message:"%s"`, keyword)
		case "!=":
			// UNIFIED STRATEGY: SimpleQuery must only use positive operators
			return fmt.Sprintf(`message:"%s"`, keyword) // Convert to =
		case "=~":
			return fmt.Sprintf(`message:~"%s"`, keyword)
		case "!~":
			// UNIFIED STRATEGY: SimpleQuery must only use positive operators
			return fmt.Sprintf(`message:~"%s"`, keyword) // Convert to =~
		default:
			return fmt.Sprintf(`message:"%s"`, keyword)
		}
	}

	// Handle regular field searches
	valueStr := fmt.Sprintf("%v", params.Value)

	// Apply regex escaping for regex operators
	if params.Operator == "=~" || params.Operator == "!~" {
		valueStr = logdata.FixVictoriaLogsRegexEscaping(valueStr)
	}

	// Use appropriate VictoriaLogs operator - only positive operators now
	switch params.Operator {
	case "=":
		return fmt.Sprintf(`%s:"%s"`, params.Field, valueStr)
	case "!=":
		// UNIFIED STRATEGY: SimpleQuery must only use positive operators
		return fmt.Sprintf(`%s:"%s"`, params.Field, valueStr) // Convert to =
	case "=~":
		return fmt.Sprintf(`%s:~"%s"`, params.Field, valueStr)
	case "!~":
		// UNIFIED STRATEGY: SimpleQuery must only use positive operators
		return fmt.Sprintf(`%s:~"%s"`, params.Field, valueStr) // Convert to =~
	default:
		// Fallback to exact match
		return fmt.Sprintf(`%s:"%s"`, params.Field, valueStr)
	}
}

// generateComplexQuery creates a query using unified strategy
func (e *VictoriaLogsExecutor) generateComplexQuery() string {
	// Use unified complex query generation
	params := logdata.GenerateUnifiedComplexQuery()

	// UNIFIED STRATEGY: Validate minimum field requirements
	// ComplexQuery must have at least 2 conditions for fair comparison
	for len(params.Fields) < 2 {
		params = logdata.GenerateUnifiedComplexQuery()
	}

	// UNIFIED STRATEGY: Ensure at least one positive matcher exists
	// Must have minimum 1 positive operator (= or =~) for Loki compatibility
	hasPositive := false
	for i := range params.Operators {
		if params.Operators[i] == "=" || params.Operators[i] == "=~" {
			hasPositive = true
			break
		}
	}

	// UNIFIED STRATEGY: Convert to positive operator if needed
	if !hasPositive {
		if params.Operators[0] == "!=" {
			params.Operators[0] = "="
		} else if params.Operators[0] == "!~" {
			params.Operators[0] = "=~"
		}
	}

	// Build terms based on unified parameters
	var terms []string
	var messageTerm string

	for i, field := range params.Fields {
		value := params.Values[i]
		operator := params.Operators[i]
		isRegex := params.IsRegex[i]
		isNegated := params.NegationMask[i]

		// Convert value to string
		valueStr := fmt.Sprintf("%v", value)

		// Handle message search specially
		if field == "message" {
			// Apply regex escaping for regex operators
			if operator == "=~" || operator == "!~" || isRegex {
				valueStr = logdata.FixVictoriaLogsRegexEscaping(valueStr)
			}
			if operator == "!=" || operator == "!~" || isNegated {
				messageTerm = fmt.Sprintf(`message!:"%s"`, valueStr)
			} else {
				messageTerm = fmt.Sprintf(`message:"%s"`, valueStr)
			}
			continue
		}

		// Build term based on operator and regex
		var term string
		if isRegex || operator == "=~" || operator == "!~" {
			// Apply regex escaping for VictoriaLogs regex operators
			valueStr = logdata.FixVictoriaLogsRegexEscaping(valueStr)
			// VictoriaLogs regex support with =~ operator
			if operator == "!~" || isNegated {
				term = fmt.Sprintf(`%s!~"%s"`, field, valueStr)
			} else {
				term = fmt.Sprintf(`%s=~"%s"`, field, valueStr)
			}
		} else {
			// Regular exact match
			if operator == "!=" || isNegated {
				term = fmt.Sprintf(`%s!:"%s"`, field, valueStr)
			} else {
				term = fmt.Sprintf(`%s:"%s"`, field, valueStr)
			}
		}

		terms = append(terms, term)
	}

	// Add message search component if present
	if params.MessageSearch != nil {
		keyword := fmt.Sprintf("%v", params.MessageSearch.Value)
		// Apply regex escaping for regex operators
		if params.MessageSearch.Operator == "=~" || params.MessageSearch.Operator == "!~" {
			keyword = logdata.FixVictoriaLogsRegexEscaping(keyword)
		}
		if params.MessageSearch.Operator == "!=" || params.MessageSearch.Operator == "!~" {
			messageTerm = fmt.Sprintf(`message!:"%s"`, keyword)
		} else {
			messageTerm = fmt.Sprintf(`message:"%s"`, keyword)
		}
	}

	// Combine terms based on logic operator
	var finalQuery string

	if len(terms) == 0 {
		// Fallback if no terms
		finalQuery = `log_type:not""`
	} else if len(terms) == 1 {
		// Single term
		finalQuery = terms[0]
	} else {
		// Multiple terms - simplified for fair comparison (only AND logic)
		// FAIRNESS: Only AND logic for fair comparison across all systems
		// Loki doesn't support OR in label selectors, so we use only AND everywhere
		finalQuery = strings.Join(terms, " AND ")
	}

	// Add message term if present
	if messageTerm != "" {
		if finalQuery != "" {
			finalQuery = fmt.Sprintf("%s AND %s", finalQuery, messageTerm)
		} else {
			finalQuery = messageTerm
		}
	}

	// Occasionally add a field:not"" to ensure some results if query is too specific
	if logdata.RandomIntn(3) == 0 {
		notEmptyField := logdata.CommonLabels[logdata.RandomIntn(len(logdata.CommonLabels))]
		finalQuery = fmt.Sprintf(`%s:not"" AND (%s)`, notEmptyField, finalQuery)
	}

	return finalQuery
}

// generateAnalyticalQuery creates a query for statistical analysis (e.g., count by field)
func (e *VictoriaLogsExecutor) generateAnalyticalQuery() string {
	// Use unified analytical query generation for maximum fairness
	params := logdata.GenerateUnifiedAnalyticalQuery()

	// Prepare filter field and value with proper operator selection
	valueStr := fmt.Sprintf("%v", params.FilterValue)

	// Check if value looks like regex pattern (contains [, ], +, *, \.)
	isRegexPattern := strings.ContainsAny(valueStr, "[]+*") || strings.Contains(valueStr, `\.`)

	var filterClause string
	if isRegexPattern {
		// Use regex operator for pattern values
		valueStr = logdata.FixVictoriaLogsRegexEscaping(valueStr)
		filterClause = fmt.Sprintf(`%s:~"%s"`, params.FilterField, valueStr)
	} else {
		// Use exact match for literal values
		filterClause = fmt.Sprintf(`%s:"%s"`, params.FilterField, valueStr)
	}

	// Add time window filter for fair comparison with Loki's required time windows
	// This ensures all systems use same time scope for aggregations
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
	filterClause = fmt.Sprintf(`%s AND _time:%s`, filterClause, timeWindow)

	var query string

	switch params.AggregationType {
	case "count":
		// Count aggregation with grouping and limit
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) count(*) as log_count`,
			filterClause,
			params.Limit,
			params.GroupByField,
		)

	case "avg":
		// Average aggregation with grouping and limit
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) avg(%s) as avg_%s`,
			filterClause,
			params.Limit,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "min":
		// Minimum aggregation with grouping and limit
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) min(%s) as min_%s`,
			filterClause,
			params.Limit,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "max":
		// Maximum aggregation with grouping and limit
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) max(%s) as max_%s`,
			filterClause,
			params.Limit,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "sum":
		// Sum aggregation with grouping and limit
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) sum(%s) as sum_%s`,
			filterClause,
			params.Limit,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "quantile":
		// Quantile aggregation with grouping and limit
		aliasNum := strings.Replace(fmt.Sprintf("%.2f", params.Quantile), "0.", "", 1)
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) quantile(%g, %s) as p%s_%s`,
			filterClause,
			params.Limit,
			params.GroupByField,
			params.Quantile,
			params.NumericField,
			aliasNum,
			params.NumericField,
		)

	default:
		// Fallback to count
		query = fmt.Sprintf(`%s | limit %d | stats by (%s) count(*) as log_count`,
			filterClause,
			params.Limit,
			params.GroupByField,
		)
	}

	return query
}

// generateTimeSeriesQuery creates a query that uses temporal aggregation for true time series data
func (e *VictoriaLogsExecutor) generateTimeSeriesQuery() string {
	// Use unified time series query generation for maximum fairness across all systems
	params := logdata.GenerateUnifiedTimeSeriesQuery()

	// Build base filter clause
	baseFilter := fmt.Sprintf(`%s:"%v"`, params.FilterField, params.FilterValue)

	var query string

	// Build query based on unified aggregation type for maximum fairness
	switch params.AggregationType {
	case "count":
		// Count aggregation with time and field grouping
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) count() as log_count`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
		)

	case "avg":
		// Average aggregation with time and field grouping
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) avg(%s) as avg_%s`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "min":
		// Minimum aggregation with time and field grouping
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) min(%s) as min_%s`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "max":
		// Maximum aggregation with time and field grouping
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) max(%s) as max_%s`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "sum":
		// Sum aggregation with time and field grouping
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) sum(%s) as sum_%s`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
			params.NumericField,
			params.NumericField,
		)

	case "quantile":
		// Quantile aggregation with time and field grouping
		aliasNum := strings.Replace(fmt.Sprintf("%.2f", params.Quantile), "0.", "", 1)
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) quantile(%g, %s) as p%s_%s`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
			params.Quantile,
			params.NumericField,
			aliasNum,
			params.NumericField,
		)

	default:
		// Fallback to count
		query = fmt.Sprintf(`%s | limit %d | stats by (time:%s, %s) count() as log_count`,
			baseFilter,
			params.Limit,
			params.TimeInterval,
			params.GroupByField,
		)
	}

	return query
}

// generateStatQuery creates a query for a single statistic using unified approach
// Returns global statistic (no grouping) for fair comparison
func (e *VictoriaLogsExecutor) generateStatQuery() string {
	// Use unified stat query generation
	params := logdata.GenerateUnifiedStatQuery()

	// Handle message search specially
	if params.FilterField == "message" {
		valueStr := fmt.Sprintf("%v", params.FilterValue)
		timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
		baseFilter := fmt.Sprintf(`message:"%s" AND _time:%s`, valueStr, timeWindow)

		// Apply statistical aggregation based on StatType - FIXED: was returning only filter
		switch params.StatType {
		case "count":
			return fmt.Sprintf(`%s | stats count(*) as total_count`, baseFilter)
		case "avg":
			return fmt.Sprintf(`%s | stats avg(%s) as avg_value`, baseFilter, params.NumericField)
		case "sum":
			return fmt.Sprintf(`%s | stats sum(%s) as sum_value`, baseFilter, params.NumericField)
		case "min":
			return fmt.Sprintf(`%s | stats min(%s) as min_value`, baseFilter, params.NumericField)
		case "max":
			return fmt.Sprintf(`%s | stats max(%s) as max_value`, baseFilter, params.NumericField)
		case "quantile":
			return fmt.Sprintf(`%s | stats quantile(%g, %s) as quantile_value`, baseFilter, params.Quantile, params.NumericField)
		default:
			return fmt.Sprintf(`%s | stats count(*) as total_count`, baseFilter)
		}
	}

	// Create base filter using unified field and value
	valueStr := fmt.Sprintf("%v", params.FilterValue)

	// Check if value looks like regex pattern (contains [, ], +, *, \.)
	isRegexPattern := strings.ContainsAny(valueStr, "[]+*") || strings.Contains(valueStr, `\.`)

	var baseFilter string
	if isRegexPattern {
		// Use regex operator for pattern values
		valueStr = logdata.FixVictoriaLogsRegexEscaping(valueStr)
		baseFilter = fmt.Sprintf(`%s:~"%s"`, params.FilterField, valueStr)
	} else {
		// Use exact match for literal values
		baseFilter = fmt.Sprintf(`%s:"%s"`, params.FilterField, valueStr)
	}

	// Add time window filter for fair comparison with Loki's required time windows
	// This ensures all systems use same time scope for statistical operations
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
	baseFilter = fmt.Sprintf(`%s AND _time:%s`, baseFilter, timeWindow)

	// Create query based on stat type
	var query string

	switch params.StatType {
	case "count":
		// Simple count
		query = fmt.Sprintf(`%s | stats count(*) as total_count`, baseFilter)

	case "avg":
		// Average aggregation - VictoriaLogs has avg()
		query = fmt.Sprintf(`%s | stats avg(%s) as avg_value`, baseFilter, params.NumericField)

	case "sum":
		// Sum aggregation - VictoriaLogs has sum()
		query = fmt.Sprintf(`%s | stats sum(%s) as sum_value`, baseFilter, params.NumericField)

	case "min":
		// Minimum aggregation - VictoriaLogs has min()
		query = fmt.Sprintf(`%s | stats min(%s) as min_value`, baseFilter, params.NumericField)

	case "max":
		// Maximum aggregation - VictoriaLogs has max()
		query = fmt.Sprintf(`%s | stats max(%s) as max_value`, baseFilter, params.NumericField)

	case "quantile":
		// Quantile aggregation - VictoriaLogs has quantile()
		query = fmt.Sprintf(`%s | stats quantile(%g, %s) as quantile_value`, baseFilter, params.Quantile, params.NumericField)
	}

	return query
}

// generateTopKQuery creates a query to find the top N values for a field using unified parameters
func (e *VictoriaLogsExecutor) generateTopKQuery(k int) string {
	// Generate unified TopK parameters for maximum fairness
	params := logdata.GenerateUnifiedTopKQuery()

	// Use the larger of unified K value or parameter k if it's > 0
	finalK := params.K
	if k > 0 && k > params.K {
		finalK = k
	}

	// Build stream selector based on unified parameters
	var streamSelector string
	if params.HasBaseFilter {
		// Use unified base filtering approach
		if str, ok := params.FilterValue.(string); ok {
			// Check if this is a regex pattern based on operator and IsRegex flag
			if params.FilterIsRegex || params.FilterOperator == "=~" || params.FilterOperator == "!~" || strings.Contains(str, "*") {
				// Handle regex patterns
				regexValue := str
				if strings.Contains(str, "*") {
					// Convert wildcard to LogsQL regex
					regexValue = strings.ReplaceAll(str, "*", ".*")
				}
				// Apply regex escaping for VictoriaLogs
				regexValue = logdata.FixVictoriaLogsRegexEscaping(regexValue)

				// Use regex operator based on the original operator
				if params.FilterOperator == "!~" {
					streamSelector = fmt.Sprintf(`%s:!~"%s"`, params.FilterField, regexValue)
				} else {
					streamSelector = fmt.Sprintf(`%s:~"%s"`, params.FilterField, regexValue)
				}
			} else {
				// Exact match - use appropriate operator
				if params.FilterOperator == "!=" {
					streamSelector = fmt.Sprintf(`%s:!"%s"`, params.FilterField, str)
				} else {
					streamSelector = fmt.Sprintf(`%s:"%s"`, params.FilterField, str)
				}
			}
		} else {
			// Non-string value - convert to string and use exact match
			if params.FilterOperator == "!=" {
				streamSelector = fmt.Sprintf(`%s:!"%v"`, params.FilterField, params.FilterValue)
			} else {
				streamSelector = fmt.Sprintf(`%s:"%v"`, params.FilterField, params.FilterValue)
			}
		}
	} else {
		// No base filter - use catch-all selector
		streamSelector = "log_type:not\"\""
	}

	// Build the complete TopK query with unified field selection
	return fmt.Sprintf(`%s | limit %d | stats by (%s) count() as count_value | sort by (count_value desc)`,
		streamSelector, finalK, params.GroupByField)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
