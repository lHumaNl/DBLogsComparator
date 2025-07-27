package executors

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/common"
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
	victoriaLogFieldValues["http_method"] = logdata.HttpMethods

	// Convert HttpStatusCodes from int to string
	statusStrings := make([]string, len(logdata.HttpStatusCodes))
	for i, code := range logdata.HttpStatusCodes {
		statusStrings[i] = strconv.Itoa(code)
	}
	victoriaLogFieldValues["status"] = statusStrings
}

// extractCountAlias extracts the alias for 'count()' from a LogQL-like query string.
// Example: "status:\"200\" | stats by (service) count() as service_count" -> "service_count"
// Example: "level:error | stats by (app) count(*) as error_count" -> "error_count"
func extractCountAlias(query string) string {
	// This regex matches "count() as alias" or "count(*) as alias"
	// The first capturing group is for '*' (optional), the second is the alias itself.
	r := regexp.MustCompile(`count\((\*?)\)\s+as\s+([a-zA-Z0-9_]+)`)
	matches := r.FindStringSubmatch(query)
	if len(matches) > 2 { // We need at least 3 elements: full match, content of (), alias
		return matches[2] // The second capturing group is the alias
	}
	return "" // No explicit "count() as alias" or "count(*) as alias" pattern found
}

// VictoriaLogsExecutor implements the QueryExecutor interface for VictoriaLogs
type VictoriaLogsExecutor struct {
	BaseURL            string
	SearchPath         string
	ClientPool         *pkg.ClientPool // Use pool instead of single client
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
	var err error

	// Add limit parameter from a predefined list
	possibleLimits := []string{"10", "50", "100", "200", "500", "1000"}
	limit := possibleLimits[rand.Intn(len(possibleLimits))]
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

	// Generate time range for the query using the common TimeRangeGenerator
	timeRange := e.timeRangeGenerator.GenerateRandomTimeRange()
	startTime := common.FormatTimeRFC3339(timeRange.Start)
	endTime := common.FormatTimeRFC3339(timeRange.End)

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
		return models.QueryResult{}, fmt.Errorf("%s query error: %v", string(queryType), err)
	}

	// Populate all necessary fields in QueryResult, including the step
	result.QueryString = query // Ensure QueryString is populated
	result.StartTime = timeRange.Start
	result.EndTime = timeRange.End
	result.Limit = limit
	result.Step = step // Populate Step in the result

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
	possibleLimits := []string{"10", "50", "100", "200", "500", "1000"}
	limit := possibleLimits[rand.Intn(len(possibleLimits))]
	intLimit, _ := strconv.Atoi(limit)

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
	// Construct the correct, full URL for the VictoriaLogs query endpoint
	// Ensure BaseURL does not have a trailing slash before appending the path
	requestURL := strings.TrimSuffix(e.BaseURL, "/") + "/select/logsql/query"

	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %w", err)
	}

	q := req.URL.Query()
	for k, v := range queryInfo {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	client := e.ClientPool.Get()
	resp, err := client.Do(req)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("failed to read VictoriaLogs response body: %w", err)
	}
	queryResult := models.QueryResult{BytesRead: int64(len(bodyBytes))}

	if resp.StatusCode != http.StatusOK {
		return queryResult, fmt.Errorf("VictoriaLogs query failed with status %d: %s. Query: %s", resp.StatusCode, string(bodyBytes), queryInfo["query"])
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
		// Attempt 1: Try to parse as a simple map[string]interface{} (e.g., for count results or single group)
		var parsedSimple map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &parsedSimple); err == nil {
			countAlias := extractCountAlias(queryInfo["query"]) // Get alias like "log_count"

			if countAlias != "" { // If an alias was found in the query string
				if val, ok := parsedSimple[countAlias]; ok { // Check if the alias exists as a key in the map
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
						log.Printf("WARN: Alias field '%s' in simple map has unexpected type %T. Value: %v. Query: %s", countAlias, val, val, queryInfo["query"])
					}

					if countStr != "" {
						if c, errConv := strconv.Atoi(countStr); errConv == nil {
							// If it's a TopK or Analytical query and we got a simple map, it's one result item.
							// Otherwise (e.g. StatQuery), the count is the value itself.
							if queryType == models.TopKQuery || queryType == models.AnalyticalQuery {
								determinedFoundCount = 1 // This map represents a single (aggregated) result item
							} else {
								determinedFoundCount = c
							}
						} else {
							log.Printf("WARN: Could not convert count from alias field '%s' (value: '%s') in simple map to int: %v. Query: %s", countAlias, countStr, errConv, queryInfo["query"])
						}
					} else {
						// Alias was in query, but not in the response map.
						log.Printf("DEBUG: Alias '%s' extracted from query, but not found in the simple map response: %v. Query: %s", countAlias, parsedSimple, queryInfo["query"])
						// determinedFoundCount remains 0, allowing other parsing methods to try.
					}
				} else if len(parsedSimple) > 0 && determinedFoundCount == 0 {
					// No "count() as alias" pattern found in the query, and the response is a simple map.
					log.Printf("DEBUG: Aggregation response is a simple map, but no 'count() as alias' pattern found in query. Map: %v. Query: %s", parsedSimple, queryInfo["query"])
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

// generateSimpleQuery creates a simple field equality query
func (e *VictoriaLogsExecutor) generateSimpleQuery() string {
	// Get available field names from our global map
	fieldNames := make([]string, 0, len(victoriaLogFieldValues))
	for k := range victoriaLogFieldValues {
		fieldNames = append(fieldNames, k)
	}
	if len(fieldNames) == 0 {
		return `_error:"no_fields_available_for_simple_query"` // Fallback
	}
	fieldName := fieldNames[rand.Intn(len(fieldNames))]

	fieldValues, ok := victoriaLogFieldValues[fieldName]
	if !ok || len(fieldValues) == 0 {
		return fmt.Sprintf(`%s:"%s"`, fieldName, "default_fallback_value") // Fallback
	}
	selectedValue := fieldValues[rand.Intn(len(fieldValues))]
	return fmt.Sprintf(`%s:"%s"`, fieldName, selectedValue)
}

// generateComplexQuery creates a query with multiple conditions (AND, OR, NOT)
func (e *VictoriaLogsExecutor) generateComplexQuery() string {
	// Term 1: primary field exact match
	primaryFieldKeys := []string{"host", "service", "log_type", "environment"} // Keep this subset for term1 logic
	label1 := primaryFieldKeys[rand.Intn(len(primaryFieldKeys))]
	primaryFieldValues, ok := victoriaLogFieldValues[label1]
	if !ok || len(primaryFieldValues) == 0 {
		primaryFieldValues = []string{"fallback_val1"} // Fallback
	}
	value1 := primaryFieldValues[rand.Intn(len(primaryFieldValues))]
	term1 := fmt.Sprintf(`%s:"%s"`, label1, value1)

	// Term 2: OR condition for a different field (e.g., datacenter)
	label2 := "datacenter"
	datacenters, ok := victoriaLogFieldValues[label2]
	if !ok || len(datacenters) < 1 { // Check for at least 1, will duplicate if only 1 for OR
		datacenters = []string{"fallback_dc1", "fallback_dc2"} // Fallback
	}
	value2aIdx := rand.Intn(len(datacenters))
	value2bIdx := value2aIdx
	if len(datacenters) > 1 {
		value2bIdx = (value2aIdx + 1 + rand.Intn(len(datacenters)-1)) % len(datacenters) // Ensure different if possible
	}
	value2a := datacenters[value2aIdx]
	value2b := datacenters[value2bIdx]
	term2 := fmt.Sprintf(`(%s:"%s" OR %s:"%s")`, label2, value2a, label2, value2b)

	// Term 3: NOT EQUAL condition for another field (e.g., level)
	label3 := "level"
	levelValues, ok := victoriaLogFieldValues[label3]
	if !ok || len(levelValues) == 0 {
		levelValues = []string{"fallback_level"} // Fallback
	}
	value3 := levelValues[rand.Intn(len(levelValues))]
	term3 := fmt.Sprintf(`%s!:"%s"`, label3, value3) // field!:"value"

	// Combine terms
	finalQuery := fmt.Sprintf("%s AND %s AND %s", term1, term2, term3)

	// Occasionally add a field:not"" to ensure some results if query is too specific
	if rand.Intn(3) == 0 {
		notEmptyFieldOptions := []string{"log_type", "host", "service"} // Keep this small list or derive from keys
		notEmptyField := notEmptyFieldOptions[rand.Intn(len(notEmptyFieldOptions))]
		finalQuery = fmt.Sprintf(`%s:not"" AND (%s)`, notEmptyField, finalQuery)
	}
	return finalQuery
}

// generateAnalyticalQuery creates a query for statistical analysis (e.g., count by field)
func (e *VictoriaLogsExecutor) generateAnalyticalQuery() string {
	allFieldKeys := make([]string, 0, len(victoriaLogFieldValues))
	for k := range victoriaLogFieldValues {
		allFieldKeys = append(allFieldKeys, k)
	}
	if len(allFieldKeys) == 0 {
		return `_error:"no_fields_for_analytical_query" | stats by (_error) count(*) as log_count`
	}

	fieldName := allFieldKeys[rand.Intn(len(allFieldKeys))]
	fieldValues, ok := victoriaLogFieldValues[fieldName]
	if !ok || len(fieldValues) == 0 {
		return fmt.Sprintf(`%s:"fallback_analytical_val" | stats by (%s) count(*) as log_count`, fieldName, fieldName)
	}
	fieldValue := fieldValues[rand.Intn(len(fieldValues))]

	groupByField := allFieldKeys[rand.Intn(len(allFieldKeys))]

	// 40% chance to use quantile aggregation for analytical queries
	if rand.Intn(10) < 4 {
		numericFields := []string{"response_time", "latency", "duration", "bytes", "cpu", "memory"}
		field := numericFields[rand.Intn(len(numericFields))]

		// Multiple unique quantiles for analytical queries
		numQuantiles := rand.Intn(3) + 1 // 1-3 quantiles
		uniqueQuantiles := common.GetUniqueRandomQuantileStrings(numQuantiles)
		quantilesParts := make([]string, len(uniqueQuantiles))
		for i, q := range uniqueQuantiles {
			aliasNum := strings.Replace(q, "0.", "", 1)
			quantilesParts[i] = fmt.Sprintf("quantile(%s, %s) as p%s_%s", q, field, aliasNum, field)
		}

		query := fmt.Sprintf(`%s:"%s" | stats by (%s) %s`,
			fieldName, fieldValue, groupByField, strings.Join(quantilesParts, ", "))
		return query
	}

	query := fmt.Sprintf(`%s:"%s" | stats by (%s) count(*) as log_count`, fieldName, fieldValue, groupByField)
	return query
}

// generateTimeSeriesQuery creates a query that could be used for time series data
func (e *VictoriaLogsExecutor) generateTimeSeriesQuery() string {
	allFieldKeys := make([]string, 0, len(victoriaLogFieldValues))
	for k := range victoriaLogFieldValues {
		allFieldKeys = append(allFieldKeys, k)
	}
	if len(allFieldKeys) == 0 {
		// Needs two group by fields, using a placeholder if no fields
		return `_error:"no_fields_for_timeseries_query" | stats by (_error1, _error2) count() as log_count`
	}

	fieldName := allFieldKeys[rand.Intn(len(allFieldKeys))]
	fieldValues, ok := victoriaLogFieldValues[fieldName]
	if !ok || len(fieldValues) == 0 {
		// Fallback if fieldName has no values, still need two group by fields
		groupByField1 := allFieldKeys[0]
		groupByField2 := allFieldKeys[0]
		if len(allFieldKeys) > 1 {
			groupByField2 = allFieldKeys[1]
		}
		return fmt.Sprintf(`%s:"fallback_ts_val" | stats by (%s, %s) count() as log_count`, fieldName, groupByField1, groupByField2)
	}
	fieldValue := fieldValues[rand.Intn(len(fieldValues))]

	if len(allFieldKeys) < 1 { // Should not happen if we passed the len(allFieldKeys) == 0 check
		return `_error:"critical_no_fields_for_timeseries_grouping" | stats by (_error1, _error2) count() as log_count`
	}

	groupByField1 := allFieldKeys[rand.Intn(len(allFieldKeys))]
	groupByField2 := allFieldKeys[rand.Intn(len(allFieldKeys))]
	// Ensure groupByField1 and groupByField2 are different if possible and more than one key exists
	if len(allFieldKeys) > 1 {
		for groupByField1 == groupByField2 {
			groupByField2 = allFieldKeys[rand.Intn(len(allFieldKeys))]
		}
	} else {
		// If only one key, field1 and field2 will be the same, which is acceptable by VictoriaMetrics LogsQL
		groupByField2 = groupByField1
	}

	query := fmt.Sprintf(`%s:"%s" | stats by (%s, %s) count() as log_count`, fieldName, fieldValue, groupByField1, groupByField2)
	return query
}

// generateStatQuery creates a query for a single statistic
func (e *VictoriaLogsExecutor) generateStatQuery() string {
	allFieldKeys := make([]string, 0, len(victoriaLogFieldValues))
	for k := range victoriaLogFieldValues {
		allFieldKeys = append(allFieldKeys, k)
	}
	if len(allFieldKeys) == 0 {
		return `_error:"no_fields_for_stat_query" | stats by (_error) count(*) as log_count`
	}

	filterField := allFieldKeys[rand.Intn(len(allFieldKeys))]
	filterValues, ok := victoriaLogFieldValues[filterField]
	if !ok || len(filterValues) == 0 {
		return fmt.Sprintf(`%s:"fallback_stat_val" | stats by (%s) count(*) as log_count`, filterField, filterField)
	}
	filterValue := filterValues[rand.Intn(len(filterValues))]

	groupByField := allFieldKeys[rand.Intn(len(allFieldKeys))]

	// 30% chance to use quantile instead of count
	if rand.Intn(10) < 3 {
		numericFields := []string{"response_time", "latency", "duration", "bytes", "cpu", "memory"}
		field := numericFields[rand.Intn(len(numericFields))]
		quantile := common.GetRandomQuantile()

		// Remove the "0." prefix for alias (e.g., "0.95" -> "95")
		aliasNum := strings.Replace(quantile, "0.", "", 1)

		query := fmt.Sprintf(`%s:"%s" | stats by (%s) quantile(%s, %s) as p%s_%s`,
			filterField, filterValue, groupByField, quantile, field, aliasNum, field)
		return query
	}

	query := fmt.Sprintf(`%s:"%s" | stats by (%s) count(*) as log_count`, filterField, filterValue, groupByField)
	return query
}

// generateTopKQuery creates a query to find the top N values for a field
func (e *VictoriaLogsExecutor) generateTopKQuery(k int) string {
	allFieldKeys := make([]string, 0, len(victoriaLogFieldValues))
	for kVal := range victoriaLogFieldValues { // Renamed k to kVal to avoid conflict with func param k
		allFieldKeys = append(allFieldKeys, kVal)
	}
	if len(allFieldKeys) == 0 {
		return fmt.Sprintf(`_error:"no_fields_for_topk_query" | stats by (_error) count() as count_value | sort by (count_value desc) | limit %d`, k)
	}
	topKField := allFieldKeys[rand.Intn(len(allFieldKeys))]

	streamSelector := "log_type:not\"\"" // Default base selector
	if rand.Intn(2) == 0 {               // 50% chance to add a more specific base filter
		baseFilterFieldOptions := []string{"environment", "datacenter", "log_type", "service"}
		validBaseFilterFields := []string{}
		for _, f := range baseFilterFieldOptions {
			if _, exists := victoriaLogFieldValues[f]; exists {
				validBaseFilterFields = append(validBaseFilterFields, f)
			}
		}

		if len(validBaseFilterFields) == 0 { // Fallback if preferred subset is not in victoriaLogFieldValues
			if len(allFieldKeys) > 0 {
				validBaseFilterFields = allFieldKeys // Use all available fields
			} // else, no valid fields, streamSelector remains default
		}

		if len(validBaseFilterFields) > 0 {
			baseFilterField := validBaseFilterFields[rand.Intn(len(validBaseFilterFields))]
			baseFilterValues, ok := victoriaLogFieldValues[baseFilterField]
			if ok && len(baseFilterValues) > 0 {
				baseFilterValue := baseFilterValues[rand.Intn(len(baseFilterValues))]
				streamSelector = fmt.Sprintf(`%s:"%s"`, baseFilterField, baseFilterValue)
			}
		}
	}

	return fmt.Sprintf(`%s | stats by (%s) count() as count_value | sort by (count_value desc) | limit %d`, streamSelector, topKField, k)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
