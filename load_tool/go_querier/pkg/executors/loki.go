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

// LokiExecutor executes queries against Loki
type LokiExecutor struct {
	BaseURL string
	Options models.Options
	Client  *http.Client

	// Cache for label values
	labelCache     map[string][]string
	labelCacheLock sync.RWMutex

	// Available labels from common data
	availableLabels []string

	// Time range generator for realistic queries
	timeRangeGenerator *common.TimeRangeGenerator
}

// NewLokiExecutor creates a new Loki executor
func NewLokiExecutor(baseURL string, options models.Options) *LokiExecutor {
	// Create a new HTTP client with a timeout
	client := &http.Client{
		Timeout: options.Timeout,
	}

	// Initialize with static labels from our common data package
	availableLabels := logdata.CommonLabels

	// Initialize label cache with static values
	labelCache := make(map[string][]string)
	labelValuesMap := logdata.GetLabelValuesMap()
	for label, values := range labelValuesMap {
		labelCache[label] = values
	}

	executor := &LokiExecutor{
		BaseURL:            baseURL,
		Options:            options,
		Client:             client,
		labelCache:         labelCache,
		availableLabels:    availableLabels,
		timeRangeGenerator: common.NewTimeRangeGenerator(),
	}

	return executor
}

// GetSystemName returns the system name
func (e *LokiExecutor) GetSystemName() string {
	return "loki"
}

// ExecuteQuery executes a query of the specified type in Loki
func (e *LokiExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Generate a query of the specified type
	queryInfo := e.GenerateRandomQuery(queryType).(map[string]string)

	// Execute the query against Loki
	return e.executeLokiQuery(queryInfo)
}

// GetLabelValues retrieves label values from static data or cache
func (e *LokiExecutor) GetLabelValues(ctx context.Context, label string) ([]string, error) {
	// Check if we have this label in our cache
	e.labelCacheLock.RLock()
	values, exists := e.labelCache[label]
	e.labelCacheLock.RUnlock()

	if exists {
		return values, nil
	}

	// For any label we don't have in our static data, return some generic values
	genericValues := []string{"value1", "value2", "value3", "value-" + fmt.Sprint(rand.Intn(100))}

	// Cache these values for future use
	e.labelCacheLock.Lock()
	e.labelCache[label] = genericValues
	e.labelCacheLock.Unlock()

	return genericValues, nil
}

// GenerateRandomQuery creates a random query of the specified type for Loki
func (e *LokiExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Generate time range for the query
	timeRange := e.timeRangeGenerator.GenerateRandomTimeRange()
	startTimeStr := common.FormatTimeForLoki(timeRange.Start)
	endTimeStr := common.FormatTimeForLoki(timeRange.End)

	// Set reasonable limits for the query - make limit random
	limits := []string{"10", "50", "100", "200", "500", "1000"}
	limit := limits[rand.Intn(len(limits))] // Random limit for log queries
	step := "10s"                           // Default step for metric queries

	// Set shorter step for short time ranges
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

	// Generate the query string
	var queryString string

	switch queryType {
	case models.SimpleQuery:
		// Simple log search with one filter
		queryString = e.generateSimpleQuery(e.availableLabels)

	case models.ComplexQuery:
		// Complex query with multiple conditions
		queryString = e.generateComplexQuery(e.availableLabels)

	case models.AnalyticalQuery:
		// Analytical queries with rate or count_over_time
		queryString = e.generateAnalyticalQuery()

	case models.TimeSeriesQuery:
		// Time-series query (rate over time bucket)
		queryString = e.generateTimeSeriesQuery()

	case models.StatQuery:
		// Single statistical value (count/avg)
		queryString = e.generateStatQuery(e.availableLabels)

	case models.TopKQuery:
		// Top-K query by label value
		queryString = e.generateTopKQuery(e.availableLabels)
	}

	return map[string]string{
		"query": queryString,
		"start": startTimeStr,
		"end":   endTimeStr,
		"limit": limit,
		"step":  step,
	}
}

// generateSimpleQuery creates a simple query using available labels
func (e *LokiExecutor) generateSimpleQuery(availableLabels []string) string {
	// Choose a random label from available labels
	var chosenLabel string

	// Try to use commonly populated labels first
	commonLabels := []string{"log_type", "host", "container_name", "service", "level", "environment"}

	// Check if any of the common labels are available
	for _, label := range commonLabels {
		if contains(availableLabels, label) {
			chosenLabel = label
			break
		}
	}

	// If no common label found, choose any random label
	if chosenLabel == "" {
		chosenLabel = availableLabels[rand.Intn(len(availableLabels))]
	}

	// Use our static data to get values for this label
	var selectedValues []string
	// Choose 1-2 random values
	count := rand.Intn(2) + 1
	selectedValues = logdata.GetMultipleRandomValuesForLabel(chosenLabel, count)

	// Create the query string
	var queryString string
	if len(selectedValues) > 0 {
		// Most frequently used pattern - exact label match (more likely to return results)
		if rand.Intn(10) < 8 {
			if len(selectedValues) == 1 {
				queryString = fmt.Sprintf("{%s=\"%s\"}", chosenLabel, selectedValues[0])
			} else {
				// Use regex for multiple values
				queryString = fmt.Sprintf("{%s=~\"(%s)\"}", chosenLabel, strings.Join(selectedValues, "|"))
			}
		} else {
			// Less frequently used - exclusion pattern
			queryString = fmt.Sprintf("{%s!=\"%s\"}", chosenLabel, selectedValues[0])
		}

		// Add text filter for some queries to make the query more realistic
		if rand.Intn(10) > 5 {
			// Common keywords that users might search for
			keywords := []string{
				"error", "warning", "failed", "exception", "timeout",
				"success", "completed", "started", "authenticated", "authorized",
				"INFO", "WARN", "ERROR", "FATAL", "DEBUG",
				"GET", "POST", "PUT", "DELETE", "PATCH",
			}
			keyword := keywords[rand.Intn(len(keywords))]

			// Randomly choose between case-sensitive or insensitive search
			if rand.Intn(2) == 0 {
				queryString = fmt.Sprintf("%s |= \"%s\"", queryString, keyword)
			} else {
				queryString = fmt.Sprintf("%s |~ \"(?i)%s\"", queryString, keyword)
			}
		}
	}

	return queryString
}

// generateComplexQuery creates a complex query with multiple conditions
func (e *LokiExecutor) generateComplexQuery(availableLabels []string) string {
	// Choose 2-4 random labels from available labels
	labelCount := rand.Intn(3) + 2
	chosenLabels := logdata.GetRandomLabels(labelCount)

	// For each chosen label, create a filter expression
	labelExpressions := make([]string, 0, len(chosenLabels))

	for _, label := range chosenLabels {
		// Generate a filter expression for this label
		// Use regex for 30% of label filters, exact match for 70%
		useRegex := rand.Intn(10) < 3
		valueCount := 1
		if useRegex {
			valueCount = rand.Intn(3) + 1 // 1-3 values for regex
		}

		expr := logdata.BuildLokiLabelFilterExpression(label, valueCount, useRegex)
		labelExpressions = append(labelExpressions, expr)
	}

	// Join all label expressions
	labelSelector := strings.Join(labelExpressions, ", ")

	// Create the query string with the label selector
	queryString := fmt.Sprintf("{%s}", labelSelector)

	// Add text filters (AND, OR, NOT conditions)
	if rand.Intn(10) > 3 { // 70% chance to add a text filter
		// Text filter keywords
		keywords := []string{
			"error", "warning", "failed", "exception", "timeout",
			"success", "completed", "started", "authenticated", "authorized",
		}

		// Choose 1-2 keywords
		keywordCount := rand.Intn(2) + 1
		for i := 0; i < keywordCount; i++ {
			keyword := keywords[rand.Intn(len(keywords))]

			// Randomly choose filter type
			filterType := rand.Intn(4)
			switch filterType {
			case 0:
				// Simple include
				queryString = fmt.Sprintf("%s |= \"%s\"", queryString, keyword)
			case 1:
				// Case insensitive include
				queryString = fmt.Sprintf("%s |~ \"(?i)%s\"", queryString, keyword)
			case 2:
				// Simple exclude
				queryString = fmt.Sprintf("%s != \"%s\"", queryString, keyword)
			case 3:
				// Case insensitive exclude
				queryString = fmt.Sprintf("%s !~ \"(?i)%s\"", queryString, keyword)
			}
		}
	}

	return queryString
}

// generateAnalyticalQuery creates an analytical query
func (e *LokiExecutor) generateAnalyticalQuery() string {
	// Fields that we can unwrap from logs
	unwrapFields := []string{"bytes", "duration", "latency", "size", "count", "level", "status_code"}

	// Define aggregation functions including those requiring unwrap
	aggregationFuncs := []string{
		"sum", "min", "max", "avg", "stddev", "stdvar",
		"sum_over_time", "min_over_time", "max_over_time",
		"avg_over_time", "stddev_over_time", "stdvar_over_time",
		"quantile_over_time", "first_over_time", "last_over_time",
		"absent_over_time",
	}

	// Choose a time window
	windows := []string{"5m", "10m", "30m", "1h", "2h"}
	timeWindow := windows[rand.Intn(len(windows))]

	// Get a random label and its possible values
	label := logdata.GetRandomLabel()
	labelValues := logdata.GetLabelValuesMap()[label]
	var selector string

	// 70% chance to use specific values instead of wildcard
	if len(labelValues) > 0 && rand.Intn(10) < 7 {
		// Use specific values
		numValues := rand.Intn(3) + 1 // 1 to 3 values
		if numValues > len(labelValues) {
			numValues = len(labelValues)
		}

		selectedValues := make([]string, numValues)
		for i := 0; i < numValues; i++ {
			selectedValues[i] = labelValues[rand.Intn(len(labelValues))]
		}

		if len(selectedValues) == 1 {
			selector = fmt.Sprintf(`{%s="%s"}`, label, selectedValues[0])
		} else {
			selector = fmt.Sprintf(`{%s=~"(%s)"}`, label, strings.Join(selectedValues, "|"))
		}
	} else {
		// Use regex for all values
		selector = fmt.Sprintf(`{%s=~".+"}`, label)
	}

	var expression string

	// Random chance to use unwrap pattern for more complex queries
	if rand.Intn(10) < 7 { // 70% chance to use unwrap
		// Select a random field to unwrap
		unwrapField := unwrapFields[rand.Intn(len(unwrapFields))]

		// Select an aggregation function that works with unwrap
		aggFunc := aggregationFuncs[rand.Intn(len(aggregationFuncs))]

		// Create a query with unwrap
		if strings.Contains(aggFunc, "_over_time") {
			// For _over_time functions, we need to unwrap first then apply the function
			expression = fmt.Sprintf("%s((%s | unwrap %s)[%s])",
				aggFunc,
				selector,
				unwrapField,
				timeWindow,
			)
		} else {
			// For other aggregations, we can do the unwrap and then apply the function
			expression = fmt.Sprintf("%s(%s | unwrap %s)",
				aggFunc,
				selector,
				unwrapField,
			)
		}
	} else {
		// Simpler query without unwrap (for log counting style queries)
		simpleFuncs := []string{"count_over_time", "rate", "bytes_rate", "bytes_over_time"}
		simpleFunc := simpleFuncs[rand.Intn(len(simpleFuncs))]

		expression = fmt.Sprintf("%s(%s[%s])",
			simpleFunc,
			selector,
			timeWindow,
		)
	}

	return expression
}

// generateTimeSeriesQuery builds a Loki rate/avg_over_time query grouped by log_type
func (e *LokiExecutor) generateTimeSeriesQuery() string {
	// List of aggregation functions
	aggFuncs := []string{"sum", "min", "max", "avg", "stddev", "stdvar", "count"}

	// List of time window functions
	timeWindowFuncs := []string{"rate", "count_over_time", "bytes_rate", "bytes_over_time"}

	// Choose aggregation and time window function randomly
	aggFunc := aggFuncs[rand.Intn(len(aggFuncs))]
	timeWindowFunc := timeWindowFuncs[rand.Intn(len(timeWindowFuncs))]

	// List of possible time windows
	timeWindows := []string{"5m", "10m", "30m", "1h"}
	timeWindow := timeWindows[rand.Intn(len(timeWindows))]

	// Choose a label to use for filtering
	filterLabel := logdata.GetRandomLabel()
	filterLabelValues := logdata.GetLabelValuesMap()[filterLabel]

	// Get a second label for grouping (make sure it's different from filter label)
	var groupByLabel string
	for {
		groupByLabel = logdata.GetRandomLabel()
		if groupByLabel != filterLabel {
			break
		}
	}

	// Create the filter selector
	var selector string

	// 70% chance to use specific values instead of wildcard
	if len(filterLabelValues) > 0 && rand.Intn(10) < 7 {
		// Use specific values
		numValues := rand.Intn(3) + 1 // 1 to 3 values
		if numValues > len(filterLabelValues) {
			numValues = len(filterLabelValues)
		}

		selectedValues := make([]string, numValues)
		for i := 0; i < numValues; i++ {
			selectedValues[i] = filterLabelValues[rand.Intn(len(filterLabelValues))]
		}

		if len(selectedValues) == 1 {
			selector = fmt.Sprintf(`{%s="%s"}`, filterLabel, selectedValues[0])
		} else {
			selector = fmt.Sprintf(`{%s=~"(%s)"}`, filterLabel, strings.Join(selectedValues, "|"))
		}
	} else {
		// Use regex for all values
		selector = fmt.Sprintf(`{%s=~".+"}`, filterLabel)
	}

	// Build the final query with aggregation by another label
	query := fmt.Sprintf("%s by(%s) (%s(%s [%s]))",
		aggFunc,
		groupByLabel,
		timeWindowFunc,
		selector,
		timeWindow,
	)

	return query
}

// generateStatQuery returns a single-value statistical aggregation (count over time)
func (e *LokiExecutor) generateStatQuery(availableLabels []string) string {
	// Define valid Loki aggregation functions
	aggregationFuncs := []string{
		"sum", "min", "max", "avg", "count", "stddev", "stdvar",
	}

	// Fields that we can unwrap from logs
	unwrapFields := []string{"bytes", "duration", "latency", "size", "count", "level", "status_code"}

	// Define valid time window functions compatible with Loki
	timeWindowFuncs := []string{
		"rate", "count_over_time", "bytes_over_time", "bytes_rate",
		// Add _over_time functions that work with unwrap
		"min_over_time", "max_over_time", "avg_over_time",
		"sum_over_time", "stddev_over_time", "stdvar_over_time",
	}

	// Select random aggregation and time window function
	aggregationFunc := aggregationFuncs[rand.Intn(len(aggregationFuncs))]
	timeWindowFunc := timeWindowFuncs[rand.Intn(len(timeWindowFuncs))]

	// Choose a time window
	windows := []string{"5m", "10m", "30m", "1h", "2h"}
	timeWindow := windows[rand.Intn(len(windows))]

	// Get a random label and its possible values
	label := logdata.GetRandomLabel()
	labelValues := logdata.GetLabelValuesMap()[label]
	var selector string

	// 70% chance to use specific values instead of wildcard
	if len(labelValues) > 0 && rand.Intn(10) < 7 {
		// Use specific values
		numValues := rand.Intn(3) + 1 // 1 to 3 values
		if numValues > len(labelValues) {
			numValues = len(labelValues)
		}

		selectedValues := make([]string, numValues)
		for i := 0; i < numValues; i++ {
			selectedValues[i] = labelValues[rand.Intn(len(labelValues))]
		}

		if len(selectedValues) == 1 {
			selector = fmt.Sprintf(`{%s="%s"}`, label, selectedValues[0])
		} else {
			selector = fmt.Sprintf(`{%s=~"(%s)"}`, label, strings.Join(selectedValues, "|"))
		}
	} else {
		// Use regex for all values
		selector = fmt.Sprintf(`{%s=~".+"}`, label)
	}

	var expression string

	// Check if we need to use unwrap pattern
	if strings.Contains(timeWindowFunc, "_over_time") &&
		timeWindowFunc != "count_over_time" &&
		timeWindowFunc != "bytes_over_time" {
		// These functions need unwrap
		unwrapField := unwrapFields[rand.Intn(len(unwrapFields))]

		expression = fmt.Sprintf("%s(%s((%s | unwrap %s)[%s]))",
			aggregationFunc,
			timeWindowFunc,
			selector,
			unwrapField,
			timeWindow,
		)
	} else {
		// Standard functions without unwrap
		expression = fmt.Sprintf("%s(%s(%s [%s]))",
			aggregationFunc,
			timeWindowFunc,
			selector,
			timeWindow,
		)
	}

	return expression
}

// generateTopKQuery returns top-K label values within a time window
func (e *LokiExecutor) generateTopKQuery(availableLabels []string) string {
	// Define possible values for k (how many top results)
	topKValues := []int{5, 10, 15, 20}
	k := topKValues[rand.Intn(len(topKValues))]

	// Choose a time window
	windows := []string{"5m", "10m", "30m", "1h"}
	timeWindow := windows[rand.Intn(len(windows))]

	// Get a random label for filtering and a random grouping label
	filterLabel := logdata.GetRandomLabel()
	groupByLabel := logdata.GetRandomLabel()

	// Avoid using the same label for filter and groupBy
	for filterLabel == groupByLabel {
		groupByLabel = logdata.GetRandomLabel()
	}

	// Generate selector using the filter label with actual values instead of regex
	labelValues := logdata.GetLabelValuesMap()[filterLabel]
	var selector string

	if len(labelValues) > 0 && rand.Intn(2) == 0 {
		// Use specific values for some queries (50% chance)
		numValues := rand.Intn(3) + 1 // 1 to 3 values
		if numValues > len(labelValues) {
			numValues = len(labelValues)
		}

		selectedValues := make([]string, numValues)
		for i := 0; i < numValues; i++ {
			selectedValues[i] = labelValues[rand.Intn(len(labelValues))]
		}

		if len(selectedValues) == 1 {
			selector = fmt.Sprintf(`{%s="%s"}`, filterLabel, selectedValues[0])
		} else {
			selector = fmt.Sprintf(`{%s=~"(%s)"}`, filterLabel, strings.Join(selectedValues, "|"))
		}
	} else {
		// Use regex for all values
		selector = fmt.Sprintf(`{%s=~".+"}`, filterLabel)
	}

	// Build the topk query according to Loki syntax
	expression := fmt.Sprintf("topk(%d, count by(%s) (rate(%s[%s])))",
		k,
		groupByLabel,
		selector,
		timeWindow,
	)

	return expression
}

// contains checks if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// executeLokiQuery executes a query to Loki
func (e *LokiExecutor) executeLokiQuery(queryInfo map[string]string) (models.QueryResult, error) {
	// Form the query URL - only the endpoint, no parameters
	queryURL := fmt.Sprintf("%s/loki/api/v1/query_range", e.BaseURL)

	// Create the request body
	params := url.Values{}
	for key, value := range queryInfo {
		params.Add(key, value)
	}

	// Create a new POST request with the parameters in the body
	req, err := http.NewRequest("POST", queryURL, strings.NewReader(params.Encode()))
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %v", err)
	}

	// Set content type for POST request
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute the request
	startTime := time.Now()
	resp, err := e.Client.Do(req)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error reading response body: %v", err)
	}

	// Check if the response status code is not 2xx
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return models.QueryResult{}, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse the response
	var lokiResp map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &lokiResp); err != nil {
		return models.QueryResult{}, fmt.Errorf("error unmarshaling response: %v", err)
	}

	// Parse start and end times - Loki uses nanosecond Unix timestamps
	var startTimeObj, endTimeObj time.Time
	if startTimeStr, ok := queryInfo["start"]; ok {
		if nanoSec, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
			startTimeObj = time.Unix(0, nanoSec)
		}
	}
	if endTimeStr, ok := queryInfo["end"]; ok {
		if nanoSec, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
			endTimeObj = time.Unix(0, nanoSec)
		}
	}

	// Create the result
	result := models.QueryResult{
		Duration:    time.Since(startTime),
		RawResponse: bodyBytes,
		BytesRead:   int64(len(bodyBytes)),
		Status:      "success",
		QueryString: queryInfo["query"],
		StartTime:   startTimeObj,
		EndTime:     endTimeObj,
		Limit:       queryInfo["limit"],
	}

	// Extract the result type
	resultType, ok := lokiResp["data"].(map[string]interface{})["resultType"]
	if !ok {
		return result, nil // Return empty result if no data
	}

	// Parse the results based on the result type
	if resultType.(string) == "matrix" {
		// Matrix results (for metrics queries)
		matrix, ok := lokiResp["data"].(map[string]interface{})["result"].([]interface{})
		if !ok {
			return result, nil
		}

		// Count the number of samples
		var totalSamples int
		for _, series := range matrix {
			seriesMap, ok := series.(map[string]interface{})
			if !ok {
				continue
			}

			values, ok := seriesMap["values"].([]interface{})
			if !ok {
				continue
			}

			totalSamples += len(values)
		}

		result.HitCount = totalSamples

	} else {
		// Streams results (for log queries)
		streams, ok := lokiResp["data"].(map[string]interface{})["result"].([]interface{})
		if !ok {
			return result, nil
		}

		// Count the number of log entries
		var totalEntries int
		for _, stream := range streams {
			streamMap, ok := stream.(map[string]interface{})
			if !ok {
				continue
			}

			entries, ok := streamMap["values"].([]interface{})
			if !ok {
				continue
			}

			totalEntries += len(entries)
		}

		result.HitCount = totalEntries
	}

	return result, nil
}
