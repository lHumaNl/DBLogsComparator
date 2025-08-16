package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// LokiExecutor executes queries against Loki
type LokiExecutor struct {
	BaseURL    string
	Options    models.Options
	ClientPool *pkg.ClientPool // Use pool instead of single client

	// Cache for label values
	labelCache     map[string][]string
	labelCacheLock sync.RWMutex

	// Available labels from common data
	availableLabels []string

	// Time range generator for realistic queries
	timeRangeGenerator *queriercommon.TimeRangeGenerator
}

// NewLokiExecutor creates a new Loki executor
func NewLokiExecutor(baseURL string, options models.Options) *LokiExecutor {
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

	executor := &LokiExecutor{
		BaseURL:            baseURL,
		Options:            options,
		ClientPool:         clientPool,
		labelCache:         labelCache,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGenerator(),
	}

	return executor
}

// NewLokiExecutorWithTimeConfig creates a new Loki executor with time configuration
func NewLokiExecutorWithTimeConfig(baseURL string, options models.Options, timeConfig *common.TimeRangeConfig) *LokiExecutor {
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

	executor := &LokiExecutor{
		BaseURL:            baseURL,
		Options:            options,
		ClientPool:         clientPool,
		labelCache:         labelCache,
		availableLabels:    availableLabels,
		timeRangeGenerator: queriercommon.NewTimeRangeGeneratorWithConfig(timeConfig),
	}

	return executor
}

// GetSystemName returns the system name
func (e *LokiExecutor) GetSystemName() string {
	return "loki"
}

// GenerateTimeRange generates a time range for queries (for error handling)
func (e *LokiExecutor) GenerateTimeRange() interface{} {
	return e.timeRangeGenerator.GenerateConfiguredTimeRange()
}

// ExecuteQuery executes a query of the specified type in Loki
func (e *LokiExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Generate time range for the query using the configured TimeRangeGenerator
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()

	// Generate a query of the specified type with the time range
	queryInfo := e.GenerateRandomQueryWithTimeRange(queryType, timeRange).(map[string]string)

	// Execute the query against Loki
	result, err := e.executeLokiQuery(queryInfo)
	if err != nil {
		return models.QueryResult{QueryString: queryInfo["query"]}, err
	}

	// Populate time information in the result
	result.StartTime = timeRange.Start
	result.EndTime = timeRange.End
	result.TimeStringRepr = timeRange.StringRepr

	return result, nil
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
	genericValues := []string{"value1", "value2", "value3", "value-" + fmt.Sprint(logdata.RandomIntn(100))}

	// Cache these values for future use
	e.labelCacheLock.Lock()
	e.labelCache[label] = genericValues
	e.labelCacheLock.Unlock()

	return genericValues, nil
}

// GenerateRandomQuery creates a random query of the specified type for Loki
// For compatibility, generates a time range internally
func (e *LokiExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Generate time range for the query using the configured generator
	timeRange := e.timeRangeGenerator.GenerateConfiguredTimeRange()
	return e.GenerateRandomQueryWithTimeRange(queryType, timeRange)
}

// GenerateRandomQueryWithTimeRange creates a random query with a specific time range for Loki
func (e *LokiExecutor) GenerateRandomQueryWithTimeRange(queryType models.QueryType, timeRange queriercommon.TimeRange) interface{} {
	startTimeStr := queriercommon.FormatTimeForLoki(timeRange.Start)
	endTimeStr := queriercommon.FormatTimeForLoki(timeRange.End)

	// Set reasonable limits for the query - use unified limits (max 500 to avoid series limits)
	limits := []string{"10", "50", "100", "200", "300", "500"}
	limit := limits[logdata.RandomIntn(len(limits))] // Random limit for log queries
	step := "10s"                                    // Default step for metric queries

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

// generateSimpleQuery creates a simple query using unified strategy with Loki cardinality optimization
func (e *LokiExecutor) generateSimpleQuery(availableLabels []string) string {
	// Use Loki-compatible unified simple query generation
	params := logdata.GenerateUnifiedSimpleQueryForLoki()

	// Handle message search specially
	if params.Field == "message" {
		// Loki doesn't have labels for message content, so use text filter
		// CARDINALITY OPTIMIZATION: Use universal selector for message searches
		baseSelector := "{log_type=~\".+\"}"

		// Convert value to string for text search
		keyword := fmt.Sprintf("%v", params.Value)

		// Fix regex escaping for Loki compatibility
		if params.IsRegex || params.Operator == "=~" || params.Operator == "!~" {
			keyword = logdata.FixLokiRegexEscaping(keyword)
		}

		// UNIFIED STRATEGY: SimpleQuery must only use positive operators (= or =~)
		// Convert all negative message operators to positive ones
		if params.IsRegex || params.Operator == "=~" || params.Operator == "!~" {
			return fmt.Sprintf("%s |~ \"%s\"", baseSelector, keyword)
		} else {
			return fmt.Sprintf("%s |= \"%s\"", baseSelector, keyword)
		}
	}

	// CARDINALITY OPTIMIZATION: Check if field is a label field or requires JSON search
	if logdata.IsLabelField(params.Field) {
		// Field is in CommonLabels - use direct label selector
		valueStr := fmt.Sprintf("%v", params.Value)

		// Convert negative conditions to positive when possible for SimpleQuery (only one condition)
		field, operator, value := logdata.ConvertNegativeToPositive(params.Field, params.Operator, params.Value)
		valueStr = fmt.Sprintf("%v", value)

		// Fix regex escaping for Loki compatibility
		if params.IsRegex || operator == "=~" || operator == "!~" {
			valueStr = logdata.FixLokiRegexEscaping(valueStr)
		}

		var queryString string
		switch operator {
		case "=":
			queryString = fmt.Sprintf("{%s=\"%s\"}", field, valueStr)
		case "=~":
			queryString = fmt.Sprintf("{%s=~\"%s\"}", field, valueStr)
		default:
			// UNIFIED STRATEGY: GenerateUnifiedSimpleQueryForLoki() already ensures only positive operators
			// Fallback to exact match for any unexpected operators
			queryString = fmt.Sprintf("{%s=\"%s\"}", field, valueStr)
		}

		return queryString
	} else {
		// Field is NOT in CommonLabels - use universal selector + JSON filtering
		// CARDINALITY OPTIMIZATION: Universal selector ensures we don't create high-cardinality streams
		baseSelector := "{log_type=~\".+\"}"

		valueStr := fmt.Sprintf("%v", params.Value)

		// Check if value needs regex escaping (contains regex patterns)
		needsEscaping := params.IsRegex || params.Operator == "=~" || params.Operator == "!~" || strings.Contains(valueStr, `\.`) || strings.Contains(valueStr, `[`) || strings.Contains(valueStr, `(`)
		if needsEscaping {
			valueStr = logdata.FixLokiRegexEscaping(valueStr)
		}

		// Build JSON filter based on operator
		var jsonFilter string
		switch params.Operator {
		case "=":
			jsonFilter = fmt.Sprintf(" | json | %s=\"%s\"", params.Field, valueStr)
		case "!=":
			jsonFilter = fmt.Sprintf(" | json | %s!=\"%s\"", params.Field, valueStr)
		case "=~":
			jsonFilter = fmt.Sprintf(" | json | %s=~\"%s\"", params.Field, valueStr)
		case "!~":
			jsonFilter = fmt.Sprintf(" | json | %s!~\"%s\"", params.Field, valueStr)
		default:
			// Fallback to exact match
			jsonFilter = fmt.Sprintf(" | json | %s=\"%s\"", params.Field, valueStr)
		}

		return baseSelector + jsonFilter
	}
}

// generateComplexQuery creates a complex query using unified strategy with Loki cardinality optimization
func (e *LokiExecutor) generateComplexQuery(availableLabels []string) string {
	// Use Loki-compatible unified complex query generation
	params := logdata.GenerateUnifiedComplexQueryForLoki()

	// CARDINALITY OPTIMIZATION: Split fields into label vs JSON fields
	split := logdata.SplitComplexQueryFields(params.Fields, params.Values, params.Operators)

	// Build base selector from label fields
	var baseSelector string
	if len(split.LabelFields) > 0 {
		// Use actual label fields for selector
		labelExpressions := make([]string, len(split.LabelFields))

		for i, field := range split.LabelFields {
			value := split.LabelValues[i]
			operator := split.LabelOperators[i]

			// Convert value to string and fix regex escaping for Loki compatibility
			valueStr := fmt.Sprintf("%v", value)
			// Apply regex escaping for regex operators OR if value looks like a regex pattern
			if operator == "=~" || operator == "!~" || strings.Contains(valueStr, `\.`) {
				valueStr = logdata.FixLokiRegexEscaping(valueStr)
			}

			// Build label expression
			var expr string
			switch operator {
			case "=":
				expr = fmt.Sprintf(`%s="%s"`, field, valueStr)
			case "!=":
				expr = fmt.Sprintf(`%s!="%s"`, field, valueStr)
			case "=~":
				expr = fmt.Sprintf(`%s=~"%s"`, field, valueStr)
			case "!~":
				expr = fmt.Sprintf(`%s!~"%s"`, field, valueStr)
			default:
				// Fallback to exact match
				expr = fmt.Sprintf(`%s="%s"`, field, valueStr)
			}

			labelExpressions[i] = expr
		}

		// Apply simplified Loki validation for ComplexQuery requirements
		labelExpressions = e.validateComplexQueryForLoki(labelExpressions)

		// Build label selector
		labelSelector := strings.Join(labelExpressions, ", ")
		baseSelector = fmt.Sprintf("{%s}", labelSelector)
	} else {
		// No label fields - use universal selector
		// CARDINALITY OPTIMIZATION: Use guaranteed low-cardinality selector
		baseSelector = "{log_type=~\".+\"}"
	}

	// Add JSON filters for non-label fields
	for i, field := range split.JsonFields {
		// Skip message field (handled separately)
		if field == "message" {
			continue
		}

		value := split.JsonValues[i]
		operator := split.JsonOperators[i]
		valueStr := fmt.Sprintf("%v", value)

		// Check if value needs regex escaping (contains regex patterns)
		needsEscaping := operator == "=~" || operator == "!~" || strings.Contains(valueStr, `\.`) || strings.Contains(valueStr, `[`) || strings.Contains(valueStr, `(`)
		if needsEscaping {
			valueStr = logdata.FixLokiRegexEscaping(valueStr)
		}

		// Build JSON filter based on operator
		var jsonFilter string
		switch operator {
		case "=":
			jsonFilter = fmt.Sprintf(" | json | %s=\"%s\"", field, valueStr)
		case "!=":
			jsonFilter = fmt.Sprintf(" | json | %s!=\"%s\"", field, valueStr)
		case "=~":
			jsonFilter = fmt.Sprintf(" | json | %s=~\"%s\"", field, valueStr)
		case "!~":
			jsonFilter = fmt.Sprintf(" | json | %s!~\"%s\"", field, valueStr)
		default:
			// Fallback to exact match
			jsonFilter = fmt.Sprintf(" | json | %s=\"%s\"", field, valueStr)
		}

		baseSelector += jsonFilter
	}

	// Add message search component if present
	if params.MessageSearch != nil {
		keyword := fmt.Sprintf("%v", params.MessageSearch.Value)

		// Fix regex escaping for Loki compatibility
		if params.MessageSearch.IsRegex || params.MessageSearch.Operator == "=~" || params.MessageSearch.Operator == "!~" {
			keyword = logdata.FixLokiRegexEscaping(keyword)
		}

		// Use appropriate text filter operator
		if params.MessageSearch.IsRegex || params.MessageSearch.Operator == "=~" {
			baseSelector = fmt.Sprintf("%s |~ \"%s\"", baseSelector, keyword)
		} else if params.MessageSearch.Operator == "!~" || params.MessageSearch.Operator == "!=" {
			baseSelector = fmt.Sprintf("%s !~ \"%s\"", baseSelector, keyword)
		} else {
			baseSelector = fmt.Sprintf("%s |= \"%s\"", baseSelector, keyword)
		}
	}

	return baseSelector
}

// generateAnalyticalQuery creates an analytical query
func (e *LokiExecutor) generateAnalyticalQuery() string {
	// Use unified analytical query generation for maximum fairness
	params := logdata.GenerateUnifiedAnalyticalQuery()

	// Build selector using unified parameters with proper Loki regex escaping
	var selector string
	filterValueStr := fmt.Sprintf("%v", params.FilterValue)

	// Apply Loki regex escaping if value contains regex patterns
	if strings.Contains(filterValueStr, `\.`) || strings.Contains(filterValueStr, `[`) || strings.Contains(filterValueStr, `*`) {
		filterValueStr = logdata.FixLokiRegexEscaping(filterValueStr)
		// Use regex operator for escaped patterns
		selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, filterValueStr)
	} else {
		// Use exact match for non-regex values
		selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, filterValueStr)
	}

	// Choose time window from UniversalTimeWindows for fair comparison with other systems
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]

	var expression string

	switch params.AggregationType {
	case "count":
		// Count aggregation with topk for limiting results
		expression = fmt.Sprintf("topk(%d, sum by(%s) (count_over_time(%s[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			timeWindow,
		)

	case "avg":
		// Average aggregation with unwrap and topk for limiting results
		expression = fmt.Sprintf("topk(%d, avg by(%s) (avg_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeWindow,
		)

	case "min":
		// Minimum aggregation with unwrap and topk for limiting results
		expression = fmt.Sprintf("topk(%d, min by(%s) (min_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeWindow,
		)

	case "max":
		// Maximum aggregation with unwrap and topk for limiting results
		expression = fmt.Sprintf("topk(%d, max by(%s) (max_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeWindow,
		)

	case "sum":
		// Sum aggregation with unwrap and topk for limiting results
		expression = fmt.Sprintf("topk(%d, sum by(%s) (sum_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeWindow,
		)

	case "quantile":
		// Quantile aggregation with grouping for consistency with other systems
		expression = fmt.Sprintf("topk(%d, quantile_over_time(%g, (%s | json | unwrap %s)[%s]) by (%s))",
			params.Limit,
			params.Quantile,
			selector,
			params.NumericField,
			timeWindow,
			params.GroupByField,
		)
	}

	return expression
}

// generateTimeSeriesQuery builds a Loki time series query with range aggregations
// FIXED: Now properly generates range queries for time series instead of instant queries
func (e *LokiExecutor) generateTimeSeriesQuery() string {
	// Use unified time series query generation for maximum fairness across all systems
	params := logdata.GenerateUnifiedTimeSeriesQuery()

	// Create selector using unified filter parameters with proper regex escaping
	filterValueStr := fmt.Sprintf("%v", params.FilterValue)
	var selector string

	// Apply Loki regex escaping if value contains regex patterns
	if strings.Contains(filterValueStr, `\.`) || strings.Contains(filterValueStr, `[`) || strings.Contains(filterValueStr, `*`) {
		filterValueStr = logdata.FixLokiRegexEscaping(filterValueStr)
		// Use regex operator for escaped patterns
		selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, filterValueStr)
	} else {
		// Use exact match for non-regex values
		selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, filterValueStr)
	}

	// FIX: Use dynamic time interval for range queries instead of fixed interval
	// This creates multiple time points similar to Elasticsearch date_histogram
	timeInterval := params.TimeInterval // Use as step interval for range aggregation
	var query string

	// Build query based on unified aggregation type for maximum fairness
	switch params.AggregationType {
	case "count":
		// Count aggregation using count_over_time with range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, sum by(%s) (count_over_time(%s[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			timeInterval, // Now supports range queries for time series
		)

	case "avg":
		// Average aggregation using avg_over_time with unwrap and range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, avg by(%s) (avg_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeInterval, // Now supports range queries for time series
		)

	case "min":
		// Minimum aggregation using min_over_time with unwrap and range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, min by(%s) (min_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeInterval, // Now supports range queries for time series
		)

	case "max":
		// Maximum aggregation using max_over_time with unwrap and range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, max by(%s) (max_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeInterval, // Now supports range queries for time series
		)

	case "sum":
		// Sum aggregation using sum_over_time with unwrap and range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, sum by(%s) (sum_over_time((%s | json | unwrap %s)[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			params.NumericField,
			timeInterval, // Now supports range queries for time series
		)

	case "quantile":
		// Quantile aggregation using quantile_over_time with unwrap and range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, quantile_over_time(%g, (%s | json | unwrap %s)[%s]) by (%s))",
			params.Limit,
			params.Quantile,
			selector,
			params.NumericField,
			timeInterval, // Now supports range queries for time series
			params.GroupByField,
		)

	default:
		// Fallback to count with range query support
		// FIX: Creates time series with multiple data points instead of single aggregated value
		query = fmt.Sprintf("topk(%d, sum by(%s) (count_over_time(%s[%s])))",
			params.Limit,
			params.GroupByField,
			selector,
			timeInterval, // Now supports range queries for time series
		)
	}

	return query
}

// generateStatQuery returns a single-value statistical aggregation (count over time)
func (e *LokiExecutor) generateStatQuery(availableLabels []string) string {
	// Use unified stat query generation
	params := logdata.GenerateUnifiedStatQuery()

	// Create base selector using unified field and value with proper Loki regex escaping
	var selector string
	if params.FilterField == "message" {
		// Handle message search specially for Loki
		baseParams := logdata.GenerateUnifiedSimpleQueryForLoki()
		field, operator, value := logdata.ConvertNegativeToPositive(baseParams.Field, baseParams.Operator, baseParams.Value)
		valueStr := fmt.Sprintf("%v", value)
		if operator == "=~" || strings.Contains(valueStr, `\.`) {
			valueStr = logdata.FixLokiRegexEscaping(valueStr)
			selector = fmt.Sprintf(`{%s=~"%s"}`, field, valueStr)
		} else {
			selector = fmt.Sprintf(`{%s="%s"}`, field, valueStr)
		}
		messageValueStr := fmt.Sprintf("%v", params.FilterValue)
		return fmt.Sprintf("%s |= \"%s\"", selector, messageValueStr)
	} else {
		// Handle regular label selectors with regex escaping
		valueStr := fmt.Sprintf("%v", params.FilterValue)

		// Apply Loki regex escaping if value contains regex patterns
		if strings.Contains(valueStr, `\.`) || strings.Contains(valueStr, `[`) || strings.Contains(valueStr, `*`) {
			valueStr = logdata.FixLokiRegexEscaping(valueStr)
			// Use regex operator for escaped patterns
			selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, valueStr)
		} else {
			// Use exact match for non-regex values
			selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, valueStr)
		}
	}

	// Choose time window from UniversalTimeWindows for fair comparison with other systems
	timeWindow := logdata.UniversalTimeWindows[logdata.RandomIntn(len(logdata.UniversalTimeWindows))]
	var expression string

	switch params.StatType {
	case "count":
		// Simple count aggregation using count_over_time with unified time window
		expression = fmt.Sprintf("sum(count_over_time(%s[%s]))", selector, timeWindow)

	case "avg":
		// Average with unwrap - Loki has avg_over_time with unified time window
		expression = fmt.Sprintf("avg_over_time((%s | json | unwrap %s)[%s])",
			selector, params.NumericField, timeWindow)

	case "sum":
		// Sum with unwrap - Loki has sum_over_time with unified time window
		expression = fmt.Sprintf("sum_over_time((%s | json | unwrap %s)[%s])",
			selector, params.NumericField, timeWindow)

	case "min":
		// Minimum with unwrap - Loki has min_over_time with unified time window
		expression = fmt.Sprintf("min_over_time((%s | json | unwrap %s)[%s])",
			selector, params.NumericField, timeWindow)

	case "max":
		// Maximum with unwrap - Loki has max_over_time with unified time window
		expression = fmt.Sprintf("max_over_time((%s | json | unwrap %s)[%s])",
			selector, params.NumericField, timeWindow)

	case "quantile":
		// Quantile with unwrap - Loki has quantile_over_time with unified time window
		expression = fmt.Sprintf("quantile_over_time(%g, (%s | json | unwrap %s)[%s])",
			params.Quantile, selector, params.NumericField, timeWindow)
	}

	return expression
}

// generateTopKQuery returns top-K label values within a time window using unified parameters
func (e *LokiExecutor) generateTopKQuery(availableLabels []string) string {
	// Generate unified TopK parameters for maximum fairness
	params := logdata.GenerateUnifiedTopKQuery()

	// Build selector based on unified parameters
	var selector string
	if params.HasBaseFilter {
		// Create selector with base filtering using unified approach
		if fieldType := logdata.FieldTypeMap[params.FilterField]; fieldType == logdata.StringField {
			// String field - use exact match or regex
			if str, ok := params.FilterValue.(string); ok {
				if strings.Contains(str, "*") {
					// Convert wildcard to regex and fix escaping for Loki
					regexValue := strings.ReplaceAll(str, "*", ".*")
					regexValue = logdata.FixLokiRegexEscaping(regexValue)
					selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, regexValue)
				} else {
					// Check if it's a regex pattern that needs escaping for Loki
					if strings.Contains(str, `\.`) {
						fixedValue := logdata.FixLokiRegexEscaping(str)
						selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, fixedValue)
					} else {
						// Exact match
						selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, str)
					}
				}
			} else {
				// Non-string value - convert to string and check for regex patterns
				valueStr := fmt.Sprintf("%v", params.FilterValue)
				if strings.Contains(valueStr, `\.`) {
					fixedValue := logdata.FixLokiRegexEscaping(valueStr)
					selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, fixedValue)
				} else {
					selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, valueStr)
				}
			}
		} else {
			// Non-string field - convert value to string and check for regex patterns
			valueStr := fmt.Sprintf("%v", params.FilterValue)
			if strings.Contains(valueStr, `\.`) {
				fixedValue := logdata.FixLokiRegexEscaping(valueStr)
				selector = fmt.Sprintf(`{%s=~"%s"}`, params.FilterField, fixedValue)
			} else {
				selector = fmt.Sprintf(`{%s="%s"}`, params.FilterField, valueStr)
			}
		}
	} else {
		// No base filter - use catch-all selector
		selector = fmt.Sprintf(`{%s=~".+"}`, params.GroupByField)
	}

	// Build the topk query according to Loki syntax with unified parameters
	// UNIFIED STRATEGY: Use count_over_time for direct count comparison with other systems
	// This provides absolute counts rather than per-second rates for fairer TopK ranking
	expression := fmt.Sprintf("topk(%d, count by(%s) (count_over_time(%s[%s])))",
		params.K,
		params.GroupByField,
		selector,
		params.TimeWindow,
	)

	return expression
}

// validateComplexQueryForLoki ensures ComplexQuery meets Loki requirements with simplified validation
func (e *LokiExecutor) validateComplexQueryForLoki(expressions []string) []string {
	if len(expressions) == 0 {
		// Generate fallback positive condition if no expressions
		return []string{fmt.Sprintf(`%s=~".+"`, logdata.CommonLabels[0])}
	}

	hasPositive := false
	validatedExpressions := make([]string, 0, len(expressions))

	// Process each expression and check for positive matchers
	for _, expr := range expressions {
		// Check if this is a positive matcher (doesn't contain != or !~)
		if !strings.Contains(expr, "!=") && !strings.Contains(expr, "!~") {
			hasPositive = true
		}
		validatedExpressions = append(validatedExpressions, expr)
	}

	// Ensure minimum 2 conditions and 1 positive matcher
	if len(validatedExpressions) < 2 || !hasPositive {
		// Add guaranteed positive condition at the beginning
		positiveExpr := fmt.Sprintf(`%s=~".+"`, logdata.CommonLabels[0])
		validatedExpressions = append([]string{positiveExpr}, validatedExpressions...)
	}

	return validatedExpressions
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
	client := e.ClientPool.Get()
	resp, err := client.Do(req)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error reading response body: %v", err)
	}

	// Debug mode: output response body
	if e.Options.Debug {
		fmt.Printf("DEBUG [Loki Response]: %s\n", string(bodyBytes))
	}

	// Check if the response status code is not 2xx
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Return error with HTTP status code and response body, but include query string for logging
		return models.QueryResult{
			QueryString: queryInfo["query"],
		}, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
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
		Duration:       time.Since(startTime),
		RawResponse:    bodyBytes,
		BytesRead:      int64(len(bodyBytes)),
		Status:         "success",
		QueryString:    queryInfo["query"],
		StartTime:      startTimeObj,
		EndTime:        endTimeObj,
		Limit:          queryInfo["limit"],
		TimeStringRepr: "", // Will be updated in querier.go
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
