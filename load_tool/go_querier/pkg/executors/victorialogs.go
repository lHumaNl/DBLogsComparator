package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// VictoriaLogsExecutor is a query executor for VictoriaLogs
type VictoriaLogsExecutor struct {
	BaseURL    string
	Client     *http.Client
	Options    models.Options
	SearchPath string
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

// Fixed keywords for searching in logs
var victoriaPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Fixed field names for searching
var victoriaLogFields = []string{
	"log_type", "host", "container_name", "environment", "datacenter",
	"version", "level", "message", "service", "remote_addr",
	"request", "status", "http_referer", "error_code",
}

// NewVictoriaLogsExecutor creates a new query executor for VictoriaLogs
func NewVictoriaLogsExecutor(baseURL string, options models.Options) *VictoriaLogsExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}

	return &VictoriaLogsExecutor{
		BaseURL:    baseURL,
		Client:     client,
		Options:    options,
		SearchPath: "/select/logsql",
	}
}

// GetSystemName returns the system name
func (e *VictoriaLogsExecutor) GetSystemName() string {
	return "victorialogs"
}

// ExecuteQuery executes a query of the specified type in VictoriaLogs
func (e *VictoriaLogsExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Create a random query of the specified type
	query := e.GenerateRandomQuery(queryType).(string)

	// Execute the query to VictoriaLogs
	result, err := e.executeVictoriaLogsQuery(ctx, query)
	if err != nil {
		return models.QueryResult{}, err
	}

	return result, nil
}

// GenerateRandomQuery creates a random query of the specified type for VictoriaLogs
func (e *VictoriaLogsExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Common time range - last 24 hours
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	// For some queries, we take a shorter time range
	var query string

	switch queryType {
	case models.SimpleQuery:
		// Simple search by one field or keyword
		if rand.Intn(2) == 0 {
			// Search by keyword
			keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
			query = fmt.Sprintf("message=~%q", keyword)
		} else {
			// Search by specific field
			field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]
			fieldValue := fmt.Sprintf("value%d", rand.Intn(100))
			query = fmt.Sprintf("%s=%q", field, fieldValue)
		}

	case models.ComplexQuery:
		// Complex search with multiple conditions
		conditions := []string{}

		// Random number of conditions from 2 to 4
		numConditions := 2 + rand.Intn(3)
		for i := 0; i < numConditions; i++ {
			// Select a random field
			field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]

			// Random condition depending on field type
			switch field {
			case "status", "error_code":
				// For numeric fields, use comparison operators
				op := []string{"=", "!=", ">", "<"}[rand.Intn(4)]
				value := 100 + rand.Intn(500)
				conditions = append(conditions, fmt.Sprintf("%s%s%d", field, op, value))
			default:
				// For string fields, use matching operators
				if rand.Intn(2) == 0 {
					// Exact match
					value := fmt.Sprintf("value%d", rand.Intn(100))
					conditions = append(conditions, fmt.Sprintf("%s=%q", field, value))
				} else {
					// Regular expression
					keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
					conditions = append(conditions, fmt.Sprintf("%s=~%q", field, keyword))
				}
			}
		}

		// Combine conditions using AND and OR operators
		for i := 0; i < len(conditions)-1; i++ {
			if rand.Intn(3) < 2 {
				// In most cases, use AND
				conditions[i] = conditions[i] + " AND "
			} else {
				// Sometimes use OR
				conditions[i] = conditions[i] + " OR "
			}
		}

		query = strings.Join(conditions, "")

	case models.AnalyticalQuery:
		// Analytical query with aggregation
		// For VictoriaLogs, we use LogsQL capabilities

		// Select a random field for aggregation
		field := victoriaLogFields[rand.Intn(len(victoriaLogFields))]

		// Select a random aggregation function
		aggregation := []string{"count", "count_distinct"}[rand.Intn(2)]

		// Form a filtering condition
		condition := ""
		if rand.Intn(2) == 0 {
			// Add condition by log level
			levels := []string{"info", "warn", "error", "debug", "critical"}
			level := levels[rand.Intn(len(levels))]
			condition = fmt.Sprintf("level=%q", level)
		} else {
			// Add condition by log type
			logTypes := []string{"web_access", "web_error", "application", "metric", "event"}
			logType := logTypes[rand.Intn(len(logTypes))]
			condition = fmt.Sprintf("log_type=%q", logType)
		}

		// Form the query depending on the aggregation type
		if aggregation == "count" {
			query = fmt.Sprintf("count() by (%s) where %s", field, condition)
		} else {
			query = fmt.Sprintf("count_distinct(%s) where %s", field, condition)
		}

	case models.TimeSeriesQuery:
		// Time series query
		// For VictoriaLogs, we use time intervals

		// Use a shorter time range - last 6 hours
		startTime = now.Add(-6 * time.Hour)

		// Select a random field for time series analysis
		field := []string{"status", "error_code"}[rand.Intn(2)]

		// Form a filtering condition
		condition := ""
		if rand.Intn(2) == 0 {
			// Add condition by log level
			levels := []string{"info", "warn", "error", "debug", "critical"}
			level := levels[rand.Intn(len(levels))]
			condition = fmt.Sprintf("level=%q", level)
		} else {
			// Add condition by log type
			logTypes := []string{"web_access", "web_error", "application", "metric", "event"}
			logType := logTypes[rand.Intn(len(logTypes))]
			condition = fmt.Sprintf("log_type=%q", logType)
		}

		// Form the time series query with time interval
		// VictoriaLogs uses the _time field for time filtering
		timeStart := fmt.Sprintf("_time>=%d", startTime.Unix())
		timeEnd := fmt.Sprintf("_time<=%d", now.Unix())

		// Form the complete query with time conditions
		if condition != "" {
			query = fmt.Sprintf("count() by (%s) where %s AND %s AND %s", field, condition, timeStart, timeEnd)
		} else {
			query = fmt.Sprintf("count() by (%s) where %s AND %s", field, timeStart, timeEnd)
		}
	}

	// Add time filter for queries that don't already have it
	if !strings.Contains(query, "_time") {
		timeFilter := fmt.Sprintf(" _time>=%d AND _time<=%d", startTime.Unix(), now.Unix())
		if strings.Contains(query, "where") {
			query = strings.Replace(query, "where", "where"+timeFilter+" AND", 1)
		} else {
			query = query + " where" + timeFilter
		}
	}

	return query
}

// executeVictoriaLogsQuery executes a query to VictoriaLogs
func (e *VictoriaLogsExecutor) executeVictoriaLogsQuery(ctx context.Context, query string) (models.QueryResult, error) {
	// Form the query URL
	queryURL, err := url.Parse(e.BaseURL + e.SearchPath)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error forming URL: %v", err)
	}

	// Add query parameters
	params := url.Values{}
	params.Add("query", query)
	queryURL.RawQuery = params.Encode()

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", queryURL.String(), nil)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %v", err)
	}

	// Set headers
	req.Header.Set("Accept", "application/json")

	// Execute the request
	startTime := time.Now()
	resp, err := e.Client.Do(req)
	duration := time.Since(startTime)

	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error reading response: %v", err)
	}

	// Check response code
	if resp.StatusCode != http.StatusOK {
		return models.QueryResult{}, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(body))
	}

	// Decode the response
	var response VictoriaLogsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return models.QueryResult{}, fmt.Errorf("error decoding response: %v", err)
	}

	// Check for errors in the response
	if response.Status == "error" {
		return models.QueryResult{}, fmt.Errorf("query error: %s: %s", response.ErrorType, response.Error)
	}

	// Count the number of results
	hitCount := 0
	for _, result := range response.Data.Result {
		if len(result.Values) > 0 {
			hitCount += len(result.Values)
		} else if len(result.Value) > 0 {
			hitCount++
		}
	}

	// Create the result
	result := models.QueryResult{
		Duration:  duration,
		HitCount:  hitCount,
		BytesRead: int64(len(body)),
		Status:    response.Status,
	}

	return result, nil
}
