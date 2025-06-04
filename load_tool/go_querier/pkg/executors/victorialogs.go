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

// Constants for query generation
var (
	// Fields available in log data based on actual logs
	victoriaLogFields = []string{
		"log_type", "host", "container_name", "environment", "datacenter", "timestamp",
		"level", "message", "remote_addr", "request", "status", "bytes_sent", "http_referer", "http_user_agent", "request_time",
		"error_code", "service", "exception", "stacktrace", "request_id", "request_path", "client_ip",
		"trace_id", "span_id", "request_method", "response_status", "response_time",
		"metric_name", "value", "region", "event_type", "resource_id", "namespace",
	}

	// Common phrases for keyword search
	victoriaPhrases = []string{
		"error", "warning", "exception", "failed", "timeout", "success",
		"completed", "running", "critical", "forbidden", "GET", "POST", "PUT", "DELETE",
	}

	// Available log types for querying
	logTypes = []string{
		"web_access", "web_error", "application", "metric", "event",
	}
)

// NewVictoriaLogsExecutor creates a new query executor for VictoriaLogs
func NewVictoriaLogsExecutor(baseURL string, options models.Options) *VictoriaLogsExecutor {
	// Ensure the base URL ends with a slash
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}

	// Define the search path for VictoriaLogs API
	searchPath := "select/logsql/query"

	return &VictoriaLogsExecutor{
		BaseURL:    baseURL,
		Client:     &http.Client{Timeout: options.Timeout},
		Options:    options,
		SearchPath: searchPath,
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

// GenerateRandomQuery generates a random LogsQL query for VictoriaLogs
func (e *VictoriaLogsExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Common time range - last 7 days
	now := time.Now()
	startTime := now.Add(-168 * time.Hour) // 7 days

	// Format timestamps as strings for use in VictoriaLogs queries
	// Note: VictoriaLogs supports both Unix timestamps and ISO format
	// startTimeStamp := startTime.Unix()
	// endTimeStamp := now.Unix()

	// Generate ISO format for queries
	startTimeISO := startTime.Format(time.RFC3339)
	endTimeISO := now.Format(time.RFC3339)

	var query string

	switch queryType {
	case models.SimpleQuery:
		// Simple query - random choice between different types
		queryType := rand.Intn(5)
		switch queryType {
		case 0:
			// Query by log_type
			logType := logTypes[rand.Intn(len(logTypes))]
			query = fmt.Sprintf("select * where log_type=%q AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
				logType, startTimeISO, endTimeISO)

		case 1:
			// Search by keyword in message
			keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
			query = fmt.Sprintf("select * where message=~%q AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
				keyword, startTimeISO, endTimeISO)

		case 2:
			// Search by level (for web_error and application logs)
			levels := []string{"debug", "info", "warn", "error", "critical"}
			level := levels[rand.Intn(len(levels))]
			query = fmt.Sprintf("select * where level=%q AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
				level, startTimeISO, endTimeISO)

		case 3:
			// Search by status (for web_access logs)
			statuses := []int{200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
			status := statuses[rand.Intn(len(statuses))]
			query = fmt.Sprintf("select * where status=%d AND log_type=\"web_access\" AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
				status, startTimeISO, endTimeISO)

		default:
			// Simple query with only time filter and container name, most likely to find logs
			containers := []string{"app", "web", "api", "db", "cache", "auth"}
			container := containers[rand.Intn(len(containers))]
			query = fmt.Sprintf("select * where container_name=~%q AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
				container, startTimeISO, endTimeISO)
		}

	case models.ComplexQuery:
		// Complex query with multiple conditions based on log type
		logType := logTypes[rand.Intn(len(logTypes))]

		// Base condition always includes log_type and timestamp
		baseCondition := fmt.Sprintf("log_type=%q AND timestamp>=\"%s\" AND timestamp<=\"%s\"",
			logType, startTimeISO, endTimeISO)

		additionalConditions := []string{}

		// Add type-specific conditions
		switch logType {
		case "web_access":
			// For web_access, add conditions related to HTTP status, method or bytes
			if rand.Intn(2) == 0 {
				// Filter by status range
				statuses := []int{200, 400, 500}
				status := statuses[rand.Intn(len(statuses))]
				condition := fmt.Sprintf("status>=%d AND status<%d", status, status+100)
				additionalConditions = append(additionalConditions, condition)
			} else {
				// Filter by method
				methods := []string{"GET", "POST", "PUT", "DELETE"}
				method := methods[rand.Intn(len(methods))]
				condition := fmt.Sprintf("request=~\"^%s \"", method)
				additionalConditions = append(additionalConditions, condition)
			}

			// Maybe add a condition for bytes_sent
			if rand.Intn(2) == 0 {
				condition := fmt.Sprintf("bytes_sent>%d", 1000*rand.Intn(10))
				additionalConditions = append(additionalConditions, condition)
			}

		case "web_error":
			// For web_error, add conditions related to error_code, level
			levels := []string{"error", "critical"}
			level := levels[rand.Intn(len(levels))]
			condition := fmt.Sprintf("level=%q", level)
			additionalConditions = append(additionalConditions, condition)

			// Maybe add condition for error_code
			if rand.Intn(2) == 0 {
				condition := fmt.Sprintf("error_code>%d", 1000+rand.Intn(1000))
				additionalConditions = append(additionalConditions, condition)
			}

		case "application":
			// For application, add conditions related to service, level, response_status
			levels := []string{"debug", "info", "warn", "error", "critical"}
			level := levels[rand.Intn(len(levels))]
			condition := fmt.Sprintf("level=%q", level)
			additionalConditions = append(additionalConditions, condition)

			// Add service condition
			services := []string{"user-service", "auth-service", "payment-service", "order-service", "catalog-service"}
			service := services[rand.Intn(len(services))]
			condition = fmt.Sprintf("service=%q", service)
			additionalConditions = append(additionalConditions, condition)

			// Maybe add response_status condition
			if rand.Intn(2) == 0 {
				statuses := []int{200, 201, 204, 400, 401, 403, 404, 500}
				status := statuses[rand.Intn(len(statuses))]
				condition := fmt.Sprintf("response_status=%d", status)
				additionalConditions = append(additionalConditions, condition)
			}

		case "metric":
			// For metric, add conditions related to metric_name, value, region
			metricNames := []string{"cpu_usage", "memory_usage", "disk_usage", "network_in", "network_out"}
			metricName := metricNames[rand.Intn(len(metricNames))]
			condition := fmt.Sprintf("metric_name=%q", metricName)
			additionalConditions = append(additionalConditions, condition)

			// Maybe add value range condition
			if rand.Intn(2) == 0 {
				condition := fmt.Sprintf("value>%f", rand.Float64()*100)
				additionalConditions = append(additionalConditions, condition)
			}

		case "event":
			// For event, add conditions related to event_type, namespace
			eventTypes := []string{"system_start", "system_stop", "deploy", "rollback", "config_change"}
			eventType := eventTypes[rand.Intn(len(eventTypes))]
			condition := fmt.Sprintf("event_type=%q", eventType)
			additionalConditions = append(additionalConditions, condition)
		}

		// Maybe add a general message keyword search
		if rand.Intn(2) == 0 {
			keyword := victoriaPhrases[rand.Intn(len(victoriaPhrases))]
			condition := fmt.Sprintf("message=~%q", keyword)
			additionalConditions = append(additionalConditions, condition)
		}

		// Form the final complex query
		query = "select * where " + baseCondition
		if len(additionalConditions) > 0 {
			query += " AND " + strings.Join(additionalConditions, " AND ")
		}

	case models.AnalyticalQuery:
		// Analytical query with aggregation based on log type
		logType := logTypes[rand.Intn(len(logTypes))]

		// Base time condition
		timeCondition := fmt.Sprintf("timestamp>=\"%s\" AND timestamp<=\"%s\"", startTimeISO, endTimeISO)

		switch logType {
		case "web_access":
			// For web_access, count by status or calculate average bytes_sent
			if rand.Intn(2) == 0 {
				// Count by status
				query = fmt.Sprintf("select count() by status where log_type=\"web_access\" AND %s", timeCondition)
			} else {
				// Average bytes sent
				query = fmt.Sprintf("select avg(bytes_sent) by status where log_type=\"web_access\" AND %s", timeCondition)
			}

		case "web_error":
			// For web_error, count by error_code or level
			if rand.Intn(2) == 0 {
				query = fmt.Sprintf("select count() by error_code where log_type=\"web_error\" AND %s", timeCondition)
			} else {
				query = fmt.Sprintf("select count() by level where log_type=\"web_error\" AND %s", timeCondition)
			}

		case "application":
			// For application, count by service or level
			if rand.Intn(2) == 0 {
				query = fmt.Sprintf("select count() by service where log_type=\"application\" AND %s", timeCondition)
			} else {
				query = fmt.Sprintf("select count() by level where log_type=\"application\" AND %s", timeCondition)
			}

		case "metric":
			// For metric, average values by metric_name
			query = fmt.Sprintf("select avg(value) by metric_name where log_type=\"metric\" AND %s", timeCondition)

		case "event":
			// For event, count by event_type
			query = fmt.Sprintf("select count() by event_type where log_type=\"event\" AND %s", timeCondition)

		default:
			// Generic count by log_type
			query = fmt.Sprintf("select count() by log_type where %s", timeCondition)
		}

	case models.TimeSeriesQuery:
		// Time-series aggregation: average metric value by minute for one metric_name
		metricNames := []string{"cpu_usage", "memory_usage", "disk_usage"}
		metricName := metricNames[rand.Intn(len(metricNames))]
		query = fmt.Sprintf("select avg(value) by time(1m) where log_type=\"metric\" AND metric_name=\"%s\" AND timestamp>=\"%s\" AND timestamp<=\"%s\"", metricName, startTimeISO, endTimeISO)

	case models.StatQuery:
		// Single number – count of logs for chosen type
		lt := logTypes[rand.Intn(len(logTypes))]
		query = fmt.Sprintf("select count() where log_type=\"%s\" AND timestamp>=\"%s\" AND timestamp<=\"%s\"", lt, startTimeISO, endTimeISO)

	case models.TopKQuery:
		// Top-K services by log volume in last day
		query = fmt.Sprintf("select topk(10)(count()) by service where timestamp>=\"%s\" AND timestamp<=\"%s\"", startTimeISO, endTimeISO)

	default:
		// Fallback to simple all
		query = fmt.Sprintf("select * where timestamp>=\"%s\" AND timestamp<=\"%s\"", startTimeISO, endTimeISO)
	}

	// Debug output
	if e.Options.Verbose {
		fmt.Printf("Debug: VictoriaLogs query: %s\n", query)
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

	// Debug - print the query
	if e.Options.Verbose {
		fmt.Printf("Debug: VictoriaLogs original query: %s\n", query)
	}

	// Override the query to match all documents if needed
	if e.Options.Verbose && rand.Intn(5) == 0 {
		// Every 5th query, try a simple query to find any logs in the system
		now := time.Now()
		startTime := now.Add(-168 * time.Hour) // 7 days
		// Use 'timestamp' field that matches what go_generator produces
		startTimeISO := startTime.Format(time.RFC3339)
		endTimeISO := now.Format(time.RFC3339)
		simpleQuery := fmt.Sprintf("select * where timestamp>=\"%s\" AND timestamp<=\"%s\"", startTimeISO, endTimeISO)
		query = simpleQuery
		fmt.Printf("Debug: Using simple time-only query: %s\n", query)
	}

	// Create the form data for POST request
	formData := url.Values{}
	formData.Set("query", query)

	// Create HTTP request - using POST with form data
	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		queryURL.String(),
		strings.NewReader(formData.Encode()),
	)
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error creating request: %v", err)
	}

	// Set headers for form POST data
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

	// For debugging - print response body
	if e.Options.Verbose {
		fmt.Printf("Debug: VictoriaLogs response body: %s\n", string(body))
	}

	// If body is empty, return empty result with success
	if len(body) == 0 || string(body) == "" {
		return models.QueryResult{
			Duration:  duration,
			HitCount:  0,
			BytesRead: 0,
			Status:    "success",
		}, nil
	}

	// Decode the response
	var response VictoriaLogsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		// If we can't parse as JSON, but status code is OK,
		// we'll return a success result but with zero hits
		return models.QueryResult{
			Duration:  duration,
			HitCount:  0,
			BytesRead: int64(len(body)),
			Status:    "success",
		}, nil
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
