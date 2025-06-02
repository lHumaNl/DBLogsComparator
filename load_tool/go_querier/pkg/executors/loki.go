package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
)

// LokiExecutor query executor for Loki
type LokiExecutor struct {
	BaseURL string
	Client  *http.Client
	Options models.Options
}

// LokiResponse represents the Loki API response for Query request
type LokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string        `json:"resultType"`
		Result     []interface{} `json:"result"`
		Stats      struct {
			Summary struct {
				BytesProcessedPerSecond int64   `json:"bytesProcessedPerSecond"`
				LinesProcessedPerSecond int64   `json:"linesProcessedPerSecond"`
				TotalBytesProcessed     int64   `json:"totalBytesProcessed"`
				TotalLinesProcessed     int64   `json:"totalLinesProcessed"`
				ExecTime                float64 `json:"execTime"`
			} `json:"summary"`
		} `json:"stats,omitempty"`
	} `json:"data"`
}

// Fixed keywords for searching in logs
var lokiQueryPhrases = []string{
	"error", "warning", "info", "debug", "critical",
	"failed", "success", "timeout", "exception",
	"unauthorized", "forbidden", "not found",
}

// Fixed labels for filtering in Loki
var lokiLabels = []string{
	"log_type", "host", "container_name", "environment", "datacenter",
	"version", "level", "service", "job", "instance",
}

// NewLokiExecutor creates a new query executor for Loki
func NewLokiExecutor(baseURL string, options models.Options) *LokiExecutor {
	client := &http.Client{
		Timeout: options.Timeout,
	}

	return &LokiExecutor{
		BaseURL: baseURL,
		Client:  client,
		Options: options,
	}
}

// GetSystemName returns the system name
func (e *LokiExecutor) GetSystemName() string {
	return "loki"
}

// ExecuteQuery executes a query of the specified type in Loki
func (e *LokiExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Create a random query of the specified type
	queryInfo := e.GenerateRandomQuery(queryType).(map[string]string)

	// Execute the query to Loki
	result, err := e.executeLokiQuery(ctx, queryInfo)
	if err != nil {
		return models.QueryResult{}, err
	}

	return result, nil
}

// GenerateRandomQuery creates a random query of the specified type for Loki
func (e *LokiExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	// Common time range - last 24 hours
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	// For some queries, we take a shorter time range
	var queryString string
	var start string
	var end string
	var limit string
	var queryPath string

	// Convert time to a format understood by Loki (UNIX nanoseconds)
	start = strconv.FormatInt(startTime.UnixNano(), 10)
	end = strconv.FormatInt(now.UnixNano(), 10)

	switch queryType {
	case models.SimpleQuery:
		// Simple log search with one filter
		if rand.Intn(2) == 0 {
			// Search by keyword in log
			keyword := lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))]
			queryString = fmt.Sprintf("{} |= \"%s\"", keyword)
		} else {
			// Search by label
			label := lokiLabels[rand.Intn(len(lokiLabels))]
			labelValue := fmt.Sprintf("value%d", rand.Intn(100))
			queryString = fmt.Sprintf("{%s=\"%s\"}", label, labelValue)
		}

		limit = strconv.Itoa(100 + rand.Intn(100)) // Limit from 100 to 199
		queryPath = "query_range"

	case models.ComplexQuery:
		// Complex search with multiple conditions

		// Start by selecting several labels for filtering
		labelFilters := []string{}

		// Random number of labels from 1 to 3
		numLabels := 1 + rand.Intn(3)
		usedLabels := map[string]bool{}

		for i := 0; i < numLabels; i++ {
			// Select a random label that hasn't been used yet
			var label string
			for {
				label = lokiLabels[rand.Intn(len(lokiLabels))]
				if !usedLabels[label] {
					usedLabels[label] = true
					break
				}
				// If all labels are already used, just exit the loop
				if len(usedLabels) == len(lokiLabels) {
					break
				}
			}

			labelValue := fmt.Sprintf("value%d", rand.Intn(100))
			labelFilters = append(labelFilters, fmt.Sprintf("%s=\"%s\"", label, labelValue))
		}

		// Create a log selector with labels
		labelSelector := "{" + strings.Join(labelFilters, ", ") + "}"

		// Add content filters
		contentFilters := []string{}

		// Random number of content filters from 0 to 2
		numContentFilters := rand.Intn(3)
		for i := 0; i < numContentFilters; i++ {
			// Select a random keyword
			keyword := lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))]

			// Randomly select a filtering operator
			operators := []string{"|=", "!=", "|~", "!~"}
			operator := operators[rand.Intn(len(operators))]

			contentFilters = append(contentFilters, fmt.Sprintf("%s \"%s\"", operator, keyword))
		}

		// Assemble the complete query
		queryString = labelSelector
		if len(contentFilters) > 0 {
			queryString += " " + strings.Join(contentFilters, " ")
		}

		limit = strconv.Itoa(100 + rand.Intn(150)) // Limit from 100 to 249
		queryPath = "query_range"

	case models.AnalyticalQuery:
		// Analytical query with logical operations

		// Select a shorter time range - last 12 hours
		startTime = now.Add(-12 * time.Hour)
		start = strconv.FormatInt(startTime.UnixNano(), 10)

		// Create a basic log selector with one label
		label := lokiLabels[rand.Intn(len(lokiLabels))]
		labelValue := fmt.Sprintf("value%d", rand.Intn(100))
		labelSelector := fmt.Sprintf("{%s=\"%s\"}", label, labelValue)

		// Randomly select the type of analytical operation
		operations := []string{
			"| json",
			"| logfmt",
			"| pattern",
			"| line_format",
		}
		operation := operations[rand.Intn(len(operations))]

		// Form the query depending on the operation
		switch operation {
		case "| json":
			// Extracting fields from JSON
			fields := []string{"level", "message", "error", "status", "duration"}
			field := fields[rand.Intn(len(fields))]
			queryString = fmt.Sprintf("%s | json | %s=\"%s\"", labelSelector, field, lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))])
		case "| logfmt":
			// Parsing logfmt format
			fields := []string{"level", "msg", "err", "status", "duration"}
			field := fields[rand.Intn(len(fields))]
			queryString = fmt.Sprintf("%s | logfmt | %s=\"%s\"", labelSelector, field, lokiQueryPhrases[rand.Intn(len(lokiQueryPhrases))])
		case "| pattern":
			// Pattern matching
			patterns := []string{
				"<_> level=<level> msg=<message>",
				"<_> method=<method> path=<path> status=<status>",
				"<timestamp> <level> <message>",
				"<service>: <message>",
			}
			pattern := patterns[rand.Intn(len(patterns))]
			queryString = fmt.Sprintf("%s | pattern \"%s\"", labelSelector, pattern)
		case "| line_format":
			// Output formatting
			queryString = fmt.Sprintf("%s | line_format \"{{.level}}: {{.message}}\"", labelSelector)
		}

		limit = strconv.Itoa(50 + rand.Intn(50)) // Limit from 50 to 99
		queryPath = "query_range"

	case models.TimeSeriesQuery:
		// Time series query

		// Use a shorter time range - last 6 hours
		startTime = now.Add(-6 * time.Hour)
		start = strconv.FormatInt(startTime.UnixNano(), 10)

		// Select a random metric type
		metrics := []string{
			"rate", "count_over_time", "bytes_rate", "bytes_over_time",
			"avg_over_time", "sum_over_time", "min_over_time", "max_over_time",
		}
		metric := metrics[rand.Intn(len(metrics))]

		// Create a log selector
		label := lokiLabels[rand.Intn(len(lokiLabels))]
		labelValue := fmt.Sprintf("value%d", rand.Intn(100))
		labelSelector := fmt.Sprintf("{%s=\"%s\"}", label, labelValue)

		// Randomly select a time window for aggregation
		windows := []string{"1m", "5m", "10m", "30m", "1h"}
		window := windows[rand.Intn(len(windows))]

		queryString = fmt.Sprintf("%s(%s[%s])", metric, labelSelector, window)

		// For some queries, add additional processing
		if rand.Intn(2) == 0 {
			// Group by label
			groupLabels := []string{"level", "service", "job", "instance"}
			groupLabel := groupLabels[rand.Intn(len(groupLabels))]
			queryString = fmt.Sprintf("sum by(%s) (%s)", groupLabel, queryString)
		}

		limit = "100"
		queryPath = "query_range"
	}

	// Form the final query
	return map[string]string{
		"query":     queryString,
		"start":     start,
		"end":       end,
		"limit":     limit,
		"queryPath": queryPath,
	}
}

// executeLokiQuery executes a query to Loki
func (e *LokiExecutor) executeLokiQuery(ctx context.Context, queryInfo map[string]string) (models.QueryResult, error) {
	// Form the query URL
	queryURL, err := url.Parse(fmt.Sprintf("%s/loki/api/v1/%s", e.BaseURL, queryInfo["queryPath"]))
	if err != nil {
		return models.QueryResult{}, fmt.Errorf("error forming URL: %v", err)
	}

	// Add query parameters
	params := url.Values{}
	params.Add("query", queryInfo["query"])
	params.Add("start", queryInfo["start"])
	params.Add("end", queryInfo["end"])
	params.Add("limit", queryInfo["limit"])

	// For range queries, add step parameter
	if queryInfo["queryPath"] == "query_range" {
		// Random step from 10s to 60s
		step := strconv.Itoa(10+rand.Intn(51)) + "s"
		params.Add("step", step)
	}

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
	var response LokiResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return models.QueryResult{}, fmt.Errorf("error decoding response: %v", err)
	}

	// Count the number of results
	resultCount := 0
	if response.Data.Result != nil {
		resultCount = len(response.Data.Result)
	}

	// Create the result
	result := models.QueryResult{
		Duration:    duration,
		ResultCount: resultCount,
		RawResponse: body,
	}

	return result, nil
}
