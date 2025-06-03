package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiExecutor_GenerateRandomQuery(t *testing.T) {
	// Setup
	options := models.Options{
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 100 * time.Millisecond,
		Verbose:    false,
	}
	executor := NewLokiExecutor("http://localhost:3100", options)

	testCases := []struct {
		name      string
		queryType models.QueryType
		validate  func(*testing.T, interface{})
	}{
		{
			name:      "SimpleQuery",
			queryType: models.SimpleQuery,
			validate: func(t *testing.T, query interface{}) {
				// Convert to map to validate structure
				queryMap, ok := query.(map[string]string)
				require.True(t, ok, "Query should be a map[string]string")

				// Validate that it has the expected structure
				queryStr, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Verify start and end time are set
				_, hasStart := queryMap["start"]
				_, hasEnd := queryMap["end"]
				require.True(t, hasStart, "Should have 'start' time")
				require.True(t, hasEnd, "Should have 'end' time")

				// Verify path and limit
				path, hasPath := queryMap["queryPath"]
				require.True(t, hasPath, "Should have 'queryPath'")
				require.Equal(t, "query_range", path, "Path should be 'query_range'")

				_, hasLimit := queryMap["limit"]
				require.True(t, hasLimit, "Should have 'limit' value")
			},
		},
		{
			name:      "ComplexQuery",
			queryType: models.ComplexQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]string)
				require.True(t, ok, "Query should be a map[string]string")

				// Check for complex query elements
				queryStr, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Complex queries should have either label selectors or operators
				hasLabelSelector := strings.Contains(queryStr, "{") && strings.Contains(queryStr, "}")
				hasOperator := strings.Contains(queryStr, "|=") ||
					strings.Contains(queryStr, "!=") ||
					strings.Contains(queryStr, "|~") ||
					strings.Contains(queryStr, "!~")

				require.True(t, hasLabelSelector || hasOperator,
					"Complex query should have either label selector or filtering operator")
			},
		},
		{
			name:      "AnalyticalQuery",
			queryType: models.AnalyticalQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]string)
				require.True(t, ok, "Query should be a map[string]string")

				queryStr, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Check for pattern or formatting operations in Loki analytical queries
				hasAnalyticalOp := strings.Contains(queryStr, "| pattern") ||
					strings.Contains(queryStr, "| json") ||
					strings.Contains(queryStr, "| logfmt") ||
					strings.Contains(queryStr, "| line_format")

				require.True(t, hasAnalyticalOp, "Analytical query should have a processing operator")
			},
		},
		{
			name:      "TimeSeriesQuery",
			queryType: models.TimeSeriesQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]string)
				require.True(t, ok, "Query should be a map[string]string")

				queryStr, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Time series queries should have range functions
				hasTimeRange := strings.Contains(queryStr, "[")
				require.True(t, hasTimeRange, "Time series query should have time window")

				// Check for time operations
				hasTimeOp := strings.Contains(queryStr, "rate(") ||
					strings.Contains(queryStr, "count_over_time(") ||
					strings.Contains(queryStr, "sum_over_time(") ||
					strings.Contains(queryStr, "avg_over_time(") ||
					strings.Contains(queryStr, "min_over_time(") ||
					strings.Contains(queryStr, "max_over_time(") ||
					strings.Contains(queryStr, "bytes_rate(") ||
					strings.Contains(queryStr, "bytes_over_time(")

				require.True(t, hasTimeOp, "Time series query should have a time-based operation")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate query
			query := executor.GenerateRandomQuery(tc.queryType)

			// Verify it's not nil
			require.NotNil(t, query, "Generated query should not be nil")

			// Pretty print the query for debugging
			bytes, err := json.MarshalIndent(query, "", "  ")
			require.NoError(t, err, "Failed to marshal query to JSON")
			t.Logf("Generated %s query: %s", tc.name, string(bytes))

			// Run specific validations
			tc.validate(t, query)
		})
	}
}

func TestLokiExecutor_ExecuteQuery(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify correct HTTP method
		assert.Equal(t, "GET", r.Method)

		// Verify endpoint
		assert.Contains(t, r.URL.Path, "/loki/api/v1/query_range")

		// Parse the query parameters
		params, err := url.ParseQuery(r.URL.RawQuery)
		require.NoError(t, err, "Failed to parse query parameters")

		// Verify required query parameters
		assert.NotEmpty(t, params.Get("query"), "Query parameter should be present")
		assert.NotEmpty(t, params.Get("start"), "Start parameter should be present")
		assert.NotEmpty(t, params.Get("end"), "End parameter should be present")
		assert.NotEmpty(t, params.Get("limit"), "Limit parameter should be present")

		// Return a mock response
		mockResponse := `{
			"status": "success",
			"data": {
				"resultType": "streams",
				"result": [
					{
						"stream": {
							"level": "info",
							"service": "api"
						},
						"values": [
							["1625126450000000000", "log message 1"],
							["1625126460000000000", "log message 2"],
							["1625126470000000000", "log message 3"]
						]
					}
				],
				"stats": {
					"summary": {
						"bytesProcessedPerSecond": 1024,
						"linesProcessedPerSecond": 100,
						"totalBytesProcessed": 2048,
						"totalLinesProcessed": 200,
						"execTime": 0.125
					}
				}
			}
		}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, mockResponse)
	}))
	defer server.Close()

	// Setup
	options := models.Options{
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 100 * time.Millisecond,
		Verbose:    false,
	}
	executor := NewLokiExecutor(server.URL, options)

	// Execute query
	ctx := context.Background()
	result, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.NoError(t, err, "ExecuteQuery should not return an error")
	assert.Equal(t, 1, result.ResultCount, "Result count should match the mock response")
	assert.NotZero(t, result.Duration, "Duration should be non-zero")
	assert.NotEmpty(t, result.RawResponse, "Raw response should not be empty")
}

func TestLokiExecutor_ExecuteQuery_Error(t *testing.T) {
	// Create a mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return an error response
		mockResponse := `{
			"status": "error",
			"errorType": "query_error",
			"error": "parse error at line 1, col 20: syntax error: unexpected IDENTIFIER"
		}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, mockResponse)
	}))
	defer server.Close()

	// Setup
	options := models.Options{
		Timeout:    10 * time.Second,
		RetryCount: 0, // No retries to simplify test
		RetryDelay: 100 * time.Millisecond,
		Verbose:    false,
	}
	executor := NewLokiExecutor(server.URL, options)

	// Execute query
	ctx := context.Background()
	_, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.Error(t, err, "ExecuteQuery should return an error")
	assert.Contains(t, err.Error(), "error response", "Error should contain details")
}
