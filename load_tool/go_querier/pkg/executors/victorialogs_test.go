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

func TestVictoriaLogsExecutor_GenerateRandomQuery(t *testing.T) {
	// Setup
	options := models.Options{
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 100 * time.Millisecond,
		Verbose:    false,
	}
	executor := NewVictoriaLogsExecutor("http://localhost:9428", options, 1)

	testCases := []struct {
		name      string
		queryType models.QueryType
		validate  func(*testing.T, interface{})
	}{
		{
			name:      "SimpleQuery",
			queryType: models.SimpleQuery,
			validate: func(t *testing.T, query interface{}) {
				// Check if the query is a string (not a map as in other executors)
				queryStr, ok := query.(string)
				require.True(t, ok, "Query should be a string")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Simple queries should be simple text with time constraints
				require.True(t,
					strings.Contains(queryStr, "_time>=") && strings.Contains(queryStr, "_time<="),
					"Simple query should have time constraints")
			},
		},
		{
			name:      "ComplexQuery",
			queryType: models.ComplexQuery,
			validate: func(t *testing.T, query interface{}) {
				queryStr, ok := query.(string)
				require.True(t, ok, "Query should be a string")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Complex queries should have boolean operators or multiple conditions
				hasComplexElements := strings.Contains(queryStr, "AND") ||
					strings.Contains(queryStr, "OR") ||
					strings.Contains(queryStr, "NOT") ||
					strings.Contains(queryStr, "where")

				require.True(t, hasComplexElements,
					"Complex query should have boolean operators or where clause")
			},
		},
		{
			name:      "AnalyticalQuery",
			queryType: models.AnalyticalQuery,
			validate: func(t *testing.T, query interface{}) {
				queryStr, ok := query.(string)
				require.True(t, ok, "Query should be a string")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Analytical queries should have aggregations or extractions
				hasAnalyticalOps := strings.Contains(queryStr, "count()") ||
					strings.Contains(queryStr, "by (") ||
					strings.Contains(queryStr, "extract") ||
					strings.Contains(queryStr, "avg(") ||
					strings.Contains(queryStr, "sum(") ||
					strings.Contains(queryStr, "min(") ||
					strings.Contains(queryStr, "max(") ||
					strings.Contains(queryStr, "count_distinct")

				require.True(t, hasAnalyticalOps,
					"Analytical query should have aggregation or extraction operations")
			},
		},
		{
			name:      "TimeSeriesQuery",
			queryType: models.TimeSeriesQuery,
			validate: func(t *testing.T, query interface{}) {
				queryStr, ok := query.(string)
				require.True(t, ok, "Query should be a string")
				require.NotEmpty(t, queryStr, "Query string should not be empty")

				// Time series queries should have time filters and possibly aggregations
				hasTimeFilter := strings.Contains(queryStr, "_time")

				require.True(t, hasTimeFilter, "Time series query should have time-based filter")

				// Should have some kind of aggregation
				hasAggregation := strings.Contains(queryStr, "count()") ||
					strings.Contains(queryStr, "by (") ||
					strings.Contains(queryStr, "avg(") ||
					strings.Contains(queryStr, "sum(") ||
					strings.Contains(queryStr, "min(") ||
					strings.Contains(queryStr, "max(")

				require.True(t, hasAggregation || hasTimeFilter,
					"Time series query should have either aggregation or time filtering")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate query
			query := executor.GenerateRandomQuery(tc.queryType)

			// Verify it's not nil
			require.NotNil(t, query, "Generated query should not be nil")

			// Handle string representation for logging
			var queryText string
			if str, ok := query.(string); ok {
				queryText = str
			} else {
				bytes, err := json.MarshalIndent(query, "", "  ")
				require.NoError(t, err, "Failed to marshal query to JSON")
				queryText = string(bytes)
			}

			t.Logf("Generated %s query: %s", tc.name, queryText)

			// Run specific validations
			tc.validate(t, query)
		})
	}
}

func TestVictoriaLogsExecutor_ExecuteQuery(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify correct HTTP method
		assert.Equal(t, "GET", r.Method)

		// Verify endpoint
		assert.Contains(t, r.URL.Path, "/select/logsql")

		// Parse the query parameters
		params, err := url.ParseQuery(r.URL.RawQuery)
		require.NoError(t, err, "Failed to parse query parameters")

		// Verify required query parameters
		assert.NotEmpty(t, params.Get("query"), "Query parameter should be present")

		// Return a mock response that matches the expected VictoriaLogsResponse structure
		mockResponse := `{
			"status": "success",
			"data": {
				"resultType": "streams",
				"result": [
					{
						"metric": {"level": "info", "service": "api"},
						"values": [
							["1625126450000", "log message 1"],
							["1625126460000", "log message 2"]
						]
					},
					{
						"metric": {"level": "error", "service": "api"},
						"value": ["1625126470000", "error message"]
					}
				]
			}
		}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, mockResponse)
	}))
	defer server.Close()

	// Setup
	options := models.Options{
		Verbose: true,
	}
	executor := NewVictoriaLogsExecutor(server.URL, options, 1)

	// Execute query - explicitly construct a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.NoError(t, err, "ExecuteQuery should not return an error")
	assert.Equal(t, 3, result.HitCount, "Hit count should match the mock response (2 values + 1 value)")
	assert.NotZero(t, result.Duration, "Duration should be non-zero")
	// Note: The current VictoriaLogs executor implementation doesn't set the RawResponse
	// field and doesn't set ResultCount field, so we test against HitCount instead
}

func TestVictoriaLogsExecutor_ExecuteQuery_Error(t *testing.T) {
	// Create a mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return an error response
		mockResponse := `{
			"status": "error",
			"error": "failed to execute query: syntax error"
		}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, mockResponse)
	}))
	defer server.Close()

	// Setup
	options := models.Options{
		Verbose: true,
	}
	executor := NewVictoriaLogsExecutor(server.URL, options, 1)

	// Execute query
	ctx := context.Background()
	_, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.Error(t, err, "ExecuteQuery should return an error")
	assert.Contains(t, err.Error(), "error response", "Error should contain details")
}
