package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/go_querier/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticsearchExecutor_GenerateRandomQuery(t *testing.T) {
	// Setup
	options := models.Options{
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 100 * time.Millisecond,
		Verbose:    false,
	}
	executor := NewElasticsearchExecutor("http://localhost:9200", options)

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
				queryMap, ok := query.(map[string]interface{})
				require.True(t, ok, "Query should be a map")

				// Validate that it has the expected structure
				queryObj, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")

				// Verify it contains bool query with must clause
				boolQuery, ok := queryObj.(map[string]interface{})["bool"]
				require.True(t, ok, "Should have a 'bool' query")

				must, ok := boolQuery.(map[string]interface{})["must"]
				require.True(t, ok, "Should have a 'must' clause")

				// Verify must is an array with at least one element
				mustArray, ok := must.([]interface{})
				require.True(t, ok, "Must clause should be an array")
				require.GreaterOrEqual(t, len(mustArray), 1, "Must array should have at least one element")
			},
		},
		{
			name:      "ComplexQuery",
			queryType: models.ComplexQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]interface{})
				require.True(t, ok, "Query should be a map")

				// Validate that it has the expected structure
				queryObj, exists := queryMap["query"]
				require.True(t, exists, "Should have a 'query' field")

				// Verify it contains bool query with either must or should clauses
				boolQuery, ok := queryObj.(map[string]interface{})["bool"]
				require.True(t, ok, "Should have a 'bool' query")

				// Check for must or should clause
				_, hasMust := boolQuery.(map[string]interface{})["must"]
				_, hasShould := boolQuery.(map[string]interface{})["should"]

				require.True(t, hasMust || hasShould, "Should have either 'must' or 'should' clause")
			},
		},
		{
			name:      "AnalyticalQuery",
			queryType: models.AnalyticalQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]interface{})
				require.True(t, ok, "Query should be a map")

				// Verify it has query and aggregations fields
				_, hasQuery := queryMap["query"]
				aggs, hasAggs := queryMap["aggregations"]

				require.True(t, hasQuery, "Should have a 'query' field")
				require.True(t, hasAggs, "Should have an 'aggregations' field")

				// Check aggregations structure
				aggsMap, ok := aggs.(map[string]interface{})
				require.True(t, ok, "Aggregations should be a map")
				require.NotEmpty(t, aggsMap, "Aggregations should not be empty")
			},
		},
		{
			name:      "TimeSeriesQuery",
			queryType: models.TimeSeriesQuery,
			validate: func(t *testing.T, query interface{}) {
				queryMap, ok := query.(map[string]interface{})
				require.True(t, ok, "Query should be a map")

				// Verify it contains query and aggregations with time buckets
				_, hasQuery := queryMap["query"]
				aggs, hasAggs := queryMap["aggregations"]

				require.True(t, hasQuery, "Should have a 'query' field")
				require.True(t, hasAggs, "Should have an 'aggregations' field")

				// Check for time-based aggregation
				aggsMap, ok := aggs.(map[string]interface{})
				require.True(t, ok, "Aggregations should be a map")

				timeBuckets, hasTimeBuckets := aggsMap["time_buckets"]
				require.True(t, hasTimeBuckets, "Should have 'time_buckets' aggregation")

				// Check for date histogram
				bucketsMap, ok := timeBuckets.(map[string]interface{})
				require.True(t, ok, "Time buckets should be a map")

				_, hasDateHist := bucketsMap["date_histogram"]
				require.True(t, hasDateHist, "Should have a 'date_histogram' aggregation")
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

func TestElasticsearchExecutor_ExecuteQuery(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify correct HTTP method
		assert.Equal(t, "POST", r.Method)

		// Verify endpoint
		assert.Contains(t, r.URL.Path, "/_search")

		// Return a mock response
		mockResponse := `{
			"took": 5,
			"timed_out": false,
			"hits": {
				"total": {
					"value": 42,
					"relation": "eq"
				},
				"max_score": 1.0,
				"hits": []
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
	executor := NewElasticsearchExecutor(server.URL, options)

	// Execute query
	ctx := context.Background()
	result, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.NoError(t, err, "ExecuteQuery should not return an error")
	assert.Equal(t, 42, result.ResultCount, "Result count should match the mock response")
	assert.NotZero(t, result.Duration, "Duration should be non-zero")
	assert.NotEmpty(t, result.RawResponse, "Raw response should not be empty")
}

func TestElasticsearchExecutor_ExecuteQuery_Error(t *testing.T) {
	// Create a mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return an error response
		mockResponse := `{
			"error": {
				"type": "index_not_found_exception",
				"reason": "no such index"
			},
			"status": 404
		}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
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
	executor := NewElasticsearchExecutor(server.URL, options)

	// Execute query
	ctx := context.Background()
	_, err := executor.ExecuteQuery(ctx, models.SimpleQuery)

	// Verify
	require.Error(t, err, "ExecuteQuery should return an error")
	assert.Contains(t, err.Error(), "elasticsearch error", "Error should contain details")
}
