package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

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
	labelCacheTime map[string]time.Time

	// Available labels in the system
	availableLabels   []string
	hasData           bool
	systemChecked     bool
	systemCheckedLock sync.RWMutex
}

// NewLokiExecutor creates a new Loki executor
func NewLokiExecutor(baseURL string, options models.Options) *LokiExecutor {
	// Create a new HTTP client with a timeout
	client := &http.Client{
		Timeout: options.Timeout,
	}

	executor := &LokiExecutor{
		BaseURL:         baseURL,
		Options:         options,
		Client:          client,
		labelCache:      make(map[string][]string),
		labelCacheTime:  make(map[string]time.Time),
		availableLabels: make([]string, 0),
		hasData:         false,
		systemChecked:   false,
	}

	// Start async system check to discover labels and verify data presence
	go executor.checkSystemData(context.Background())

	return executor
}

// GetSystemName returns the system name
func (e *LokiExecutor) GetSystemName() string {
	return "loki"
}

// checkSystemData checks if the Loki system has data and what labels are available
func (e *LokiExecutor) checkSystemData(ctx context.Context) {
	if e.Options.Verbose {
		log.Println("Debug: Starting Loki system check...")
	}

	// Get available labels
	labels, err := e.getAvailableLabels(ctx)
	if err != nil {
		if e.Options.Verbose {
			log.Printf("Debug: Error getting labels: %v", err)
		}
		// Set default values
		e.systemCheckedLock.Lock()
		e.availableLabels = []string{}
		e.hasData = false
		e.systemChecked = true
		e.systemCheckedLock.Unlock()
		return
	}

	// Store available labels
	e.systemCheckedLock.Lock()
	e.availableLabels = labels
	e.systemCheckedLock.Unlock()

	// Check if we have data by trying a simple query with each label
	hasData := false
	if len(labels) > 0 {
		for _, label := range labels {
			// Get values for this label
			values, err := e.fetchLabelValues(ctx, label)
			if err != nil || len(values) == 0 {
				continue
			}

			// Try a simple query with this label and value
			dataFound, err := e.executeSimpleTestQuery(ctx, label, values[0])
			if err != nil {
				if e.Options.Verbose {
					log.Printf("Debug: Error executing test query: %v", err)
				}
				continue
			}

			if dataFound {
				hasData = true
				break
			}
		}
	}

	// If no specific label queries worked, try a generic query
	if !hasData && len(labels) > 0 {
		dataFound, err := e.executeSimpleTestQuery(ctx, "job", ".+")
		if err == nil && dataFound {
			hasData = true
		}
	}

	// Update system status
	e.systemCheckedLock.Lock()
	e.hasData = hasData
	e.systemChecked = true
	e.systemCheckedLock.Unlock()

	if e.Options.Verbose {
		log.Printf("Debug: Loki system check completed. Has data: %v, Available labels: %v", hasData, labels)
	}

	// Start refreshing the label cache
	go e.refreshLabelCache(ctx)
}

// executeSimpleTestQuery executes a simple query to check if there's data in the system
func (e *LokiExecutor) executeSimpleTestQuery(ctx context.Context, label string, value string) (bool, error) {
	// Build a simple query to check for data
	queryString := fmt.Sprintf("{%s=\"%s\"}", label, value)

	// Create query parameters
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)

	params := url.Values{}
	params.Add("query", queryString)
	params.Add("start", fmt.Sprintf("%d", startTime.UnixNano()))
	params.Add("end", fmt.Sprintf("%d", now.UnixNano()))
	params.Add("limit", "1")

	// Build the URL
	queryURL := fmt.Sprintf("%s/loki/api/v1/query_range", e.BaseURL)

	// Create a POST request with the parameters in the body
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(params.Encode()))
	if err != nil {
		return false, fmt.Errorf("error creating request: %v", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute the request
	resp, err := e.Client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// If we get a non-200 response, it might be a query syntax error
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse the response
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string        `json:"resultType"`
			Result     []interface{} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return false, fmt.Errorf("error decoding response: %v", err)
	}

	// Check if we have any results
	hasData := len(response.Data.Result) > 0

	if e.Options.Verbose {
		log.Printf("Debug: Test query %s returned data: %v", queryString, hasData)
	}

	return hasData, nil
}

// getAvailableLabels gets all available labels from Loki
func (e *LokiExecutor) getAvailableLabels(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/loki/api/v1/labels", e.BaseURL)

	// Create a POST request with empty body
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	if e.Options.Verbose {
		log.Printf("Debug: Found %d available labels in Loki: %v", len(response.Data), response.Data)
	}

	return response.Data, nil
}

// ExecuteQuery executes a query of the specified type in Loki
func (e *LokiExecutor) ExecuteQuery(ctx context.Context, queryType models.QueryType) (models.QueryResult, error) {
	// Wait for system check to complete if it hasn't already
	if !e.IsSystemChecked() {
		if e.Options.Verbose {
			log.Println("Debug: Waiting for system check to complete...")
		}

		// Wait for up to 5 seconds for system check to complete
		for i := 0; i < 10; i++ {
			if e.IsSystemChecked() {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		if !e.IsSystemChecked() && e.Options.Verbose {
			log.Println("Debug: System check still not complete, proceeding anyway")
		}
	}

	// Generate the query
	queryInfo := e.GenerateRandomQuery(queryType).(map[string]string)

	// For analytical queries, we need to modify the query parameter to use the correct format
	if queryType == models.AnalyticalQuery {
		// Check if the query is a valid analytical query
		if !strings.Contains(queryInfo["query"], "count_over_time") &&
			!strings.Contains(queryInfo["query"], "rate") &&
			!strings.Contains(queryInfo["query"], "sum") &&
			!strings.Contains(queryInfo["query"], "avg") {
			// If not, replace with a valid analytical query
			queryInfo["query"] = e.generateAnalyticalQuery(e.availableLabels)
		}
	}

	// Check if we need to use a fallback query
	e.systemCheckedLock.RLock()
	hasData := e.hasData
	hasLabels := len(e.availableLabels) > 0
	e.systemCheckedLock.RUnlock()

	if !hasData || !hasLabels {
		if e.Options.Verbose {
			log.Printf("Debug: Using fallback query: %s", e.generateFallbackQuery())
		}
		queryInfo["query"] = e.generateFallbackQuery()
	}

	// Execute the query
	return e.executeLokiQuery(queryInfo)
}

// generateFallbackQuery creates a query that should find any logs in the system
func (e *LokiExecutor) generateFallbackQuery() string {
	e.systemCheckedLock.RLock()
	defer e.systemCheckedLock.RUnlock()

	// If we have available labels, use the first one with values
	if len(e.availableLabels) > 0 {
		for _, label := range e.availableLabels {
			values, err := e.GetLabelValues(context.Background(), label)
			if err == nil && len(values) > 0 {
				// Use the first value of this label
				return fmt.Sprintf("{%s=~\"%s\"}", label, values[0])
			}
		}
	}

	// If no labels or values found, use a matcher that will work with empty Loki
	// Use a pattern that matches any value but is not empty-compatible
	return `{job=~".+"}`
}

// GetLabelValues retrieves label values from Loki API and caches them
func (e *LokiExecutor) GetLabelValues(ctx context.Context, label string) ([]string, error) {
	// Check if we have cached values that are still fresh (less than 5 minutes old)
	e.labelCacheLock.RLock()
	if values, ok := e.labelCache[label]; ok && time.Since(e.labelCacheTime[label]) < 5*time.Minute {
		e.labelCacheLock.RUnlock()
		return values, nil
	}
	e.labelCacheLock.RUnlock()

	// Get fresh values from API
	url := fmt.Sprintf("%s/loki/api/v1/label/%s/values", e.BaseURL, label)

	if e.Options.Verbose {
		log.Printf("Debug: Fetching label values for %s from Loki API", label)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	client := &http.Client{
		Timeout: e.Options.Timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status %d: %s", resp.StatusCode, body)
	}

	var response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Update cache
	e.labelCacheLock.Lock()
	defer e.labelCacheLock.Unlock()

	e.labelCache[label] = response.Data
	e.labelCacheTime[label] = time.Now()

	if e.Options.Verbose {
		log.Printf("Debug: Found %d values for label %s", len(response.Data), label)
	}

	return response.Data, nil
}

// refreshLabelCache refreshes the label cache asynchronously
func (e *LokiExecutor) refreshLabelCache(ctx context.Context) {
	// Get all available labels
	labels, err := e.getAvailableLabels(ctx)
	if err != nil {
		log.Printf("Error refreshing label cache: %v", err)
		return
	}

	// Update available labels
	e.systemCheckedLock.Lock()
	e.availableLabels = labels
	e.systemCheckedLock.Unlock()

	// For each label, get its values
	for _, label := range labels {
		values, err := e.fetchLabelValues(ctx, label)
		if err != nil {
			if e.Options.Verbose {
				log.Printf("Debug: Error fetching values for label %s: %v", label, err)
			}
			continue
		}

		// Update the cache
		e.labelCacheLock.Lock()
		e.labelCache[label] = values
		e.labelCacheTime[label] = time.Now()
		e.labelCacheLock.Unlock()

		if e.Options.Verbose {
			log.Printf("Debug: Refreshed cache for label %s with %d values", label, len(values))
		}
	}
}

// fetchLabelValues fetches values for a specific label directly from Loki API
func (e *LokiExecutor) fetchLabelValues(ctx context.Context, label string) ([]string, error) {
	// Build the URL for label values API endpoint
	url := fmt.Sprintf("%s/loki/api/v1/label/%s/values", e.BaseURL, label)

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Set content type
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute the request
	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response is successful
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response: code %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse the response
	var response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	if e.Options.Verbose {
		log.Printf("Debug: Found %d values for label %s: %v", len(response.Data), label, response.Data)
	}

	return response.Data, nil
}

// GenerateRandomQuery creates a random query of the specified type for Loki
func (e *LokiExecutor) GenerateRandomQuery(queryType models.QueryType) interface{} {
	now := time.Now()
	startTime := now.Add(-24 * time.Hour)
	startTimeStr := fmt.Sprintf("%d", startTime.UnixNano())
	endTimeStr := fmt.Sprintf("%d", now.UnixNano())

	limit := "100"
	step := "10s"

	var queryString string

	// Get available labels and their values
	e.systemCheckedLock.RLock()
	availableLabels := e.availableLabels
	hasData := e.hasData
	e.systemCheckedLock.RUnlock()

	// If we have no data or no labels, use a valid fallback query
	if !hasData || len(availableLabels) == 0 {
		// Use a pattern that matches any value but is not empty-compatible
		return map[string]string{
			"query": `{job=~".+"}`,
			"start": startTimeStr,
			"end":   endTimeStr,
			"limit": limit,
			"step":  step,
		}
	}

	switch queryType {
	case models.SimpleQuery:
		// Simple log search with one filter
		queryString = e.generateSimpleQuery(availableLabels)

	case models.ComplexQuery:
		// Complex query with multiple conditions
		queryString = e.generateComplexQuery(availableLabels)

	case models.AnalyticalQuery:
		// Analytical queries with rate or count_over_time
		queryString = e.generateAnalyticalQuery(availableLabels)

	case models.TimeSeriesQuery:
		// Time-series query (rate over time bucket)
		queryString = e.generateTimeSeriesQuery(availableLabels)

	case models.StatQuery:
		// Single statistical value (count/avg)
		queryString = e.generateStatQuery(availableLabels)

	case models.TopKQuery:
		// Top-K query by label value
		queryString = e.generateTopKQuery(availableLabels)
	}

	if e.Options.Verbose {
		log.Printf("Debug: Generated Loki query: %s", queryString)
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
	// If we have no available labels, return a valid fallback query
	if len(availableLabels) == 0 {
		return `{job=~".+"}`
	}

	// Choose a random label from available labels
	chosenLabel := availableLabels[rand.Intn(len(availableLabels))]

	// Get values for this label
	values, err := e.GetLabelValues(context.Background(), chosenLabel)
	if err != nil || len(values) == 0 {
		// If no values, use a valid fallback
		return `{job=~".+"}`
	}

	// Choose 1-2 random values
	count := rand.Intn(2) + 1
	selectedValues := make([]string, 0, count)
	for i := 0; i < count && i < len(values); i++ {
		// Choose a random value that's not already selected
		for {
			value := values[rand.Intn(len(values))]
			// Check if value is already selected
			alreadySelected := false
			for _, selected := range selectedValues {
				if selected == value {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected {
				selectedValues = append(selectedValues, value)
				break
			}
			// If we've tried too many times, just use what we have
			if len(selectedValues) > 0 && rand.Intn(5) == 0 {
				break
			}
		}
	}

	// Create the query string
	if len(selectedValues) > 0 {
		queryString := fmt.Sprintf("{%s=~\"%s\"}", chosenLabel, strings.Join(selectedValues, "|"))

		// Add text filter for some queries
		if rand.Intn(10) > 6 {
			filters := []string{"error", "exception", "warning", "info", "debug"}
			filter := filters[rand.Intn(len(filters))]
			queryString = fmt.Sprintf(`%s |= "%s"`, queryString, filter)
		}

		return queryString
	}

	// Fallback to valid query
	return `{job=~".+"}`
}

// generateComplexQuery creates a complex query with multiple conditions
func (e *LokiExecutor) generateComplexQuery(availableLabels []string) string {
	// If we have no available labels, return a valid fallback query
	if len(availableLabels) == 0 {
		return `{job=~".+"}`
	}

	// Choose 1-3 random labels from available labels
	labelCount := rand.Intn(3) + 1
	chosenLabels := make([]string, 0, labelCount)

	// Select random labels
	for i := 0; i < labelCount && i < len(availableLabels); i++ {
		// Choose a random label that's not already selected
		for {
			label := availableLabels[rand.Intn(len(availableLabels))]
			// Check if label is already selected
			alreadySelected := false
			for _, selected := range chosenLabels {
				if selected == label {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected {
				chosenLabels = append(chosenLabels, label)
				break
			}
			// If we've tried too many times, just use what we have
			if len(chosenLabels) > 0 && rand.Intn(5) == 0 {
				break
			}
		}
	}

	// Build conditions for each chosen label
	var conditions []string
	for _, label := range chosenLabels {
		values, err := e.GetLabelValues(context.Background(), label)
		if err != nil || len(values) == 0 {
			continue
		}

		// Choose 1-2 random values
		count := rand.Intn(2) + 1
		selectedValues := make([]string, 0, count)
		for i := 0; i < count && i < len(values); i++ {
			// Choose a random value that's not already selected
			for {
				value := values[rand.Intn(len(values))]
				// Check if value is already selected
				alreadySelected := false
				for _, selected := range selectedValues {
					if selected == value {
						alreadySelected = true
						break
					}
				}
				if !alreadySelected {
					selectedValues = append(selectedValues, value)
					break
				}
				// If we've tried too many times, just use what we have
				if len(selectedValues) > 0 && rand.Intn(5) == 0 {
					break
				}
			}
		}

		if len(selectedValues) > 0 {
			conditions = append(conditions, fmt.Sprintf("%s=~\"%s\"", label, strings.Join(selectedValues, "|")))
		}
	}

	// Ensure we have at least one condition
	if len(conditions) == 0 {
		return `{job=~".+"}`
	}

	// Create the query string
	queryString := fmt.Sprintf("{%s}", strings.Join(conditions, ", "))

	// Add text match filter
	if rand.Intn(10) > 3 {
		filters := []string{"error", "warning", "info", "debug", "GET", "POST", "DELETE"}
		filter := filters[rand.Intn(len(filters))]
		queryString = fmt.Sprintf(`%s |= "%s"`, queryString, filter)
	}

	return queryString
}

// generateAnalyticalQuery creates an analytical query
func (e *LokiExecutor) generateAnalyticalQuery(availableLabels []string) string {
	// If we have no available labels, return a valid count query
	if len(availableLabels) == 0 {
		return `count_over_time({job=~".+"}[5m])`
	}

	// Choose a random label from available labels
	chosenLabel := availableLabels[rand.Intn(len(availableLabels))]

	// Get values for this label
	values, err := e.GetLabelValues(context.Background(), chosenLabel)
	if err != nil || len(values) == 0 {
		// If no values, use a valid fallback
		return `count_over_time({job=~".+"}[5m])`
	}

	// Choose 1-2 random values
	count := rand.Intn(2) + 1
	selectedValues := make([]string, 0, count)
	for i := 0; i < count && i < len(values); i++ {
		// Choose a random value that's not already selected
		for {
			value := values[rand.Intn(len(values))]
			// Check if value is already selected
			alreadySelected := false
			for _, selected := range selectedValues {
				if selected == value {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected {
				selectedValues = append(selectedValues, value)
				break
			}
			// If we've tried too many times, just use what we have
			if len(selectedValues) > 0 && rand.Intn(5) == 0 {
				break
			}
		}
	}

	// Create the selector
	var selector string
	if len(selectedValues) > 0 {
		selector = fmt.Sprintf("{%s=~\"%s\"}", chosenLabel, strings.Join(selectedValues, "|"))
	} else {
		selector = `{job=~".+"}`
	}

	// Choose time window
	timeWindows := []string{"1m", "5m", "10m", "15m"}
	timeWindow := timeWindows[rand.Intn(len(timeWindows))]

	// Choose query pattern
	pattern := rand.Intn(5)

	switch pattern {
	case 0:
		// Simple count_over_time
		return fmt.Sprintf("count_over_time(%s[%s])", selector, timeWindow)
	case 1:
		// Rate with proper syntax
		return fmt.Sprintf("rate(%s[%s])", selector, timeWindow)
	case 2:
		// Sum by with count_over_time
		// Choose a random label to group by
		if len(availableLabels) > 1 {
			groupByLabel := availableLabels[rand.Intn(len(availableLabels))]
			// Make sure it's different from the selector label
			if groupByLabel == chosenLabel && len(availableLabels) > 1 {
				for _, label := range availableLabels {
					if label != chosenLabel {
						groupByLabel = label
						break
					}
				}
			}
			return fmt.Sprintf("sum by(%s) (count_over_time(%s[%s]))", groupByLabel, selector, timeWindow)
		}
		// Fallback if only one label is available
		return fmt.Sprintf("sum(count_over_time(%s[%s]))", selector, timeWindow)
	case 3:
		// Topk with count_over_time
		k := rand.Intn(5) + 1
		return fmt.Sprintf("topk(%d, count_over_time(%s[%s]))", k, selector, timeWindow)
	case 4:
		// Avg_over_time with proper unwrap syntax for Loki
		// First, find a numeric field to unwrap if possible
		numericLabels := []string{"bytes", "status", "duration", "size", "count", "value"}
		var unwrapExpr string

		// Try to find a numeric field in the available labels
		foundNumeric := false
		for _, label := range numericLabels {
			if contains(availableLabels, label) {
				unwrapExpr = fmt.Sprintf("| unwrap %s", label)
				foundNumeric = true
				break
			}
		}

		if !foundNumeric {
			// If no numeric field found, use count_over_time instead
			return fmt.Sprintf("avg(count_over_time(%s[%s]))", selector, timeWindow)
		}

		return fmt.Sprintf("avg_over_time(%s %s [%s])", selector, unwrapExpr, timeWindow)
	}

	// Fallback to simple count_over_time
	return fmt.Sprintf("count_over_time(%s[%s])", selector, timeWindow)
}

// generateTimeSeriesQuery builds a Loki rate/avg_over_time query grouped by log_type
func (e *LokiExecutor) generateTimeSeriesQuery(availableLabels []string) string {
	selector := e.generateSimpleQuery(availableLabels) // reuse simple selector
	// Compute per-second rate aggregated by log_type for the last 5m
	return fmt.Sprintf("sum by (log_type) (rate(%s[5m]))", selector)
}

// generateStatQuery returns a single-value statistical aggregation (count over time)
func (e *LokiExecutor) generateStatQuery(availableLabels []string) string {
	selector := e.generateSimpleQuery(availableLabels)
	return fmt.Sprintf("count_over_time(%s[10m])", selector)
}

// generateTopKQuery returns top-K label values within a time window
func (e *LokiExecutor) generateTopKQuery(availableLabels []string) string {
	// Prefer a commonly existing label
	label := "log_type"
	if len(availableLabels) > 0 {
		label = availableLabels[rand.Intn(len(availableLabels))]
	}
	selector := e.generateSimpleQuery(availableLabels)
	return fmt.Sprintf("topk(10, sum by (%s) (%s))", label, selector)
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
	// Form the query URL
	queryURL := fmt.Sprintf("%s/loki/api/v1/query_range", e.BaseURL)

	// Create the request body
	params := url.Values{}
	for key, value := range queryInfo {
		params.Add(key, value)
	}

	if e.Options.Verbose {
		log.Printf("Debug: Loki URL: %s", queryURL)
		log.Printf("Debug: Loki query: %s", queryInfo["query"])
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

	// Create the result
	result := models.QueryResult{
		Duration:    time.Since(startTime),
		RawResponse: bodyBytes,
		BytesRead:   int64(len(bodyBytes)),
		Status:      "success",
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
		result.ResultCount = totalSamples

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
		result.ResultCount = totalEntries
	}

	// If no hits, perform a generic fallback query once
	if result.HitCount == 0 {
		if e.Options.Verbose {
			log.Printf("Debug: Loki query returned 0 results, performing fallback")
		}
		// Use wide time range and generic pattern
		now := time.Now()
		start := fmt.Sprintf("%d", now.Add(-24*time.Hour).UnixNano())
		end := fmt.Sprintf("%d", now.UnixNano())
		fallback := map[string]string{
			"query": "{log_type=~\".+\"}",
			"start": start,
			"end":   end,
			"limit": "100",
			"step":  "30s",
		}
		// Call recursively but avoid infinite loop by checking a flag in queryInfo
		if _, tried := queryInfo["_fallback"]; !tried {
			fallback["_fallback"] = "1"
			return e.executeLokiQuery(fallback)
		}
	}

	// Extract stats if available (but don't store them in the result as the struct doesn't have these fields)
	if stats, ok := lokiResp["stats"].(map[string]interface{}); ok {
		if e.Options.Verbose {
			if summary, ok := stats["summary"].(map[string]interface{}); ok {
				if bytesProcessedPerSecond, ok := summary["bytesProcessedPerSecond"].(float64); ok {
					log.Printf("Debug: Loki bytesProcessedPerSecond: %f", bytesProcessedPerSecond)
				}
				if linesProcessedPerSecond, ok := summary["linesProcessedPerSecond"].(float64); ok {
					log.Printf("Debug: Loki linesProcessedPerSecond: %f", linesProcessedPerSecond)
				}
			}
		}
	}

	if e.Options.Verbose {
		log.Printf("Debug: Loki found %d records, %d bytes", result.HitCount, result.BytesRead)
	}

	return result, nil
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

// IsSystemChecked checks if the system check has completed
func (e *LokiExecutor) IsSystemChecked() bool {
	e.systemCheckedLock.RLock()
	defer e.systemCheckedLock.RUnlock()
	return e.systemChecked
}
