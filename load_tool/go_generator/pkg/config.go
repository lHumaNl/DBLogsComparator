package pkg

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dblogscomparator/DBLogsComparator/load_tool/common"
)

// GeneratorConfig wraps common.GeneratorConfig with additional runtime settings
// This provides a clean interface while avoiding legacy configuration conflicts
type GeneratorConfig struct {
	*common.GeneratorConfig
	System        string        // Target logging system (loki, elasticsearch, victoria)
	Duration      time.Duration // Test duration
	EnableMetrics bool          // Whether metrics are enabled
	MetricsPort   int           // Metrics server port
	// Global settings for verbose/debug priority logic
	GlobalVerbose *bool // Global verbose setting from root config
	GlobalDebug   *bool // Global debug setting from root config
	// Runtime settings (can be modified during execution)
	RuntimeRPS float64 // Current RPS (overrides config RPS if > 0)
}

// GetLogGenerationWorkers returns the number of workers for log generation
// FIXED: Always limits to CPU*4 to prevent excessive goroutine creation
func (c *GeneratorConfig) GetLogGenerationWorkers() int {
	cpuCores := runtime.NumCPU()
	maxWorkers := cpuCores

	if maxWorkers > 16 {
		maxWorkers = 16
	}

	// Never exceed bulkSize (no point in having more workers than work items)
	if maxWorkers > c.BulkSize {
		maxWorkers = c.BulkSize
	}

	return maxWorkers
}

// GetRPS returns current RPS (runtime RPS if set, otherwise config RPS)
func (c *GeneratorConfig) GetRPS() float64 {
	// Use runtime RPS if set (for dynamic load testing)
	if c.RuntimeRPS > 0 {
		return c.RuntimeRPS
	}
	// Otherwise use stability config BaseRPS
	return c.Stability.BaseRPS
}

// SetRPS sets the runtime RPS for dynamic load testing
func (c *GeneratorConfig) SetRPS(rps float64) {
	c.RuntimeRPS = rps
}

// GetURL returns the URL for the target system
func (c *GeneratorConfig) GetURL() string {
	return c.GeneratorConfig.GetURL(c.System)
}

// IsVerbose returns the effective verbose setting with priority logic
// Module-specific setting overrides global setting, defaults to false
func (c *GeneratorConfig) IsVerbose() bool {
	return c.GeneratorConfig.IsVerbose(c.GlobalVerbose)
}

// IsDebug returns the effective debug setting with priority logic
// Module-specific setting overrides global setting, defaults to false
func (c *GeneratorConfig) IsDebug() bool {
	return c.GeneratorConfig.IsDebug(c.GlobalDebug)
}

// Stats - execution statistics with thread-safe operations
type Stats struct {
	TotalRequests      int64
	SuccessfulRequests int64
	FailedRequests     int64
	TotalLogs          int64
	LogsByType         sync.Map // Thread-safe map replaces map[string]int64
	RetriedRequests    int64
	BackpressureEvents int64 // Count of backpressure events
	StartTime          time.Time
}

// calculateLogGenerationWorkers - DEPRECATED: Replaced with fixed CPU*4 limit
// This function caused excessive goroutine creation with large bulkSize values
// Now using fixed limit in GetLogGenerationWorkers() to prevent resource exhaustion
func calculateLogGenerationWorkers(rps, bulkSize, cpuCores int) int {
	// This function is no longer used but kept for backward compatibility
	// All calls now redirect to the fixed CPU*4 implementation
	maxWorkers := cpuCores

	if maxWorkers > 16 {
		maxWorkers = 16
	}

	if maxWorkers > bulkSize {
		maxWorkers = bulkSize
	}

	return maxWorkers
}

// Thread-safe methods for Stats operations

// IncrementLogType atomically increments the counter for a specific log type
// CRITICAL FIX: Replaced infinite CAS loop with atomic operations to prevent CPU spin
func (s *Stats) IncrementLogType(logType string) {
	// Use atomic map pattern instead of CAS loop to prevent CPU spinning
	actualValue, loaded := s.LogsByType.LoadOrStore(logType, new(int64))
	counter := actualValue.(*int64)
	atomic.AddInt64(counter, 1)

	// If we just created the counter, initialize it properly
	if !loaded {
		atomic.StoreInt64(counter, 1)
	}
}

// GetLogTypeCount returns the current count for a specific log type
func (s *Stats) GetLogTypeCount(logType string) int64 {
	if value, ok := s.LogsByType.Load(logType); ok {
		return value.(int64)
	}
	return 0
}

// GetLogsByTypeCopy returns a snapshot of all log type counters
// This method is thread-safe and provides a consistent view
func (s *Stats) GetLogsByTypeCopy() map[string]int64 {
	result := make(map[string]int64)

	s.LogsByType.Range(func(key, value interface{}) bool {
		if logType, ok := key.(string); ok {
			if count, ok := value.(int64); ok {
				result[logType] = count
			}
		}
		return true // Continue iteration
	})

	return result
}

// ResetLogsByType clears all log type counters
func (s *Stats) ResetLogsByType() {
	s.LogsByType.Range(func(key, value interface{}) bool {
		s.LogsByType.Delete(key)
		return true
	})
}
