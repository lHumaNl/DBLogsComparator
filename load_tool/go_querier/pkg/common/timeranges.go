package common

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	config "github.com/dblogscomparator/DBLogsComparator/load_tool/common"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
)

// TimeRange represents a time range for log queries
type TimeRange struct {
	Start          time.Time
	End            time.Time
	Description    string
	StringRepr     string // String representation for metrics (e.g., "Last 1h", "15m - 1h")
	IsCustom       bool   // Whether this is a custom range
	LeftOffsetMin  int    // Left border offset in minutes (for custom ranges)
	RightOffsetMin int    // Right border offset in minutes (for custom ranges)
}

// TimeRangeType defines the type of time range to generate
type TimeRangeType int

const (
	// RelativeRange represents ranges like "Last 15min", "Last 1h"
	RelativeRange TimeRangeType = iota
	// AbsoluteRange represents custom time ranges with specific start/end
	AbsoluteRange
	// TodayRange represents ranges within the current day
	TodayRange
	// YesterdayRange represents ranges within yesterday
	YesterdayRange
)

// TimeRangeGenerator generates time ranges for log queries
type TimeRangeGenerator struct {
	currentTime time.Time
	timeConfig  *config.TimeRangeConfig // Configuration for time ranges
}

// NewTimeRangeGenerator creates a new TimeRangeGenerator
func NewTimeRangeGenerator() *TimeRangeGenerator {
	return &TimeRangeGenerator{
		currentTime: time.Now(),
	}
}

// NewTimeRangeGeneratorWithConfig creates a new TimeRangeGenerator with configuration
func NewTimeRangeGeneratorWithConfig(timeConfig *config.TimeRangeConfig) *TimeRangeGenerator {
	return &TimeRangeGenerator{
		currentTime: time.Now(),
		timeConfig:  timeConfig,
	}
}

// WithTime allows setting a specific time as "now" for testing
func (g *TimeRangeGenerator) WithTime(t time.Time) *TimeRangeGenerator {
	g.currentTime = t
	return g
}

// GetCurrentTime returns the generator's current time
func (g *TimeRangeGenerator) GetCurrentTime() time.Time {
	return g.currentTime
}

// GenerateTimeRange creates a random time range based on the specified type
func (g *TimeRangeGenerator) GenerateTimeRange(rangeType TimeRangeType) TimeRange {
	switch rangeType {
	case RelativeRange:
		return g.generateRelativeTimeRange()
	case AbsoluteRange:
		return g.generateAbsoluteTimeRange()
	case TodayRange:
		return g.generateTodayTimeRange()
	case YesterdayRange:
		return g.generateYesterdayTimeRange()
	default:
		// Default to relative range if type is unknown
		return g.generateRelativeTimeRange()
	}
}

// GenerateRandomTimeRange creates a completely random time range of any type
func (g *TimeRangeGenerator) GenerateRandomTimeRange() TimeRange {
	rangeType := TimeRangeType(logdata.RandomIntn(4))
	return g.GenerateTimeRange(rangeType)
}

// GenerateConfiguredTimeRange generates a time range based on configuration weights
func (g *TimeRangeGenerator) GenerateConfiguredTimeRange() TimeRange {
	if g.timeConfig == nil {
		// Fallback to random generation if no config
		return g.GenerateRandomTimeRange()
	}

	// Generate random number (0-100 for percentage)
	randomValue := logdata.RandomFloat64() * 100.0
	currentWeight := 0.0

	// Check predefined ranges
	predefinedRanges := []struct {
		weight   float64
		duration time.Duration
		name     string
	}{
		{g.timeConfig.Last5m, 5 * time.Minute, "Last 5m"},
		{g.timeConfig.Last15m, 15 * time.Minute, "Last 15m"},
		{g.timeConfig.Last30m, 30 * time.Minute, "Last 30m"},
		{g.timeConfig.Last1h, 1 * time.Hour, "Last 1h"},
		{g.timeConfig.Last2h, 2 * time.Hour, "Last 2h"},
		{g.timeConfig.Last4h, 4 * time.Hour, "Last 4h"},
		{g.timeConfig.Last8h, 8 * time.Hour, "Last 8h"},
		{g.timeConfig.Last12h, 12 * time.Hour, "Last 12h"},
		{g.timeConfig.Last24h, 24 * time.Hour, "Last 24h"},
		{g.timeConfig.Last48h, 48 * time.Hour, "Last 48h"},
		{g.timeConfig.Last72h, 72 * time.Hour, "Last 72h"},
	}

	for _, rangeInfo := range predefinedRanges {
		currentWeight += rangeInfo.weight
		if randomValue <= currentWeight {
			end := g.currentTime
			start := end.Add(-rangeInfo.duration)
			return TimeRange{
				Start:       start,
				End:         end,
				Description: rangeInfo.name,
				StringRepr:  rangeInfo.name,
				IsCustom:    false,
			}
		}
	}

	// Check custom_period
	if g.timeConfig.CustomPeriod.Percent > 0 {
		currentWeight += g.timeConfig.CustomPeriod.Percent
		if randomValue <= currentWeight {
			return g.generateCustomPeriodTimeRange()
		}
	}

	// Check custom range
	if g.timeConfig.Custom.Percent > 0 {
		currentWeight += g.timeConfig.Custom.Percent
		if randomValue <= currentWeight {
			return g.generateConfiguredCustomTimeRange()
		}
	}

	// Fallback to first predefined range if all weights don't add up
	end := g.currentTime
	start := end.Add(-15 * time.Minute)
	return TimeRange{
		Start:       start,
		End:         end,
		Description: "Last 15m (fallback)",
		StringRepr:  "Last 15m (fallback)",
		IsCustom:    false,
	}
}

// generateCustomPeriodTimeRange generates a time range within custom period based on configuration
func (g *TimeRangeGenerator) generateCustomPeriodTimeRange() TimeRange {
	// Parse period dates - these should be pre-validated during config loading
	periodStart, err := time.Parse("02.01.2006 15:04:05", g.timeConfig.CustomPeriod.PeriodStart)
	if err != nil {
		panic(fmt.Sprintf("invalid period_start format '%s': %v (this should have been caught during config validation)", g.timeConfig.CustomPeriod.PeriodStart, err))
	}

	periodEnd, err := time.Parse("02.01.2006 15:04:05", g.timeConfig.CustomPeriod.PeriodEnd)
	if err != nil {
		panic(fmt.Sprintf("invalid period_end format '%s': %v (this should have been caught during config validation)", g.timeConfig.CustomPeriod.PeriodEnd, err))
	}

	// Select duration based on weights
	durationStr := g.selectWeightedDuration(g.timeConfig.CustomPeriod.Times)
	durationMin := g.parseDurationToMinutes(durationStr)
	duration := time.Duration(durationMin) * time.Minute

	// Generate random start time within the period
	periodDuration := periodEnd.Sub(periodStart)
	maxStartOffset := periodDuration - duration

	// If duration is longer than period, use the entire period
	if maxStartOffset < 0 {
		return TimeRange{
			Start:       periodStart,
			End:         periodEnd,
			Description: fmt.Sprintf("Custom Period (Full: %s)", durationStr),
			StringRepr:  fmt.Sprintf("Custom Period (Full: %s)", durationStr),
			IsCustom:    true,
		}
	}

	// Generate random offset within valid range
	startOffset := time.Duration(logdata.RandomFloat64() * float64(maxStartOffset))
	start := periodStart.Add(startOffset)
	end := start.Add(duration)

	// Ensure end doesn't exceed period end (should not happen due to calculation above)
	if end.After(periodEnd) {
		end = periodEnd
		start = end.Add(-duration)
	}

	description := fmt.Sprintf("Custom Period (%s): %s to %s",
		durationStr,
		start.Format("02.01.2006 15:04"),
		end.Format("02.01.2006 15:04"))

	return TimeRange{
		Start:       start,
		End:         end,
		Description: description,
		StringRepr:  fmt.Sprintf("Custom Period (%s)", durationStr),
		IsCustom:    true,
	}
}

// generateConfiguredCustomTimeRange generates a custom time range based on configuration
func (g *TimeRangeGenerator) generateConfiguredCustomTimeRange() TimeRange {
	// Select left border offset
	leftOffsetStr := g.selectWeightedDuration(g.timeConfig.Custom.PercentsOffsetLeftBorder)
	leftOffsetMin := g.parseDurationToMinutes(leftOffsetStr)

	// Select right border offset
	rightOffsetStr := g.selectWeightedDuration(g.timeConfig.Custom.PercentsOffsetRightBorder)
	rightOffsetMin := g.parseDurationToMinutes(rightOffsetStr)

	// Calculate actual times
	end := g.currentTime
	start := end.Add(-time.Duration(leftOffsetMin) * time.Minute)
	actualEnd := start.Add(time.Duration(rightOffsetMin) * time.Minute)

	// Ensure we don't go beyond current time
	if actualEnd.After(g.currentTime) {
		actualEnd = g.currentTime
	}

	stringRepr := fmt.Sprintf("Custom %s - %s", leftOffsetStr, rightOffsetStr)

	return TimeRange{
		Start:          start,
		End:            actualEnd,
		Description:    stringRepr,
		StringRepr:     stringRepr,
		IsCustom:       true,
		LeftOffsetMin:  leftOffsetMin,
		RightOffsetMin: rightOffsetMin,
	}
}

// selectWeightedDuration selects a duration based on weights
func (g *TimeRangeGenerator) selectWeightedDuration(weightMap map[string]float64) string {
	totalWeight := 0.0
	for _, weight := range weightMap {
		totalWeight += weight
	}

	randomValue := logdata.RandomFloat64() * totalWeight
	currentWeight := 0.0

	for duration, weight := range weightMap {
		currentWeight += weight
		if randomValue <= currentWeight {
			return duration
		}
	}

	// Fallback - return first available duration
	for duration := range weightMap {
		return duration
	}
	return "1h" // Ultimate fallback
}

// parseDurationToMinutes converts duration string to minutes
func (g *TimeRangeGenerator) parseDurationToMinutes(durationStr string) int {
	durationStr = strings.ToLower(strings.TrimSpace(durationStr))

	if strings.HasSuffix(durationStr, "m") {
		if val, err := strconv.Atoi(strings.TrimSuffix(durationStr, "m")); err == nil {
			return val
		}
	} else if strings.HasSuffix(durationStr, "h") {
		if val, err := strconv.Atoi(strings.TrimSuffix(durationStr, "h")); err == nil {
			return val * 60
		}
	}

	// Fallback - try to parse as number (assume minutes)
	if val, err := strconv.Atoi(durationStr); err == nil {
		return val
	}

	return 60 // Default to 1 hour if parsing fails
}

// generateRelativeTimeRange creates time ranges like "Last 15min", "Last 1h"
func (g *TimeRangeGenerator) generateRelativeTimeRange() TimeRange {
	// Define possible durations in minutes
	durations := []int{5, 15, 30, 60, 180, 360, 720, 1440, 4320, 10080} // 5m, 15m, 30m, 1h, 3h, 6h, 12h, 24h, 3d, 7d

	// Pick a random duration
	durationMinutes := durations[logdata.RandomIntn(len(durations))]

	// Calculate the start and end times
	end := g.currentTime
	start := end.Add(time.Duration(-durationMinutes) * time.Minute)

	// Create description
	var description string
	if durationMinutes < 60 {
		description = fmt.Sprintf("Last %dmin", durationMinutes)
	} else if durationMinutes < 24*60 {
		description = fmt.Sprintf("Last %dh", durationMinutes/60)
	} else {
		description = fmt.Sprintf("Last %dd", durationMinutes/(24*60))
	}

	return TimeRange{
		Start:       start,
		End:         end,
		Description: description,
		StringRepr:  description,
		IsCustom:    false,
	}
}

// generateAbsoluteTimeRange creates custom time ranges with specific start/end times
func (g *TimeRangeGenerator) generateAbsoluteTimeRange() TimeRange {
	// Maximum days to go back
	maxDaysBack := 7

	// Random number of days to go back (1-7)
	daysBack := logdata.RandomIntn(maxDaysBack) + 1

	// Generate a random start time from the past
	dayOffset := time.Duration(-daysBack) * 24 * time.Hour

	// Base day (N days ago)
	baseDay := g.currentTime.Add(dayOffset)

	// Start time is at a random hour between 0-20
	startHour := logdata.RandomIntn(21)
	// Random minute
	startMinute := logdata.RandomIntn(60)

	// Create start time
	start := time.Date(
		baseDay.Year(),
		baseDay.Month(),
		baseDay.Day(),
		startHour,
		startMinute,
		0, 0,
		baseDay.Location(),
	)

	// Duration of the range (1-4 hours)
	durationHours := logdata.RandomIntn(4) + 1

	// Ensure we don't go beyond current time
	potentialEnd := start.Add(time.Duration(durationHours) * time.Hour)
	var end time.Time
	if potentialEnd.After(g.currentTime) {
		end = g.currentTime
	} else {
		end = potentialEnd
	}

	// Create description
	description := fmt.Sprintf("%d days ago, %02d:%02d to %02d:%02d",
		daysBack,
		start.Hour(), start.Minute(),
		end.Hour(), end.Minute(),
	)

	// Calculate offsets for string representation
	leftOffsetMin := int(g.currentTime.Sub(start).Minutes())
	rightOffsetMin := int(end.Sub(start).Minutes())
	stringRepr := fmt.Sprintf("Left border offset %d - Right border offset %d", leftOffsetMin, rightOffsetMin)

	return TimeRange{
		Start:          start,
		End:            end,
		Description:    description,
		StringRepr:     stringRepr,
		IsCustom:       true,
		LeftOffsetMin:  leftOffsetMin,
		RightOffsetMin: rightOffsetMin,
	}
}

// generateTodayTimeRange creates ranges within the current day
func (g *TimeRangeGenerator) generateTodayTimeRange() TimeRange {
	now := g.currentTime

	// Start of today
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	// Random start hour (0 to current hour-1)
	maxStartHour := now.Hour() - 1
	if maxStartHour < 0 {
		maxStartHour = 0
	}

	startHour := logdata.RandomIntn(maxStartHour + 1)
	startMinute := logdata.RandomIntn(60)

	// Create start time
	start := todayStart.Add(time.Duration(startHour)*time.Hour + time.Duration(startMinute)*time.Minute)

	// Ensure we don't go beyond current time
	var end time.Time
	if startHour == now.Hour() {
		// If we're in the same hour, end time is current time
		end = now
	} else {
		// Otherwise, random end time between start and now
		maxEndHour := now.Hour()
		endHour := startHour + logdata.RandomIntn(maxEndHour-startHour) + 1
		endMinute := logdata.RandomIntn(60)

		potentialEnd := todayStart.Add(time.Duration(endHour)*time.Hour + time.Duration(endMinute)*time.Minute)
		if potentialEnd.After(now) {
			end = now
		} else {
			end = potentialEnd
		}
	}

	// Create description
	description := fmt.Sprintf("Today, %02d:%02d to %02d:%02d",
		start.Hour(), start.Minute(),
		end.Hour(), end.Minute(),
	)

	return TimeRange{
		Start:       start,
		End:         end,
		Description: description,
		StringRepr:  description,
		IsCustom:    false,
	}
}

// generateYesterdayTimeRange creates ranges within yesterday
func (g *TimeRangeGenerator) generateYesterdayTimeRange() TimeRange {
	now := g.currentTime

	// Start of yesterday
	yesterdayStart := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())

	// Random start hour (0-23)
	startHour := logdata.RandomIntn(24)
	startMinute := logdata.RandomIntn(60)

	// Create start time
	start := yesterdayStart.Add(time.Duration(startHour)*time.Hour + time.Duration(startMinute)*time.Minute)

	// Random duration (1-4 hours)
	durationHours := logdata.RandomIntn(4) + 1

	// End time
	endHour := startHour + durationHours
	endMinute := logdata.RandomIntn(60)

	var end time.Time
	if endHour < 24 {
		end = yesterdayStart.Add(time.Duration(endHour)*time.Hour + time.Duration(endMinute)*time.Minute)
	} else {
		// If we go beyond the day, cap to end of day
		end = yesterdayStart.Add(24*time.Hour - time.Minute)
	}

	// Create description
	description := fmt.Sprintf("Yesterday, %02d:%02d to %02d:%02d",
		start.Hour(), start.Minute(),
		end.Hour(), end.Minute(),
	)

	return TimeRange{
		Start:       start,
		End:         end,
		Description: description,
		StringRepr:  description,
		IsCustom:    false,
	}
}

// FormatTimeForLoki formats a time for use in Loki queries
func FormatTimeForLoki(t time.Time) string {
	// Loki uses nanosecond precision Unix timestamps
	return fmt.Sprintf("%d", t.UnixNano())
}

// FormatTimeRFC3339 formats a time in RFC3339 format
func FormatTimeRFC3339(t time.Time) string {
	return t.Format(time.RFC3339)
}

// GetRandomQuantile returns a random quantile value for query systems
func GetRandomQuantile() string {
	quantiles := []string{"0.5", "0.75", "0.9", "0.95", "0.99"}
	return quantiles[logdata.RandomIntn(len(quantiles))]
}

// GetRandomQuantileFloat returns a random quantile value as float64 for Elasticsearch
func GetRandomQuantileFloat() float64 {
	quantiles := []float64{0.5, 0.75, 0.9, 0.95, 0.99}
	return quantiles[logdata.RandomIntn(len(quantiles))]
}

// GetUniqueRandomQuantiles returns a slice of unique random quantile values
func GetUniqueRandomQuantiles(count int) []float64 {
	quantiles := []float64{0.5, 0.75, 0.9, 0.95, 0.99}
	if count > len(quantiles) {
		count = len(quantiles)
	}

	// Shuffle the slice and take first 'count' elements
	shuffled := make([]float64, len(quantiles))
	copy(shuffled, quantiles)

	for i := len(shuffled) - 1; i > 0; i-- {
		j := logdata.RandomIntn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:count]
}

// GetUniqueRandomQuantileStrings returns a slice of unique random quantile values as strings
func GetUniqueRandomQuantileStrings(count int) []string {
	floatQuantiles := GetUniqueRandomQuantiles(count)
	stringQuantiles := make([]string, len(floatQuantiles))
	for i, q := range floatQuantiles {
		stringQuantiles[i] = fmt.Sprintf("%.2f", q)
	}
	return stringQuantiles
}
