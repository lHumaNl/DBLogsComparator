package common

import (
	"fmt"
	"math/rand"
	"time"
)

// TimeRange represents a time range for log queries
type TimeRange struct {
	Start       time.Time
	End         time.Time
	Description string
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
}

// NewTimeRangeGenerator creates a new TimeRangeGenerator
func NewTimeRangeGenerator() *TimeRangeGenerator {
	return &TimeRangeGenerator{
		currentTime: time.Now(),
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
	rangeType := TimeRangeType(rand.Intn(4))
	return g.GenerateTimeRange(rangeType)
}

// generateRelativeTimeRange creates time ranges like "Last 15min", "Last 1h"
func (g *TimeRangeGenerator) generateRelativeTimeRange() TimeRange {
	// Define possible durations in minutes
	durations := []int{5, 15, 30, 60, 180, 360, 720, 1440, 4320, 10080} // 5m, 15m, 30m, 1h, 3h, 6h, 12h, 24h, 3d, 7d

	// Pick a random duration
	durationMinutes := durations[rand.Intn(len(durations))]

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
	}
}

// generateAbsoluteTimeRange creates custom time ranges with specific start/end times
func (g *TimeRangeGenerator) generateAbsoluteTimeRange() TimeRange {
	// Maximum days to go back
	maxDaysBack := 7

	// Random number of days to go back (1-7)
	daysBack := rand.Intn(maxDaysBack) + 1

	// Generate a random start time from the past
	dayOffset := time.Duration(-daysBack) * 24 * time.Hour

	// Base day (N days ago)
	baseDay := g.currentTime.Add(dayOffset)

	// Start time is at a random hour between 0-20
	startHour := rand.Intn(21)
	// Random minute
	startMinute := rand.Intn(60)

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
	durationHours := rand.Intn(4) + 1

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

	return TimeRange{
		Start:       start,
		End:         end,
		Description: description,
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

	startHour := rand.Intn(maxStartHour + 1)
	startMinute := rand.Intn(60)

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
		endHour := startHour + rand.Intn(maxEndHour-startHour) + 1
		endMinute := rand.Intn(60)

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
	}
}

// generateYesterdayTimeRange creates ranges within yesterday
func (g *TimeRangeGenerator) generateYesterdayTimeRange() TimeRange {
	now := g.currentTime

	// Start of yesterday
	yesterdayStart := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())

	// Random start hour (0-23)
	startHour := rand.Intn(24)
	startMinute := rand.Intn(60)

	// Create start time
	start := yesterdayStart.Add(time.Duration(startHour)*time.Hour + time.Duration(startMinute)*time.Minute)

	// Random duration (1-4 hours)
	durationHours := rand.Intn(4) + 1

	// End time
	endHour := startHour + durationHours
	endMinute := rand.Intn(60)

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
