package common

import (
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadTestLogger handles logging for load testing phases
type LoadTestLogger struct {
	logFile   *os.File
	logger    *log.Logger
	startTime time.Time
	steps     []StepInfo
}

// TestStartTime stores the global test start time for consistent file naming
var TestStartTime time.Time

// StepInfo contains information about a test step
type StepInfo struct {
	StepNumber         int     `yaml:"step_number"`
	StepStart          int64   `yaml:"step_start"`
	StepEnd            int64   `yaml:"step_end"`
	StepDuration       int     `yaml:"step_duration"`
	StabilityTimeStart int64   `yaml:"stability_time_start"`
	StabilityDuration  int     `yaml:"stability_duration"`
	StepSpeed          float64 `yaml:"step_speed"`
	StepPercent        float64 `yaml:"step_percent"`
}

// TestReport contains the complete test report data
type TestReport struct {
	TestFolder        string     `yaml:"test_folder"`
	TestType          string     `yaml:"test_type"`
	TestStartTime     int64      `yaml:"test_start_time"`
	TestEndTime       int64      `yaml:"test_end_time"`
	ImpactTime        int        `yaml:"impact_time"`
	StabilityTime     int        `yaml:"stability_time"`
	SpeedUnit         string     `yaml:"speed_unit"`
	BaseSpeed         float64    `yaml:"base_speed"`
	StartSpeedPercent float64    `yaml:"start_speed_percent,omitempty"`
	IncrementPercent  float64    `yaml:"increment_percent,omitempty"`
	StepsCount        int        `yaml:"steps_count"`
	Steps             []StepInfo `yaml:"steps"`
}

// NewLoadTestLogger creates a new load test logger
func NewLoadTestLogger() (*LoadTestLogger, error) {
	now := time.Now()
	TestStartTime = now // Set global test start time
	filename := fmt.Sprintf("./logs/times_%s.log", now.Format("2006-01-02_15-04-05"))

	// Create or open the log file
	logFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file %s: %v", filename, err)
	}

	logger := log.New(logFile, "", 0) // No prefix, we'll format timestamps ourselves

	return &LoadTestLogger{
		logFile:   logFile,
		logger:    logger,
		startTime: now,
		steps:     make([]StepInfo, 0),
	}, nil
}

// LogTestStart logs the start of a test
func (l *LoadTestLogger) LogTestStart(testType string) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Test started (%s mode)", timestamp, testType)
}

// LogStepStart logs the start of a test step
func (l *LoadTestLogger) LogStepStart(stepNumber int, rps float64) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Step %d started (%.1f RPS)", timestamp, stepNumber, rps)

	// Initialize step info
	stepInfo := StepInfo{
		StepNumber: stepNumber,
		StepStart:  timestamp,
		StepSpeed:  rps,
	}

	l.steps = append(l.steps, stepInfo)
}

// LogImpactEnd logs the end of impact period and start of stable load
func (l *LoadTestLogger) LogImpactEnd(stepNumber int) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Impact period ended, stable load started (Step %d)", timestamp, stepNumber)

	// Update the last step info
	if len(l.steps) > 0 {
		l.steps[len(l.steps)-1].StabilityTimeStart = timestamp
	}
}

// LogStepEnd logs the end of a test step
func (l *LoadTestLogger) LogStepEnd(stepNumber int) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Step %d ended", timestamp, stepNumber)

	// Update the last step info
	if len(l.steps) > 0 {
		lastStep := &l.steps[len(l.steps)-1]
		lastStep.StepEnd = timestamp
		lastStep.StepDuration = int(timestamp - lastStep.StepStart)
		if lastStep.StabilityTimeStart > 0 {
			lastStep.StabilityDuration = int(timestamp - lastStep.StabilityTimeStart)
		}
	}
}

// LogTestEnd logs the end of the entire test
func (l *LoadTestLogger) LogTestEnd() {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Test completed", timestamp)
}

// Close closes the log file
func (l *LoadTestLogger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// GenerateYAMLReport generates a YAML report file based on the logged data
func (l *LoadTestLogger) GenerateYAMLReport(testType string, config LoadTestConfig) error {
	now := time.Now()
	yamlFilename := fmt.Sprintf("./logs/times_%s.yaml", l.startTime.Format("2006-01-02_15-04-05"))

	// Calculate test parameters
	baseSpeed := float64(config.BaseRPS)
	var startSpeedPercent, incrementPercent float64
	var stepsCount int

	if testType == "maxPerf" {
		startSpeedPercent = float64(config.StartPercent)
		incrementPercent = float64(config.IncrementPercent)
		stepsCount = config.Steps
	} else {
		stepsCount = 1 // Stability mode has one "step"
	}

	// Update step percentages
	for i := range l.steps {
		if testType == "maxPerf" {
			l.steps[i].StepPercent = startSpeedPercent + float64(i)*incrementPercent
		} else {
			l.steps[i].StepPercent = float64(config.StepPercent)
		}
	}

	report := TestReport{
		TestFolder:        fmt.Sprintf("load_tests\\%s\\", testType),
		TestType:          testType,
		TestStartTime:     l.startTime.Unix(),
		TestEndTime:       now.Unix(),
		ImpactTime:        config.Impact,
		StabilityTime:     config.StepDuration,
		SpeedUnit:         "RPS",
		BaseSpeed:         baseSpeed,
		StartSpeedPercent: startSpeedPercent,
		IncrementPercent:  incrementPercent,
		StepsCount:        stepsCount,
		Steps:             l.steps,
	}

	// Generate YAML
	data, err := yaml.Marshal(&report)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML report: %v", err)
	}

	// Write to file
	err = os.WriteFile(yamlFilename, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write YAML report %s: %v", yamlFilename, err)
	}

	fmt.Printf("Test report generated: %s\n", yamlFilename)
	return nil
}

// GetCurrentStepCount returns the number of steps logged so far
func (l *LoadTestLogger) GetCurrentStepCount() int {
	return len(l.steps)
}
