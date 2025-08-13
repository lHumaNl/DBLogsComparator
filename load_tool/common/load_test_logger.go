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
	logFile       *os.File
	logger        *log.Logger
	startTime     time.Time
	steps         []StepInfo
	combinedSteps []CombinedStepInfo
	mode          string
	testType      string
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

// CombinedStepInfo contains information about a test step for combined mode
type CombinedStepInfo struct {
	StepNumber         int                `yaml:"step_number"`
	StepStart          int64              `yaml:"step_start"`
	StepEnd            int64              `yaml:"step_end"`
	StepDuration       int                `yaml:"step_duration"`
	StabilityTimeStart int64              `yaml:"stability_time_start"`
	StabilityDuration  int                `yaml:"stability_duration"`
	StepSpeed          map[string]float64 `yaml:"step_speed"`
	StepPercent        map[string]float64 `yaml:"step_percent"`
}

// TestReport contains the complete test report data
type TestReport struct {
	TestFolder        string     `yaml:"test_folder"`
	TestType          string     `yaml:"test_type"`
	TestMode          string     `yaml:"test_mode"`
	TestStartTime     int64      `yaml:"test_start_time"`
	TestEndTime       int64      `yaml:"test_end_time"`
	ImpactTime        int        `yaml:"impact_time"`
	StabilityTime     int        `yaml:"stability_time"`
	SpeedUnit         string     `yaml:"speed_unit"`
	BaseSpeed         float64    `yaml:"base_speed"`
	BulkSize          int        `yaml:"bulk_size,omitempty"`
	StartSpeedPercent float64    `yaml:"start_speed_percent,omitempty"`
	IncrementPercent  float64    `yaml:"increment_percent,omitempty"`
	StepsCount        int        `yaml:"steps_count"`
	Steps             []StepInfo `yaml:"steps"`
}

// CombinedTestReport contains the complete test report data for combined mode
type CombinedTestReport struct {
	TestFolder        string             `yaml:"test_folder"`
	TestType          string             `yaml:"test_type"`
	TestMode          string             `yaml:"test_mode"`
	TestStartTime     int64              `yaml:"test_start_time"`
	TestEndTime       int64              `yaml:"test_end_time"`
	ImpactTime        int                `yaml:"impact_time"`
	StabilityTime     int                `yaml:"stability_time"`
	SpeedUnit         string             `yaml:"speed_unit"`
	BaseSpeed         map[string]float64 `yaml:"base_speed"`
	BulkSize          int                `yaml:"bulk_size"`
	StartSpeedPercent map[string]float64 `yaml:"start_speed_percent,omitempty"`
	IncrementPercent  map[string]float64 `yaml:"increment_percent,omitempty"`
	StepsCount        int                `yaml:"steps_count"`
	Steps             []CombinedStepInfo `yaml:"steps"`
}

// NewLoadTestLogger creates a new load test logger
func NewLoadTestLogger(mode, testType string) (*LoadTestLogger, error) {
	now := time.Now()
	TestStartTime = now // Set global test start time

	// Ensure logs subdirectory exists
	logDir := fmt.Sprintf("./load_tests/%s", testType)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory %s: %v", logDir, err)
	}

	filename := fmt.Sprintf("%s/times_%s_%s.log", logDir, mode, now.Format("2006-01-02_15-04-05"))

	// Create or open the log file
	logFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file %s: %v", filename, err)
	}

	logger := log.New(logFile, "", 0) // No prefix, we'll format timestamps ourselves

	return &LoadTestLogger{
		logFile:       logFile,
		logger:        logger,
		startTime:     now,
		steps:         make([]StepInfo, 0),
		combinedSteps: make([]CombinedStepInfo, 0),
		mode:          mode,
		testType:      testType,
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
	l.logger.Printf("%d - Step %d started", timestamp, stepNumber)

	// Initialize step info
	stepInfo := StepInfo{
		StepNumber: stepNumber,
		StepStart:  timestamp,
		StepSpeed:  rps,
	}

	l.steps = append(l.steps, stepInfo)
}

// LogCombinedStepStart logs the start of a test step for combined mode
func (l *LoadTestLogger) LogCombinedStepStart(stepNumber int, generatorRPS, querierRPS float64) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Step %d started (Generator RPS: %.2f, Querier RPS: %.2f)", timestamp, stepNumber, generatorRPS, querierRPS)

	// Initialize combined step info
	stepInfo := CombinedStepInfo{
		StepNumber: stepNumber,
		StepStart:  timestamp,
		StepSpeed: map[string]float64{
			"base_speed_generator": generatorRPS,
			"base_speed_querier":   querierRPS,
		},
		StepPercent: map[string]float64{
			"step_percent_generator": 0, // Will be updated later
			"step_percent_querier":   0, // Will be updated later
		},
	}

	l.combinedSteps = append(l.combinedSteps, stepInfo)
}

// LogImpactEnd logs the end of impact period and start of stable load
func (l *LoadTestLogger) LogImpactEnd(stepNumber int) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Impact period ended, stable load started (Step %d)", timestamp, stepNumber)

	// Update the last step info
	if len(l.steps) > 0 {
		l.steps[len(l.steps)-1].StabilityTimeStart = timestamp
	}

	// Also update combined steps if they exist
	if len(l.combinedSteps) > 0 {
		l.combinedSteps[len(l.combinedSteps)-1].StabilityTimeStart = timestamp
	}
}

// LogCombinedImpactEnd logs the end of impact period and start of stable load for combined mode
func (l *LoadTestLogger) LogCombinedImpactEnd(stepNumber int) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Impact period ended, stable load started (Step %d)", timestamp, stepNumber)

	// Update the last combined step info
	if len(l.combinedSteps) > 0 {
		l.combinedSteps[len(l.combinedSteps)-1].StabilityTimeStart = timestamp
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

	// Also update combined steps if they exist
	if len(l.combinedSteps) > 0 {
		lastStep := &l.combinedSteps[len(l.combinedSteps)-1]
		lastStep.StepEnd = timestamp
		lastStep.StepDuration = int(timestamp - lastStep.StepStart)
		if lastStep.StabilityTimeStart > 0 {
			lastStep.StabilityDuration = int(timestamp - lastStep.StabilityTimeStart)
		}
	}
}

// LogCombinedStepEnd logs the end of a test step for combined mode
func (l *LoadTestLogger) LogCombinedStepEnd(stepNumber int) {
	timestamp := time.Now().Unix()
	l.logger.Printf("%d - Step %d ended", timestamp, stepNumber)

	// Update the last combined step info
	if len(l.combinedSteps) > 0 {
		lastStep := &l.combinedSteps[len(l.combinedSteps)-1]
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
	if l.mode == "combined" {
		return l.generateCombinedYAMLReport(testType, config)
	}
	return l.generateSingleYAMLReport(testType, config)
}

// generateSingleYAMLReport generates YAML report for single mode (generator or querier)
func (l *LoadTestLogger) generateSingleYAMLReport(testType string, config LoadTestConfig) error {
	now := time.Now()
	yamlFilename := fmt.Sprintf("./load_tests/%s/times_%s_%s.yaml", l.testType, l.mode, l.startTime.Format("2006-01-02_15-04-05"))

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
		TestMode:          l.mode,
		TestStartTime:     l.startTime.Unix(),
		TestEndTime:       now.Unix(),
		ImpactTime:        config.Impact,
		StabilityTime:     config.StepDuration,
		SpeedUnit:         "RPS",
		BaseSpeed:         baseSpeed,
		BulkSize:          0, // Will be set externally for generator mode
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

// generateCombinedYAMLReport generates YAML report for combined mode (both generator and querier)
func (l *LoadTestLogger) generateCombinedYAMLReport(testType string, config LoadTestConfig) error {
	now := time.Now()
	yamlFilename := fmt.Sprintf("./load_tests/%s/times_%s_%s.yaml", l.testType, l.mode, l.startTime.Format("2006-01-02_15-04-05"))

	// We'll get base RPS values from the first combined step
	var baseSpeedGenerator, baseSpeedQuerier float64
	if len(l.combinedSteps) > 0 {
		baseSpeedGenerator = l.combinedSteps[0].StepSpeed["base_speed_generator"]
		baseSpeedQuerier = l.combinedSteps[0].StepSpeed["base_speed_querier"]
	}

	var startSpeedPercentGenerator, startSpeedPercentQuerier float64
	var incrementPercentGenerator, incrementPercentQuerier float64
	var stepsCount int

	if testType == "maxPerf" {
		startSpeedPercentGenerator = float64(config.StartPercent)
		startSpeedPercentQuerier = float64(config.StartPercent)
		incrementPercentGenerator = float64(config.IncrementPercent)
		incrementPercentQuerier = float64(config.IncrementPercent)
		stepsCount = config.Steps
	} else {
		stepsCount = 1 // Stability mode has one "step"
	}

	// Update combined step percentages
	for i := range l.combinedSteps {
		if testType == "maxPerf" {
			l.combinedSteps[i].StepPercent["step_percent_generator"] = startSpeedPercentGenerator + float64(i)*incrementPercentGenerator
			l.combinedSteps[i].StepPercent["step_percent_querier"] = startSpeedPercentQuerier + float64(i)*incrementPercentQuerier
		} else {
			l.combinedSteps[i].StepPercent["step_percent_generator"] = float64(config.StepPercent)
			l.combinedSteps[i].StepPercent["step_percent_querier"] = float64(config.StepPercent)
		}
	}

	report := CombinedTestReport{
		TestFolder:    fmt.Sprintf("load_tests\\%s\\", testType),
		TestType:      testType,
		TestMode:      l.mode,
		TestStartTime: l.startTime.Unix(),
		TestEndTime:   now.Unix(),
		ImpactTime:    config.Impact,
		StabilityTime: config.StepDuration,
		SpeedUnit:     "RPS",
		BaseSpeed: map[string]float64{
			"base_speed_generator": baseSpeedGenerator,
			"base_speed_querier":   baseSpeedQuerier,
		},
		StartSpeedPercent: map[string]float64{
			"start_speed_percent_generator": startSpeedPercentGenerator,
			"start_speed_percent_querier":   startSpeedPercentQuerier,
		},
		IncrementPercent: map[string]float64{
			"increment_percent_generator": incrementPercentGenerator,
			"increment_percent_querier":   incrementPercentQuerier,
		},
		StepsCount: stepsCount,
		Steps:      l.combinedSteps,
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

	fmt.Printf("Combined test report generated: %s\n", yamlFilename)
	return nil
}

// generateCombinedYAMLReportWithConfigs generates YAML report for combined mode with separate configs
func (l *LoadTestLogger) generateCombinedYAMLReportWithConfigs(testType string, timingConfig, generatorConfig, querierConfig LoadTestConfig, bulkSize int) error {
	now := time.Now()
	yamlFilename := fmt.Sprintf("./load_tests/%s/times_%s_%s.yaml", l.testType, l.mode, l.startTime.Format("2006-01-02_15-04-05"))

	// Get base RPS values from configs (100% values)
	baseSpeedGenerator := float64(generatorConfig.BaseRPS)
	baseSpeedQuerier := float64(querierConfig.BaseRPS)

	var startSpeedPercentGenerator, startSpeedPercentQuerier float64
	var incrementPercentGenerator, incrementPercentQuerier float64
	var stepsCount int

	if testType == "maxPerf" {
		startSpeedPercentGenerator = float64(generatorConfig.StartPercent)
		startSpeedPercentQuerier = float64(querierConfig.StartPercent)
		incrementPercentGenerator = float64(generatorConfig.IncrementPercent)
		incrementPercentQuerier = float64(querierConfig.IncrementPercent)
		stepsCount = timingConfig.Steps
	} else {
		startSpeedPercentGenerator = float64(generatorConfig.StepPercent)
		startSpeedPercentQuerier = float64(querierConfig.StepPercent)
		stepsCount = 1 // Stability mode has one "step"
	}

	// Update combined step percentages
	for i := range l.combinedSteps {
		if testType == "maxPerf" {
			l.combinedSteps[i].StepPercent["step_percent_generator"] = startSpeedPercentGenerator + float64(i)*incrementPercentGenerator
			l.combinedSteps[i].StepPercent["step_percent_querier"] = startSpeedPercentQuerier + float64(i)*incrementPercentQuerier
		} else {
			l.combinedSteps[i].StepPercent["step_percent_generator"] = startSpeedPercentGenerator
			l.combinedSteps[i].StepPercent["step_percent_querier"] = startSpeedPercentQuerier
		}
	}

	report := CombinedTestReport{
		TestFolder:    fmt.Sprintf("load_tests\\%s\\", testType),
		TestType:      testType,
		TestMode:      l.mode,
		TestStartTime: l.startTime.Unix(),
		TestEndTime:   now.Unix(),
		ImpactTime:    timingConfig.Impact,
		StabilityTime: timingConfig.StepDuration,
		SpeedUnit:     "RPS",
		BaseSpeed: map[string]float64{
			"base_speed_generator": baseSpeedGenerator,
			"base_speed_querier":   baseSpeedQuerier,
		},
		BulkSize: bulkSize,
		StartSpeedPercent: map[string]float64{
			"start_speed_percent_generator": startSpeedPercentGenerator,
			"start_speed_percent_querier":   startSpeedPercentQuerier,
		},
		IncrementPercent: map[string]float64{
			"increment_percent_generator": incrementPercentGenerator,
			"increment_percent_querier":   incrementPercentQuerier,
		},
		StepsCount: stepsCount,
		Steps:      l.combinedSteps,
	}

	// Generate YAML
	data, err := yaml.Marshal(&report)
	if err != nil {
		return fmt.Errorf("failed to marshal combined YAML report: %v", err)
	}

	// Write to file
	err = os.WriteFile(yamlFilename, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write combined YAML report %s: %v", yamlFilename, err)
	}

	fmt.Printf("Combined test report (with separate configs) generated: %s\n", yamlFilename)
	return nil
}

// SetBulkSize sets bulk size for generator mode (will be included in YAML report)
func (l *LoadTestLogger) SetBulkSize(bulkSize int) {
	// We need to store this for the YAML report generation
	// For now, we'll implement it via extending the config parameter in GenerateYAMLReport
}

// GenerateYAMLReportWithBulkSize generates a YAML report with bulk size for generator modes
func (l *LoadTestLogger) GenerateYAMLReportWithBulkSize(testType string, config LoadTestConfig, bulkSize int) error {
	if l.mode == "combined" {
		return l.generateCombinedYAMLReport(testType, config)
	}

	// For single mode, generate report and update BulkSize if it's generator mode
	err := l.generateSingleYAMLReport(testType, config)
	if err != nil {
		return err
	}

	// If this is generator mode, we need to update the YAML file to include bulk_size
	if l.mode == "generator" && bulkSize > 0 {
		return l.updateYAMLWithBulkSize(testType, bulkSize)
	}

	return nil
}

// GenerateYAMLReportCombined generates a YAML report for combined mode with separate configs
func (l *LoadTestLogger) GenerateYAMLReportCombined(testType string, timingConfig, generatorConfig, querierConfig LoadTestConfig) error {
	return l.generateCombinedYAMLReportWithConfigs(testType, timingConfig, generatorConfig, querierConfig, 0)
}

// GenerateYAMLReportCombinedWithBulkSize generates a YAML report for combined mode with separate configs and bulkSize
func (l *LoadTestLogger) GenerateYAMLReportCombinedWithBulkSize(testType string, timingConfig, generatorConfig, querierConfig LoadTestConfig, bulkSize int) error {
	return l.generateCombinedYAMLReportWithConfigs(testType, timingConfig, generatorConfig, querierConfig, bulkSize)
}

// updateYAMLWithBulkSize updates existing YAML file with bulk_size field
func (l *LoadTestLogger) updateYAMLWithBulkSize(testType string, bulkSize int) error {
	yamlFilename := fmt.Sprintf("./load_tests/%s/times_%s_%s.yaml", l.testType, l.mode, l.startTime.Format("2006-01-02_15-04-05"))

	// Read existing file
	data, err := os.ReadFile(yamlFilename)
	if err != nil {
		return fmt.Errorf("failed to read existing YAML file: %v", err)
	}

	var report TestReport
	err = yaml.Unmarshal(data, &report)
	if err != nil {
		return fmt.Errorf("failed to unmarshal existing YAML: %v", err)
	}

	// Update bulk size
	report.BulkSize = bulkSize

	// Write back
	updatedData, err := yaml.Marshal(&report)
	if err != nil {
		return fmt.Errorf("failed to marshal updated YAML: %v", err)
	}

	err = os.WriteFile(yamlFilename, updatedData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write updated YAML: %v", err)
	}

	return nil
}

// GetCurrentStepCount returns the number of steps logged so far
func (l *LoadTestLogger) GetCurrentStepCount() int {
	if l.mode == "combined" {
		return len(l.combinedSteps)
	}
	return len(l.steps)
}

// StatsLogger handles simple statistics logging for generators and queriers
type StatsLogger struct {
	logFile   *os.File
	logger    *log.Logger
	startTime time.Time
	mode      string
	testType  string
	module    string // "generator" or "querier"
}

// NewStatsLogger creates a new statistics logger for a specific module
func NewStatsLogger(mode, testType, module string) (*StatsLogger, error) {
	now := TestStartTime // Use the same timestamp as main logger
	if now.IsZero() {
		now = time.Now()
	}

	// Ensure logs subdirectory exists
	logDir := fmt.Sprintf("./load_tests/%s", testType)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory %s: %v", logDir, err)
	}

	filename := fmt.Sprintf("%s/stats_%s_%s_%s.log", logDir, module, mode, now.Format("2006-01-02_15-04-05"))

	// Create or open the log file
	logFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create stats log file %s: %v", filename, err)
	}

	logger := log.New(logFile, "", 0) // No prefix, we'll format ourselves

	return &StatsLogger{
		logFile:   logFile,
		logger:    logger,
		startTime: now,
		mode:      mode,
		testType:  testType,
		module:    module,
	}, nil
}

// LogGeneratorStats logs simple generator statistics for a step
func (s *StatsLogger) LogGeneratorStats(stepNumber, totalSteps int, system string, duration float64,
	targetRPS, actualRPS float64, totalRequests, successfulRequests, failedRequests, totalLogs int64) {

	s.logger.Printf("=== Generator Step %d/%d Statistics ===", stepNumber, totalSteps)
	s.logger.Printf("System: %s", system)
	s.logger.Printf("Duration: %.2f seconds", duration)
	s.logger.Printf("Target RPS: %.2f", targetRPS)
	s.logger.Printf("Actual RPS: %.2f", actualRPS)
	s.logger.Printf("Total requests: %d", totalRequests)

	if totalRequests > 0 {
		successPercent := float64(successfulRequests) * 100.0 / float64(totalRequests)
		failPercent := float64(failedRequests) * 100.0 / float64(totalRequests)
		s.logger.Printf("Successful: %d (%.2f%%)", successfulRequests, successPercent)
		s.logger.Printf("Failed: %d (%.2f%%)", failedRequests, failPercent)
	} else {
		s.logger.Printf("Successful: 0 (0.00%%)")
		s.logger.Printf("Failed: 0 (0.00%%)")
	}

	s.logger.Printf("Total logs: %d", totalLogs)
	s.logger.Printf("=== Generator Step %d Completed ===", stepNumber)
	s.logger.Printf("")
}

// LogQuerierStats logs simple querier statistics for a step
func (s *StatsLogger) LogQuerierStats(stepNumber, totalSteps int, system string, duration float64,
	targetQPS, actualQPS float64, totalQueries, successfulQueries, failedQueries, totalResults int64, efficiency float64) {

	s.logger.Printf("=== Querier Step %d/%d Statistics ===", stepNumber, totalSteps)
	s.logger.Printf("System: %s", system)
	s.logger.Printf("Duration: %.2f seconds", duration)
	s.logger.Printf("Target QPS: %.2f", targetQPS)
	s.logger.Printf("Actual QPS: %.2f", actualQPS)
	s.logger.Printf("Total queries: %d", totalQueries)

	if totalQueries > 0 {
		successPercent := float64(successfulQueries) * 100.0 / float64(totalQueries)
		failPercent := float64(failedQueries) * 100.0 / float64(totalQueries)
		s.logger.Printf("Successful: %d (%.2f%%)", successfulQueries, successPercent)
		s.logger.Printf("Failed: %d (%.2f%%)", failedQueries, failPercent)
	} else {
		s.logger.Printf("Successful: 0 (0.00%%)")
		s.logger.Printf("Failed: 0 (0.00%%)")
	}

	s.logger.Printf("Total results: %d docs", totalResults)
	s.logger.Printf("QPS Efficiency: %.1f%%", efficiency)
	s.logger.Printf("=== Querier Step %d Completed ===", stepNumber)
	s.logger.Printf("")
}

// Close closes the stats log file
func (s *StatsLogger) Close() error {
	if s.logFile != nil {
		return s.logFile.Close()
	}
	return nil
}
