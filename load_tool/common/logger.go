package common

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// Logger is the structured logger instance
var Logger *logrus.Logger

// InitLogger initializes the structured logger
func InitLogger(verbose bool) {
	Logger = logrus.New()

	// Set output to stdout
	Logger.SetOutput(os.Stdout)

	// Set formatter to JSON for structured logging
	Logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})

	// Set log level based on verbose flag
	if verbose {
		Logger.SetLevel(logrus.DebugLevel)
	} else {
		Logger.SetLevel(logrus.InfoLevel)
	}
}

// InitTextLogger initializes the logger with text formatter (for development)
func InitTextLogger(verbose bool) {
	Logger = logrus.New()

	// Set output to stdout
	Logger.SetOutput(os.Stdout)

	// Set formatter to text for human-readable logs
	Logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// Set log level based on verbose flag
	if verbose {
		Logger.SetLevel(logrus.DebugLevel)
	} else {
		Logger.SetLevel(logrus.InfoLevel)
	}
}

// LogError logs an error with context
func LogError(component, operation string, err error, fields logrus.Fields) {
	if Logger == nil {
		InitTextLogger(false)
	}

	if fields == nil {
		fields = logrus.Fields{}
	}

	fields["component"] = component
	fields["operation"] = operation

	Logger.WithFields(fields).Error(err.Error())
}

// LogInfo logs an info message with context
func LogInfo(component, message string, fields logrus.Fields) {
	if Logger == nil {
		InitTextLogger(false)
	}

	if fields == nil {
		fields = logrus.Fields{}
	}

	fields["component"] = component

	Logger.WithFields(fields).Info(message)
}

// LogDebug logs a debug message with context
func LogDebug(component, message string, fields logrus.Fields) {
	if Logger == nil {
		InitTextLogger(false)
	}

	if fields == nil {
		fields = logrus.Fields{}
	}

	fields["component"] = component

	Logger.WithFields(fields).Debug(message)
}

// LogQueryError logs a query error with detailed context and to error file
func LogQueryError(workerID int, queryType, system string, err error, query string) {
	LogError("querier", "query_execution", err, logrus.Fields{
		"worker_id":  workerID,
		"query_type": queryType,
		"system":     system,
		"query":      query,
	})

	// Also log to timestamped error file (using test start time)
	timestamp := TestStartTime.Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("querier_err_%s.log", timestamp)
	logToErrorFile(filename, fmt.Sprintf("Processor %d: %s query error: %v. Query: %s",
		workerID, queryType, err, query))
}

// LogQuerySuccess logs a successful query with context
func LogQuerySuccess(workerID int, queryType, system string, duration float64, hits int, bytes int64) {
	LogInfo("querier", "query_success", logrus.Fields{
		"worker_id":    workerID,
		"query_type":   queryType,
		"system":       system,
		"duration_ms":  duration * 1000,
		"result_hits":  hits,
		"result_bytes": bytes,
	})
}

// LogGeneratorError logs a generator error with detailed context and to error file
func LogGeneratorError(workerID int, system string, err error, request string) {
	LogError("generator", "request_execution", err, logrus.Fields{
		"worker_id": workerID,
		"system":    system,
		"request":   request,
	})

	// Also log to timestamped error file (using test start time)
	timestamp := TestStartTime.Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("generator_err_%s.log", timestamp)
	logToErrorFile(filename, fmt.Sprintf("Processor %d: %s request error: %v. Request: %s",
		workerID, system, err, request))
}

// logToErrorFile writes error messages to timestamped error files
func logToErrorFile(filename, message string) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		Logger.WithError(err).Error("Failed to open error log file")
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	_, err = file.WriteString(fmt.Sprintf("[%s] %s\n", timestamp, message))
	if err != nil {
		Logger.WithError(err).Error("Failed to write to error log file")
	}
}

// LogLokiError logs a Loki-specific error with HTTP details
func LogLokiError(statusCode int, query, url, response string) {
	LogError("loki_executor", "http_request", nil, logrus.Fields{
		"http_status":   statusCode,
		"query":         query,
		"url":           url,
		"response_body": response,
	})
}
