package pkg

import (
	"fmt"
	"github.com/dblogscomparator/DBLogsComparator/load_tool/common/logdata"
	"time"
)

// Structures for different log types
type BaseLog struct {
	Timestamp     string `json:"timestamp"`
	LogType       string `json:"log_type"`
	Host          string `json:"host"`
	ContainerName string `json:"container_name"`
	Environment   string `json:"environment,omitempty"`
	DataCenter    string `json:"datacenter,omitempty"`
	Version       string `json:"version,omitempty"`
	GitCommit     string `json:"git_commit,omitempty"`
}

type WebAccessLog struct {
	BaseLog
	RemoteAddr    string  `json:"remote_addr"`
	Request       string  `json:"request"`
	Status        int     `json:"status"`
	BytesSent     int     `json:"bytes_sent"`
	HttpReferer   string  `json:"http_referer"`
	HttpUserAgent string  `json:"http_user_agent"`
	RequestTime   float64 `json:"request_time"`
	Message       string  `json:"message"`
}

type WebErrorLog struct {
	BaseLog
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	ErrorCode   int               `json:"error_code"`
	Service     string            `json:"service"`
	Exception   string            `json:"exception,omitempty"`
	Stacktrace  string            `json:"stacktrace,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
	RequestPath string            `json:"request_path,omitempty"`
	ClientIP    string            `json:"client_ip,omitempty"`
	Duration    float64           `json:"duration,omitempty"`
	RetryCount  int               `json:"retry_count,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Context     map[string]string `json:"context,omitempty"`
}

type ApplicationLog struct {
	BaseLog
	Level          string            `json:"level"`
	Message        string            `json:"message"`
	Service        string            `json:"service"`
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	RequestMethod  string            `json:"request_method"`
	RequestPath    string            `json:"request_path"`
	RequestParams  map[string]string `json:"request_params"`
	ResponseStatus int               `json:"response_status"`
	ResponseTime   float64           `json:"response_time"`
	Exception      string            `json:"exception,omitempty"`
	Stacktrace     string            `json:"stacktrace,omitempty"`
	UserID         string            `json:"user_id,omitempty"`
	SessionID      string            `json:"session_id,omitempty"`
	Dependencies   []string          `json:"dependencies,omitempty"`
	Memory         float64           `json:"memory,omitempty"`
	CPU            float64           `json:"cpu,omitempty"`
}

type MetricLog struct {
	BaseLog
	MetricName string  `json:"metric_name"`
	Value      float64 `json:"value"`
	Service    string  `json:"service"`
	Region     string  `json:"region"`
	Message    string  `json:"message"`
}

type EventLog struct {
	BaseLog
	EventType  string `json:"event_type"`
	Message    string `json:"message"`
	ResourceID string `json:"resource_id"`
	Namespace  string `json:"namespace"`
	Service    string `json:"service"`
}

// Random data generators
func GenerateRandomIP() string {
	return logdata.GetRandomIP()
}

func GenerateRandomUserAgent() string {
	return logdata.GetRandomUserAgent()
}

func GenerateRandomHttpStatus() int {
	return logdata.GetRandomHttpStatusCode()
}

func GenerateRandomHttpMethod() string {
	return logdata.GetRandomHttpMethod()
}

func GenerateRandomPath() string {
	return logdata.GetRandomPath()
}

func GenerateRandomLogLevel() string {
	return logdata.GetRandomLogLevel()
}

func GenerateRandomErrorMessage() string {
	return logdata.GetRandomErrorMessage()
}

func GenerateRandomException() string {
	return logdata.GetRandomException()
}

func GenerateRandomStackTrace(exception string) string {
	return logdata.GetRandomStackTrace(exception)
}

func GenerateRandomDataCenter() string {
	return logdata.GetRandomDataCenter()
}

func GenerateRandomEnvironment() string {
	return logdata.GetRandomEnvironment()
}

func GenerateRandomVersion() string {
	return logdata.GetRandomVersion()
}

func GenerateRandomGitCommit() string {
	return logdata.GetRandomGitCommit()
}

func GenerateRandomRequestID() string {
	return logdata.GetRandomRequestID()
}

func GenerateRandomTags() []string {
	return logdata.GetRandomTags()
}

func GenerateRandomErrorContext() map[string]string {
	return logdata.GetRandomErrorContext()
}

func GenerateRandomDependencies() []string {
	return logdata.GetRandomDependencies()
}

func GenerateLogMessage(logLevel string) string {
	return logdata.GetLogMessage(logLevel)
}

func SelectRandomLogType(distribution map[string]int) string {
	return logdata.SelectRandomLogType(distribution)
}

func CountLogTypes(payload string) map[string]int {
	return logdata.CountLogTypes(payload)
}

func GenerateLog(logType string, timestamp string) interface{} {
	// If the timestamp is not provided, generate a current one
	if timestamp == "" {
		timestamp = time.Now().Format(time.RFC3339Nano)
	}

	// Create the base log
	baseLog := BaseLog{
		Timestamp:     timestamp,
		LogType:       logType,
		Host:          GenerateRandomHost(),
		ContainerName: GenerateRandomContainer(),
		Environment:   GenerateRandomEnvironment(),
		DataCenter:    GenerateRandomDataCenter(),
		Version:       GenerateRandomVersion(),
		GitCommit:     GenerateRandomGitCommit(),
	}

	// Generate the specific log based on the type
	switch logType {
	case "web_access":
		return WebAccessLog{
			BaseLog:       baseLog,
			RemoteAddr:    GenerateRandomIP(),
			Request:       fmt.Sprintf("%s %s HTTP/1.1", GenerateRandomHttpMethod(), GenerateRandomPath()),
			Status:        GenerateRandomHttpStatus(),
			BytesSent:     logdata.RandomIntn(10000) + 100,
			HttpReferer:   fmt.Sprintf("https://%s.example.com%s", GenerateRandomService(), GenerateRandomPath()),
			HttpUserAgent: GenerateRandomUserAgent(),
			RequestTime:   logdata.RandomFloat64(),
			Message:       fmt.Sprintf("%s %s - %d", GenerateRandomHttpMethod(), GenerateRandomPath(), GenerateRandomHttpStatus()),
		}

	case "web_error":
		return WebErrorLog{
			BaseLog:     baseLog,
			Level:       GenerateRandomLogLevel(),
			Message:     GenerateRandomErrorMessage(),
			ErrorCode:   logdata.RandomIntn(100) + 400,
			Service:     GenerateRandomService(),
			Exception:   GenerateRandomException(),
			Stacktrace:  GenerateRandomStackTrace(GenerateRandomException()),
			RequestID:   GenerateRandomRequestID(),
			RequestPath: GenerateRandomPath(),
			ClientIP:    GenerateRandomIP(),
			Duration:    logdata.RandomFloat64() * 10.0,
			RetryCount:  logdata.RandomIntn(5),
			Tags:        GenerateRandomTags(),
			Context:     GenerateRandomErrorContext(),
		}

	case "application":
		var exception, stacktrace string
		if logdata.RandomIntn(10) < 2 { // 20% chance of having exception
			exception = GenerateRandomException()
			stacktrace = GenerateRandomStackTrace(exception)
		}

		return ApplicationLog{
			BaseLog:        baseLog,
			Level:          GenerateRandomLogLevel(),
			Message:        GenerateLogMessage(GenerateRandomLogLevel()),
			Service:        GenerateRandomService(),
			TraceID:        GenerateRandomRequestID(),
			SpanID:         GenerateRandomRequestID()[0:16],
			RequestMethod:  GenerateRandomHttpMethod(),
			RequestPath:    GenerateRandomPath(),
			RequestParams:  GenerateRandomErrorContext(),
			ResponseStatus: GenerateRandomHttpStatus(),
			ResponseTime:   logdata.RandomFloat64() * 0.5,
			Exception:      exception,
			Stacktrace:     stacktrace,
			UserID:         fmt.Sprintf("user-%d", logdata.RandomIntn(10000)),
			SessionID:      GenerateRandomRequestID(),
			Dependencies:   GenerateRandomDependencies(),
			Memory:         logdata.RandomFloat64() * 1024.0,
			CPU:            logdata.RandomFloat64() * 100.0,
		}

	case "metric":
		metrics := logdata.MetricNames
		values := []float64{
			logdata.RandomFloat64() * 100.0,
			logdata.RandomFloat64() * 1024.0,
			logdata.RandomFloat64() * 100.0,
		}

		randomMetric := metrics[logdata.RandomIntn(len(metrics))]
		randomValue := values[logdata.RandomIntn(len(values))]

		return MetricLog{
			BaseLog:    baseLog,
			MetricName: randomMetric,
			Value:      randomValue,
			Service:    GenerateRandomService(),
			Region:     GenerateRandomDataCenter(),
			Message:    fmt.Sprintf("Metric value for %s: %.2f", randomMetric, randomValue),
		}

	case "event":
		eventType := logdata.GetRandomEventType()
		return EventLog{
			BaseLog:    baseLog,
			EventType:  eventType,
			Message:    fmt.Sprintf("Event: %s occurred at %s", eventType, timestamp),
			ResourceID: fmt.Sprintf("resource-%d", logdata.RandomIntn(1000)),
			Namespace:  fmt.Sprintf("namespace-%d", logdata.RandomIntn(10)),
			Service:    GenerateRandomService(),
		}

	default:
		// Default to application log if type is not recognized
		return ApplicationLog{
			BaseLog:        baseLog,
			Level:          "info",
			Message:        fmt.Sprintf("Unknown log type: %s", logType),
			Service:        GenerateRandomService(),
			TraceID:        GenerateRandomRequestID(),
			SpanID:         GenerateRandomRequestID()[0:16],
			RequestMethod:  GenerateRandomHttpMethod(),
			RequestPath:    GenerateRandomPath(),
			RequestParams:  map[string]string{},
			ResponseStatus: 200,
			ResponseTime:   0.001,
		}
	}
}

func GenerateRandomHost() string {
	return logdata.GetRandomHost()
}

func GenerateRandomContainer() string {
	return logdata.GetRandomContainer()
}

func GenerateRandomService() string {
	return logdata.GetRandomService()
}
