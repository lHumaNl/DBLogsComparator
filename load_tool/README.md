# Load Testing Tool for Log Storage Systems

A comprehensive Go-based tool for load testing and performance benchmarking of log storage systems. Supports log
generation, querying, and combined testing scenarios with built-in Prometheus metrics.

## Architecture Overview

This tool provides three main components:

### 1. Generator (`go_generator/`)

Generates synthetic logs and sends them to log systems:

- Supports bulk operations with configurable batch sizes
- Uses CPU-based worker allocation (`runtime.NumCPU() * 4`)
- Implements retry logic with exponential backoff
- Supports multiple log types with configurable distribution

### 2. Querier (`go_querier/`)

Executes queries against log systems to measure performance:

- Supports various query types (simple, complex, analytical, timeseries, stat, topk)
- Measures query latency, throughput, and result sizes
- Configurable concurrency and query timeout

### 3. Common (`common/`)

Shared configuration, metrics, and utilities:

- YAML-based configuration system
- Prometheus metrics integration with `dblogscomp_` prefix
- Load test logging and reporting

## Supported Log Systems

- **Loki**: Grafana Loki with structured metadata
- **Elasticsearch**: Full-text search and analytics (use `elasticsearch` parameter)
- **VictoriaLogs**: VictoriaMetrics log storage system (use `victorialogs` parameter)

## Operation Modes

- `generator`: Log generation only
- `querier`: Query execution only
- `combined`: Both generation and querying simultaneously

## Load Testing Modes

- `stability`: Constant load testing
- `maxPerf`: Step-by-step RPS increase to find performance limits

## Configuration

The tool uses YAML configuration with two main files:

### Main Configuration (`config.yaml`)

```yaml
metrics: true

generator:
  maxPerf:
    steps: 3
    stepDuration: 10
    impact: 2
    baseRPS: 10
    startPercent: 10
    incrementPercent: 10

  stability:
    stepDuration: 60
    impact: 5
    baseRPS: 10
    stepPercent: 100

  bulkSize: 10
  maxRetries: 3
  retryDelayMs: 500
  timeoutMs: 5000

  verbose: false

  distribution:
    web_access: 60
    web_error: 10
    application: 20
    event: 5
    metric: 5

querier:
  maxPerf:
    steps: 3
    stepDuration: 10
    impact: 2
    baseRPS: 10
    startPercent: 10
    incrementPercent: 10

  stability:
    stepDuration: 60
    impact: 5
    baseRPS: 10
    stepPercent: 100

  maxRetries: 3
  retryDelayMs: 500
  timeoutMs: 30000

  verbose: false

  distribution:
    simple: 20
    complex: 20
    analytical: 15
    timeseries: 15
    stat: 15
    topk: 15
```

**db_hosts.yaml**: Backend-specific endpoints
   ```yaml
   urlLoki: http://localhost:3100
   urlES: http://localhost:9200
   urlVictoria: http://localhost:9428
   ```

## Build and Development Commands

```bash
# Build the load_tool binary
go build -o load_tool

# Run tests
go test ./...

# Run with Go directly (development)
go run main.go -system loki -mode generator

# Build for Docker
docker build -t load_tool .

# Run with Docker Compose
SYSTEM=loki MODE=generator docker-compose up -d
```

## Usage Examples

### Command Line

```bash
# Generator mode
./load_tool -system loki -mode generator

# Querier mode  
./load_tool -system elasticsearch -mode querier

# Combined mode
./load_tool -system victorialogs -mode combined

# With custom config
./load_tool -config custom-config.yaml -hosts custom-hosts.yaml
```

### Docker Compose

```bash
# Generator
SYSTEM=loki MODE=generator docker-compose up -d

# Querier
SYSTEM=elasticsearch MODE=querier docker-compose up -d

# Combined
SYSTEM=victorialogs MODE=combined docker-compose up -d
```

## Log Types Generated

1. **Web Access Logs** (60%): HTTP requests with IP, method, path, status, response time
2. **Web Error Logs** (12%): Application errors with stack traces and error codes
3. **Application Logs** (20%): General application logs with severity levels
4. **Metric Logs** (5%): Numerical metrics with dimensions
5. **Event Logs** (3%): Discrete events with timestamps and metadata

## Prometheus Metrics

All metrics use the `dblogscomp_` prefix and are exposed on the configured port (default: 9090).

### Write Operation Metrics

| Metric                                            | Type      | Labels                             | Description                                 |
|---------------------------------------------------|-----------|------------------------------------|---------------------------------------------|
| `dblogscomp_write_requests_total`                 | Counter   | `system`                           | Total write requests                        |
| `dblogscomp_write_requests_success`               | Counter   | `system`, `log_type`               | Successful write requests by log type       |
| `dblogscomp_write_requests_failure`               | Counter   | `system`, `log_type`, `error_type` | Failed write requests by log and error type |
| `dblogscomp_write_duration_seconds`               | Histogram | `system`, `status`                 | Write request duration                      |
| `dblogscomp_generator_logs_sent_total`            | Counter   | `log_type`, `system`               | Total logs sent by type                     |
| `dblogscomp_generator_batch_size`                 | Gauge     | `system`                           | Current batch size                          |
| `dblogscomp_generator_throughput_logs_per_second` | Gauge     | `system`, `log_type`               | Generator throughput                        |

### Read Operation Metrics

| Metric                                 | Type      | Labels                               | Description                                  |
|----------------------------------------|-----------|--------------------------------------|----------------------------------------------|
| `dblogscomp_read_requests_total`       | Counter   | `system`                             | Total read requests                          |
| `dblogscomp_read_requests_success`     | Counter   | `system`, `query_type`               | Successful read requests by query type       |
| `dblogscomp_read_requests_failure`     | Counter   | `system`, `query_type`, `error_type` | Failed read requests by query and error type |
| `dblogscomp_read_duration_seconds`     | Histogram | `system`, `status`                   | Read request duration                        |
| `dblogscomp_querier_query_type_total`  | Counter   | `type`, `system`                     | Queries by type                              |
| `dblogscomp_querier_result_hits`       | Histogram | `system`, `type`                     | Query result hit counts                      |
| `dblogscomp_querier_result_size_bytes` | Histogram | `system`, `type`                     | Query result sizes                           |

### System Metrics

| Metric                              | Type      | Labels                              | Description                  |
|-------------------------------------|-----------|-------------------------------------|------------------------------|
| `dblogscomp_current_rps`            | Gauge     | `component`, `system`               | Current requests per second  |
| `dblogscomp_system_latency_seconds` | Histogram | `system`, `operation_type`          | System latency comparison    |
| `dblogscomp_operations_total`       | Counter   | `type`, `system`                    | Total operations by type     |
| `dblogscomp_error_total`            | Counter   | `error_type`, `operation`, `system` | Errors by type and operation |

## Key Design Patterns

### Dynamic Worker Allocation

Workers are allocated based on CPU count rather than fixed configuration:

```go
defaultWorkers := runtime.NumCPU() * 4
```

### Retry Logic

All components implement retry logic with configurable delays and max attempts.

### Metrics Integration

Comprehensive Prometheus metrics with consistent `dblogscomp_` prefix for monitoring performance across all systems.

### Context-Based Cancellation

Uses Go contexts for graceful shutdown in combined mode operations.

### Verbose Output Control

- `verbose: true`: Shows detailed configuration, real-time stats, and comprehensive logging
- `verbose: false`: Shows only errors and final results (like querier behavior)

## Performance Tuning

### Generator Optimization

- **Bulk Size**: Balance throughput vs memory usage (default: 10)
- **Workers**: Automatically set to `CPU cores * 4`
- **Connections**: Configure based on network capacity
- **RPS**: Adjust based on target load

### Querier Optimization

- **Concurrency**: Match to available CPU cores
- **Query Timeout**: Adjust based on query complexity
- **Query Distribution**: Balance query types for realistic testing

### System-Specific Notes

- **Elasticsearch**: Supports both bulk and single document operations
- **Loki**: Uses structured metadata for efficient querying
- **VictoriaLogs**: Optimized for high-volume log ingestion

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify database URLs in `db_hosts.yaml`
2. **Memory Issues**: Reduce bulk size or worker count
3. **Timeout Errors**: Increase query timeout or reduce complexity
4. **Metric Collection**: Ensure metrics port is accessible

### Debug Mode

Enable verbose logging for detailed troubleshooting:

```yaml
verbose: true
```

### Log Analysis

- Check error patterns in failed requests
- Monitor backpressure events for system overload
- Analyze query latency distributions

## Integration with Monitoring

The tool integrates with centralized monitoring through:

- VictoriaMetrics for metric collection
- Grafana dashboards for visualization
- Docker network (`monitoring-network`) for service discovery

All metrics are automatically scraped and available in the monitoring dashboard.

## Development Notes

- Error handling is comprehensive with detailed error messages
- Logging uses structured logging with configurable verbosity
- All timeouts and retry configurations are externalized to YAML
- Docker support with multi-stage builds for production deployment
- System labels are normalized to lowercase for consistent Grafana visualization