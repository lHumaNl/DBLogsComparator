# Shared Components for DBLogsComparator

This directory contains shared components used across the DBLogsComparator project. These components provide the foundation for log generation and common functionality used by all log storage systems being compared.

## Components

### Go Log Generator (`/go_generator`)

The main component is a high-performance log generator written in Go with the following features:

- Supports multiple log storage backends:
  - VictoriaLogs
  - Elasticsearch
  - Loki
- Configurable throughput (requests per second - RPS)
- Customizable log type distribution
- Bulk sending capabilities
- Prometheus metrics for performance monitoring
- Configurable retry mechanism with exponential backoff

#### Configuration

The Go log generator is configured via environment variables in the `.env` file:

- `GENERATOR_MODE`: Backend to use (victoria, es, loki)
- `GENERATOR_RPS`: Requests per second
- `GENERATOR_DURATION`: Test duration
- `GENERATOR_BULK_SIZE`: Number of logs per request
- `GENERATOR_WORKERS`: Number of worker goroutines
- `GENERATOR_CONNECTIONS`: Number of HTTP connections
- `GENERATOR_MAX_RETRIES`: Maximum retry attempts
- `GENERATOR_RETRY_DELAY`: Delay between retries
- `GENERATOR_LOG_DISTRIBUTION_*`: Distribution percentages for different log types

#### Usage

```bash
cd shared/go_generator
docker-compose up -d
```

#### Metrics

The Go log generator exposes Prometheus metrics on port 8080 (configurable), which include:

- Requests per second (RPS)
- Logs per second (LPS)
- Request durations
- Retry counts
- Log type distribution by backend

#### Log Types

The generator creates various log types to simulate real-world scenarios:

1. **Web Access Logs**: HTTP access logs with client IP, method, path, status code, and response time
2. **Web Error Logs**: Application errors with stack traces and error codes
3. **Application Logs**: General application logs with different severity levels
4. **Metric Logs**: Numerical metrics with dimensions
5. **Event Logs**: Discrete events with timestamps and metadata

## Integration with Monitoring

All shared components are integrated with the centralized monitoring system through the `monitoring-network` Docker network. The metrics are scraped by VictoriaMetrics and visualized in Grafana dashboards.

## Known Issues

There is a known issue with the RPS parameter in the Go log generator. Despite setting `GENERATOR_RPS=2` in the `.env` file, the generator might start with a higher RPS value. This can cause excessive load on the logging systems. The issue is related to parameter handling in the code.
