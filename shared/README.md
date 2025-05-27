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

#### Backend-Specific Configuration

Each backend has specific configuration options:

1. **VictoriaLogs**:
   - `GENERATOR_URL`: URL endpoint for VictoriaLogs (default: http://victoria-logs:9428)

2. **Elasticsearch**:
   - `GENERATOR_URL`: URL endpoint for Elasticsearch (default: http://elasticsearch:9200)

3. **Loki**:
   - `GENERATOR_URL`: URL endpoint for Loki (default: http://loki:3100)

#### Usage

```bash
cd shared/go_generator
docker-compose up -d
```

To change the mode of operation, update the `.env` file or set environment variables directly:

```bash
# For Loki
GENERATOR_MODE=loki docker-compose up -d

# For Elasticsearch
GENERATOR_MODE=es docker-compose up -d

# For VictoriaLogs
GENERATOR_MODE=victoria docker-compose up -d
```

#### Metrics

The Go log generator exposes Prometheus metrics on port 8080 (configurable), which include:

- Requests per second (RPS)
- Logs per second (LPS)
- Request durations
- Retry counts
- Log type distribution by backend
- HTTP status codes and errors

#### Log Types

The generator creates various log types to simulate real-world scenarios:

1. **Web Access Logs**: HTTP access logs with client IP, method, path, status code, and response time
2. **Web Error Logs**: Application errors with stack traces and error codes
3. **Application Logs**: General application logs with different severity levels (debug, info, warn, error, fatal)
4. **Metric Logs**: Numerical metrics with dimensions
5. **Event Logs**: Discrete events with timestamps and metadata

#### Log Structure

All generated logs follow a consistent JSON structure with common fields:

- `timestamp`: ISO-8601 formatted timestamp
- `log_type`: Type of log (web_access, web_error, application, metric, event)
- `message`: The log message content
- `host`: Simulated source host name
- `container_name`: Simulated container identifier
- `service_name`: Simulated service identifier

Each log type adds specific fields relevant to that type (e.g., HTTP status for web_access logs).

## Integration with Monitoring

All shared components are integrated with the centralized monitoring system through the `monitoring-network` Docker network. The metrics are scraped by VictoriaMetrics and visualized in Grafana dashboards.

## Performance Considerations

The log generator is designed for high performance, but there are some considerations:

- Increasing `GENERATOR_BULK_SIZE` improves throughput but increases memory usage
- `GENERATOR_WORKERS` should be tuned based on available CPU cores
- `GENERATOR_CONNECTIONS` affects network resource usage
- Very high RPS values may require additional resources on both generator and target systems

## Troubleshooting

- **Connection issues**: Ensure network connectivity between services (check Docker networks)
- **Performance problems**: Adjust worker count, connections, and bulk size
- **Metric collection issues**: Verify the generator is connected to the monitoring network
- **Log format errors**: Check the specific requirements of each backend system

## Known Issues

- There is a known issue with the RPS parameter in the Go log generator. Despite setting `GENERATOR_RPS=2` in the `.env` file, the generator might start with a higher RPS value. This can cause excessive load on the logging systems. The issue is related to parameter handling in the code.
