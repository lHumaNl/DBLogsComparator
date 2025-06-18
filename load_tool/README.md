# Log Generator and Querier Tool (load_tool)

This directory contains a versatile Go-based tool that serves as both a log generator and a query tool for the
DBLogsComparator project. It provides a unified platform for log generation, querying, and benchmarking across multiple
log storage systems.

## Operational Modes

The tool operates in three distinct modes:

### 1. Generator Mode

Generates synthetic logs and sends them to the specified log system:

- Configurable throughput (requests per second - RPS)
- Customizable log type distribution
- Bulk sending capabilities
- Retry mechanism with exponential backoff

### 2. Querier Mode

Executes predefined queries against log systems to measure query performance:

- Customizable query patterns
- Measurement of query latency and throughput
- Support for various query types (range, filter, aggregate)
- Parallel query execution

### 3. Combined Mode

Runs both generator and querier simultaneously to test under load:

- Evaluates query performance while system is receiving logs
- Realistic load testing scenario
- Performance correlation between ingest and query operations

## Supported Log Systems

The tool supports multiple log storage backends:

- **Elasticsearch/ELK** (`elasticsearch` or `es`): Supports both parameter names for compatibility
- **Loki**: Grafana Loki with structured metadata
- **VictoriaLogs** (`victoria`): VictoriaMetrics log storage system

## Configuration

### Command Line Parameters

```
Usage:
  load_tool [flags]

Flags:
  -config string
        Path to configuration file YAML (default "config.yaml")
  -hosts string
        Path to hosts file with log system URLs (default "db_hosts.yaml")
  -mode string
        Operation mode: generator, querier, combined (default "generator")
  -system string
        Log system type: loki, elasticsearch/es, victoria
  -metrics-port int
        Prometheus metrics port (default 9090)
```

### YAML Configuration

The tool uses two main configuration files:

1. **config.yaml**: General settings
   ```yaml
   generator:
     rps: 100
     duration: 1h
     bulk_size: 10
     workers: 4
     connections: 10
     max_retries: 3
     retry_delay: 1s
     log_distribution:
       web_access: 40
       web_error: 10
       application: 30
       metric: 10
       event: 10
   
   querier:
     queries_per_second: 10
     duration: 1h
     concurrency: 4
     query_types:
       range: 30
       filter: 40
       aggregate: 30
   ```

2. **db_hosts.yaml**: Backend-specific endpoints
   ```yaml
   urlLoki: http://localhost:3100
   urlES: http://localhost:9200
   urlVictoria: http://localhost:9428
   ```

## Running the Tool

### Docker Usage

To run the tool using Docker:

```bash
# Set environment variables and start the container
SYSTEM=loki MODE=generator docker-compose up -d

# For querier mode
SYSTEM=elasticsearch MODE=querier docker-compose up -d

# For combined mode
SYSTEM=victoria MODE=combined docker-compose up -d
```

To stop the tool:

```bash
docker-compose down
```

### Native Usage

For development or performance testing, you can run the tool natively:

```bash
# Build the tool
go build -o load_tool

# Run in generator mode
./load_tool -system loki -mode generator

# Run in querier mode
./load_tool -system elasticsearch -mode querier

# Run in combined mode
./load_tool -system victoria -mode combined
```

## Log Types and Structure

The generator creates various log types to simulate real-world scenarios:

1. **Web Access Logs**: HTTP access logs with client IP, method, path, status code, and response time
2. **Web Error Logs**: Application errors with stack traces and error codes
3. **Application Logs**: General application logs with different severity levels
4. **Metric Logs**: Numerical metrics with dimensions
5. **Event Logs**: Discrete events with timestamps and metadata

All logs follow a consistent JSON structure with common fields and type-specific fields.

## Metrics

The tool exposes Prometheus metrics on the configured port (default: 9090):

### Generator Metrics

- Logs generated per second
- Request durations by log system
- Error rates and retry counts
- Log type distribution

### Querier Metrics

- Queries per second
- Query latency (min, max, avg, percentiles)
- Query errors by type
- Response size distribution

## Integration with Monitoring

The tool is integrated with the centralized monitoring system through the `monitoring-network` Docker network. All
metrics are scraped by VictoriaMetrics and visualized in dedicated Grafana dashboards.

## Detailed Prometheus Metrics Reference

The load_tool exposes comprehensive metrics with the `dblogscomp_` prefix on port 9090. These metrics are designed to
monitor and analyze the performance of both the generator and querier components across different log systems.

### Write Operation Metrics

| Metric Name                                       | Type      | Labels                    | Description                                        |
|---------------------------------------------------|-----------|---------------------------|----------------------------------------------------|
| `dblogscomp_write_requests_total`                 | Counter   | `system`                  | Total number of write requests sent to log systems |
| `dblogscomp_write_requests_success`               | Counter   | `system`                  | Count of successful write requests                 |
| `dblogscomp_write_requests_failure`               | Counter   | `system`, `error_type`    | Count of failed write requests by error type       |
| `dblogscomp_write_requests_retried`               | Counter   | `system`, `retry_attempt` | Number of retried write requests by attempt number |
| `dblogscomp_write_duration_seconds`               | Histogram | `system`, `status`        | Write request duration histogram (seconds)         |
| `dblogscomp_generator_logs_sent_total`            | Counter   | `log_type`, `system`      | Total number of logs sent by type and system       |
| `dblogscomp_generator_batch_size`                 | Gauge     | -                         | Current batch size used by the generator           |
| `dblogscomp_generator_throughput_logs_per_second` | Gauge     | `system`, `log_type`      | Current throughput of the generator (logs/sec)     |

### Read Operation Metrics

| Metric Name                             | Type      | Labels                         | Description                                            |
|-----------------------------------------|-----------|--------------------------------|--------------------------------------------------------|
| `dblogscomp_read_requests_total`        | Counter   | `system`                       | Total number of read/query requests                    |
| `dblogscomp_read_requests_success`      | Counter   | `system`                       | Count of successful read requests                      |
| `dblogscomp_read_requests_failure`      | Counter   | `system`, `error_type`         | Count of failed read requests by error type            |
| `dblogscomp_read_requests_retried`      | Counter   | `system`, `retry_attempt`      | Number of retried read requests by attempt number      |
| `dblogscomp_read_duration_seconds`      | Histogram | `system`, `status`             | Read/query duration histogram (seconds)                |
| `dblogscomp_querier_query_type_total`   | Counter   | `type`, `system`               | Count of queries by query type (simple, complex, etc.) |
| `dblogscomp_querier_failed_query_types` | Counter   | `type`, `system`, `error_type` | Count of failed queries by type and error              |

### Performance and Response Metrics

| Metric Name                            | Type      | Labels                     | Description                                     |
|----------------------------------------|-----------|----------------------------|-------------------------------------------------|
| `dblogscomp_querier_result_size_bytes` | Histogram | `system`                   | Size of query results in bytes                  |
| `dblogscomp_querier_result_hits`       | Histogram | `system`                   | Number of records/documents returned by queries |
| `dblogscomp_current_rps`               | Gauge     | `component`                | Current rate of requests per second             |
| `dblogscomp_system_latency_seconds`    | Histogram | `system`, `operation_type` | System latency comparison histogram             |

### Error and System Metrics

| Metric Name                    | Type    | Labels                              | Description                                              |
|--------------------------------|---------|-------------------------------------|----------------------------------------------------------|
| `dblogscomp_error_total`       | Counter | `error_type`, `operation`, `system` | Total errors by type, operation, and system              |
| `dblogscomp_connection_errors` | Counter | `system`, `error_type`              | Connection errors by system and error type               |
| `dblogscomp_operations_total`  | Counter | `type`, `system`                    | Total operations by type and system                      |
| `dblogscomp_resource_usage`    | Gauge   | `resource_type`                     | Resource usage during test execution (CPU, memory, etc.) |

### Using Metrics for Analysis

These metrics can be used to:

1. **Compare System Performance**
    - Use `dblogscomp_system_latency_seconds` to directly compare latency across systems
    - Analyze `dblogscomp_write_duration_seconds` and `dblogscomp_read_duration_seconds` to compare write and read
      performance

2. **Identify Bottlenecks**
    - Monitor `dblogscomp_resource_usage` to detect resource constraints
    - Track `dblogscomp_error_total` to identify recurring issues

3. **Tune Performance**
    - Analyze the impact of batch size changes using `dblogscomp_generator_batch_size` and throughput metrics
    - Optimize query performance by monitoring `dblogscomp_querier_result_size_bytes` and latency metrics

4. **Validate Test Scenarios**
    - Ensure expected log distribution using `dblogscomp_generator_logs_sent_total`
    - Verify query distribution with `dblogscomp_querier_query_type_total`

Metrics are automatically collected by VictoriaMetrics and can be visualized through the integrated Grafana dashboards.

## Performance Tuning

For optimal performance:

- Adjust `bulk_size` to balance throughput and memory usage
- Set `workers` based on available CPU cores
- Configure `connections` according to network capabilities
- For query performance, tune `concurrency` based on system resources

## Troubleshooting

- **Connection issues**: Ensure network connectivity between services
- **Performance problems**: Adjust worker count, connections, and bulk size
- **Query timeout errors**: Increase timeout settings or reduce query complexity
- **Metric collection issues**: Verify the tool is connected to the monitoring network
