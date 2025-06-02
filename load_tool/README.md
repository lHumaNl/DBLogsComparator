# Log Generator and Querier Tool (load_tool)

This directory contains a versatile Go-based tool that serves as both a log generator and a query tool for the DBLogsComparator project. It provides a unified platform for log generation, querying, and benchmarking across multiple log storage systems.

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

The tool is integrated with the centralized monitoring system through the `monitoring-network` Docker network. All metrics are scraped by VictoriaMetrics and visualized in dedicated Grafana dashboards.

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
