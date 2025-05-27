# VictoriaLogs for DBLogsComparator

This directory contains the configuration for VictoriaLogs, a high-performance, cost-effective and scalable logs storage and monitoring solution from the creators of VictoriaMetrics.

## Components

- **VictoriaLogs**: Log storage and analytics engine optimized for high ingestion rates and efficient querying

## Configuration

The setup includes:

- Docker Compose configuration for VictoriaLogs service
- Resource constraints for fair comparison with other log systems
- Integration with the monitoring network for metrics collection

### Key Configuration Features

- **High Performance**: Optimized for fast log ingestion and efficient querying
- **Columnar Storage**: Uses columnar storage for efficient compression and fast queries
- **LogsQL Query Language**: Powerful query language similar to LogQL but with extended features
- **Local File System Storage**: All data stored in a volume for easy inspection and persistence
- **Prometheus Metrics**: Exposes metrics in Prometheus format for monitoring

## Environment Variables

Environment variables are defined in the `.env` file:

- `VICTORIALOGS_VERSION`: The version of VictoriaLogs to use (currently v0.27.1-victorialogs)
- `VICTORIALOGS_PORT`: The port to expose VictoriaLogs on (default: 9428)
- `VICTORIALOGS_GO_MAX_PROCS`: Sets the maximum number of CPUs that can be used (default: 2)
- `VICTORIALOGS_MEMORY_ALLOWED_BYTES`: Memory limit for VictoriaLogs (default: 4096MB)
- `VICTORIALOGS_RETENTION_PERIOD`: How long to keep logs (default: 72h)
- `VICTORIALOGS_MAX_DISK_SIZE`: Maximum disk space to use (default: 40GB)
- `VICTORIALOGS_MAX_QUERY_CONCURRENCY`: Maximum concurrent queries (default: 2)
- `VICTORIALOGS_MAX_CONCURRENT_REQUESTS`: Maximum concurrent requests (default: 4)
- `VICTORIALOGS_LOG_LEVEL`: Logging level for VictoriaLogs itself (default: INFO)
- `VICTORIALOGS_DATA_DIR`: Optional custom directory for data storage

## Resource Controls

VictoriaLogs is configured with the following resource constraints:

- **CPU Limitations**: 
  - `VICTORIALOGS_GO_MAX_PROCS`: Limits the number of CPU cores used
  - `VICTORIALOGS_MAX_QUERY_CONCURRENCY`: Limits concurrent query execution
  - `VICTORIALOGS_MAX_CONCURRENT_REQUESTS`: Limits concurrent request handling

- **Memory Limitations**:
  - `VICTORIALOGS_MEMORY_ALLOWED_BYTES`: Sets maximum memory usage
  - Docker ulimits for memory locks

- **Disk Space Limitations**:
  - `VICTORIALOGS_MAX_DISK_SIZE`: Controls the maximum storage size
  - `VICTORIALOGS_RETENTION_PERIOD`: Controls how long logs are kept

These resource controls ensure fair comparison with other log management systems in the DBLogsComparator project.

## Usage

### Starting VictoriaLogs

To start VictoriaLogs:

```bash
cd /path/to/DBLogsComparator/victorialogs
docker-compose up -d
```

### Stopping VictoriaLogs

To stop VictoriaLogs:

```bash
cd /path/to/DBLogsComparator/victorialogs
docker-compose down
```

### Viewing Logs

To view logs from VictoriaLogs:

1. **Web UI**: Access the VictoriaLogs UI at http://localhost:9428/vmui/
   - Use the LogsQL interface to query and filter logs
   - Create and save queries for later use

2. **REST API**: Query logs directly using the VictoriaLogs API:
   ```bash
   curl -G 'http://localhost:9428/select/logsql/query' --data-urlencode 'query=filter{log_type="web_access"}'
   ```

### Using VictoriaLogs with the Log Generator

The Go log generator is pre-configured to send logs to VictoriaLogs when using the `victoria` mode:

```bash
# Configure in .env file
GENERATOR_MODE=victoria
URL=http://victorialogs:9428
```

When using Docker Compose, the services are connected via the Docker network.

## Data Storage

All VictoriaLogs data is stored in a Docker volume (or optional custom directory) with the following features:

- **Efficient Compression**: Data is compressed to minimize storage requirements
- **Columnar Format**: Data stored in columnar format for fast querying
- **Automatic Garbage Collection**: Old data automatically removed according to retention settings
- **Built-in Backups**: Supports creating consistent backups of log data

## Integration with Monitoring

VictoriaLogs is connected to the `monitoring-network` Docker network, allowing metrics to be collected by the monitoring system. The service exposes metrics in Prometheus format at `/metrics` endpoint.

## Comparison with Other Log Storage Systems

VictoriaLogs differs from Elasticsearch and Loki in several key aspects:

- **Columnar Storage**: More efficient storage than Elasticsearch's document storage
- **Schema-Free**: No need to define mappings like in Elasticsearch
- **Lower Resource Usage**: Generally uses less resources than Elasticsearch for similar workloads
- **Simpler Configuration**: Easier to configure and operate than Loki
- **Native JSON Support**: Better handling of structured JSON logs than Loki's label-based approach

## Troubleshooting

- Check VictoriaLogs logs: `docker logs victorialogs`
- Verify VictoriaLogs is healthy: `curl http://localhost:9428/health`
- Ensure proper network connectivity between services
- Monitor memory usage if you see OOM (Out of Memory) errors
- Check disk space if logs stop ingesting
