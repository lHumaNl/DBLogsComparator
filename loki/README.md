# Loki Setup for DBLogsComparator

This directory contains the configuration for Grafana Loki, a horizontally-scalable, highly-available, multi-tenant log aggregation system.

## Components

- **Loki**: The main log aggregation service (version 3.5.1)
- **Integration with Grafana**: Uses the existing Grafana instance from the monitoring system
- **Custom Grafana Dashboard**: Pre-configured dashboard for log visualization and analysis

## Configuration

The setup includes:

- Docker Compose configuration for Loki service
- Advanced Loki configuration with TSDB storage and structured metadata support
- Integration with the monitoring network

### Key Configuration Features

- **Schema v13**: Using the latest schema version for optimal performance
- **TSDB Storage**: Modern storage engine for better performance and scalability
- **Structured Metadata Support**: Allows storing high-cardinality fields separate from labels
- **Local File System Storage**: All data stored in the local `./data` directory for easy inspection
- **Single Instance Setup**: Using simplified configuration without replication_factor for standalone mode

## Environment Variables

Environment variables are defined in the `.env` file:

- `LOKI_VERSION`: The version of Loki to use (currently 3.5.1)
- `LOKI_PORT`: The port to expose Loki on (default: 3100)
- `STRUCTURED_METADATA`: Enable structured metadata support (default: true)
- `LOKI_DATA_DIR`: Directory for Loki data storage (default: loki-data)

## Resource Controls

Loki is configured with the following resource constraints:

- **CPU Limitations**: 
  - `max_concurrent_queries`: Controls the number of queries that can run simultaneously
  - `query_parallelism`: Controls parallel processing of queries
  
- **Memory Limitations**:
  - `max_streams`: Limits the number of active streams to prevent memory exhaustion
  - `max_chunks_per_query`: Limits the chunks processed per query
  - Docker memory limits via ulimits

- **Disk Space Limitations**:
  - `max_disk_size`: Controls the maximum storage size
  - `retention_period`: Controls how long logs are kept

## Usage

### Starting Loki

To start Loki:

```bash
cd /path/to/DBLogsComparator/loki
docker-compose up -d
```

### Stopping Loki

To stop Loki:

```bash
cd /path/to/DBLogsComparator/loki
docker-compose down
```

### Viewing Logs

To view logs from Loki, you can use:

1. **Grafana Dashboard**: Access the custom Loki dashboard in Grafana at http://localhost:3000
   - Navigate to the "DBLogsComparator - Loki Logs Dashboard" in the Loki folder
   - Use the dashboard to visualize logs by type, level, and more

2. **Loki API**: Query logs directly using the Loki API:
   ```bash
   curl -G 'http://localhost:3100/loki/api/v1/query_range' --data-urlencode 'query={log_type="web_access"}'
   ```

3. **Loki Labels**: View available labels:
   ```bash
   curl -s 'http://localhost:3100/loki/api/v1/labels' | jq
   ```

### Using Loki with the Log Generator

The Go log generator is pre-configured to send logs to Loki when using the `loki` mode:

```bash
# Configure in .env file
GENERATOR_MODE=loki
URL=http://loki:3100
```

When using Docker Compose, the services are connected via the Docker network.

## Data Storage

All Loki data is stored in the `./data` directory with the following structure:

- `tsdb-index`: TSDB index files
- `tsdb-cache`: TSDB cache files
- `chunks`: Log content chunks
- `wal`: Write-ahead logs for crash recovery
- `compactor`: Compaction working directory
- `rules`: Alerting rules

## Dashboard Visualization

A custom Grafana dashboard has been created specifically for Loki logs in the DBLogsComparator project. The dashboard includes:

- Distribution of logs by type
- Log volume over time
- Application logs by level
- HTTP status codes visualization
- Top hosts by error count
- Recent errors view
- Full logs view with filtering

The dashboard is automatically provisioned to Grafana when the monitoring system starts.

## Integration with Monitoring

Loki is connected to the `monitoring-network` Docker network, allowing metrics to be collected by the monitoring system. The service exposes metrics in Prometheus format at `/metrics` endpoint.

## Comparison with Other Log Storage Systems

Unlike Elasticsearch and VictoriaLogs which natively support structured data, Loki requires explicit configuration through `allow_structured_metadata: true` and schema v13 to support structured metadata. This reflects Loki's evolution from a simple log aggregator to a more full-featured log management system.

## Troubleshooting

- Check Loki logs: `docker logs loki`
- Verify Loki is healthy: `curl http://localhost:3100/ready`
- Ensure proper network connectivity between services
- If having permission issues with data directories, ensure proper ownership with `chmod -R 777 ./data`
- Verify logs are being ingested using the Loki API: `curl -s -G 'http://localhost:3100/loki/api/v1/query_range' --data-urlencode 'query={}'`
