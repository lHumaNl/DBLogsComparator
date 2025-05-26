# Loki Setup for DBLogsComparator

This directory contains the configuration for Grafana Loki, a horizontally-scalable, highly-available, multi-tenant log aggregation system.

## Components

- **Loki**: The main log aggregation service
- **Integration with Grafana**: Uses the existing Grafana instance from the monitoring system

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

## Environment Variables

Environment variables are defined in the `.env` file:

- `LOKI_VERSION`: The version of Loki to use (currently 3.5.1)
- `LOKI_PORT`: The port to expose Loki on (default: 3100)
- `STRUCTURED_METADATA`: Enable structured metadata support (default: true)

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

To view logs from Loki, use the existing Grafana instance and add Loki as a data source:

1. Open Grafana at your configured address
2. Go to Configuration > Data Sources
3. Add a new Loki data source with URL `http://loki:3100`
4. Save and test the connection

### Using Loki with the Log Generator

To send logs to Loki using the Go log generator, use the `loki` mode:

```bash
go run main.go --mode=loki --loki-url=http://localhost:3100
```

Or when using Docker:

```bash
docker run -e MODE=loki -e LOKI_URL=http://loki:3100 dblogscomparator/log-generator
```

## Structured Metadata

Loki 3.5.1 supports structured metadata, which provides several advantages:

- Store high-cardinality fields without impacting query performance
- Support for OpenTelemetry metadata format
- Better handling of fields like process_id, thread_id, and other non-label metadata
- Advanced filtering capabilities for large log volumes

To use structured metadata:

1. When sending logs, include a `metadata` field in your JSON logs
2. In Grafana's LogQL queries, use `metadata` filter operations to search and filter

## Performance Tuning

The default configuration is suitable for testing and development. For production or benchmarking:

- Adjust `limits_config` in `loki-config.yaml` for higher throughput
- Consider using a different storage backend for larger log volumes
- Configure retention and compaction settings based on your requirements

## Data Storage

All Loki data is stored in the `./data` directory with the following structure:

- `tsdb-index`: TSDB index files
- `tsdb-cache`: TSDB cache files
- `chunks`: Log content chunks
- `wal`: Write-ahead logs for crash recovery
- `compactor`: Compaction working directory
- `rules`: Alerting rules

## Integration with Monitoring

Loki is connected to the `monitoring-network` Docker network, allowing metrics to be collected by the monitoring system. The service exposes metrics in Prometheus format at `/metrics` endpoint.

## Comparison with Other Log Storage Systems

Unlike Elasticsearch and VictoriaLogs which natively support structured data, Loki requires explicit configuration through `allow_structured_metadata: true` and schema v13 to support structured metadata. This reflects Loki's evolution from a simple log aggregator to a more full-featured log management system.

## Troubleshooting

- Check Loki logs: `docker logs loki`
- Verify Loki is healthy: `curl http://localhost:3100/ready`
- Ensure proper network connectivity between services
- If having permission issues with data directories, ensure proper ownership with `chmod -R 777 ./data`
