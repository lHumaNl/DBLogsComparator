# Elasticsearch for DBLogsComparator

This directory contains the configuration for Elasticsearch and Kibana for the DBLogsComparator project.

## Components

- **Elasticsearch**: Primary log storage and search engine
- **Kibana**: Data visualization and exploration tool

## Configuration

Configuration is done through the `.env` file, where you can change:
- Elasticsearch and Kibana version
- Memory allocation for Elasticsearch (via ES_JAVA_OPTS)
- CPU allocation (via processors setting)
- Storage limits (via disk watermarks)
- Access ports

## Resource Controls

Elasticsearch is configured with the following resource constraints:

- **CPU Limitations**: 
  - `processors` parameter limits the number of CPU cores used by Elasticsearch

- **Memory Limitations**:
  - `ES_JAVA_OPTS`: Controls Java heap size (Xms and Xmx settings)
  - Docker memory limits

- **Disk Space Limitations**:
  - `cluster.routing.allocation.disk.watermark.low`: Low disk watermark
  - `cluster.routing.allocation.disk.watermark.high`: High disk watermark
  - `cluster.routing.allocation.disk.watermark.flood_stage`: Critical disk watermark

These resource controls ensure fair comparison with other log management systems in the DBLogsComparator project.

## Startup

```bash
cd elk
docker-compose up -d
```

## Using with Go Log Generator

To send logs to Elasticsearch using the Go log generator:

1. Start Elasticsearch and Kibana:
   ```bash
   cd elk
   docker-compose up -d
   ```

2. Run the Go generator in `es` mode:
   ```bash
   # Configure in .env file
   GENERATOR_MODE=es
   URL=http://elasticsearch:9200
   ```

   When using Docker Compose, the services are connected via the Docker network.

## Endpoints

- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601

## Log Structure

The logs sent to Elasticsearch are structured with the following key fields:

- `timestamp`: ISO-8601 formatted timestamp
- `log_type`: Type of log (web_access, web_error, application, metric, event)
- `message`: The log message content
- `level`: For application logs, indicates severity (debug, info, warn, error, fatal)
- `host`: Source host name
- `container_name`: Container identifier
- `service_name`: Service identifier

Additional fields are included based on the log type (HTTP status, response time, request path, etc.)

## Initial Kibana Setup

After the first launch of Kibana:

1. Navigate to http://localhost:5601
2. In "Stack Management" -> "Index Patterns", create an index pattern `logs-*`
3. Select `timestamp` as the Time Field
4. Create visualizations based on log fields like log_type, level, etc.

## Integration with Monitoring

Elasticsearch is connected to the `monitoring-network` Docker network, allowing metrics to be collected by the monitoring system. These metrics are visualized in the centralized Grafana dashboards.

## Performance Monitoring

Elasticsearch performance monitoring is done through the project's centralized monitoring system (Grafana + VictoriaMetrics). Key metrics include:

- Indexing rate
- Query performance
- Resource usage (CPU, memory, disk)
- JVM metrics
- Cache hit ratios

## Troubleshooting

- Check Elasticsearch logs: `docker logs elasticsearch`
- Verify cluster health: `curl http://localhost:9200/_cluster/health`
- Check index status: `curl http://localhost:9200/_cat/indices`
- If Elasticsearch fails to start, check for resource constraints (memory limits, vm.max_map_count, etc.)
