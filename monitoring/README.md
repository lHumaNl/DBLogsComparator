# Monitoring for DBLogsComparator

This directory contains the centralized monitoring infrastructure for the DBLogsComparator project. The monitoring system provides a unified view of metrics from all log storage solutions and components.

## Components

- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards
- **Grafana Image Renderer**: Service for creating and saving dashboard snapshots

## Structure

- `config/` - Configuration files for all monitoring components
  - `grafana/` - Grafana provisioning and dashboard configurations
  - `victoria-metrics/` - VictoriaMetrics scrape configuration

## Startup

```bash
cd monitoring
docker-compose up -d
```

## Endpoints

- Grafana: http://localhost:3000 (default credentials: admin/admin)
- VictoriaMetrics: http://localhost:8428

## Dashboards

The monitoring system includes pre-configured dashboards for:

1. **Log Generator Performance**:
   - Requests per second (RPS)
   - Logs per second (LPS)
   - Request durations
   - Retry counts
   - Log type distribution

2. **Log Storage System Comparison**:
   - Ingestion rates
   - Query performance
   - Resource usage (CPU, memory)
   - Storage efficiency

## Dashboard Snapshots

The system uses Grafana Image Renderer to create and save dashboard snapshots. These snapshots are stored in the Grafana database (grafana.db) which is persisted in a volume, ensuring they aren't lost when containers are restarted.

## Network Configuration

The monitoring system creates a Docker network called `monitoring-network` which is used by all components that need to be monitored. This network is created as external, so other services can connect to it.

## Extending the Monitoring

To add monitoring for new components:

1. Add the component to the VictoriaMetrics scrape configuration in `config/victoria-metrics/victoria-metrics.yaml`
2. Connect the component to the `monitoring-network`
3. Ensure the component exposes Prometheus-compatible metrics
