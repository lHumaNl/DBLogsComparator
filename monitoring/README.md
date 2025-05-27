# Monitoring for DBLogsComparator

This directory contains the centralized monitoring infrastructure for the DBLogsComparator project. The monitoring system provides a unified view of metrics from all log storage solutions and components.

## Components

- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards
- **Grafana Image Renderer**: Service for creating and saving dashboard snapshots

## Structure

- `config/` - Configuration files for all monitoring components
  - `grafana/` - Grafana provisioning and dashboard configurations
    - `dashboards/` - Pre-configured dashboards for all systems
    - `provisioning/` - Automatic dashboard and datasource provisioning
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

3. **Loki Logs Dashboard**:
   - Distribution of logs by type
   - Log volume over time
   - Application logs by level
   - HTTP status codes visualization
   - Top hosts by error count
   - Recent errors view
   - Full logs view with filtering

## Dashboard Access

Access dashboards through Grafana:
1. Open http://localhost:3000
2. Login with the default credentials (admin/admin) or configured credentials
3. Navigate to Dashboards
4. Select the dashboard you want to view from the Loki, ELK, or VictoriaLogs folders

## Dashboard Provisioning

The dashboards are automatically provisioned to Grafana through the provisioning system. New dashboards can be added by:

1. Creating a JSON dashboard definition in the appropriate folder under `config/grafana/dashboards/`
2. Adding a provisioning configuration in `config/grafana/provisioning/dashboards/`

## Dashboard Snapshots

The system uses Grafana Image Renderer to create and save dashboard snapshots. These snapshots are stored in the Grafana database (grafana.db) which is persisted in a volume, ensuring they aren't lost when containers are restarted.

## Network Configuration

The monitoring system creates a Docker network called `monitoring-network` which is used by all components that need to be monitored. This network is created as external, so other services can connect to it.

## Extending the Monitoring

To add monitoring for new components:

1. Add the component to the VictoriaMetrics scrape configuration in `config/victoria-metrics/victoria-metrics.yaml`
2. Connect the component to the `monitoring-network`
3. Ensure the component exposes Prometheus-compatible metrics

## Troubleshooting

- **Grafana can't connect to data sources**: Ensure all services are on the same Docker network and using correct hostnames
- **Dashboards not loading**: Check provisioning configurations and paths
- **Missing metrics**: Verify that VictoriaMetrics is successfully scraping the targets
- **Renderer issues**: Check the Grafana Image Renderer logs with `docker logs grafana-renderer`
