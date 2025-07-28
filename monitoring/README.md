# Monitoring for DBLogsComparator

This directory contains the centralized monitoring infrastructure for the DBLogsComparator project. The monitoring
system provides a unified view of metrics from all log storage solutions and components.

## Components

- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards
- **Grafana Image Renderer**: Service for creating and saving dashboard snapshots
- **Telegraf**: System metrics collection agent (deployed separately on the database logging host)

## Structure

- `config/` - Configuration files for all monitoring components
    - `grafana/` - Grafana provisioning and dashboard configurations
        - `dashboards/` - Pre-configured dashboards for all systems
        - `provisioning/` - Automatic dashboard and datasource provisioning
    - `victoria-metrics/` - VictoriaMetrics scrape configuration
- `telegraf/` - Telegraf system metrics collection (must be deployed on database logging hosts)

## Startup

```bash
cd monitoring
docker-compose up -d
```

## Endpoints

- Grafana: http://localhost:3000 (default credentials: admin/admin)
- VictoriaMetrics: http://localhost:8428

## Dashboards

The monitoring system includes several pre-configured dashboards to visualize the performance and health of different
components. These dashboards are automatically provisioned in Grafana and can be found in folders corresponding to each
system (Loki, ELK, VictoriaLogs).

### Included Dashboards

1. **Log Generator Performance**:
    * **Purpose**: Monitors the `go_generator` component.
    * **Metrics**: Visualizes key performance indicators like Requests Per Second (RPS), Logs Per Second (LPS), request
      durations, retry counts, and the distribution of generated log types.
    * **Origin**: This dashboard was custom-built for the DBLogsComparator project.

2. **VictoriaLogs Overview**:
    * **Purpose**: Provides a comprehensive overview of a VictoriaLogs single-node instance.
    * **Metrics**: Shows ingestion rate, query performance, resource usage, and internal VictoriaLogs metrics.
    * **Origin**: Based on the official dashboard from VictoriaMetrics.

3. **Elasticsearch Overview**:
    * **Purpose**: Monitors the health and performance of the Elasticsearch cluster.
    * **Metrics**: Tracks document indexing rates, search performance, JVM health, and node status.
    * **Origin**: Based on a popular community dashboard for Elasticsearch.

4. **Loki Metrics**:
    * **Purpose**: Provides insights into Loki's operational metrics.
    * **Metrics**: Visualizes query performance, ingestion rates, and cache utilization.
    * **Origin**: This is a modified version of a community dashboard, adapted for the needs of this project.

## Acknowledgements

This project utilizes several excellent open-source dashboards created by the community. We are grateful to the original
authors for their work, which served as a foundation for our monitoring setup.

- **VictoriaLogs Single Node Dashboard**:
    - **Author**: VictoriaMetrics
    - **Source
      **: [grafana.com/grafana/dashboards/22084-victorialogs-single-node/](https://grafana.com/grafana/dashboards/22084-victorialogs-single-node/)

- **Elasticsearch Overview Dashboard**:
    - **Author**: GrafanaLabs
    - **Source
      **: [grafana.com/grafana/dashboards/14191-elasticsearch-overview/](https://grafana.com/grafana/dashboards/14191-elasticsearch-overview/)

- **Loki Metrics Dashboard**:
    - **Author**: frankfegert
    - **Source
      **: [grafana.com/grafana/dashboards/17781-loki-metrics-dashboard/](https://grafana.com/grafana/dashboards/17781-loki-metrics-dashboard/)

- **System Metrics for the Linux Hosts**:
    - **Author**: ulricqin
    - **Source
      **: [https://grafana.com/grafana/dashboards/15365-system-metrics-for-the-linux-hosts/](https://grafana.com/grafana/dashboards/15365-system-metrics-for-the-linux-hosts/)

- **Host System and Docker Container Metrics**:
    - **Author**: san-gg
    - **Source
      **: [https://grafana.com/grafana/dashboards/21740-host-system-and-docker-container-metrics/](https://grafana.com/grafana/dashboards/21740-host-system-and-docker-container-metrics/)

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

The system uses Grafana Image Renderer to create and save dashboard snapshots. These snapshots are stored in the Grafana
database (grafana.db) which is persisted in a volume, ensuring they aren't lost when containers are restarted.

## Network Configuration

The monitoring system creates a Docker network called `monitoring-network` which is used by all components that need to
be monitored. This network is created as external, so other services can connect to it.

## Telegraf System Metrics Collection

The `telegraf/` directory contains a separate Docker Compose configuration for collecting system and Docker metrics from the host where database logging solutions (ELK/Loki/VictoriaLogs) are deployed.

### Important Deployment Requirements

**⚠️ Critical:** Telegraf must be deployed on the same host as your database logging infrastructure to collect accurate system metrics.

### Configuration

1. **Deploy Telegraf on the database host:**
   ```bash
   cd telegraf
   docker-compose up -d
   ```

2. **Update VictoriaMetrics configuration:**
   In `config/victoria-metrics/victoria-metrics.yaml`, ensure the telegraf job points to the correct host/port where Telegraf is running:
   ```yaml
   - job_name: 'telegraf'
     static_configs:
       - targets: ['your-db-host:9273']  # Replace with actual host/port
   ```

For detailed setup instructions, see `telegraf/README.md`.

## Extending the Monitoring

To add monitoring for new components:

1. Add the component to the VictoriaMetrics scrape configuration in `config/victoria-metrics/victoria-metrics.yaml`
2. Connect the component to the `monitoring-network`
3. Ensure the component exposes Prometheus-compatible metrics

## Troubleshooting

- **Grafana can't connect to data sources**: Ensure all services are on the same Docker network and using correct
  hostnames
- **Dashboards not loading**: Check provisioning configurations and paths
- **Missing metrics**: Verify that VictoriaMetrics is successfully scraping the targets
- **Renderer issues**: Check the Grafana Image Renderer logs with `docker logs grafana-renderer`
