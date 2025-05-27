# DBLogsComparator

A comprehensive benchmarking platform for comparing log storage and analysis solutions including Elasticsearch (ELK Stack), Loki, and VictoriaLogs. This project evaluates each solution in terms of performance, resource utilization, and query capabilities.

## Project Overview

DBLogsComparator allows you to:
- Compare different log management systems under similar conditions
- Generate realistic log data for testing using the Go log generator
- Monitor resource usage and performance metrics using Grafana and VictoriaMetrics
- Perform automated benchmarking and comparison reporting
- Visualize logs and metrics using pre-configured Grafana dashboards

## Architecture

The project consists of the following components:

### Log Management Systems
- **ELK Stack** (`/elk` directory): Elasticsearch, Kibana
- **Loki Stack** (`/loki` directory): Loki with TSDB storage and structured metadata support
- **VictoriaLogs** (`/victorialogs` directory): VictoriaLogs log storage system

### Shared Components (`/shared` directory)
- **Log Generator**: 
  - Go generator for synthetic logs with customizable RPS (requests per second) and log types
  - Supports multiple log formats and transport protocols
  - Provides Prometheus metrics for performance monitoring

### Monitoring (`/monitoring` directory)
- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards for each log system
- **Grafana Image Renderer**: For creating and saving dashboard snapshots

## Resource Controls

All three logging systems have comparable resource constraints:

1. **VictoriaLogs**:
   - Uses Docker resource limits (cpus, memory)
   - Configuration through docker-compose.yml

2. **Elasticsearch**:
   - CPU limitation through the processors parameter
   - Memory limitation through ES_JAVA_OPTS (Xms, Xmx)
   - Disk space limitation through watermark settings

3. **Loki**:
   - CPU limitations through max_concurrent_queries and query_parallelism
   - Memory limitations through max_streams and max_chunks_per_query
   - Disk space limitation through max_disk_size and retention_period

This ensures fair comparison between the systems under test.

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM for running all components simultaneously

### Quick Start
1. Clone the repository:
   ```
   git clone https://github.com/yourusername/DBLogsComparator.git
   cd DBLogsComparator
   ```

2. Start the monitoring infrastructure:
   ```
   cd monitoring
   docker-compose up -d
   ```

3. Start VictoriaLogs:
   ```
   cd ../victorialogs
   docker-compose up -d
   ```

4. Start Loki:
   ```
   cd ../loki
   docker-compose up -d
   ```

5. Start Elasticsearch and Kibana:
   ```
   cd ../elk
   docker-compose up -d
   ```

6. Start the log generator:
   ```
   cd ../shared/go_generator
   docker-compose up -d
   ```

### Configuration
Each component can be configured via their respective `.env` files:
- VictoriaLogs: `victorialogs/.env`
- Loki: `loki/.env`
- Elasticsearch: `elk/.env`
- Go Generator: `shared/go_generator/.env`

## Usage

### Accessing Dashboards
- Grafana: http://localhost:3000 (default credentials: admin/admin)
- VictoriaLogs UI: http://localhost:9428/vmui/
- Kibana: http://localhost:5601
- Loki API: http://localhost:3100

### Monitoring Log Generator Performance
The Go log generator exposes Prometheus metrics on port 8080, which are collected by VictoriaMetrics. These metrics include:
- Requests per second (RPS)
- Logs per second (LPS)
- Request durations
- Retry counts
- Log type distribution

### Comparing Log Systems
Pre-configured Grafana dashboards allow you to compare:
- Ingestion rates
- Query performance
- Resource usage (CPU, memory)
- Storage efficiency
- Log visualization and exploration

## Key Features

- **Centralized Monitoring**: All components are monitored through a unified Grafana/VictoriaMetrics setup
- **Realistic Log Generation**: Configurable Go-based log generator with controllable RPS and log types
- **Performance Metrics**: Comprehensive metrics collection for detailed analysis
- **Dashboard Snapshots**: Ability to save dashboard states using the Grafana image renderer
- **Resource Controls**: Docker Compose configurations with resource limits for fair comparison
- **Log Visualization**: Custom Grafana dashboards for each log management system