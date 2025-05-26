# DBLogsComparator

A comprehensive benchmarking platform for comparing log storage and analysis solutions including Elasticsearch (ELK Stack), Loki, and VictoriaLogs. This project evaluates each solution in terms of performance, resource utilization, and query capabilities.

## Project Overview

DBLogsComparator allows you to:
- Compare different log management systems under similar conditions
- Generate realistic log data for testing using the Go log generator
- Monitor resource usage and performance metrics using Grafana and VictoriaMetrics
- Perform automated benchmarking and comparison reporting

## Architecture

The project consists of the following components:

### Log Management Systems
- **ELK Stack** (`/elk` directory): Elasticsearch, Kibana
- **Loki Stack** (`/loki` directory): Loki
- **VictoriaLogs** (`/victorialogs` directory): VictoriaLogs

### Shared Components (`/shared` directory)
- **Log Generator**: 
  - Go generator for synthetic logs with customizable RPS (requests per second) and log types

### Monitoring (`/monitoring` directory)
- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards
- **Grafana Image Renderer**: For creating and saving dashboard snapshots

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

4. Start the log generator:
   ```
   cd ../shared/go_generator
   docker-compose up -d
   ```

### Configuration
Each component can be configured via their respective `.env` files:
- VictoriaLogs: `victorialogs/.env`
- Go Generator: `shared/go_generator/.env`

## Usage

### Accessing Dashboards
- Grafana: http://localhost:3000 (default credentials: admin/admin)
- VictoriaLogs UI: http://localhost:9428/vmui/

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

## Key Features

- **Centralized Monitoring**: All components are monitored through a unified Grafana/VictoriaMetrics setup
- **Realistic Log Generation**: Configurable Go-based log generator with controllable RPS and log types
- **Performance Metrics**: Comprehensive metrics collection for detailed analysis
- **Dashboard Snapshots**: Ability to save dashboard states using the Grafana image renderer
- **Resource Controls**: Docker Compose configurations with resource limits for fair comparison