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

### Load Tool (`/load_tool` directory)
- **Multi-purpose Tool**: 
  - Go-based tool for log generation, querying, and benchmarking
  - Supports multiple operational modes: generator, querier, and combined
  - Configurable log generation with customizable RPS and log types
  - Query performance measurement capabilities
  - Provides Prometheus metrics for comprehensive monitoring
  - Compatible with all supported log systems (Elasticsearch/ELK, Loki, VictoriaLogs)

### Monitoring (`/monitoring` directory)
- **VictoriaMetrics**: Time-series database for metrics collection
- **Grafana**: Visualization platform with pre-configured dashboards for each log system
- **Grafana Image Renderer**: For creating and saving dashboard snapshots

## Setup and Usage

### Quick Start with Helper Scripts

DBLogsComparator provides convenient shell scripts for managing and monitoring the system:

#### start_dbsystem.sh

A master script for controlling all components of the benchmarking platform.

```bash
./start_dbsystem.sh <log_system> [options]
```

**Log Systems:**
- `monitoring` - Start only the monitoring system (VictoriaMetrics, Grafana, Grafana Renderer)
- `elk` - Start ELK Stack (Elasticsearch + Kibana) and monitoring
- `loki` - Start Loki and monitoring
- `victorialogs` - Start VictoriaLogs and monitoring
- `telegraf` - Start only Telegraf
- `all` - Only available with --down option to stop all systems

**Options:**
- `--generator` - Start the log generator after starting the log systems (native by default)
- `--querier` - Start the log querier after starting the log systems (native by default)
- `--combined` - Start the log generator and querier in combined mode (native by default)
- `--docker` - Start the log tools with Docker (instead of native)
- `--rebuild-native` - Rebuild the load_tool before starting (only for native mode)
- `--stability` - Use stability load testing mode (default)
- `--maxPerf` - Use maxPerf load testing mode
- `--telegraf` - Start Telegraf for monitoring
- `--ignore_docker` - Skip docker.sock permission check for Telegraf
- `--no-monitoring` - Don't start or check monitoring system (ignored with 'monitoring' log system)
- `--down` - Stop the specified log system and its containers
- `--stop-load` - Stop only the log load tool (doesn't affect other systems)
- `--help` - Show help information

**Examples:**
```bash
./start_dbsystem.sh monitoring                    # Start only monitoring
./start_dbsystem.sh elk                           # Start ELK Stack with monitoring
./start_dbsystem.sh loki --generator              # Start Loki, monitoring, and native log generator
./start_dbsystem.sh loki --generator --docker     # Start Loki, monitoring, and Docker log generator
./start_dbsystem.sh elk --querier                 # Start ELK Stack with native log querier
./start_dbsystem.sh loki --generator --maxPerf    # Start Loki with generator in maxPerf mode
./start_dbsystem.sh victorialogs --combined       # Start VictoriaLogs with combined generator and querier (native)
./start_dbsystem.sh loki --querier --rebuild-native # Start Loki with querier and rebuild
./start_dbsystem.sh victorialogs --no-monitoring  # Start VictoriaLogs without monitoring
./start_dbsystem.sh telegraf                      # Start only Telegraf
./start_dbsystem.sh monitoring --telegraf         # Start monitoring and Telegraf
./start_dbsystem.sh telegraf --ignore_docker      # Start Telegraf, skip docker.sock check
./start_dbsystem.sh --stop-load                   # Stop only the log load tool
./start_dbsystem.sh elk --down                    # Stop ELK Stack but leave monitoring running
./start_dbsystem.sh telegraf --down               # Stop Telegraf
./start_dbsystem.sh all --down                    # Stop all systems
```

#### check_health.sh

A comprehensive script for checking the health and operational status of all components in the system.

```bash
./check_health.sh [options]
```

**Options:**
- `--all` - Check all systems (default if no options specified)
- `--monitoring` - Check only the monitoring system
- `--elk` - Check only the ELK Stack
- `--loki` - Check only Loki
- `--victorialogs` - Check only VictoriaLogs
- `--generator` - Check only the log generator
- `--telegraf` - Check only Telegraf
- `--help` - Show help information

**Features:**
- Container status verification
- HTTP endpoint availability testing
- Metrics export validation
- Detailed status reporting with color-coded output
- Automatic retry mechanism for transient failures

**Example:**
```bash
./check_health.sh --all          # Check all systems
./check_health.sh --elk          # Check only ELK Stack components
```

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM for running all components simultaneously

### Manual Setup
If you prefer to start components manually instead of using the helper scripts:

1. Clone the repository:
   ```
   git clone https://github.com/lHumaNl/DBLogsComparator.git DBLogsComparator
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
   cd ../load_tool
   docker-compose up -d
   ```

### Configuration
Each component can be configured via their respective `.env` files:
- VictoriaLogs: `victorialogs/.env`
- Loki: `loki/.env`
- Elasticsearch: `elk/.env`
- Load Tool: `load_tool/config.yaml` and `load_tool/db_hosts.yaml`

## Usage

### Accessing Dashboards
- Grafana: http://localhost:3000 (default credentials: admin/admin)
- VictoriaLogs UI: http://localhost:9428/vmui/
- Kibana: http://localhost:5601
- Loki API: http://localhost:3100

### Monitoring Log Generator Performance
The Load Tool exposes Prometheus metrics on port 8080, which are collected by VictoriaMetrics. These metrics include:
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

## Key Features

- **Centralized Monitoring**: All components are monitored through a unified Grafana/VictoriaMetrics setup
- **Realistic Log Generation**: Configurable Go-based log generator with controllable RPS and log types
- **Performance Metrics**: Comprehensive metrics collection for detailed analysis
- **Dashboard Snapshots**: Ability to save dashboard states using the Grafana image renderer
- **Resource Controls**: Docker Compose configurations with resource limits for fair comparison
- **Log Visualization**: Custom Grafana dashboards for each log management system

## Log Generation and Querying Tool (load_tool)

The `load_tool` is a versatile Go application that can generate synthetic logs and query log systems to perform benchmarking.

### Operational Modes

- **Generator Mode**: Generates synthetic logs and sends them to the specified log system
- **Querier Mode**: Queries logs from the log system to measure query performance
- **Combined Mode**: Runs both generator and querier simultaneously to test under load

### Supported Log Systems

- **Elasticsearch**: Via the Elasticsearch API (compatible with all common versions)
- **Loki**: Via the Loki API (supports both pre-3.5.1 and 3.5.1+ API paths)
- **VictoriaLogs**: Via the VictoriaLogs HTTP API

### Configuration

The tool can be configured via:

1. **YAML Configuration**: `config.yaml` for general settings and `db_hosts.yaml` for endpoints
2. **Command Line Flags**:
   ```
   -config string
        Path to configuration file YAML (default "config.yaml")
   -hosts string
        Path to hosts file with log system URLs (default "db_hosts.yaml")
   -system string
        Log system type: loki, elasticsearch, victoria
   -mode string
        Operation mode: generator, querier, combined (default "generator")
   -metrics-port int
        Prometheus metrics port (default 9090)
   ```

### Docker Usage

The tool can be run in Docker with automatic rebuilding when code changes:

```bash
# Build and run in Docker
cd load_tool
docker-compose build
SYSTEM=loki MODE=generator docker-compose up -d

# View logs
docker logs go-log-generator

# Stop the tool
docker-compose down
```

### Native Usage

For development or testing, the tool can be run natively:

```bash
cd load_tool
go build -o load_tool
./load_tool -system loki -mode generator
```

## Benchmarking Methodology