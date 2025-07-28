# Telegraf System Metrics Collection

This directory contains the Telegraf configuration for collecting system and Docker metrics from the host where database
logging solutions (ELK/Loki/VictoriaLogs) are deployed.

## Overview

Telegraf is a plugin-driven server agent for collecting and sending metrics and events from databases, systems, and IoT
sensors. In this setup, it collects:

- **System metrics**: CPU, memory, disk, network, processes
- **Docker metrics**: Container statistics, resource usage
- **Host metrics**: Kernel, swap, filesystem information

The collected metrics are exposed in Prometheus format and scraped by VictoriaMetrics for visualization in Grafana.

## Architecture

```
Database Host (where logging DBs run)
├── Telegraf Container
│   ├── Collects system metrics
│   ├── Collects Docker metrics
│   └── Exposes metrics on port 9273
│
└── Monitoring Host
    └── VictoriaMetrics scrapes metrics from Telegraf
```

## Prerequisites

- Docker and Docker Compose installed
- Access to Docker daemon socket
- Network connectivity between Telegraf host and VictoriaMetrics

## Configuration Files

### docker-compose.yml

- Defines the Telegraf service
- Mounts Docker socket and system directories
- Configures user permissions for Docker access

### config/telegraf.conf

- Main Telegraf configuration
- Defines input plugins for system and Docker metrics
- Configures Prometheus output format

### .env

- Environment variables for configuration
- **Contains critical user ID setting for Docker access**

## Installation and Setup

### Step 1: Configure Environment Variables

**⚠️ CRITICAL:** Before starting Telegraf, you must set the correct `PRIVILEGED_USER_ID` in the `.env` file.

```bash
# Edit .env file
nano .env
```

Update the `PRIVILEGED_USER_ID` variable with the user ID that has access to Docker socket:

```bash
# Find the group ID of docker group
getent group docker

# Example output: docker:x:127:user1,user2
# In this case, set PRIVILEGED_USER_ID=127
PRIVILEGED_USER_ID=127
```

**Why this is important:**

- The docker-compose.yml contains: `user: "telegraf:$PRIVILEGED_USER_ID"`
- If this ID is incorrect, Telegraf won't be able to access Docker metrics
- Docker metrics will not be collected if permissions are wrong

### Step 2: Deploy Telegraf

```bash
# Navigate to telegraf directory
cd telegraf

# Start Telegraf service
docker-compose up -d

# Verify container is running
docker-compose ps

# Check logs for any errors
docker-compose logs telegraf
```

### Step 3: Verify Metrics Collection

```bash
# Check if metrics endpoint is accessible
curl http://localhost:9273/metrics

# You should see Prometheus-formatted metrics including:
# - cpu_* metrics (CPU usage)
# - mem_* metrics (Memory usage)
# - disk_* metrics (Disk usage)
# - docker_* metrics (Docker container stats)
```

### Step 4: Update VictoriaMetrics Configuration

On the monitoring host, update the VictoriaMetrics scrape configuration to point to your Telegraf instance:

```yaml
# In config/victoria-metrics/victoria-metrics.yaml
- job_name: 'telegraf'
  static_configs:
    - targets: [ 'your-database-host:9273' ]  # Replace with actual host
      labels:
        instance: 'os-metrics'
        service: 'telegraf'
        environment: 'development'
  metrics_path: '/metrics'
```

## Environment Variables

| Variable              | Default              | Description                                    |
|-----------------------|----------------------|------------------------------------------------|
| `PROMETHEUS_PORT`     | 9273                 | Port for Prometheus metrics endpoint           |
| `PROMETHEUS_LISTEN`   | 0.0.0.0              | Listen address for metrics endpoint            |
| `COLLECTION_INTERVAL` | 30s                  | How often to collect metrics                   |
| `FLUSH_INTERVAL`      | 30s                  | How often to flush metrics                     |
| `DOCKER_SOCKET`       | /var/run/docker.sock | Path to Docker socket                          |
| `PRIVILEGED_USER_ID`  | 127                  | **CRITICAL**: User/Group ID with Docker access |

## Collected Metrics

### System Metrics

- **CPU**: Usage per core and total, time spent in different states
- **Memory**: Available, used, cached, buffered memory
- **Disk**: Usage, free space, I/O statistics
- **Network**: Bytes sent/received, packets, errors
- **Processes**: Count, states, resource usage
- **System**: Load average, uptime, users

### Docker Metrics

- **Containers**: CPU usage, memory usage, network I/O
- **Images**: Count, size information
- **Engine**: Version information, statistics
- **Volumes**: Usage statistics

## Troubleshooting

### Container won't start

```bash
# Check logs
docker-compose logs telegraf

# Common issues:
# 1. Docker socket permission denied
# 2. Invalid PRIVILEGED_USER_ID
# 3. Port conflicts
```

### No Docker metrics

```bash
# Verify Docker socket access
docker exec telegraf ls -la /var/run/docker.sock

# Check user permissions
docker exec telegraf id

# Verify PRIVILEGED_USER_ID matches docker group
getent group docker
```

### Metrics endpoint not accessible

```bash
# Check if port is bound
netstat -tlnp | grep 9273

# Test from inside container
docker exec telegraf curl localhost:9273/metrics

# Check firewall rules
sudo ufw status
```

### VictoriaMetrics can't scrape

```bash
# Test connectivity from monitoring host
curl http://database-host:9273/metrics

# Check VictoriaMetrics logs
docker logs victoriametrics

# Verify target configuration in victoria-metrics.yaml
```

## Security Considerations

- Telegraf runs with access to Docker socket (security risk)
- Metrics may contain sensitive system information
- Ensure proper network isolation between hosts
- Consider using TLS for metrics transport in production

## Performance Tuning

### Reduce Resource Usage

```bash
# Increase collection intervals in .env
COLLECTION_INTERVAL=60s
FLUSH_INTERVAL=60s
```

### Optimize Docker Plugin

```bash
# In telegraf.conf, exclude unnecessary containers
container_name_exclude = ["telegraf", "monitoring-*"]
```

## Advanced Configuration

### Custom Hostname

Edit `telegraf.conf`:

```toml
[agent]
    hostname = "production-db-host"
```

## Monitoring Best Practices

1. **Deploy on target hosts**: Always run Telegraf on hosts you want to monitor
2. **Set correct permissions**: Ensure PRIVILEGED_USER_ID is correct for Docker metrics
3. **Monitor Telegraf itself**: Check Telegraf container health and logs
4. **Network connectivity**: Verify VictoriaMetrics can reach Telegraf endpoints
5. **Resource limits**: Set appropriate CPU/memory limits in docker-compose.yml

## Integration with Grafana

The collected metrics work with these pre-configured dashboards:

- **System Metrics for Linux Hosts**: OS-level metrics visualization
- **Host System and Docker Container Metrics**: Combined host and container view

Access dashboards at: http://monitoring-host:3000

## Support

For issues:

1. Check container logs: `docker-compose logs telegraf`
2. Verify configuration: `docker exec telegraf telegraf config`
3. Test connectivity: `curl http://localhost:9273/metrics`
4. Review VictoriaMetrics scraping: Check VictoriaMetrics UI

## Version Information

- **Telegraf**: 1.28-alpine
- **Configuration format**: TOML
- **Output format**: Prometheus metrics
- **Docker socket**: v1.41+ API