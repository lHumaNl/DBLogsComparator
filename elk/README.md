# Elasticsearch for DBLogsComparator

This directory contains the configuration for Elasticsearch and Kibana for the DBLogsComparator project.

## Components

- **Elasticsearch**: Primary log storage and search engine
- **Kibana**: Data visualization and exploration tool

## Configuration

Configuration is done through the `.env` file, where you can change:
- Elasticsearch and Kibana version
- Memory allocation
- Access ports

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
   cd shared/go_generator
   GENERATOR_MODE=es GENERATOR_URL=http://localhost:9200 docker-compose up -d
   ```

## Endpoints

- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601

## Initial Kibana Setup

After the first launch of Kibana:

1. Navigate to http://localhost:5601
2. In "Stack Management" -> "Index Patterns", create an index pattern `logs-*`
3. Select `timestamp` as the Time Field

## Performance Monitoring

Elasticsearch performance monitoring is done through the project's centralized monitoring system (Grafana + VictoriaMetrics).
