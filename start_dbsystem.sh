#!/bin/bash

set -e

# Help function
show_help() {
    echo "Usage: $0 <log_system> [options]"
    echo ""
    echo "Log systems:"
    echo "  monitoring  - Start only the monitoring system (VictoriaMetrics, Grafana, Grafana Renderer)"
    echo "  elk         - Start ELK Stack (Elasticsearch + Kibana) and monitoring"
    echo "  loki        - Start Loki and monitoring"
    echo "  victorialogs - Start VictoriaLogs and monitoring"
    echo "  all         - Only available with --down option to stop all systems"
    echo ""
    echo "Options:"
    echo "  --generator     - Start the log generator after starting the log systems"
    echo "  --no-monitoring - Don't start or check monitoring system (ignored with 'monitoring' log system)"
    echo "  --down          - Stop the specified log system and its containers"
    echo "  --help          - Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 monitoring                # Start only monitoring (VictoriaMetrics, Grafana)"
    echo "  $0 elk                       # Start ELK Stack and monitoring"
    echo "  $0 loki --generator          # Start Loki, monitoring, and log generator"
    echo "  $0 victorialogs --no-monitoring # Start VictoriaLogs without monitoring"
    echo "  $0 elk --down                # Stop ELK Stack but leave monitoring running"
    echo "  $0 monitoring --down         # Stop the monitoring system"
    echo "  $0 all --down                # Stop all systems"
    exit 0
}

# Function to read environment variables from a file
read_env_file() {
    local env_file=$1
    if [ -f "$env_file" ]; then
        # Export all variables from the env file
        set -a
        source "$env_file"
        set +a
    else
        echo "Warning: Environment file $env_file not found"
    fi
}

# Function to start monitoring
start_monitoring() {
    echo "Starting monitoring system..."
    
    # Read monitoring environment variables
    cd monitoring
    read_env_file ".env"
    cd - > /dev/null
    
    # Start the monitoring stack (Grafana, VictoriaMetrics)
    cd monitoring
    docker-compose up -d
    cd - > /dev/null
    
    # Check that monitoring components are healthy
    echo "Verifying monitoring system status..."
    ./check_health.sh --monitoring
    
    if [ $? -eq 0 ]; then
        echo "Monitoring system started successfully!"
    else
        echo "Warning: Some monitoring components may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs victoria-metrics"
        echo "  docker logs grafana"
    fi
}

# Function to stop monitoring
stop_monitoring() {
    echo "Stopping monitoring system..."
    
    # Read monitoring environment variables
    cd monitoring
    read_env_file ".env"
    cd - > /dev/null
    
    # Stop the monitoring stack
    cd monitoring
    docker-compose down
    cd - > /dev/null
    
    echo "Monitoring system stopped successfully!"
}

# Function to start ELK Stack
start_elk() {
    echo "Starting ELK Stack..."
    
    # Read ELK environment variables
    cd elk
    read_env_file ".env"
    cd - > /dev/null
    
    # Start the ELK stack
    cd elk
    docker-compose up -d
    cd - > /dev/null
    
    # Check that ELK components are healthy
    echo "Verifying ELK Stack status..."
    ./check_health.sh --elk
    
    if [ $? -eq 0 ]; then
        echo "ELK Stack started successfully!"
    else
        echo "Warning: Some ELK components may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs elasticsearch"
        echo "  docker logs kibana"
    fi
}

# Function to stop ELK Stack
stop_elk() {
    echo "Stopping ELK Stack..."
    
    # Read ELK environment variables
    cd elk
    read_env_file ".env"
    cd - > /dev/null
    
    # Stop the ELK stack
    cd elk
    docker-compose down
    cd - > /dev/null
    
    echo "ELK Stack stopped successfully!"
}

# Function to start Loki
start_loki() {
    echo "Starting Loki..."
    
    # Read Loki environment variables
    cd loki
    read_env_file ".env"
    cd - > /dev/null
    
    # Start Loki
    cd loki
    docker-compose up -d
    cd - > /dev/null
    
    # Check that Loki is healthy
    echo "Verifying Loki status..."
    ./check_health.sh --loki
    
    if [ $? -eq 0 ]; then
        echo "Loki started successfully!"
    else
        echo "Warning: Loki may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs loki"
    fi
}

# Function to stop Loki
stop_loki() {
    echo "Stopping Loki..."
    
    # Read Loki environment variables
    cd loki
    read_env_file ".env"
    cd - > /dev/null
    
    # Stop Loki
    cd loki
    docker-compose down
    cd - > /dev/null
    
    echo "Loki stopped successfully!"
}

# Function to start VictoriaLogs
start_victorialogs() {
    echo "Starting VictoriaLogs..."
    
    # Read VictoriaLogs environment variables
    cd victorialogs
    read_env_file ".env"
    cd - > /dev/null
    
    # Start VictoriaLogs
    cd victorialogs
    docker-compose up -d
    cd - > /dev/null
    
    # Check that VictoriaLogs is healthy
    echo "Verifying VictoriaLogs status..."
    ./check_health.sh --victorialogs
    
    if [ $? -eq 0 ]; then
        echo "VictoriaLogs started successfully!"
    else
        echo "Warning: VictoriaLogs may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs victorialogs"
    fi
}

# Function to stop VictoriaLogs
stop_victorialogs() {
    echo "Stopping VictoriaLogs..."
    
    # Read VictoriaLogs environment variables
    cd victorialogs
    read_env_file ".env"
    cd - > /dev/null
    
    # Stop VictoriaLogs
    cd victorialogs
    docker-compose down
    cd - > /dev/null
    
    echo "VictoriaLogs stopped successfully!"
}

# Function to stop log generator
stop_generator() {
    echo "Stopping Log Generator..."
    
    # Stop the generator
    cd load_tool/go_generator
    if [ -f "docker-compose.yml" ]; then
        docker-compose down
        echo "Log Generator stopped successfully!"
    else
        echo "Warning: docker-compose.yml not found in shared/go_generator directory"
    fi
    cd - > /dev/null
}

# Function to start log generator
start_generator() {
    echo "Starting Log Generator..."
    
    # Read generator environment variables
    cd load_tool/go_generator
    read_env_file ".env"
    cd - > /dev/null
    
    # Check if we need to specify the destination
    if [ -z "$DB_SYSTEM" ] || [ "$DB_SYSTEM" = "monitoring" ]; then
        echo "Error: Cannot start log generator without a specific log system target."
        echo "Please specify elk, loki, or victorialogs."
        exit 1
    fi

    # Set appropriate environment variables for the log generator based on DB_SYSTEM
    export GENERATOR_TARGET=$DB_SYSTEM
    
    # Start the log generator
    cd load_tool/go_generator
    if [ -f "docker-compose.yml" ]; then
        docker-compose up -d
        echo "Log Generator started successfully!"
    else
        echo "Warning: docker-compose.yml not found in shared/go_generator directory"
    fi
    cd - > /dev/null
    
    # Check that generator is healthy
    echo "Verifying Log Generator status..."
    ./check_health.sh --generator
    
    if [ $? -eq 0 ]; then
        echo "Log Generator started successfully!"
    else
        echo "Warning: Log Generator may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs go-generator"
    fi
}

# Function to stop all systems
stop_all() {
    echo "Stopping all systems..."
    
    # Stop log generator if it's running
    if docker ps --format '{{.Names}}' | grep -q "^go-generator$"; then
        stop_generator
    fi
    
    # Stop all logging systems
    if [ -d "elk" ] && [ -f "elk/docker-compose.yml" ]; then
        stop_elk
    fi
    
    if [ -d "loki" ] && [ -f "loki/docker-compose.yml" ]; then
        stop_loki
    fi
    
    if [ -d "victorialogs" ] && [ -f "victorialogs/docker-compose.yml" ]; then
        stop_victorialogs
    fi
    
    # Finally stop monitoring
    if [ -d "monitoring" ] && [ -f "monitoring/docker-compose.yml" ]; then
        stop_monitoring
    fi
    
    echo "All systems stopped successfully!"
}

# Check for arguments
if [ $# -lt 1 ]; then
    show_help
fi

# Parse arguments
DB_SYSTEM=""
LAUNCH_GENERATOR=false
NO_MONITORING=false
BRING_DOWN=false

for arg in "$@"; do
    case $arg in
        --generator )    LAUNCH_GENERATOR=true
                         ;;
        --no-monitoring ) NO_MONITORING=true
                         ;;
        --down )        BRING_DOWN=true
                         ;;
        --help )        show_help
                         ;;
        * )             if [ -z "$DB_SYSTEM" ]; then
                            DB_SYSTEM="$arg"
                        else
                            echo "Unknown argument: $arg"
                            show_help
                        fi
                         ;;
    esac
done

if [ -z "$DB_SYSTEM" ]; then
    echo "Error: You must specify a log system."
    show_help
fi

# Handle the 'all' system (valid only with --down flag)
if [[ "$DB_SYSTEM" == "all" ]]; then
    if [[ "$BRING_DOWN" == "true" ]]; then
        stop_all
        exit 0
    else
        echo "Error: The 'all' option is only available with the --down flag"
        show_help
        exit 1
    fi
fi

# If --down flag is set, stop the specified system and exit
if $BRING_DOWN; then
    case $DB_SYSTEM in
        monitoring )    stop_monitoring
                        ;;
        elk )           stop_elk
                        ;;
        loki )          stop_loki
                        ;;
        victorialogs )  stop_victorialogs
                        ;;
        * )             echo "Unknown system: $DB_SYSTEM"
                        show_help
                        ;;
    esac
    
    # Also stop the generator if it's the specific DB system
    if [ "$DB_SYSTEM" != "monitoring" ]; then
        # Check if generator is running
        if docker ps --format '{{.Names}}' | grep -q "^go-generator$"; then
            echo "Detected log generator running, stopping it..."
            stop_generator
        fi
    fi
    
    echo "System $DB_SYSTEM has been stopped!"
    exit 0
fi

# Start selected systems based on the specified argument
case $DB_SYSTEM in
    monitoring )    start_monitoring
                    ;;
    elk )           if ! $NO_MONITORING; then
                        start_monitoring
                    fi
                    start_elk
                    ;;
    loki )          if ! $NO_MONITORING; then
                        start_monitoring
                    fi
                    start_loki
                    ;;
    victorialogs )  if ! $NO_MONITORING; then
                        start_monitoring
                    fi
                    start_victorialogs
                    ;;
    * )             echo "Unknown system: $DB_SYSTEM"
                    show_help
                    ;;
esac

# Start log generator if flag is specified
if $LAUNCH_GENERATOR; then
    start_generator
fi

echo "All systems started successfully!"

# Output information about availability of monitoring
if ! $NO_MONITORING || [ "$DB_SYSTEM" = "monitoring" ]; then
    echo "Monitoring is available at:"
    echo "  - Grafana: http://localhost:${GRAFANA_PORT} (${GRAFANA_ADMIN_USER}/${GRAFANA_ADMIN_PASSWORD})"
    echo "  - VictoriaMetrics: http://localhost:${VICTORIA_METRICS_PORT}/vmui/"
fi

# Output information about availability of other systems
case $DB_SYSTEM in
    elk )     echo "ELK Stack is available at:"
              echo "  - Elasticsearch: http://localhost:${ES_PORT}"
              echo "  - Kibana: http://localhost:${KIBANA_PORT}"
              ;;
esac

case $DB_SYSTEM in
    loki )    echo "Loki is available at: http://localhost:${LOKI_PORT}"
              ;;
esac

case $DB_SYSTEM in
    victorialogs ) 
              echo "VictoriaLogs is available at: http://localhost:${VICTORIALOGS_PORT}/vmui/"
              ;;
esac
