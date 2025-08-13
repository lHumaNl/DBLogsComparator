#!/bin/bash

set -e

# Function to determine which docker-compose command to use
setup_compose_command() {
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        echo "Error: Neither 'docker-compose' nor 'docker compose' is available."
        echo "Please install Docker and Docker Compose to continue."
        exit 1
    fi
}

# Function to ensure the monitoring network exists
ensure_monitoring_network() {
    if ! docker network inspect monitoring-network >/dev/null 2>&1; then
        echo "Network 'monitoring-network' not found. Creating it..."
        docker network create monitoring-network
        echo "Network 'monitoring-network' created successfully."
    fi
}

# Help function
show_help() {
    echo "Usage: $0 <log_system> [options]"
    echo ""
    echo "Log systems:"
    echo "  monitoring  - Start only the monitoring system (VictoriaMetrics, Grafana, Grafana Renderer)"
    echo "  elk         - Start ELK Stack (Elasticsearch + Kibana) without monitoring"
    echo "  loki        - Start Loki without monitoring"
    echo "  victorialogs - Start VictoriaLogs without monitoring"
    echo "  telegraf    - Start only Telegraf"
    echo "  all         - Only available with --down option to stop all systems"
    echo ""
    echo "Options:"
    echo "  --generator     - Start the log generator after starting the log systems (native by default)"
    echo "  --querier       - Start the log querier after starting the log systems (native by default)"
    echo "  --combined      - Start the log combined after starting the log systems (native by default)"
    echo "  --docker        - Start the log tools with Docker (instead of native)"
    echo "  --rebuild       - Rebuild the load_tool before starting (only for non-docker mode)"
    echo "  --monitoring    - Start monitoring system along with the database"
    echo "  --stability     - Use stability load testing mode (default)"
    echo "  --maxPerf       - Use maxPerf load testing mode"
    echo "  --telegraf      - Start Telegraf for monitoring"
    echo "  --ignore_docker - Skip docker.sock permission check for Telegraf"
    echo "  --down          - Stop the specified log system and its containers"
    echo "  --stop-load - Stop only the log load tool (doesn't affect other systems)"
    echo "  --help          - Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 monitoring                      # Start only monitoring (VictoriaMetrics, Grafana)"
    echo "  $0 loki                            # Start only Loki (without monitoring)"
    echo "  $0 loki --monitoring               # Start Loki with monitoring"
    echo "  $0 loki --generator                # Start log generator only (without Loki DB)"
    echo "  $0 loki --querier                  # Start log querier only (without Loki DB)"
    echo "  $0 loki --combined                 # Start log combined only (without Loki DB)"
    echo "  $0 loki --generator --monitoring   # Start Loki, monitoring, and generator"
    echo "  $0 loki --generator --docker       # Start generator with Docker (without Loki DB)"
    echo "  $0 loki --generator --maxPerf      # Start generator in maxPerf mode (without Loki DB)"
    echo "  $0 telegraf                        # Start only Telegraf"
    echo "  $0 telegraf --ignore_docker        # Start Telegraf, skip docker.sock check"
    echo "  $0 elk --down                      # Stop ELK Stack"
    echo "  $0 monitoring --down               # Stop the monitoring system"
    echo "  $0 telegraf --down                 # Stop Telegraf"
    echo "  $0 all --down                      # Stop all systems"
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
    $COMPOSE_CMD up -d
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
    $COMPOSE_CMD down
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
    $COMPOSE_CMD up -d
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
    $COMPOSE_CMD down
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
    $COMPOSE_CMD up -d
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
    $COMPOSE_CMD down
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
    $COMPOSE_CMD up -d
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
    $COMPOSE_CMD down
    cd - > /dev/null
    
    echo "VictoriaLogs stopped successfully!"
}

# Function to start Telegraf
start_telegraf() {
    echo "Starting Telegraf..."
    
    # Read Telegraf environment variables
    cd monitoring/telegraf
    read_env_file ".env"
    cd - > /dev/null
    
    # Start Telegraf
    cd monitoring/telegraf
    $COMPOSE_CMD up -d
    cd - > /dev/null
    
    # Check that Telegraf is healthy
    echo "Verifying Telegraf status..."
    IGNORE_DOCKER=$IGNORE_DOCKER ./check_health.sh --telegraf
    
    if [ $? -eq 0 ]; then
        echo "Telegraf started successfully!"
    else
        echo "Warning: Telegraf may not be fully operational."
        echo "Check the logs for more details:"
        echo "  docker logs telegraf"
    fi
}

# Function to stop Telegraf
stop_telegraf() {
    echo "Stopping Telegraf..."
    
    # Read Telegraf environment variables
    cd monitoring/telegraf
    read_env_file ".env"
    cd - > /dev/null
    
    # Stop Telegraf
    cd monitoring/telegraf
    $COMPOSE_CMD down
    cd - > /dev/null
    
    echo "Telegraf stopped successfully!"
}

# Function to stop log generator
stop_generator() {
    echo "Stopping Log Generator..."
    
    # Attempt to stop native generator if PID file exists
    if [ -f "load_tool/load_tool.pid" ]; then
        echo "Attempting to stop natively running generator..."
        PID=$(cat load_tool/load_tool.pid)
        if ps -p $PID > /dev/null; then
            kill $PID
            echo "Native log generator (PID: $PID) stopped successfully!"
        else
            echo "Native log generator process (PID: $PID) not found, removing stale PID file"
        fi
        rm -f load_tool/load_tool.pid
    fi
    
    # Always try to stop Docker container as well
    if [ -d "load_tool" ]; then
        cd load_tool
        if [ -f "docker-compose.yml" ]; then
            echo "Attempting to stop Docker-based generator..."
            $COMPOSE_CMD down
            echo "Docker-based log generator stopped!"
        else
            echo "Note: docker-compose.yml not found in load_tool directory"
        fi
        cd - > /dev/null
    else
        echo "Note: load_tool directory not found"
    fi
    
    # Double check if any processes with name "load_tool" are still running
    RUNNING_PIDS=$(pgrep -f "load_tool.*-mode (generator|querier|combined)")
    if [ -n "$RUNNING_PIDS" ]; then
        echo "Found additional load_tool processes still running, stopping them:"
        for PID in $RUNNING_PIDS; do
            echo "Stopping process $PID..."
            kill $PID
        done
        echo "All load_tool processes stopped!"
    fi
}

# Function to start log generator
start_generator() {
    echo "Starting Log Generator..."
    
    # Read generator environment variables
    cd load_tool
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
    
    # Convert DB_SYSTEM to what load_tool expects
    case $DB_SYSTEM in
        elk )           SYSTEM_ARG="es"
                        ;;
        loki )          SYSTEM_ARG="loki"
                        ;;
        victorialogs )  SYSTEM_ARG="victoria"
                        ;;
        * )             echo "Unknown system: $DB_SYSTEM"
                        exit 1
                        ;;
    esac
    
    # Start the log generator based on mode (native or Docker)
    if [ "$USE_NATIVE" = "true" ] && [ "$USE_DOCKER" = "false" ]; then
        echo "Starting log generator in native mode..."
        cd load_tool
        
        # Check system availability before starting
        if [ "$DB_SYSTEM" = "loki" ]; then
            echo -n "Checking Loki API availability... "
            status_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3100/ready)
            echo "$status_code"
            if [ "$status_code" != "200" ]; then
                echo "Warning: Failed to connect to Loki API"
            fi
        fi
        
        # Check if load_tool executable exists and build it if necessary
        # Or rebuild if REBUILD_NATIVE is true
        if [ ! -f "./load_tool" ] || [ ! -x "./load_tool" ] || [ "$REBUILD_NATIVE" = "true" ]; then
            if [ "$REBUILD_NATIVE" = "true" ]; then
                echo "Forced rebuild requested with --rebuild"
            else
                echo "load_tool executable not found or doesn't have execution permissions"
            fi
            echo "Building load_tool..."
            go build -o load_tool || {
                echo "Error building load_tool"
                cd - > /dev/null
                return 1
            }
            echo "load_tool built successfully"
        fi
        
        # Run with specified config and hosts file for local execution
        echo ""
        echo "Starting load_tool with config: config.yaml and hosts file db_hosts.yaml (load mode: $LOAD_MODE)"
        ./load_tool -config "config.yaml" -hosts "db_hosts.yaml" -system "$SYSTEM_ARG" -mode "generator" -load-mode "$LOAD_MODE" &
        GENERATOR_PID=$!
        
        echo "Log Generator started natively with PID: $GENERATOR_PID"
        # Save PID to file for later stopping
        echo $GENERATOR_PID > ./load_tool.pid
        cd - > /dev/null
    else
        # Start with Docker
        echo "Starting log generator with Docker Compose..."
        cd load_tool
        
        # First stop previous generator instances to avoid conflicts
        echo "Stopping previous instances of log generator..."
        $COMPOSE_CMD down
        
        # Override environment variables for docker-compose
        # Pass the selected logging system and mode through variables
        # Add --build flag for automatic image rebuild when changes occur
        echo "Starting log generator with updated image (load mode: $LOAD_MODE)..."
        SYSTEM=$SYSTEM_ARG MODE="generator" LOAD_MODE=$LOAD_MODE $COMPOSE_CMD up -d --build
        echo "Log Generator started with Docker"
        cd - > /dev/null
    fi
}

# Function to start log querier
start_querier() {
    echo "Starting Log Querier..."
    
    # Read querier environment variables
    cd load_tool
    read_env_file ".env"
    cd - > /dev/null
    
    # Set appropriate environment variables for the log generator based on DB_SYSTEM
    export QUERIER_TARGET=$DB_SYSTEM
    
    # Convert DB_SYSTEM to what load_tool expects
    case $DB_SYSTEM in
        elk )           SYSTEM_ARG="es"
                        ;;
        loki )          SYSTEM_ARG="loki"
                        ;;
        victorialogs )  SYSTEM_ARG="victoria"
                        ;;
        * )             echo "Unknown system: $DB_SYSTEM"
                        exit 1
                        ;;
    esac
    
    # Start the log querier based on mode (native or Docker)
    if [ "$USE_NATIVE" = "true" ] && [ "$USE_DOCKER" = "false" ]; then
        echo "Starting log querier in native mode..."
        cd load_tool
        
        # Check system availability before starting
        if [ "$DB_SYSTEM" = "loki" ]; then
            echo -n "Checking Loki API availability... "
            status_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3100/ready)
            echo "$status_code"
            if [ "$status_code" != "200" ]; then
                echo "Warning: Failed to connect to Loki API"
            fi
        fi
        
        # Check if load_tool executable exists and build it if necessary
        # Or rebuild if REBUILD_NATIVE is true
        if [ ! -f "./load_tool" ] || [ ! -x "./load_tool" ] || [ "$REBUILD_NATIVE" = "true" ]; then
            if [ "$REBUILD_NATIVE" = "true" ]; then
                echo "Forced rebuild requested with --rebuild"
            else
                echo "load_tool executable not found or doesn't have execution permissions"
            fi
            echo "Building load_tool..."
            go build -o load_tool || {
                echo "Error building load_tool"
                cd - > /dev/null
                return 1
            }
            echo "load_tool built successfully"
        fi
        
        # Run with specified config and hosts file
        echo ""
        echo "Starting load_tool with config: config.yaml and hosts file db_hosts.yaml (load mode: $LOAD_MODE)"
        ./load_tool -config "config.yaml" -hosts "db_hosts.yaml" -system "$SYSTEM_ARG" -mode "querier" -load-mode "$LOAD_MODE" &
        QUERIER_PID=$!
        
        echo "Log Querier started natively with PID: $QUERIER_PID"
        # Save PID to file for later stopping
        echo $QUERIER_PID > ./load_tool.pid
        cd - > /dev/null
    else
        # Start with Docker
        echo "Starting log querier with Docker Compose..."
        cd load_tool
        
        # First stop any previous instances
        echo "Stopping previous instances of log querier..."
        $COMPOSE_CMD down
        
        # Override environment variables for docker-compose
        echo "Starting log querier with updated image (load mode: $LOAD_MODE)..."
        SYSTEM=$SYSTEM_ARG MODE="querier" LOAD_MODE=$LOAD_MODE $COMPOSE_CMD up -d --build
        echo "Log Querier started with Docker"
        cd - > /dev/null
    fi
}

# Function to start log combined
start_combined() {
    echo "Starting Log Combined..."
    
    # Read combined environment variables
    cd load_tool
    read_env_file ".env"
    cd - > /dev/null
    
    # Set appropriate environment variables for the log generator based on DB_SYSTEM
    export COMBINED_TARGET=$DB_SYSTEM
    
    # Convert DB_SYSTEM to what load_tool expects
    case $DB_SYSTEM in
        elk )           SYSTEM_ARG="es"
                        ;;
        loki )          SYSTEM_ARG="loki"
                        ;;
        victorialogs )  SYSTEM_ARG="victoria"
                        ;;
        * )             echo "Unknown system: $DB_SYSTEM"
                        exit 1
                        ;;
    esac
    
    # Start the log combined based on mode (native or Docker)
    if [ "$USE_NATIVE" = "true" ] && [ "$USE_DOCKER" = "false" ]; then
        echo "Starting log combined in native mode..."
        cd load_tool
        
        # Check system availability before starting
        if [ "$DB_SYSTEM" = "loki" ]; then
            echo -n "Checking Loki API availability... "
            status_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3100/ready)
            echo "$status_code"
            if [ "$status_code" != "200" ]; then
                echo "Warning: Failed to connect to Loki API"
            fi
        fi
        
        # Check if load_tool executable exists and build it if necessary
        # Or rebuild if REBUILD_NATIVE is true
        if [ ! -f "./load_tool" ] || [ ! -x "./load_tool" ] || [ "$REBUILD_NATIVE" = "true" ]; then
            if [ "$REBUILD_NATIVE" = "true" ]; then
                echo "Forced rebuild requested with --rebuild"
            else
                echo "load_tool executable not found or doesn't have execution permissions"
            fi
            echo "Building load_tool..."
            go build -o load_tool || {
                echo "Error building load_tool"
                cd - > /dev/null
                return 1
            }
            echo "load_tool built successfully"
        fi
        
        # Run with specified config and hosts file
        echo ""
        echo "Starting load_tool with config: config.yaml and hosts file db_hosts.yaml (load mode: $LOAD_MODE)"
        ./load_tool -config "config.yaml" -hosts "db_hosts.yaml" -system "$SYSTEM_ARG" -mode "combined" -load-mode "$LOAD_MODE" &
        COMBINED_PID=$!
        
        echo "Log Combined started natively with PID: $COMBINED_PID"
        # Save PID to file for later stopping
        echo $COMBINED_PID > ./load_tool.pid
        cd - > /dev/null
    else
        # Start with Docker
        echo "Starting log combined with Docker Compose..."
        cd load_tool
        
        # First stop any previous instances
        echo "Stopping previous instances of log combined..."
        $COMPOSE_CMD down
        
        # Override environment variables for docker-compose
        echo "Starting log combined with updated image (load mode: $LOAD_MODE)..."
        SYSTEM=$SYSTEM_ARG MODE="combined" LOAD_MODE=$LOAD_MODE $COMPOSE_CMD up -d --build
        echo "Log Combined started with Docker"
        cd - > /dev/null
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
    
    # Stop Telegraf if it's running
    if [ -d "monitoring/telegraf" ] && [ -f "monitoring/telegraf/docker-compose.yml" ]; then
        stop_telegraf
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

# Setup docker-compose command
setup_compose_command

# Ensure the monitoring network exists before we do anything else
ensure_monitoring_network

# Parse arguments
DB_SYSTEM=""
LAUNCH_GENERATOR=false
LAUNCH_QUERIER=false
LAUNCH_COMBINED=false
NO_MONITORING=false
LAUNCH_MONITORING=false
BRING_DOWN=false
STOP_GENERATOR=false
USE_NATIVE=true
USE_DOCKER=false
REBUILD_NATIVE=false
LOAD_MODE="stability"
LAUNCH_TELEGRAF=false
IGNORE_DOCKER=false

for arg in "$@"; do
    case $arg in
        --generator )   LAUNCH_GENERATOR=true
                         ;;
        --querier )     LAUNCH_QUERIER=true
                         ;;
        --combined )    LAUNCH_COMBINED=true
                         ;;
        --docker )      USE_NATIVE=false
                         USE_DOCKER=true
                         ;;
        --rebuild )     REBUILD_NATIVE=true
                         ;;
        --monitoring )  LAUNCH_MONITORING=true
                         ;;
        --down )        BRING_DOWN=true
                         ;;
        --stop-load ) STOP_GENERATOR=true
                         ;;
        --stability )   LOAD_MODE="stability"
                         ;;
        --maxPerf )     LOAD_MODE="maxPerf"
                         ;;
        --telegraf )    LAUNCH_TELEGRAF=true
                         ;;
        --ignore_docker ) IGNORE_DOCKER=true
                         ;;
        --help )        show_help
                         ;;
        * )             if [ -z "$DB_SYSTEM" ]; then
                            DB_SYSTEM=$arg
                        else
                            echo "Unknown argument: $arg"
                            show_help
                        fi
                         ;;
    esac
done

# If --stop-load flag is set, stop the generator and exit regardless of other arguments
if $STOP_GENERATOR; then
    stop_generator
    echo "Log generator stopped!"
    exit 0
fi

# Check if DB_SYSTEM is specified (unless --stop-load is used)
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
        telegraf )      stop_telegraf
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

# Check if we need to start only load tools (without DB)
START_ONLY_TOOLS=false
if $LAUNCH_GENERATOR || $LAUNCH_QUERIER || $LAUNCH_COMBINED; then
    if ! $LAUNCH_MONITORING; then
        START_ONLY_TOOLS=true
    fi
fi

# Start selected systems based on the specified argument
case $DB_SYSTEM in
    monitoring )    start_monitoring
                    ;;
    elk )           if ! $START_ONLY_TOOLS; then
                        if $LAUNCH_MONITORING; then
                            start_monitoring
                        fi
                        start_elk
                    fi
                    ;;
    loki )          if ! $START_ONLY_TOOLS; then
                        if $LAUNCH_MONITORING; then
                            start_monitoring
                        fi
                        start_loki
                    fi
                    ;;
    victorialogs )  if ! $START_ONLY_TOOLS; then
                        if $LAUNCH_MONITORING; then
                            start_monitoring
                        fi
                        start_victorialogs
                    fi
                    ;;
    telegraf )      start_telegraf
                    ;;
    * )             echo "Unknown system: $DB_SYSTEM"
                    show_help
                    ;;
esac

# Start log generator if flag is specified
if $LAUNCH_GENERATOR; then
    start_generator
fi

if $LAUNCH_QUERIER; then
    start_querier
fi

if $LAUNCH_COMBINED; then
    start_combined
fi

# Start Telegraf if flag is specified
if $LAUNCH_TELEGRAF; then
    start_telegraf
fi

echo "All systems started successfully!"

# Output information about availability of monitoring
if $LAUNCH_MONITORING || [ "$DB_SYSTEM" = "monitoring" ]; then
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
              echo "VictoriaLogs is available at: http://localhost:${VICTORIALOGS_PORT}/select/vmui/"
              ;;
esac
