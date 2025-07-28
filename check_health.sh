#!/bin/bash

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Help function
show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all           - Check all systems"
    echo "  --monitoring    - Check only the monitoring system"
    echo "  --elk           - Check only the ELK Stack"
    echo "  --loki          - Check only Loki"
    echo "  --victorialogs  - Check only VictoriaLogs"
    echo "  --generator     - Check only the log generator"
    echo "  --telegraf      - Check only Telegraf"
    echo "  --help          - Show this help"
    echo ""
    echo "If no options are specified, all systems will be checked."
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

# Read environment variables for all components
read_monitoring_env() {
    cd "$(dirname "$0")/monitoring"
    read_env_file ".env"
    VICTORIA_METRICS_PORT=${VICTORIA_METRICS_PORT:-8428}
    GRAFANA_PORT=${GRAFANA_PORT:-3000}
    cd - > /dev/null
}

read_elk_env() {
    cd "$(dirname "$0")/elk"
    read_env_file ".env"
    ES_PORT=${ES_PORT:-9200}
    KIBANA_PORT=${KIBANA_PORT:-5601}
    cd - > /dev/null
}

read_loki_env() {
    cd "$(dirname "$0")/loki"
    read_env_file ".env"
    LOKI_PORT=${LOKI_PORT:-3100}
    cd - > /dev/null
}

read_victorialogs_env() {
    cd "$(dirname "$0")/victorialogs"
    read_env_file ".env"
    VICTORIALOGS_PORT=${VICTORIALOGS_PORT:-9428}
    cd - > /dev/null
}

read_telegraf_env() {
    cd "$(dirname "$0")/monitoring/telegraf"
    read_env_file ".env"
    PROMETHEUS_PORT=${PROMETHEUS_PORT:-9273}
    PRIVILEGED_USER_ID=${PRIVILEGED_USER_ID:-999}
    cd - > /dev/null
}

# Load all environment variables
read_monitoring_env
read_elk_env
read_loki_env
read_victorialogs_env
read_telegraf_env

# Print header function
print_header() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Status check function
check_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}OK${NC}: $2"
        return 0
    else
        echo -e "${RED}ERROR${NC}: $3"
        return 1
    fi
}

# URL availability check function with retries
check_url() {
    local url=$1
    local description=$2
    local error_msg=$3
    local max_attempts=${4:-5}  # Default to 5 attempts if not specified
    local retry_delay=${5:-15}   # Default to 15 seconds delay if not specified
    local attempt=1
    local success=false
    
    echo -e "Checking URL: $url"
    
    while [ $attempt -le $max_attempts ] && [ "$success" = false ]; do
        if curl -s --head --fail "$url" &>/dev/null || curl -s --fail "$url" &>/dev/null; then
            success=true
            break
        else
            if [ $attempt -lt $max_attempts ]; then
                echo -e "${YELLOW}Attempt $attempt failed, retrying in $retry_delay seconds...${NC}"
                sleep $retry_delay
            fi
            attempt=$((attempt+1))
        fi
    done
    
    if [ "$success" = true ]; then
        echo -e "${GREEN}OK${NC}: $description"
        return 0
    else
        echo -e "${RED}ERROR${NC}: $error_msg (after $max_attempts attempts)"
        return 1
    fi
}

# Function to check if VictoriaMetrics is available
is_victoria_metrics_available() {
    if ! docker ps --format '{{.Names}}' | grep -q "^victoria-metrics$"; then
        return 1
    fi
    
    if ! curl -s --head --fail "http://localhost:${VICTORIA_METRICS_PORT}/-/healthy" &>/dev/null; then
        return 1
    fi
    
    return 0
}

# Function to check for metrics in VictoriaMetrics
check_metrics_in_vm() {
    local metric_name=$1
    local description=$2
    local direct_url=$3
    local max_attempts=${4:-5}  # Default to 5 attempts if not specified
    local retry_delay=${5:-15}  # Default to 15 seconds delay if not specified
    local attempt=1
    local success=false

    # Check if VictoriaMetrics is available
    if is_victoria_metrics_available; then
        # VictoriaMetrics is available, check metrics through it
        echo -e "Checking metric $metric_name in ${description} on ${direct_url}"
        while [ $attempt -le $max_attempts ] && [ "$success" = false ]; do
            if curl -s "${direct_url}" | grep -q "$metric_name"; then
                success=true
                break
            else
                if [ $attempt -lt $max_attempts ]; then
                    echo -e "${YELLOW}Metrics check attempt $attempt failed, retrying in $retry_delay seconds...${NC}"
                    sleep $retry_delay
                fi
                attempt=$((attempt+1))
            fi
        done
    elif [ -n "$direct_url" ]; then
        # VictoriaMetrics is not available, but we have a direct URL to check
        echo -e "VictoriaMetrics is not available, checking metrics directly from exporter..."
        while [ $attempt -le $max_attempts ] && [ "$success" = false ]; do
            if curl -s "$direct_url" | grep -q "$metric_name"; then
                success=true
                break
            else
                if [ $attempt -lt $max_attempts ]; then
                    echo -e "${YELLOW}Direct metrics check attempt $attempt failed, retrying in $retry_delay seconds...${NC}"
                    sleep $retry_delay
                fi
                attempt=$((attempt+1))
            fi
        done
    else
        # VictoriaMetrics is not available and no direct URL provided
        echo -e "${YELLOW}SKIPPED${NC}: $description (VictoriaMetrics is not available and no direct metrics URL provided)"
        return 0  # Return success to avoid counting this as an error
    fi
    
    if [ "$success" = true ]; then
        echo -e "${GREEN}OK${NC}: $description"
        return 0
    else
        echo -e "${YELLOW}WARNING${NC}: Metrics '$metric_name' not found (after $max_attempts attempts)"
        return 1
    fi
}

# Function to check running containers
check_container_running() {
    local container_name=$1
    local description=$2
    
    if docker ps --format '{{.Names}}' | grep -q "^$container_name$"; then
        if [ "$(docker inspect --format='{{.State.Health.Status}}' $container_name 2>/dev/null)" = "healthy" ]; then
            echo -e "${GREEN}OK${NC}: $description (container is running and healthy)"
            return 0
        elif [ "$(docker inspect --format='{{.State.Status}}' $container_name 2>/dev/null)" = "running" ]; then
            echo -e "${GREEN}OK${NC}: $description (container is running)"
            return 0
        else
            echo -e "${YELLOW}WARNING${NC}: Container $container_name is running, but healthcheck is not successful"
            return 1
        fi
    else
        echo -e "${RED}ERROR${NC}: Container $container_name is not running"
        return 1
    fi
}

# Monitoring system check
check_monitoring() {
    print_header "MONITORING SYSTEM CHECK"
    local errors=0
    
    # Check running containers
    check_container_running "victoria-metrics" "VictoriaMetrics is running" || ((errors++))
    check_container_running "grafana" "Grafana is running" || ((errors++))
    check_container_running "grafana-renderer" "Grafana Renderer is running" || ((errors++))
    
    # Check service availability using URLs from healthcheck
    check_url "http://localhost:${VICTORIA_METRICS_PORT}/-/healthy" "VictoriaMetrics HTTP is accessible" "VictoriaMetrics HTTP is not accessible" || ((errors++))
    
    # Special check for Grafana with more attempts and longer wait time
    check_url "http://localhost:${GRAFANA_PORT}/api/health" "Grafana HTTP is accessible" "Grafana HTTP is not accessible" || ((errors++))
    
    # Check VictoriaMetrics' own metrics
    check_metrics_in_vm "vm_app_version" "VictoriaMetrics exports its own metrics" "http://localhost:${VICTORIA_METRICS_PORT}/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}Monitoring system is working correctly!${NC}"
    else
        echo -e "\n${RED}Monitoring system has $errors errors!${NC}"
    fi
    
    return $errors
}

# ELK Stack check
check_elk() {
    print_header "ELK STACK CHECK"
    local errors=0
    
    # Check running containers
    check_container_running "elasticsearch" "Elasticsearch is running" || ((errors++))
    check_container_running "kibana" "Kibana is running" || ((errors++))
    check_container_running "elasticsearch-exporter" "Elasticsearch Exporter is running" || ((errors++))
    
    # Check service availability using URLs from healthcheck
    check_url "http://localhost:${ES_PORT}" "Elasticsearch HTTP is accessible" "Elasticsearch HTTP is not accessible" || ((errors++))
    
    # Special check for Kibana with more attempts and longer wait time
    check_url "http://localhost:${KIBANA_PORT}/api/status" "Kibana HTTP is accessible" "Kibana HTTP is not accessible" || ((errors++))
    
    # Check Elasticsearch Exporter metrics
    check_url "http://localhost:9114/metrics" "Elasticsearch Exporter exports metrics" "Elasticsearch Exporter does not export metrics" || ((errors++))
    
    # Check metrics in VictoriaMetrics or directly from Elasticsearch Exporter
    check_metrics_in_vm "elasticsearch_cluster_health_status" "Elasticsearch metrics are available" "http://localhost:9114/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}ELK Stack is working correctly!${NC}"
    else
        echo -e "\n${RED}ELK Stack has $errors errors!${NC}"
    fi
    
    return $errors
}

# Loki check
check_loki() {
    print_header "LOKI CHECK"
    local errors=0
    
    # Check running containers
    check_container_running "loki" "Loki is running" || ((errors++))
    
    # Check service availability using URLs from healthcheck
    check_url "http://localhost:${LOKI_PORT}/ready" "Loki HTTP is accessible and ready" "Loki HTTP is not accessible or not ready" || ((errors++))
    check_url "http://localhost:${LOKI_PORT}/metrics" "Loki exports metrics" "Loki does not export metrics" || ((errors++))
    
    # Check metrics in VictoriaMetrics or directly from Loki
    check_metrics_in_vm "loki_build_info" "Loki metrics are available" "http://localhost:${LOKI_PORT}/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}Loki is working correctly!${NC}"
    else
        echo -e "\n${RED}Loki has $errors errors!${NC}"
    fi
    
    return $errors
}

# VictoriaLogs check
check_victorialogs() {
    print_header "VICTORIALOGS CHECK"
    local errors=0
    
    # Check running containers
    check_container_running "victorialogs" "VictoriaLogs is running" || ((errors++))
    
    # Check service availability using URLs from healthcheck
    check_url "http://localhost:${VICTORIALOGS_PORT}/health" "VictoriaLogs HTTP is accessible" "VictoriaLogs HTTP is not accessible" || ((errors++))
    check_url "http://localhost:${VICTORIALOGS_PORT}/metrics" "VictoriaLogs exports metrics" "VictoriaLogs does not export metrics" || ((errors++))
    
    # Check metrics in VictoriaMetrics or directly from VictoriaLogs
    check_metrics_in_vm "vm_app_version" "VictoriaLogs metrics are available" "http://localhost:${VICTORIALOGS_PORT}/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}VictoriaLogs is working correctly!${NC}"
    else
        echo -e "\n${RED}VictoriaLogs has $errors errors!${NC}"
    fi
    
    return $errors
}

# Log generator check
check_generator() {
    print_header "LOG GENERATOR CHECK"
    local errors=0
    
    # Check running containers
    check_container_running "go-generator" "Go Log Generator is running" || ((errors++))
    
    # Check service availability using metrics
    check_url "http://localhost:9090/metrics" "Log Generator exports metrics" "Log Generator does not export metrics" || ((errors++))
    
    # Check metrics in VictoriaMetrics or directly from Generator
    check_metrics_in_vm "generator_logs_total" "Log Generator metrics are available" "http://localhost:9090/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}Log Generator is working correctly!${NC}"
    else
        echo -e "\n${RED}Log Generator has $errors errors!${NC}"
    fi
    
    return $errors
}

# Telegraf check
check_telegraf() {
    print_header "TELEGRAF CHECK"
    local errors=0
    local ignore_docker_check=${IGNORE_DOCKER:-false}
    
    # Check running containers
    check_container_running "telegraf" "Telegraf is running" || ((errors++))
    
    # Check service availability using metrics
    check_url "http://localhost:${PROMETHEUS_PORT}/metrics" "Telegraf exports metrics" "Telegraf does not export metrics" || ((errors++))
    
    # Check for docker.sock permission issues if not ignoring
    if [ "$ignore_docker_check" != "true" ]; then
        echo "Checking Telegraf logs for docker.sock permission issues..."
        echo "Waiting 25 seconds for Telegraf to fully initialize..."
        sleep 25
        if docker logs telegraf 2>&1 | grep -q "permission denied while trying to connect to the Docker daemon socket"; then
            echo -e "${RED}ERROR${NC}: Неправильно указан id пользователя в .env, который не имеет доступа к docker.sock"
            echo "Остановка Telegraf..."
            cd "$(dirname "$0")/monitoring/telegraf"
            docker-compose down 2>/dev/null || docker compose down 2>/dev/null
            cd - > /dev/null
            ((errors++))
        else
            echo -e "${GREEN}OK${NC}: No docker.sock permission issues found"
        fi
    else
        echo -e "${YELLOW}SKIPPED${NC}: Docker.sock permission check (--ignore_docker flag used)"
    fi
    
    # Check metrics in VictoriaMetrics or directly from Telegraf
    check_metrics_in_vm "system_uptime" "Telegraf metrics are available" "http://localhost:${PROMETHEUS_PORT}/metrics" || ((errors++))
    
    if [ $errors -eq 0 ]; then
        echo -e "\n${GREEN}Telegraf is working correctly!${NC}"
    else
        echo -e "\n${RED}Telegraf has $errors errors!${NC}"
    fi
    
    return $errors
}

# Parse arguments
CHECK_MONITORING=false
CHECK_ELK=false
CHECK_LOKI=false
CHECK_VICTORIALOGS=false
CHECK_GENERATOR=false
CHECK_TELEGRAF=false
CHECK_ALL=true

if [ $# -gt 0 ]; then
    CHECK_ALL=false
    
    while [ "$1" != "" ]; do
        case $1 in
            --monitoring )     CHECK_MONITORING=true
                             ;;
            --elk )           CHECK_ELK=true
                             ;;
            --loki )          CHECK_LOKI=true
                             ;;
            --victorialogs )  CHECK_VICTORIALOGS=true
                             ;;
            --generator )     CHECK_GENERATOR=true
                             ;;
            --telegraf )      CHECK_TELEGRAF=true
                             ;;
            --all )           CHECK_ALL=true
                             ;;
            --help )          show_help
                             ;;
            * )               echo "Unknown parameter: $1"
                             show_help
                             ;;
        esac
        shift
    done
fi

TOTAL_ERRORS=0

# If --all was specified or no arguments were provided, check all systems
if $CHECK_ALL; then
    check_monitoring
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    
    check_elk
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    
    check_loki
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    
    check_victorialogs
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    
    check_generator
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    
    check_telegraf
    TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
else
    # Otherwise, check only the specified systems
    if $CHECK_MONITORING; then
        check_monitoring
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
    
    if $CHECK_ELK; then
        check_elk
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
    
    if $CHECK_LOKI; then
        check_loki
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
    
    if $CHECK_VICTORIALOGS; then
        check_victorialogs
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
    
    if $CHECK_GENERATOR; then
        check_generator
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
    
    if $CHECK_TELEGRAF; then
        check_telegraf
        TOTAL_ERRORS=$((TOTAL_ERRORS + $?))
    fi
fi

if [ $TOTAL_ERRORS -eq 0 ]; then
    echo -e "\n${GREEN}All checked systems are working correctly!${NC}"
    exit 0
else
    echo -e "\n${RED}Found $TOTAL_ERRORS errors in the checked systems!${NC}"
    exit 1
fi
