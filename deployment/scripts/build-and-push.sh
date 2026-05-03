#!/bin/bash

# Script to build and push Docker images to a registry (multi-architecture: amd64 and arm64)
# Usage: ./build-and-push.sh [registry/repository] [tag] [platforms]
# Example: ./build-and-push.sh myregistry/lina latest
# Example: ./build-and-push.sh docker.io/username/lina v1.0.0
# Example: ./build-and-push.sh docker.io/username/lina latest linux/amd64,linux/arm64

set -e

# Always run from the repository root (this script lives in deployment/scripts/)
cd "$(dirname "$0")/../.."

# Default values
REGISTRY="${1:-docker.io/username/lina}"
TAG="${2:-latest}"
PLATFORMS="${3:-linux/amd64,linux/arm64}"
MAX_PARALLEL_BUILDS="${MAX_PARALLEL_BUILDS:-3}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Building and pushing multi-arch images to ${REGISTRY} with tag ${TAG}${NC}"
echo -e "${BLUE}Platforms: ${PLATFORMS}${NC}"
echo -e "${BLUE}Max parallel builds: ${MAX_PARALLEL_BUILDS}${NC}\n"

# Ensure buildx is available and create a builder if needed
echo -e "${YELLOW}Setting up Docker Buildx...${NC}"
if ! docker buildx version > /dev/null 2>&1; then
    echo -e "${YELLOW}Docker Buildx is not available. Please install Docker Buildx.${NC}"
    exit 1
fi

# Create or use a multi-platform builder
BUILDER_NAME="lina-multiarch-builder"
if ! docker buildx inspect "$BUILDER_NAME" > /dev/null 2>&1; then
    echo -e "${YELLOW}Creating multi-platform builder: ${BUILDER_NAME}...${NC}"
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
    docker buildx inspect --bootstrap
else
    echo -e "${YELLOW}Using existing builder: ${BUILDER_NAME}...${NC}"
    docker buildx use "$BUILDER_NAME"
fi

# Function to build and push an image for multiple architectures
build_and_push() {
    local service_name=$1
    local dockerfile_path=$2
    local build_context=$3
    local cache_ref=$4
    shift 4
    local build_args=("$@")
    
    local image_name="${REGISTRY}-${service_name}:${TAG}"
    
    echo -e "${GREEN}Building ${service_name} for platforms: ${PLATFORMS}...${NC}"
    
    # Build for multiple platforms and push directly
    if [ ${#build_args[@]} -gt 0 ]; then
        docker buildx build \
            --platform "$PLATFORMS" \
            -t "$image_name" \
            -f "$dockerfile_path" \
            --cache-from "type=registry,ref=${cache_ref}" \
            --cache-to "type=registry,ref=${cache_ref},mode=max,image-manifest=true,oci-mediatypes=true" \
            "${build_args[@]}" \
            --push \
            "$build_context"
    else
        docker buildx build \
            --platform "$PLATFORMS" \
            -t "$image_name" \
            -f "$dockerfile_path" \
            --cache-from "type=registry,ref=${cache_ref}" \
            --cache-to "type=registry,ref=${cache_ref},mode=max,image-manifest=true,oci-mediatypes=true" \
            --push \
            "$build_context"
    fi
    
    echo -e "${GREEN}✓ ${service_name} built and pushed successfully for ${PLATFORMS}${NC}\n"
}

# Run builds in parallel and wait when limit is reached
run_build() {
    build_and_push "$@" &
    while [ "$(jobs -pr | wc -l | tr -d ' ')" -ge "$MAX_PARALLEL_BUILDS" ]; do
        local running_pid
        running_pid="$(jobs -pr | awk 'NR==1 {print $1}')"
        if [ -n "$running_pid" ]; then
            wait "$running_pid"
        fi
    done
}

# Build and push all services
echo -e "${BLUE}=== Building infrastructure services ===${NC}\n"

# run_build "caddy" "./infrastructure/caddy/Dockerfile" "./infrastructure/caddy" "${REGISTRY}-caddy:buildcache"
# run_build "redis" "./infrastructure/redis/Dockerfile" "./infrastructure/redis" "${REGISTRY}-redis:buildcache"
# run_build "mosquitto" "./infrastructure/mosquitto/Dockerfile" "./infrastructure/mosquitto" "${REGISTRY}-mosquitto:buildcache"
# run_build "prometheus" "./infrastructure/prometheus/Dockerfile" "./infrastructure/prometheus" "${REGISTRY}-prometheus:buildcache"
# run_build "grafana" "./infrastructure/grafana/Dockerfile" "./infrastructure/grafana" "${REGISTRY}-grafana:buildcache"

# wait

echo -e "${BLUE}=== Building application services ===${NC}\n"

# Shared cache for all services that use the same multi-stage Dockerfile.
SERVICES_CACHE_REF="${REGISTRY}-services:buildcache"
run_build "device" "./services/Dockerfile" "." "${SERVICES_CACHE_REF}" "--build-arg" "SERVICE=device"
run_build "ledger" "./services/Dockerfile" "." "${SERVICES_CACHE_REF}" "--build-arg" "SERVICE=ledger"
run_build "consumption" "./services/Dockerfile" "." "${SERVICES_CACHE_REF}" "--build-arg" "SERVICE=consumption"
run_build "lightning" "./services/Dockerfile" "." "${SERVICES_CACHE_REF}" "--build-arg" "SERVICE=lightning"
run_build "autopay" "./services/Dockerfile" "." "${SERVICES_CACHE_REF}" "--build-arg" "SERVICE=autopay"

wait

echo -e "${BLUE}=== Building testing tools ===${NC}\n"

# Smartmeter WebSocket URL is now dynamically determined from browser location
run_build "smartmeter" "./testing/smartmeter/Dockerfile" "." "${REGISTRY}-smartmeter:buildcache"

# HTTP devices service for load testing
run_build "httpdevices" "./testing/loadtest/httpdevices/Dockerfile" "." "${REGISTRY}-httpdevices:buildcache"

wait

echo -e "${GREEN}=== All images built and pushed successfully! ===${NC}"
echo -e "${BLUE}You can now use docker-compose.edge.yml to pull and run these images${NC}"

