#!/bin/bash

# Script to build and push Docker images to a registry (multi-architecture: amd64 and arm64)
# Usage: ./build-and-push.sh [registry/repository] [tag] [platforms]
# Example: ./build-and-push.sh myregistry/lnpay latest
# Example: ./build-and-push.sh docker.io/username/lnpay v1.0.0
# Example: ./build-and-push.sh docker.io/username/lnpay latest linux/amd64,linux/arm64

set -e

# Always run from the repository root (this script lives in deployment/scripts/)
cd "$(dirname "$0")/../.."

# Default values
REGISTRY="${1:-docker.io/username/lnpay}"
TAG="${2:-latest}"
PLATFORMS="${3:-linux/amd64,linux/arm64}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Building and pushing multi-arch images to ${REGISTRY} with tag ${TAG}${NC}"
echo -e "${BLUE}Platforms: ${PLATFORMS}${NC}\n"

# Ensure buildx is available and create a builder if needed
echo -e "${YELLOW}Setting up Docker Buildx...${NC}"
if ! docker buildx version > /dev/null 2>&1; then
    echo -e "${YELLOW}Docker Buildx is not available. Please install Docker Buildx.${NC}"
    exit 1
fi

# Create or use a multi-platform builder
BUILDER_NAME="lnpay-multiarch-builder"
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
    shift 3
    local build_args=("$@")
    
    local image_name="${REGISTRY}-${service_name}:${TAG}"
    
    echo -e "${GREEN}Building ${service_name} for platforms: ${PLATFORMS}...${NC}"
    
    # Build for multiple platforms and push directly
    if [ ${#build_args[@]} -gt 0 ]; then
        docker buildx build \
            --platform "$PLATFORMS" \
            -t "$image_name" \
            -f "$dockerfile_path" \
            "${build_args[@]}" \
            --push \
            "$build_context"
    else
        docker buildx build \
            --platform "$PLATFORMS" \
            -t "$image_name" \
            -f "$dockerfile_path" \
            --push \
            "$build_context"
    fi
    
    echo -e "${GREEN}✓ ${service_name} built and pushed successfully for ${PLATFORMS}${NC}\n"
}

# Build and push all services
echo -e "${BLUE}=== Building infrastructure services ===${NC}\n"

build_and_push "caddy" "./infrastructure/caddy/Dockerfile" "./infrastructure/caddy"
build_and_push "redis" "./infrastructure/redis/Dockerfile" "./infrastructure/redis"
build_and_push "mosquitto" "./infrastructure/mosquitto/Dockerfile" "./infrastructure/mosquitto"

echo -e "${BLUE}=== Building application services ===${NC}\n"

build_and_push "device" "./services/Dockerfile" "." "--build-arg" "SERVICE=device"
build_and_push "ledger" "./services/Dockerfile" "." "--build-arg" "SERVICE=ledger"
build_and_push "consumption" "./services/Dockerfile" "." "--build-arg" "SERVICE=consumption"
build_and_push "lightning" "./services/Dockerfile" "." "--build-arg" "SERVICE=lightning"

echo -e "${BLUE}=== Building smartmeter ===${NC}\n"

# Smartmeter WebSocket URL is now dynamically determined from browser location
build_and_push "smartmeter" "./testing/smartmeter/Dockerfile" "."

echo -e "${GREEN}=== All images built and pushed successfully! ===${NC}"
echo -e "${BLUE}You can now use docker-compose.prod.yml to pull and run these images${NC}"

