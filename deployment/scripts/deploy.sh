#!/bin/bash
# Deployment script for LNPay system
# Supports both local and remote (SSH) deployment
#
# Usage:
#   Local:  ./deploy.sh local
#   Remote: ./deploy.sh remote user@hostname
#   Remote: ./deploy.sh remote user@hostname -p 2222
#
#   Help:   ./deploy.sh help

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the project root (parent of deployment directory)
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Change to project root directory
cd "$PROJECT_ROOT"

COMPOSE_FILE="deployment/docker-compose.prod.yml"
COMPOSE_MEAS_FILE="deployment/docker-compose.loadtest.edge.yml"
CERTS_DIR="./infrastructure/certs"
REMOTE_DIR="~/lnpay"
# Extract just the filename for remote operations
COMPOSE_FILE_BASENAME=$(basename "$COMPOSE_FILE")
COMPOSE_MEAS_FILE_BASENAME=$(basename "$COMPOSE_MEAS_FILE")

# Show usage/help
show_usage() {
    echo "LNPay Deployment Script"
    echo "======================"
    echo ""
    echo "Usage:"
    echo "  ./deploy.sh <type> [ssh_target] [ssh_options]"
    echo ""
    echo "Deployment Types:"
    echo "  local              Deploy on local machine"
    echo "  remote <target>    Deploy on remote machine via SSH"
    echo "  help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh local"
    echo "  ./deploy.sh remote user@hostname"
    echo "  ./deploy.sh remote user@hostname -p 2222"
    echo ""
    echo "Remote deployment will:"
    echo "  - Copy deployment/docker-compose.prod.yml, deployment/docker-compose.loadtest.edge.yml to ~/lnpay/deployment on remote"
    echo "  - Copy infrastructure/certs/ to ~/lnpay/infrastructure/certs on remote"
    echo "  - Verify prerequisites (Docker, docker-compose)"
    echo "  - Pull Docker images"
    echo "  - Provide instructions to start services"
    echo ""
    exit 0
}

# Parse arguments
DEPLOY_TYPE="${1:-}"

# Check if help requested
if [ "$DEPLOY_TYPE" = "help" ] || [ "$DEPLOY_TYPE" = "-h" ] || [ "$DEPLOY_TYPE" = "--help" ]; then
    show_usage
fi

# Require deployment type
if [ -z "$DEPLOY_TYPE" ]; then
    echo -e "${RED}ERROR: Deployment type required${NC}"
    echo ""
    show_usage
fi

# Validate deployment type
if [ "$DEPLOY_TYPE" != "local" ] && [ "$DEPLOY_TYPE" != "remote" ]; then
    echo -e "${RED}ERROR: Invalid deployment type: $DEPLOY_TYPE${NC}"
    echo "Valid types: local, remote"
    echo ""
    show_usage
fi

# Initialize SSH multiplexing variable (empty for local)
SSH_MULTIPLEX_OPTS=""

# Parse SSH target for remote deployment
if [ "$DEPLOY_TYPE" = "remote" ]; then
    SSH_TARGET="${2:-}"
    if [ -z "$SSH_TARGET" ]; then
        echo -e "${RED}ERROR: SSH target required for remote deployment${NC}"
        echo "Usage: ./deploy.sh remote user@hostname"
        echo ""
        show_usage
    fi
    REMOTE_DEPLOY=true
    # Collect remaining SSH options (e.g., -p 2222)
    SSH_OPTS="${@:3}"
    echo -e "${BLUE}LNPay Remote Deployment Script${NC}"
    echo "======================================"
    echo "Target: $SSH_TARGET"
    [ -n "$SSH_OPTS" ] && echo "SSH Options: $SSH_OPTS"
    echo ""
else
    REMOTE_DEPLOY=false
    echo -e "${BLUE}LNPay Local Deployment Script${NC}"
    echo "===================================="
    echo ""
fi

# Setup SSH connection multiplexing for remote deployments
if [ "$REMOTE_DEPLOY" = "true" ]; then
    # Create a unique control path for this connection
    SSH_CONTROL_DIR="$HOME/.ssh/lnpay-deploy"
    mkdir -p "$SSH_CONTROL_DIR"
    SSH_CONTROL_PATH="$SSH_CONTROL_DIR/$(echo "$SSH_TARGET" | tr '@:' '_')"
    
    # Setup SSH multiplexing - use 'auto' for all commands (creates master if needed, reuses if exists)
    SSH_MULTIPLEX_OPTS="-o ControlMaster=auto -o ControlPath=$SSH_CONTROL_PATH -o ControlPersist=300"
    
    # Function to cleanup SSH connection
    cleanup_ssh() {
        if [ -S "$SSH_CONTROL_PATH" ]; then
            ssh $SSH_OPTS -o ControlPath="$SSH_CONTROL_PATH" -O exit "$SSH_TARGET" 2>/dev/null || true
        fi
        rm -f "$SSH_CONTROL_PATH"
    }
    
    # Trap to cleanup on exit
    trap cleanup_ssh EXIT
    
    # Clean up any existing control socket from previous runs
    if [ -S "$SSH_CONTROL_PATH" ] || [ -f "$SSH_CONTROL_PATH" ]; then
        echo "Cleaning up existing SSH connection..."
        # Try to gracefully close existing connection
        ssh $SSH_OPTS -o ControlPath="$SSH_CONTROL_PATH" -O exit "$SSH_TARGET" 2>/dev/null || true
        # Remove the socket/file
        rm -f "$SSH_CONTROL_PATH"
        sleep 0.3  # Brief pause to ensure cleanup completes
    fi
    
    # Establish initial connection (this will prompt for password once)
    echo "Establishing SSH connection (you may be prompted for password once)..."
    # Use ControlMaster=auto - it will create the master on first use, reuse on subsequent
    if ssh $SSH_OPTS $SSH_MULTIPLEX_OPTS "$SSH_TARGET" "echo 'Connection established'" >/dev/null 2>&1; then
        # Verify the control socket was created
        sleep 0.2  # Brief pause for socket creation
        if [ -S "$SSH_CONTROL_PATH" ]; then
            echo -e "${GREEN}✓ SSH connection established (multiplexing enabled)${NC}"
            echo "  Subsequent commands will reuse this connection (no more password prompts)"
        else
            echo -e "${YELLOW}Note: Control socket not created, using standard SSH${NC}"
            echo "  Each command will require password authentication"
            SSH_MULTIPLEX_OPTS=""
        fi
    else
        # If connection fails, try without multiplexing (fallback)
        echo -e "${YELLOW}Note: SSH multiplexing not available, using standard SSH${NC}"
        echo "  Each command will require password authentication"
        SSH_MULTIPLEX_OPTS=""
    fi
    echo ""
fi

# Function to run command locally or remotely
run_cmd() {
    if [ "$REMOTE_DEPLOY" = "true" ]; then
        ssh $SSH_OPTS $SSH_MULTIPLEX_OPTS "$SSH_TARGET" "$@"
    else
        eval "$@"
    fi
}

# Function to check if command exists locally or remotely
check_cmd() {
    if [ "$REMOTE_DEPLOY" = "true" ]; then
        ssh $SSH_OPTS $SSH_MULTIPLEX_OPTS "$SSH_TARGET" "command -v $1 >/dev/null 2>&1"
    else
        command -v "$1" >/dev/null 2>&1
    fi
}

# Function to check if file exists locally or remotely
check_file() {
    if [ "$REMOTE_DEPLOY" = "true" ]; then
        ssh $SSH_OPTS $SSH_MULTIPLEX_OPTS "$SSH_TARGET" "test -f $1"
    else
        test -f "$1"
    fi
}

# Check if docker-compose.prod.yml exists locally
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}ERROR: $COMPOSE_FILE not found${NC}"
    echo "Please ensure you're in the project directory"
    exit 1
fi

# Check if docker-compose.loadtest.edge.yml exists locally (optional, just warn if missing)
if [ ! -f "$COMPOSE_MEAS_FILE" ]; then
    echo -e "${YELLOW}Note: $COMPOSE_MEAS_FILE not found (optional file)${NC}"
fi

# Check Docker and docker-compose on target
echo -e "${BLUE}Step 1: Verifying Prerequisites${NC}"
echo "--------------------------------"

if ! check_cmd docker; then
    echo -e "${RED}ERROR: Docker is not installed${NC}"
    if [ "$REMOTE_DEPLOY" = "true" ]; then
        echo "Please install Docker on $SSH_TARGET"
    else
        echo "Please install Docker"
    fi
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

if ! check_cmd docker-compose && ! run_cmd "docker compose version >/dev/null 2>&1"; then
    echo -e "${RED}ERROR: docker-compose is not installed${NC}"
    if [ "$REMOTE_DEPLOY" = "true" ]; then
        echo "Please install docker-compose on $SSH_TARGET"
    else
        echo "Please install docker-compose"
    fi
    exit 1
fi

# Determine docker-compose command
if run_cmd "docker compose version >/dev/null 2>&1"; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi
echo -e "${GREEN}✓ docker-compose found${NC}"

# Certificate setup
echo ""
echo -e "${BLUE}Step 2: Certificate Setup${NC}"
echo "----------------------------"

if [ "$REMOTE_DEPLOY" = "true" ]; then
    # Remote deployment: copy certificates or generate script
    echo "Preparing certificates for remote deployment..."
    
    # Check if certificates exist locally
    if [ -d "$CERTS_DIR" ] && [ -f "$CERTS_DIR/ca.crt" ] && [ -f "$CERTS_DIR/server.crt" ] && [ -f "$CERTS_DIR/server.key" ]; then
        echo -e "${GREEN}✓ Found certificates locally${NC}"
        echo "Will copy certificates to remote machine"
        COPY_CERTS=true
    elif [ -f "$CERTS_DIR/generate-certs.sh" ]; then
        echo -e "${YELLOW}No certificates found locally${NC}"
        echo "Will copy certificate generation script to remote machine"
        COPY_CERTS=false
    else
        echo -e "${RED}ERROR: No certificates or generation script found${NC}"
        echo "Please ensure either:"
        echo "  - Certificates exist in $CERTS_DIR/"
        echo "  - Or certs/generate-certs.sh exists"
        exit 1
    fi
    
    # Create remote directories
    echo "Creating remote directory: $REMOTE_DIR"
    run_cmd "mkdir -p $REMOTE_DIR/deployment"
    run_cmd "mkdir -p $REMOTE_DIR/infrastructure/certs"
    
    # Copy docker-compose.prod.yml
    echo "Copying docker-compose.prod.yml..."
    scp $SSH_OPTS $SSH_MULTIPLEX_OPTS "$COMPOSE_FILE" "$SSH_TARGET:$REMOTE_DIR/deployment/$COMPOSE_FILE_BASENAME"
    
    # Copy docker-compose.loadtest.edge.yml if it exists
    if [ -f "$COMPOSE_MEAS_FILE" ]; then
        echo "Copying docker-compose.loadtest.edge.yml..."
        scp $SSH_OPTS $SSH_MULTIPLEX_OPTS "$COMPOSE_MEAS_FILE" "$SSH_TARGET:$REMOTE_DIR/deployment/$COMPOSE_MEAS_FILE_BASENAME"
    else
        echo -e "${YELLOW}Note: docker-compose.loadtest.edge.yml not found locally, skipping${NC}"
    fi
    
    # Copy .env.example if .env doesn't exist on remote (in deployment directory)
    if [ -f "deployment/.env.example" ]; then
        echo "Checking for .env file on remote..."
        if ! run_cmd "test -f $REMOTE_DIR/deployment/.env"; then
            echo "Copying .env.example to remote (no .env found)..."
            scp $SSH_OPTS $SSH_MULTIPLEX_OPTS "deployment/.env.example" "$SSH_TARGET:$REMOTE_DIR/deployment/"
            echo -e "${YELLOW}⚠ IMPORTANT: Rename .env.example to .env and update variables:${NC}"
            if [ -n "$SSH_OPTS" ]; then
                echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET${NC}"
            else
                echo -e "${YELLOW}  ssh $SSH_TARGET${NC}"
            fi
            echo -e "${YELLOW}  cd ~/lnpay/deployment${NC}"
            echo -e "${YELLOW}  mv .env.example .env${NC}"
            echo -e "${YELLOW}  nano .env  # Update with your actual values${NC}"
            echo ""
        else
            echo -e "${GREEN}✓ .env file already exists on remote (in deployment/ directory)${NC}"
        fi
    else
        echo -e "${YELLOW}Note: .env.example not found locally, skipping${NC}"
    fi
    
    # Copy certificates or generation script
    if [ "$COPY_CERTS" = "true" ]; then
        echo "Copying certificates..."
        scp $SSH_OPTS $SSH_MULTIPLEX_OPTS -r "$CERTS_DIR"/* "$SSH_TARGET:$REMOTE_DIR/infrastructure/certs/"
    else
        echo "Copying certificate generation script..."
        scp $SSH_OPTS $SSH_MULTIPLEX_OPTS "$CERTS_DIR/generate-certs.sh" "$SSH_TARGET:$REMOTE_DIR/infrastructure/certs/"
        
        # Generate certificates on remote
        echo "Generating certificates on remote machine..."
        run_cmd "cd $REMOTE_DIR && ./infrastructure/certs/generate-certs.sh"
    fi
    
    # Set permissions on remote
    # Note: server.key needs to be readable by mosquitto user (UID 1883 typically)
    # We use 644 (readable by all) to ensure mosquitto can read it, even if UID doesn't match
    echo "Setting certificate permissions..."
    run_cmd "chmod 644 $REMOTE_DIR/infrastructure/certs/*.crt 2>/dev/null || true"
    run_cmd "chmod 644 $REMOTE_DIR/infrastructure/certs/*.key 2>/dev/null || true"
    echo -e "${YELLOW}Note: Using 644 permissions for server.key to ensure mosquitto can read it${NC}"
    
    echo -e "${GREEN}✓ Certificates ready on remote machine${NC}"
    
else
    # Local deployment: check/generate certificates
    if [ -d "$CERTS_DIR" ] && [ -f "$CERTS_DIR/ca.crt" ] && [ -f "$CERTS_DIR/server.crt" ] && [ -f "$CERTS_DIR/server.key" ]; then
        echo -e "${GREEN}✓ Certificates already exist${NC}"
    else
        echo "No certificates found. Generating..."
        
        if [ -f "$CERTS_DIR/generate-certs.sh" ]; then
            mkdir -p "$CERTS_DIR"
            "$CERTS_DIR/generate-certs.sh"
            echo -e "${GREEN}✓ Certificates generated${NC}"
        else
            echo -e "${RED}ERROR: Certificate generation script not found${NC}"
            echo "Please provide certificates manually or ensure certs/generate-certs.sh exists"
            exit 1
        fi
    fi
    # Set permissions on local certificates
    # Note: server.key needs to be readable by mosquitto user (UID 1883 typically)
    # We use 644 (readable by all) to ensure mosquitto can read it, even if UID doesn't match
    echo "Setting certificate permissions..."
    chmod 644 "$CERTS_DIR"/*.crt 2>/dev/null || true
    chmod 644 "$CERTS_DIR"/*.key 2>/dev/null || true
    echo -e "${YELLOW}Note: Using 644 permissions for server.key to ensure mosquitto can read it${NC}"
fi

# Verify certificates on target
echo ""
echo -e "${BLUE}Step 3: Verifying Setup${NC}"
echo "------------------------"

if [ "$REMOTE_DEPLOY" = "true" ]; then
    REMOTE_COMPOSE_FILE="$REMOTE_DIR/deployment/$COMPOSE_FILE_BASENAME"
    REMOTE_CERTS_DIR="$REMOTE_DIR/infrastructure/certs"
else
    REMOTE_COMPOSE_FILE="$COMPOSE_FILE"
    REMOTE_CERTS_DIR="$CERTS_DIR"
fi

# Check certificates exist
if run_cmd "test -f $REMOTE_CERTS_DIR/ca.crt && test -f $REMOTE_CERTS_DIR/server.crt && test -f $REMOTE_CERTS_DIR/server.key"; then
    echo -e "${GREEN}✓ All required certificates found${NC}"
else
    echo -e "${RED}ERROR: Required certificates missing${NC}"
    exit 1
fi

# Check compose file exists
if run_cmd "test -f $REMOTE_COMPOSE_FILE"; then
    echo -e "${GREEN}✓ docker-compose.prod.yml found${NC}"
else
    echo -e "${RED}ERROR: docker-compose.prod.yml not found${NC}"
    exit 1
fi

# Pull images
echo ""
echo -e "${BLUE}Step 4: Pulling Docker Images${NC}"
echo "-------------------------------"

if [ "$REMOTE_DEPLOY" = "true" ]; then
    echo "Pulling images on remote machine..."
    run_cmd "cd $REMOTE_DIR/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME pull"
else
    echo "Pulling images..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" pull
fi

echo -e "${GREEN}✓ Images pulled successfully${NC}"

# Print instructions
echo ""
echo -e "${GREEN}=========================================="
echo -e "Deployment Setup Complete!${NC}"
echo -e "${GREEN}=========================================="
echo ""

if [ "$REMOTE_DEPLOY" = "true" ]; then
    echo -e "${BLUE}Remote machine: $SSH_TARGET${NC}"
    echo -e "${BLUE}Deployment base directory: $REMOTE_DIR${NC}"
    echo ""
    
    # Check if .env needs to be set up (in deployment directory)
    if run_cmd "test -f $REMOTE_DIR/deployment/.env.example && ! test -f $REMOTE_DIR/deployment/.env" 2>/dev/null; then
        echo -e "${YELLOW}⚠ Before starting services, configure .env file:${NC}"
        if [ -n "$SSH_OPTS" ]; then
            echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET${NC}"
        else
            echo -e "${YELLOW}  ssh $SSH_TARGET${NC}"
        fi
        echo -e "${YELLOW}  cd ~/lnpay/deployment${NC}"
        echo -e "${YELLOW}  mv .env.example .env${NC}"
        echo -e "${YELLOW}  nano .env  # Update with your actual values${NC}"
        echo ""
    fi
    
    echo "To start the services, SSH into the machine and run:"
    echo ""
    if [ -n "$SSH_OPTS" ]; then
        echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET${NC}"
    else
        echo -e "${YELLOW}  ssh $SSH_TARGET${NC}"
    fi
    echo -e "${YELLOW}  cd ~/lnpay/deployment${NC}"
    echo -e "${YELLOW}  $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME up -d${NC}"
    echo ""
    echo "Or run from local machine:"
    if [ -n "$SSH_OPTS" ]; then
        echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME up -d\"${NC}"
    else
        echo -e "${YELLOW}  ssh $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME up -d\"${NC}"
    fi
    echo ""
    echo "To check status:"
    if [ -n "$SSH_OPTS" ]; then
        echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME ps\"${NC}"
    else
        echo -e "${YELLOW}  ssh $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME ps\"${NC}"
    fi
    echo ""
    echo "To view logs:"
    if [ -n "$SSH_OPTS" ]; then
        echo -e "${YELLOW}  ssh $SSH_OPTS $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME logs -f\"${NC}"
    else
        echo -e "${YELLOW}  ssh $SSH_TARGET \"cd ~/lnpay/deployment && $DOCKER_COMPOSE -f $COMPOSE_FILE_BASENAME logs -f\"${NC}"
    fi
else
    echo "To start the services, run:"
    echo ""
    echo -e "${YELLOW}  $DOCKER_COMPOSE -f $COMPOSE_FILE up -d${NC}"
    echo ""
    echo "To check status:"
    echo -e "${YELLOW}  $DOCKER_COMPOSE -f $COMPOSE_FILE ps${NC}"
    echo ""
    echo "To view logs:"
    echo -e "${YELLOW}  $DOCKER_COMPOSE -f $COMPOSE_FILE logs -f${NC}"
fi

echo ""
