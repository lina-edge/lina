#!/bin/sh
# Entrypoint script for Mosquitto MQTT broker

# Generate password file (optional - only needed if password_file is enabled in mosquitto.conf)
# Uncomment the lines below if you need password_file authentication:
# /mosquitto/generate-passwd.sh
# if [ $? -ne 0 ]; then
#     echo "Failed to generate password file"
#     exit 1
# fi
# echo "Password file generated successfully"

# Runs generate-dynsec.sh on first startup if dynamic-security.json doesn't exist

DYNSEC_FILE="/mosquitto/data/dynamic-security.json"
DYNSEC_DIR="/mosquitto/data"

# Ensure data directory exists and has correct permissions
mkdir -p "$DYNSEC_DIR"
chmod 755 "$DYNSEC_DIR"

# Check if dynamic-security.json exists, if not, generate it
if [ ! -f "$DYNSEC_FILE" ]; then
    echo "dynamic-security.json not found. Generating it..."
    
    # First, initialize the dynamic-security.json file (this doesn't need a running broker)
    /mosquitto/generate-dynsec.sh init-only
    if [ $? -ne 0 ]; then
        echo "Failed to initialize dynamic-security.json"
        exit 1
    fi
    
    # Fix permissions so mosquitto can read the file
    # Security requirement: file must be 0700 (readable/writable only by owner)
    # Try to set ownership to mosquitto user if it exists, otherwise just set permissions
    if id mosquitto >/dev/null 2>&1; then
        chown mosquitto:mosquitto "$DYNSEC_FILE" 2>/dev/null || true
        chown mosquitto:mosquitto "$DYNSEC_DIR" 2>/dev/null || true
    fi
    chmod 0700 "$DYNSEC_FILE"
    chmod 755 "$DYNSEC_DIR"
    
    # Start mosquitto in the background
    echo "Starting mosquitto in background to configure dynamic security..."
    mosquitto -c /mosquitto/config/mosquitto.conf &
    MOSQUITTO_PID=$!
    
    # Wait for mosquitto to be ready (accepting connections and plugin loaded)
    echo "Waiting for mosquitto to be ready..."
    MAX_ATTEMPTS=30
    ATTEMPT=0
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        # Try to connect - if we get "Connection refused", broker isn't ready yet
        OUTPUT=$(mosquitto_sub -h 127.0.0.1 -p 8883 --cafile /mosquitto/certs/ca.crt -t 'test/health' -W 1 -C 1 -i healthcheck-test 2>&1)
        if echo "$OUTPUT" | grep -q "Connection refused"; then
            ATTEMPT=$((ATTEMPT + 1))
            sleep 1
        else
            # Any other result means broker is accepting connections
            # Wait a bit more to ensure dynamic security plugin is fully loaded
            sleep 3
            echo "Mosquitto is ready"
            break
        fi
    done
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "Mosquitto failed to start within timeout"
        kill $MOSQUITTO_PID 2>/dev/null
        exit 1
    fi
    
    # Now configure dynamic security (this needs a running broker)
    /mosquitto/generate-dynsec.sh configure
    if [ $? -ne 0 ]; then
        echo "Failed to configure dynamic-security.json"
        kill $MOSQUITTO_PID 2>/dev/null
        exit 1
    fi
    
    # Stop the background mosquitto
    echo "Stopping background mosquitto..."
    kill $MOSQUITTO_PID 2>/dev/null
    wait $MOSQUITTO_PID 2>/dev/null
    
    echo "dynamic-security.json generated and configured successfully"
else
    echo "dynamic-security.json already exists, skipping generation"
fi

# Start mosquitto with the provided command
exec "$@"

