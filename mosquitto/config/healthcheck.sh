#!/bin/sh
# Healthcheck script for Mosquitto MQTT broker
# Checks if the broker process is running and listening on port 8883
# This approach avoids making any connections, preventing SSL/auth errors in logs

# Check if mosquitto process is running
# Use ps without aux flags for better compatibility (works as non-root user)
# The [m] pattern prevents grep from matching itself
if ! ps | grep -q '[m]osquitto'; then
    exit 1
fi

# Check if port 8883 is in LISTEN state (broker is ready to accept connections)
# Use 'ss' (socket statistics) which is part of iproute2 in Alpine
# -l: show only listening sockets
# -n: don't resolve service names
# -t: TCP sockets
# Exit code 0 if port is found, 1 if not
if command -v ss >/dev/null 2>&1; then
    ss -lnt | grep -q ':8883 '
    exit $?
elif command -v netstat >/dev/null 2>&1; then
    netstat -lnt | grep -q ':8883 '
    exit $?
else
    # Fallback: if neither ss nor netstat is available, just check process
    # This is less ideal but better than nothing
    exit 0
fi

