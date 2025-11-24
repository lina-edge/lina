#!/bin/sh
# Generate password file for Mosquitto MQTT broker (container version)
# Creates a password file with credentials for the device service

set -e

PASSWD_FILE="/mosquitto/config/passwd"

# Get username and password from environment or use defaults
USERNAME="${MQTT_USERNAME:-device-service}"
PASSWORD="${MQTT_PASSWORD:-}"

# If password not provided, generate a random one
if [ -z "$PASSWORD" ]; then
    # Generate a random password (16 characters, alphanumeric)
    PASSWORD=$(openssl rand -base64 12 | tr -d "=+/" | cut -c1-16)
    echo "No password provided, generating random password"
fi

# Get dynsec admin credentials from environment
DYNSEC_ADMIN_USER="${MQTT_DYNSEC_ADMIN_USER:-admin}"
DYNSEC_ADMIN_PASSWORD="${MQTT_DYNSEC_ADMIN_PASSWORD:-admin}"

# Check if password file already exists
FILE_EXISTS=false
if [ -f "$PASSWD_FILE" ]; then
    echo "Password file already exists: $PASSWD_FILE"
    FILE_EXISTS=true
else
    echo "Creating new password file"
fi

# Create or update device-service user
if [ "$FILE_EXISTS" = false ]; then
    echo "Adding user: $USERNAME"
    echo -e "$PASSWORD\n$PASSWORD" | mosquitto_passwd -c "$PASSWD_FILE" "$USERNAME"
else
    echo "Updating user: $USERNAME"
    echo -e "$PASSWORD\n$PASSWORD" | mosquitto_passwd "$PASSWD_FILE" "$USERNAME"
fi

# Add or update dynsec admin user (without -c flag to append/update)
echo "Adding/updating dynsec admin user: $DYNSEC_ADMIN_USER"
echo -e "$DYNSEC_ADMIN_PASSWORD\n$DYNSEC_ADMIN_PASSWORD" | mosquitto_passwd "$PASSWD_FILE" "$DYNSEC_ADMIN_USER"

if [ $? -eq 0 ]; then
    # Set proper permissions and ownership so mosquitto user can read it
    # mosquitto requires 0700 permissions (read/write/execute for owner only)
    # mosquitto user typically has UID 1883 in eclipse-mosquitto image
    if id -u mosquitto >/dev/null 2>&1; then
        chown mosquitto:mosquitto "$PASSWD_FILE"
        chmod 0700 "$PASSWD_FILE"
    else
        # If mosquitto user doesn't exist, use 0600 (read/write for owner only)
        chmod 0600 "$PASSWD_FILE"
    fi
    # Also ensure the config directory is accessible
    chmod 755 "$(dirname "$PASSWD_FILE")"
    
    echo "Password file created successfully: $PASSWD_FILE"
    echo "Added users: $USERNAME, $DYNSEC_ADMIN_USER"
    # echo "Password: $PASSWORD"
else
    echo "Failed to create password file"
    exit 1
fi

