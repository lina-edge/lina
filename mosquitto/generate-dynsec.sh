#!/bin/sh
# Set Default Admin Credentials for Dynamic Security Plugin Configuration
DEFAULT_DYNSEC_ADMIN=admin
DEFAULT_DYNSEC_ADMIN_PASSWORD=admin

# Set values if provided via Environment Variables in the Docker Init Container
MQTT_DYNSEC_ADMIN_USER=${MQTT_DYNSEC_ADMIN_USER:-$DEFAULT_DYNSEC_ADMIN}
MQTT_DYNSEC_ADMIN_PASSWORD=${MQTT_DYNSEC_ADMIN_PASSWORD:-$DEFAULT_DYNSEC_ADMIN_PASSWORD}

# Connection options for mosquitto_ctrl (using TLS on localhost)
CTRL_OPTS="-h 127.0.0.1 -p 8883 --cafile /mosquitto/certs/ca.crt -u ${MQTT_DYNSEC_ADMIN_USER} -P ${MQTT_DYNSEC_ADMIN_PASSWORD}"

# Check the mode
MODE=${1:-full}

if [ "$MODE" = "init-only" ]; then
    # Only initialize the dynamic-security.json file (doesn't need a running broker)
    echo "Initializing dynamic-security.json file..."
    mosquitto_ctrl dynsec init /mosquitto/data/dynamic-security.json ${MQTT_DYNSEC_ADMIN_USER} ${MQTT_DYNSEC_ADMIN_PASSWORD}
    exit $?
fi

# Configure mode - requires a running broker
if [ "$MODE" != "configure" ] && [ "$MODE" != "full" ]; then
    echo "Unknown mode: $MODE"
    exit 1
fi

# If full mode, initialize first
if [ "$MODE" = "full" ]; then
    echo "Initializing dynamic-security.json file..."
    mosquitto_ctrl dynsec init /mosquitto/data/dynamic-security.json ${MQTT_DYNSEC_ADMIN_USER} ${MQTT_DYNSEC_ADMIN_PASSWORD}
    if [ $? -ne 0 ]; then
        echo "Failed to initialize dynamic-security.json"
        exit 1
    fi
fi

# Add user with provided credentials, defaulting as needed
MQTT_USERNAME=${MQTT_USERNAME:-device-service}
MQTT_PASSWORD=${MQTT_PASSWORD:-}

# If password is not provided, generate a random one (16 alphanum chars)
if [ -z "$MQTT_PASSWORD" ]; then
    MQTT_PASSWORD=$(openssl rand -base64 12 | tr -d "=+/" | cut -c1-16)
    echo "No MQTT_PASSWORD provided, generated random password: $MQTT_PASSWORD"
fi

# Create the user
echo "Creating user: $MQTT_USERNAME"
# Note: createClient syntax is: createClient <username> <password>
# The password is passed as a positional argument, not a flag
mosquitto_ctrl $CTRL_OPTS dynsec createClient "$MQTT_USERNAME" "$MQTT_PASSWORD"
if [ $? -ne 0 ]; then
    echo "Failed to create user"
    exit 1
fi

# Create healtcheck_role for test/health topic access
echo "Creating role: healtcheck_role for test/health topic access"
mosquitto_ctrl $CTRL_OPTS dynsec createRole healtcheck_role
if [ $? -ne 0 ]; then
    echo "Failed to create role healtcheck_role"
    exit 1
fi

# Allow subscribe/publish to test/health
# aclspec format: <acltype> <topicFilter> allow|deny
echo "Adding ACL for healtcheck_role to allow subscribe,publish for topic test/health"
mosquitto_ctrl $CTRL_OPTS dynsec addRoleACL healtcheck_role publishClientSend test/health allow 1
if [ $? -ne 0 ]; then
    echo "Failed to add publish ACL for healtcheck_role"
    exit 1
fi
mosquitto_ctrl $CTRL_OPTS dynsec addRoleACL healtcheck_role subscribePattern test/health allow 1
if [ $? -ne 0 ]; then
    echo "Failed to add subscribe ACL for healtcheck_role"
    exit 1
fi

# Create devices_role for /devices/# wildcard topic access
echo "Creating role: devices_role for /devices/# wildcard topic access"
mosquitto_ctrl $CTRL_OPTS dynsec createRole devices_role
if [ $? -ne 0 ]; then
    echo "Failed to create role devices_role"
    exit 1
fi

# Allow subscribe/publish to /devices/# (all topics under /devices/)
# aclspec format: <acltype> <topicFilter> allow|deny
echo "Adding ACL for devices_role to allow subscribe,publish for topic /devices/#"
mosquitto_ctrl $CTRL_OPTS dynsec addRoleACL devices_role publishClientSend '/devices/#' allow 1
if [ $? -ne 0 ]; then
    echo "Failed to add publish ACL for devices_role"
    exit 1
fi
mosquitto_ctrl $CTRL_OPTS dynsec addRoleACL devices_role subscribePattern '/devices/#' allow 1
if [ $? -ne 0 ]; then
    echo "Failed to add subscribe ACL for devices_role"
    exit 1
fi

# Assign both roles to the created user
echo "Assigning role healtcheck_role to user $MQTT_USERNAME"
mosquitto_ctrl $CTRL_OPTS dynsec addClientRole "$MQTT_USERNAME" healtcheck_role 1
if [ $? -ne 0 ]; then
    echo "Failed to assign role healtcheck_role to user"
    exit 1
fi

echo "Assigning role devices_role to user $MQTT_USERNAME"
mosquitto_ctrl $CTRL_OPTS dynsec addClientRole "$MQTT_USERNAME" devices_role 1
if [ $? -ne 0 ]; then
    echo "Failed to assign role devices_role to user"
    exit 1
fi

echo "Dynamic security configuration completed successfully"
