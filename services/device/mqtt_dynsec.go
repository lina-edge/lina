package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	lina "github.com/robertodantas/lina/internal"
)

// MQTTDynSecService handles dynamic security plugin operations via MQTT topic API
type MQTTDynSecService struct {
	client      mqtt.Client
	responseCh  chan map[string]interface{}
	commandID   int
	commandIDMu sync.Mutex
}

// NewMQTTDynSecService creates a new dynamic security service using MQTT topic API
func NewMQTTDynSecService(ctx context.Context, cfg Config) (*MQTTDynSecService, error) {
	broker := cfg.MQTTBroker
	useTLS := cfg.MQTTUseTLS

	var port int
	var protocol string
	if useTLS {
		port = cfg.MQTTTLSPort
		protocol = cfg.MQTTTLSProtocol
	} else {
		port = cfg.MQTTPort
		protocol = "tcp"
	}

	adminUser := cfg.MQTTDynSecAdminUser
	adminPass := cfg.MQTTDynSecAdminPassword
	clientID := fmt.Sprintf("dynsec-admin-%d", time.Now().Unix())

	logger.InfoWithFields(ctx, "Connecting to MQTT broker for dynamic security on southbound mqtt", map[string]interface{}{
		"protocol": protocol,
		"broker":   broker,
		"port":     port,
	})

	dial := lina.MQTTConnectConfig{
		Connection: lina.MQTTConnectionSpec{
			ClientID:       clientID,
			Username:       adminUser,
			Password:       adminPass,
			UseTLS:         useTLS,
			Broker:         broker,
			Port:           port,
			Protocol:       protocol,
			ConnectTimeout: 30 * time.Second,
			KeepAlive:      60 * time.Second,
		},
	}
	if useTLS {
		dial.TLS = &lina.MQTTTLSParams{
			BrokerHost:      broker,
			SkipVerify:      cfg.MQTTTLSSkipVerify,
			ServerName:      cfg.MQTTTLSServerName,
			CACertPath:      cfg.MQTTTLSCACert,
			RequireEdgeCert: cfg.MQTTTLSRequireEdgeCert,
			EdgeCertPath:    cfg.MQTTTLSEdgeCert,
			EdgeKeyPath:     cfg.MQTTTLSEdgeKey,
		}
	}
	adminClient, err := lina.ConnectMQTT(dial, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", err)
	}
	client := adminClient.GetClient()

	service := &MQTTDynSecService{
		client:     client,
		responseCh: make(chan map[string]interface{}, 100), // Increased buffer to handle multiple concurrent commands
		commandID:  1,
	}

	// Subscribe to response topic
	responseTopic := "$CONTROL/dynamic-security/v1/response"
	if token := client.Subscribe(responseTopic, 1, service.handleResponse); token.Wait() && token.Error() != nil {
		client.Disconnect(250)
		return nil, fmt.Errorf("failed to subscribe to response topic: %w", token.Error())
	}

	logger.InfoWithFields(ctx, "Connected to MQTT broker and subscribed on southbound mqtt", map[string]interface{}{
		"response_topic": responseTopic,
	})

	return service, nil
}

// handleResponse handles responses from the dynamic security plugin
func (mds *MQTTDynSecService) handleResponse(client mqtt.Client, msg mqtt.Message) {
	var response map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &response); err != nil {
		logger.Error(context.Background(), "Failed to parse dynamic security response on southbound mqtt", err)
		return
	}
	select {
	case mds.responseCh <- response:
	default:
		logger.Warn(context.Background(), "Response channel full, dropping dynamic security response on southbound mqtt")
	}
}

// getNextCommandID returns the next command ID
func (mds *MQTTDynSecService) getNextCommandID() int {
	mds.commandIDMu.Lock()
	defer mds.commandIDMu.Unlock()
	id := mds.commandID
	mds.commandID++
	return id
}

// isAlreadyExistsError checks if an error message indicates an "already exists" condition
// These are non-fatal errors that we can safely ignore
func isAlreadyExistsError(errMsg string) bool {
	errLower := strings.ToLower(errMsg)
	return strings.Contains(errLower, "already exists") ||
		strings.Contains(errLower, "role already exists") ||
		strings.Contains(errLower, "acl with this topic already exists") ||
		strings.Contains(errLower, "client already exists")
}

// listRoles lists all roles using the dynamic security API
func (mds *MQTTDynSecService) listRoles(ctx context.Context) ([]string, error) {
	commandID := mds.getNextCommandID()
	command := map[string]interface{}{
		"commands": []map[string]interface{}{
			{
				"command": "listRoles",
			},
		},
	}

	// Drain old responses
	for {
		select {
		case <-mds.responseCh:
		default:
			goto sendCommand
		}
	}
sendCommand:

	commandJSON, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	controlTopic := "$CONTROL/dynamic-security/v1"
	logger.DebugWithFields(ctx, "Listing roles on southbound mqtt", map[string]interface{}{
		"command_id": commandID,
	})

	token := mds.client.Publish(controlTopic, 1, false, commandJSON)
	if !token.WaitTimeout(5 * time.Second) {
		return nil, fmt.Errorf("timeout publishing listRoles command")
	}
	if token.Error() != nil {
		return nil, fmt.Errorf("failed to publish listRoles command: %w", token.Error())
	}

	timeout := time.After(10 * time.Second)
	select {
	case response := <-mds.responseCh:
		// Parse response format: response["responses"][0]["data"]["roles"]
		if resp, ok := response["responses"].([]interface{}); ok && len(resp) > 0 {
			if respMap, ok := resp[0].(map[string]interface{}); ok {
				// Check for error first
				if errMsg, hasErr := respMap["error"]; hasErr {
					return nil, fmt.Errorf("listRoles failed: %v", errMsg)
				}
				// Extract roles from data field
				if data, ok := respMap["data"].(map[string]interface{}); ok {
					if roles, ok := data["roles"].([]interface{}); ok {
						roleNames := make([]string, 0, len(roles))
						for _, role := range roles {
							// Roles can be strings or objects with rolename field
							if roleStr, ok := role.(string); ok {
								roleNames = append(roleNames, roleStr)
							} else if roleMap, ok := role.(map[string]interface{}); ok {
								if rolename, ok := roleMap["rolename"].(string); ok {
									roleNames = append(roleNames, rolename)
								}
							}
						}
						return roleNames, nil
					}
				}
			}
		}
		// Log the response for debugging
		responseJSON, _ := json.MarshalIndent(response, "", "  ")
		logger.WarnWithFields(ctx, "Unexpected listRoles response format on southbound mqtt", map[string]interface{}{
			"response": string(responseJSON),
		})
		return nil, fmt.Errorf("unexpected response format from listRoles")
	case <-timeout:
		return nil, fmt.Errorf("timeout waiting for listRoles response")
	}
}

// listClients lists all clients using the dynamic security API
func (mds *MQTTDynSecService) listClients(ctx context.Context) ([]string, error) {
	commandID := mds.getNextCommandID()
	command := map[string]interface{}{
		"commands": []map[string]interface{}{
			{
				"command": "listClients",
			},
		},
	}

	// Drain old responses
	for {
		select {
		case <-mds.responseCh:
		default:
			goto sendCommand
		}
	}
sendCommand:

	commandJSON, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	controlTopic := "$CONTROL/dynamic-security/v1"
	logger.InfoWithFields(ctx, "Listing clients on southbound mqtt", map[string]interface{}{
		"command_id": commandID,
	})

	token := mds.client.Publish(controlTopic, 1, false, commandJSON)
	if !token.WaitTimeout(5 * time.Second) {
		return nil, fmt.Errorf("timeout publishing listClients command")
	}
	if token.Error() != nil {
		return nil, fmt.Errorf("failed to publish listClients command: %w", token.Error())
	}

	timeout := time.After(10 * time.Second)
	select {
	case response := <-mds.responseCh:
		// Parse response format: response["responses"][0]["data"]["clients"]
		if resp, ok := response["responses"].([]interface{}); ok && len(resp) > 0 {
			if respMap, ok := resp[0].(map[string]interface{}); ok {
				// Check for error first
				if errMsg, hasErr := respMap["error"]; hasErr {
					return nil, fmt.Errorf("listClients failed: %v", errMsg)
				}
				// Extract clients from data field
				if data, ok := respMap["data"].(map[string]interface{}); ok {
					if clients, ok := data["clients"].([]interface{}); ok {
						clientNames := make([]string, 0, len(clients))
						for _, client := range clients {
							// Clients are returned as strings (usernames)
							if clientStr, ok := client.(string); ok {
								clientNames = append(clientNames, clientStr)
							} else if clientMap, ok := client.(map[string]interface{}); ok {
								// Handle object format if it exists
								if username, ok := clientMap["username"].(string); ok {
									clientNames = append(clientNames, username)
								}
							}
						}
						return clientNames, nil
					}
				}
			}
		}
		// Log the response for debugging
		responseJSON, _ := json.MarshalIndent(response, "", "  ")
		logger.WarnWithFields(ctx, "Unexpected listClients response format on southbound mqtt", map[string]interface{}{
			"response": string(responseJSON),
		})
		return nil, fmt.Errorf("unexpected response format from listClients")
	case <-timeout:
		return nil, fmt.Errorf("timeout waiting for listClients response")
	}
}

// roleExists checks if a role exists
func (mds *MQTTDynSecService) roleExists(ctx context.Context, roleName string) (bool, error) {
	roles, err := mds.listRoles(ctx)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		if role == roleName {
			return true, nil
		}
	}
	return false, nil
}

// clientExists checks if a client exists
func (mds *MQTTDynSecService) clientExists(ctx context.Context, username string) (bool, error) {
	clients, err := mds.listClients(ctx)
	if err != nil {
		return false, err
	}
	for _, client := range clients {
		if client == username {
			return true, nil
		}
	}
	return false, nil
}

// listGroups lists all groups using the dynamic security API
func (mds *MQTTDynSecService) listGroups(ctx context.Context) ([]string, error) {
	commandID := mds.getNextCommandID()
	command := map[string]interface{}{
		"commands": []map[string]interface{}{
			{
				"command": "listGroups",
			},
		},
	}

	// Drain old responses
	for {
		select {
		case <-mds.responseCh:
		default:
			goto sendCommand
		}
	}
sendCommand:

	commandJSON, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %w", err)
	}

	controlTopic := "$CONTROL/dynamic-security/v1"
	logger.DebugWithFields(ctx, "Listing groups on southbound mqtt", map[string]interface{}{
		"command_id": commandID,
	})

	token := mds.client.Publish(controlTopic, 1, false, commandJSON)
	if !token.WaitTimeout(5 * time.Second) {
		return nil, fmt.Errorf("timeout publishing listGroups command")
	}
	if token.Error() != nil {
		return nil, fmt.Errorf("failed to publish listGroups command: %w", token.Error())
	}

	timeout := time.After(10 * time.Second)
	select {
	case response := <-mds.responseCh:
		// Parse response format: response["responses"][0]["data"]["groups"]
		if resp, ok := response["responses"].([]interface{}); ok && len(resp) > 0 {
			if respMap, ok := resp[0].(map[string]interface{}); ok {
				// Check for error first
				if errMsg, hasErr := respMap["error"]; hasErr {
					return nil, fmt.Errorf("listGroups failed: %v", errMsg)
				}
				// Extract groups from data field
				if data, ok := respMap["data"].(map[string]interface{}); ok {
					if groups, ok := data["groups"].([]interface{}); ok {
						groupNames := make([]string, 0, len(groups))
						for _, group := range groups {
							// Groups can be strings or objects with groupname field
							if groupStr, ok := group.(string); ok {
								groupNames = append(groupNames, groupStr)
							} else if groupMap, ok := group.(map[string]interface{}); ok {
								if groupname, ok := groupMap["groupname"].(string); ok {
									groupNames = append(groupNames, groupname)
								}
							}
						}
						return groupNames, nil
					}
				}
			}
		}
		// Log the response for debugging
		responseJSON, _ := json.MarshalIndent(response, "", "  ")
		logger.WarnWithFields(ctx, "Unexpected listGroups response format on southbound mqtt", map[string]interface{}{
			"response": string(responseJSON),
		})
		return nil, fmt.Errorf("unexpected response format from listGroups")
	case <-timeout:
		return nil, fmt.Errorf("timeout waiting for listGroups response")
	}
}

// groupExists checks if a group exists
func (mds *MQTTDynSecService) groupExists(ctx context.Context, groupName string) (bool, error) {
	groups, err := mds.listGroups(ctx)
	if err != nil {
		return false, err
	}
	for _, group := range groups {
		if group == groupName {
			return true, nil
		}
	}
	return false, nil
}

// executeCommand sends a command to the dynamic security plugin and waits for response
func (mds *MQTTDynSecService) executeCommand(ctx context.Context, command map[string]interface{}) error {
	commandID := mds.getNextCommandID()
	// Do not add a root-level "command" key: the broker plugin only accepts {"commands":[...]}.

	// Drain any old responses from the channel to ensure we get the response for this command
	// This prevents matching responses from previous commands
	drained := 0
	for {
		select {
		case <-mds.responseCh:
			drained++
			if drained == 1 {
				logger.Debugf(ctx, "Draining old responses before command %d on southbound mqtt", commandID)
			}
		default:
			// No more old responses
			if drained > 0 {
				logger.DebugWithFields(ctx, "Drained old responses on southbound mqtt", map[string]interface{}{
					"drained":    drained,
					"command_id": commandID,
				})
			}
			goto sendCommand
		}
	}
sendCommand:

	commandJSON, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	controlTopic := "$CONTROL/dynamic-security/v1"
	logger.DebugWithFields(ctx, "Publishing dynamic security command on southbound mqtt", map[string]interface{}{
		"command_id": commandID,
		"topic":      controlTopic,
		"command":    string(commandJSON),
	})

	token := mds.client.Publish(controlTopic, 1, false, commandJSON)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("timeout publishing command")
	}
	if token.Error() != nil {
		return fmt.Errorf("failed to publish command: %w", token.Error())
	}

	// Wait for response with timeout
	// Note: Mosquitto Dynamic Security API doesn't echo back the command ID in responses,
	// so we accept the first response that arrives after sending the command.
	// This works because we process commands sequentially and we've drained old responses.
	timeout := time.After(10 * time.Second)

	select {
	case response := <-mds.responseCh:
		// Log the full response for debugging
		responseJSON, _ := json.MarshalIndent(response, "", "  ")
		logger.DebugWithFields(ctx, "Received dynamic security response on southbound mqtt", map[string]interface{}{
			"command_id": commandID,
			"response":   string(responseJSON),
		})

		// Check for errors in response (handle both single and batched commands)
		if resp, ok := response["responses"].([]interface{}); ok && len(resp) > 0 {
			// Check all responses in the batch for errors
			var errors []string
			var alreadyExistsCount int
			for i, r := range resp {
				if respMap, ok := r.(map[string]interface{}); ok {
					if errMsg, hasErr := respMap["error"]; hasErr {
						errStr := fmt.Sprintf("%v", errMsg)
						// Check if this is an "already exists" error (non-fatal)
						if isAlreadyExistsError(errStr) {
							alreadyExistsCount++
							logger.DebugWithFields(ctx, "Command response (already exists) on southbound mqtt", map[string]interface{}{
								"command_id":     commandID,
								"response_index": i,
								"error":          errStr,
							})
						} else {
							errorJSON, _ := json.MarshalIndent(respMap, "", "  ")
							logger.ErrorWithFields(ctx, "Command response failed on southbound mqtt", fmt.Errorf("%v", errMsg), map[string]interface{}{
								"command_id":     commandID,
								"response_index": i,
								"error":          string(errorJSON),
							})
							errors = append(errors, fmt.Sprintf("response %d: %v", i, errMsg))
						}
					}
				}
			}
			if len(errors) > 0 {
				return fmt.Errorf("command failed with %d error(s): %v", len(errors), errors)
			}
			if alreadyExistsCount > 0 {
				logger.InfoWithFields(ctx, "Command completed with warnings on southbound mqtt", map[string]interface{}{
					"command_id":           commandID,
					"already_exists_count": alreadyExistsCount,
					"total_responses":      len(resp),
				})
			} else {
				logger.InfoWithFields(ctx, "Command executed successfully on southbound mqtt", map[string]interface{}{
					"command_id":     commandID,
					"response_count": len(resp),
				})
			}
		} else {
			logger.InfoWithFields(ctx, "Command executed successfully on southbound mqtt", map[string]interface{}{
				"command_id": commandID,
			})
		}
		return nil
	case <-timeout:
		return fmt.Errorf("timeout waiting for response to command %d", commandID)
	}
}

// ProvisionDevice provisions a single device by delegating to batch provisioning.
func (mds *MQTTDynSecService) ProvisionDevice(ctx context.Context, deviceID, password string) error {
	logger.WithDeviceID(deviceID).Info(ctx, "Provisioning single device via batch path on southbound mqtt")
	device := &Device{DeviceID: deviceID}
	deviceSecrets := map[string]string{deviceID: password}
	if err := mds.ProvisionDevicesBatch(ctx, []*Device{device}, deviceSecrets, "", "", ""); err != nil {
		return fmt.Errorf("failed to provision device %s via batch path: %w", deviceID, err)
	}
	logger.WithDeviceID(deviceID).Info(ctx, "Successfully provisioned device via batch path on southbound mqtt")
	return nil
}

// GetDeviceCredentials returns the credentials for a provisioned device
func (mds *MQTTDynSecService) GetDeviceCredentials(deviceID string) (username, password string) {
	username = deviceID
	password = fmt.Sprintf("%s_password", deviceID)
	return username, password
}

// ProvisionDevicesBatch provisions multiple devices in batch using the shared device_role
// All devices are assigned to the same role which is created once during service initialization
func (mds *MQTTDynSecService) ProvisionDevicesBatch(ctx context.Context, devices []*Device, deviceSecrets map[string]string, groupName, roleName, deviceIDPattern string) error {
	if len(devices) == 0 {
		return nil
	}

	// Use the shared device_role for all batch-provisioned devices
	sharedRoleName := "device_role"

	logger.Infof(ctx, "Starting batch provisioning of %d devices in dynsec with shared role %s", len(devices), sharedRoleName)

	// Step 2: Create all clients in chunks to avoid command limits
	// Chunk size: 100 commands per batch (reasonable limit to avoid MQTT message size issues)
	dynsecCommandChunkSize := 100
	logger.Infof(ctx, "Creating %d clients in chunks of %d", len(devices), dynsecCommandChunkSize)

	for chunkStart := 0; chunkStart < len(devices); chunkStart += dynsecCommandChunkSize {
		chunkEnd := chunkStart + dynsecCommandChunkSize
		if chunkEnd > len(devices) {
			chunkEnd = len(devices)
		}
		chunk := devices[chunkStart:chunkEnd]

		createClientCommands := make([]map[string]interface{}, 0, len(chunk))
		for _, device := range chunk {
			clientUsername := device.DeviceID
			clientPassword := deviceSecrets[device.DeviceID]
			// Create client with role already assigned
			createClientCommands = append(createClientCommands, map[string]interface{}{
				"command":  "createClient",
				"username": clientUsername,
				"password": clientPassword,
				"roles": []map[string]interface{}{
					{
						"rolename": sharedRoleName,
						"priority": 5,
					},
				},
			})
		}

		createClientsCmd := map[string]interface{}{
			"commands": createClientCommands,
		}
		if err := mds.executeCommand(ctx, createClientsCmd); err != nil {
			logger.Warnf(ctx, "Some clients in chunk %d-%d may have failed to create (non-fatal if already exist): %v", chunkStart, chunkEnd-1, err)
		} else {
			logger.Infof(ctx, "Created client chunk %d-%d with role %s (%d clients)", chunkStart, chunkEnd-1, sharedRoleName, len(chunk))
		}

		// Add delay between chunks (except after the last chunk) to allow MQTT to process
		if chunkEnd < len(devices) {
			time.Sleep(1 * time.Second) // 1 second delay between chunks
		}
	}

	logger.Infof(ctx, "Completed batch provisioning of %d devices in dynsec with shared role %s", len(devices), sharedRoleName)
	return nil
}

// ProvisionDeviceService provisions the device service user with ACLs to subscribe to device topics
func (mds *MQTTDynSecService) ProvisionDeviceService(ctx context.Context, username, password string) error {
	if username == "" {
		return fmt.Errorf("device service username is required")
	}

	roleName := "device_service_role"

	// Step 1: Check if role exists, create if missing
	logger.DebugWithFields(ctx, "Checking if device service role exists on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
	})
	roleExists, err := mds.roleExists(ctx, roleName)
	if err != nil {
		logger.Warnf(ctx, "Failed to check if role exists on southbound mqtt, will attempt to create: %v", err)
		roleExists = false
	}

	if !roleExists {
		logger.InfoWithFields(ctx, "Creating device service role on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
		createRoleCmd := map[string]interface{}{
			"commands": []map[string]interface{}{
				{
					"command":  "createRole",
					"rolename": roleName,
				},
			},
		}
		if err := mds.executeCommand(ctx, createRoleCmd); err != nil {
			return fmt.Errorf("failed to create device service role %s: %w", roleName, err)
		}
		logger.InfoWithFields(ctx, "Device service role created successfully on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	} else {
		logger.DebugWithFields(ctx, "Device service role already exists, skipping creation on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	}

	// Step 2: Add subscribe ACLs for device topics
	subscribeTopics := []string{
		"/devices/+/heartbeat",
		"/devices/+/usage",
		"/devices/+/request/authorize",
		"/devices/+/request/invoice",
	}

	logger.InfoWithFields(ctx, "Adding subscribe ACLs for device service role on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
		"count":     len(subscribeTopics),
	})
	subscribeACLCommands := make([]map[string]interface{}, 0, len(subscribeTopics))
	for _, topic := range subscribeTopics {
		logger.DebugWithFields(ctx, "Adding subscribe ACL for topic on southbound mqtt", map[string]interface{}{
			"topic": topic,
		})
		subscribeACLCommands = append(subscribeACLCommands, map[string]interface{}{
			"command":  "addRoleACL",
			"rolename": roleName,
			"acltype":  "subscribePattern",
			"topic":    topic,
			"allow":    true,
			"priority": 5,
		})
	}

	addSubscribeACLCmd := map[string]interface{}{
		"commands": subscribeACLCommands,
	}
	if err := mds.executeCommand(ctx, addSubscribeACLCmd); err != nil {
		logger.Error(ctx, "Failed to add subscribe ACLs on southbound mqtt", err)
		// Continue even if some ACLs already exist
	}

	// Step 3: Add publish ACLs for device service responses
	publishTopics := []string{
		"/devices/+/response/authorize",
		"/devices/+/response/invoice",
		"/devices/+/events/invoice",
		"/devices/+/config",
		"/devices/+/control",
		"/devices/+/balance",
	}

	logger.InfoWithFields(ctx, "Adding publish ACLs for device service role on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
		"count":     len(publishTopics),
	})
	publishACLCommands := make([]map[string]interface{}, 0, len(publishTopics))
	for _, topic := range publishTopics {
		logger.DebugWithFields(ctx, "Adding publish ACL for topic on southbound mqtt", map[string]interface{}{
			"topic": topic,
		})
		publishACLCommands = append(publishACLCommands, map[string]interface{}{
			"command":  "addRoleACL",
			"rolename": roleName,
			"acltype":  "publishClientSend",
			"topic":    topic,
			"allow":    true,
			"priority": 5,
		})
	}

	addPublishACLCmd := map[string]interface{}{
		"commands": publishACLCommands,
	}
	if err := mds.executeCommand(ctx, addPublishACLCmd); err != nil {
		logger.Error(ctx, "Failed to add publish ACLs on southbound mqtt", err)
		// Continue even if some ACLs already exist
	}

	// Step 4: Check if device service client exists, create if missing
	logger.InfoWithFields(ctx, "Checking if device service client exists on southbound mqtt", map[string]interface{}{
		"username": username,
	})
	clientExists, err := mds.clientExists(ctx, username)
	if err != nil {
		logger.Warnf(ctx, "Failed to check if client exists on southbound mqtt, will attempt to create: %v", err)
		clientExists = false
	}

	if !clientExists {
		logger.InfoWithFields(ctx, "Creating device service client with role on southbound mqtt", map[string]interface{}{
			"username":  username,
			"role_name": roleName,
		})
		// Create client with role already assigned
		createClientCmd := map[string]interface{}{
			"commands": []map[string]interface{}{
				{
					"command":  "createClient",
					"username": username,
					"password": password,
					"roles": []map[string]interface{}{
						{
							"rolename": roleName,
							"priority": 5,
						},
					},
				},
			},
		}
		if err := mds.executeCommand(ctx, createClientCmd); err != nil {
			return fmt.Errorf("failed to create device service client %s: %w", username, err)
		}
		logger.InfoWithFields(ctx, "Device service client created successfully with role on southbound mqtt", map[string]interface{}{
			"username": username,
		})
	} else {
		logger.InfoWithFields(ctx, "Device service client already exists, updating password and ensuring role is assigned on southbound mqtt", map[string]interface{}{
			"username": username,
		})
		// Update password if client exists
		setPasswordCmd := map[string]interface{}{
			"commands": []map[string]interface{}{
				{
					"command":  "setClientPassword",
					"username": username,
					"password": password,
				},
			},
		}
		if err := mds.executeCommand(ctx, setPasswordCmd); err != nil {
			logger.Warnf(ctx, "Failed to update password for device service client on southbound mqtt: %v", err)
		}

		// Assign device_service_role to the existing device service client
		logger.InfoWithFields(ctx, "Assigning device service role to existing client on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
			"username":  username,
		})
		addRoleCmd := map[string]interface{}{
			"commands": []map[string]interface{}{
				{
					"command":  "addClientRole",
					"username": username,
					"rolename": roleName,
					"priority": 5,
				},
			},
		}
		if err := mds.executeCommand(ctx, addRoleCmd); err != nil {
			logger.Warnf(ctx, "Failed to assign role to device service client on southbound mqtt: %v (role might already be assigned)", err)
			// Continue even if role is already assigned
		}
	}

	logger.InfoWithFields(ctx, "Successfully provisioned device service on southbound mqtt", map[string]interface{}{
		"username": username,
	})
	return nil
}

// ProvisionDevicesAnyRole provisions a shared role for all devices with ACLs to publish and subscribe
// This role is used for batch-provisioned devices instead of creating a role per batch
func (mds *MQTTDynSecService) ProvisionDevicesAnyRole(ctx context.Context) error {
	roleName := "device_role"

	// Step 1: Check if role exists, create if missing
	logger.DebugWithFields(ctx, "Checking if shared device role exists on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
	})
	roleExists, err := mds.roleExists(ctx, roleName)
	if err != nil {
		logger.Warnf(ctx, "Failed to check if role exists on southbound mqtt, will attempt to create: %v", err)
		roleExists = false
	}

	if !roleExists {
		logger.InfoWithFields(ctx, "Creating shared device role on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
		createRoleCmd := map[string]interface{}{
			"commands": []map[string]interface{}{
				{
					"command":  "createRole",
					"rolename": roleName,
				},
			},
		}
		if err := mds.executeCommand(ctx, createRoleCmd); err != nil {
			return fmt.Errorf("failed to create shared device role %s: %w", roleName, err)
		}
		logger.InfoWithFields(ctx, "Devices any role created successfully on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	} else {
		logger.DebugWithFields(ctx, "Devices any role already exists, ensuring ACLs are configured on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	}

	// Step 2: Add subscribe ACLs for device topics (devices subscribe to server messages).
	// Use MQTT wildcard (+) for compatibility with broker ACL matching behavior.
	subscribeTopics := []string{
		"/devices/%u/config",             // Device configuration
		"/devices/%u/control",            // Control commands
		"/devices/%u/balance",            // Balance updates
		"/devices/%u/response/authorize", // Authorization responses
		"/devices/%u/response/invoice",   // Invoice responses
		"/devices/%u/events/invoice",     // Invoice events
		"/devices/%u/#",                  // All topics under the device path (for discovery)
	}

	logger.InfoWithFields(ctx, "Adding subscribe ACLs for shared device role on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
		"count":     len(subscribeTopics),
	})
	subscribeACLCommands := make([]map[string]interface{}, 0, len(subscribeTopics))
	for _, topic := range subscribeTopics {
		logger.DebugWithFields(ctx, "Adding subscribe ACL for topic on southbound mqtt", map[string]interface{}{
			"topic": topic,
		})
		subscribeACLCommands = append(subscribeACLCommands, map[string]interface{}{
			"command":  "addRoleACL",
			"rolename": roleName,
			"acltype":  "subscribePattern",
			"topic":    topic,
			"allow":    true,
			"priority": 5,
		})
	}

	addSubscribeACLCmd := map[string]interface{}{
		"commands": subscribeACLCommands,
	}
	if err := mds.executeCommand(ctx, addSubscribeACLCmd); err != nil {
		logger.Error(ctx, "Failed to add subscribe ACLs on southbound mqtt", err)
		// Continue even if some ACLs already exist
	} else {
		logger.InfoWithFields(ctx, "Subscribe ACLs added successfully for shared device role on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	}

	// Step 3: Add publish ACLs for device topics (devices publish to server).
	publishTopics := []string{
		"/devices/%u/heartbeat",         // Heartbeat messages
		"/devices/%u/usage",             // Usage reports
		"/devices/%u/request/authorize", // Authorization requests
		"/devices/%u/request/invoice",   // Invoice requests
	}

	logger.InfoWithFields(ctx, "Adding publish ACLs for shared device role on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
		"count":     len(publishTopics),
	})
	publishACLCommands := make([]map[string]interface{}, 0, len(publishTopics))
	for _, topic := range publishTopics {
		logger.DebugWithFields(ctx, "Adding publish ACL for topic on southbound mqtt", map[string]interface{}{
			"topic": topic,
		})
		publishACLCommands = append(publishACLCommands, map[string]interface{}{
			"command":  "addRoleACL",
			"rolename": roleName,
			"acltype":  "publishClientSend",
			"topic":    topic,
			"allow":    true,
			"priority": 5,
		})
	}

	addPublishACLCmd := map[string]interface{}{
		"commands": publishACLCommands,
	}
	if err := mds.executeCommand(ctx, addPublishACLCmd); err != nil {
		logger.Error(ctx, "Failed to add publish ACLs on southbound mqtt", err)
		// Continue even if some ACLs already exist
	} else {
		logger.InfoWithFields(ctx, "Publish ACLs added successfully for shared device role on southbound mqtt", map[string]interface{}{
			"role_name": roleName,
		})
	}

	logger.InfoWithFields(ctx, "Successfully provisioned shared device role on southbound mqtt", map[string]interface{}{
		"role_name": roleName,
	})
	return nil
}

// Disconnect disconnects from the MQTT broker
func (mds *MQTTDynSecService) Disconnect(ctx context.Context) {
	if mds.client != nil {
		mds.client.Disconnect(250)
		logger.Debug(ctx, "Disconnected from MQTT broker (dynamic security service) on southbound mqtt")
	}
}
