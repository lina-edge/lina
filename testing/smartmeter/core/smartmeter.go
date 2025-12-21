package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
	devicepkg "github.com/robertodantas/lnpay/testing/device"
)

// SmartMeter encapsulates all meter-specific logic and state
// It implements DeviceCallback directly
type SmartMeter struct {
	mu                       sync.RWMutex
	meterState               SmartMeterState
	device                   devicepkg.DeviceInterface
	powerUpdateTicker        *time.Ticker
	usageTicker              *time.Ticker
	currentReportingInterval int32 // Track current reporting interval to detect changes
	savedApplianceStates     map[string]bool
	logCallback              func(message, logType string)
	deviceSecret             string
	deviceID                 string
}

// withState provides safe, write-locked access to the internal SmartMeterState.
// Callers MUST NOT call methods that themselves acquire m.mu from within fn.
func (m *SmartMeter) withState(fn func(state *SmartMeterState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(&m.meterState)
}

// NewSmartMeter creates a new smart meter instance
func NewSmartMeter(deviceID, deviceSecret string, cfg *Config) *SmartMeter {
	// Make a copy of default appliances
	appliances := make([]Appliance, len(defaultAppliances))
	copy(appliances, defaultAppliances)

	m := &SmartMeter{
		deviceSecret: deviceSecret,
		deviceID:     deviceID,
		meterState: SmartMeterState{
			Appliances:       appliances,
			TotalConsumption: 0,
			InstantPower:     0,
			Logs:             []LogEntry{},
		},
		savedApplianceStates: make(map[string]bool),
	}
	// attach device interface - SmartMeter implements DeviceCallback directly
	deviceCfg := &devicepkg.Config{
		HTTPPort:          cfg.HTTPPort,
		MQTTBroker:        cfg.MQTTBroker,
		MQTTUseTLS:        cfg.MQTTUseTLS,
		MQTTPort:          cfg.MQTTPort,
		MQTTTLSPort:       cfg.MQTTTLSPort,
		MQTTTLSCACert:     cfg.MQTTTLSCACert,
		MQTTTLSSkipVerify: cfg.MQTTTLSSkipVerify,
		MQTTTLSServerName: cfg.MQTTTLSServerName,
	}
	m.device = devicepkg.NewDeviceInterface(m, deviceCfg, deviceID)
	// Initialize device context with default config (DeviceInterface will manage it)
	// The config will be updated via MQTT retained message, but we set a default here
	// Note: DeviceInterface doesn't expose ctx, so we'll rely on MQTT config update
	return m
}

// SetLogCallback sets the callback for log messages
func (m *SmartMeter) SetLogCallback(cb func(message, logType string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logCallback = cb
}

// GetState returns a copy of the current state (combines DeviceContext and SmartMeterState)
func (m *SmartMeter) GetState() DeviceState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ctx := m.device.GetDeviceContext()

	// Make a copy of appliances to avoid race conditions
	appliancesCopy := make([]Appliance, len(m.meterState.Appliances))
	copy(appliancesCopy, m.meterState.Appliances)

	return DeviceState{
		DeviceID:             ctx.DeviceID,
		DeviceStatus:         ctx.DeviceStatus,
		Appliances:           appliancesCopy,
		Balance:              ctx.Balance,
		Config:               ctx.Config,
		TotalConsumption:     m.meterState.TotalConsumption,
		InstantPower:         m.meterState.InstantPower,
		Invoice:              ctx.Invoice,
		CurrentAuthorization: ctx.CurrentAuthorization,
		Logs:                 m.meterState.Logs,
		MQTTStatus:           ctx.MQTTStatus,
	}
}

// DeviceStateJSON is a JSON-friendly representation of DeviceState with converted invoice
type DeviceStateJSON struct {
	DeviceID             string               `json:"deviceId"`
	DeviceStatus         string               `json:"deviceStatus"`
	Appliances           []Appliance          `json:"appliances"`
	Balance              *BalanceMessage      `json:"balance"`
	Config               *DeviceConfig        `json:"config"`
	TotalConsumption     float64              `json:"totalConsumption"`
	InstantPower         int                  `json:"instantPower"`
	Invoice              *InvoiceResponseJSON `json:"invoice"`
	CurrentAuthorization *Authorization       `json:"currentAuthorization"`
	Logs                 []LogEntry           `json:"logs"`
	MQTTStatus           string               `json:"mqttStatus"`
}

// GetStateJSON returns the state as JSON with invoice status converted to string
func (m *SmartMeter) GetStateJSON() json.RawMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ctx := m.device.GetDeviceContext()

	// Make a copy of appliances to avoid race conditions
	appliancesCopy := make([]Appliance, len(m.meterState.Appliances))
	copy(appliancesCopy, m.meterState.Appliances)

	// Convert to JSON-friendly format
	stateJSON := DeviceStateJSON{
		DeviceID:             ctx.DeviceID,
		DeviceStatus:         ctx.DeviceStatus,
		Appliances:           appliancesCopy,
		Balance:              ctx.Balance,
		Config:               ctx.Config,
		TotalConsumption:     m.meterState.TotalConsumption,
		InstantPower:         m.meterState.InstantPower,
		Invoice:              ConvertInvoiceResponseToJSON(ctx.Invoice),
		CurrentAuthorization: ctx.CurrentAuthorization,
		Logs:                 m.meterState.Logs,
		MQTTStatus:           ctx.MQTTStatus,
	}

	data, err := json.Marshal(&stateJSON)
	if err != nil {
		logger.Error(context.Background(), "Error marshaling state to JSON", err)
		return json.RawMessage("{}")
	}
	return data
}

// Log logs a message (required by DeviceCallback interface)
func (m *SmartMeter) Log(message, logType string) {
	ctx := context.Background()

	m.mu.Lock()
	entry := LogEntry{
		ID:        generateID(),
		Timestamp: time.Now().Format(time.RFC3339),
		Message:   message,
		Type:      logType,
	}

	m.meterState.Logs = append([]LogEntry{entry}, m.meterState.Logs...)
	if len(m.meterState.Logs) > 50 {
		m.meterState.Logs = m.meterState.Logs[:50]
	}
	m.mu.Unlock()

	// Use structured logger with device context
	logEntry := logger.WithDeviceID(m.deviceID)

	switch logType {
	case "error":
		logEntry.Error(ctx, message, nil)
	case "warning", "warn":
		logEntry.Warn(ctx, message)
	case "success":
		logEntry.Info(ctx, message)
	default:
		logEntry.Info(ctx, message)
	}

	// Call log callback if set
	m.mu.RLock()
	if m.logCallback != nil {
		m.logCallback(message, logType)
	}
	m.mu.RUnlock()
}

// OnMQTTStatus is called when MQTT connection status changes
func (m *SmartMeter) OnMQTTStatus(status string) {}

// OnDeviceStatus is called when device status changes
func (m *SmartMeter) OnDeviceStatus(status string) {}

// Start boots the smart meter: connect MQTT and start simulation
// DeviceInterface will handle connection, subscriptions, heartbeat, and authorization
func (m *SmartMeter) Start() {
	// Connect to MQTT - DeviceInterface will handle the rest
	m.device.Connect(m.deviceID, m.deviceSecret)

	// Start simulation loops
	m.startSimulationLoops()
}

// OnConnected is called when the device has successfully connected to MQTT,
// subscriptions are ready, and initial heartbeat/authorization have been sent
func (m *SmartMeter) OnConnected() {
}

// GetDeviceStatus returns the current device status
func (m *SmartMeter) GetDeviceStatus() string {
	return m.device.GetDeviceStatus()
}

// GetDeviceID returns the device ID
func (m *SmartMeter) GetDeviceID() string {
	return m.deviceID // DeviceID is stored in SmartMeter
}

// GetDeviceConfig returns the current configuration
func (m *SmartMeter) GetDeviceConfig() *DeviceConfig {
	return m.device.GetDeviceConfig()
}

// OnConfigUpdated is called when device configuration is updated
// DeviceInterface has already updated the DeviceContext and restarted heartbeat if needed
func (m *SmartMeter) OnConfigUpdated(config *DeviceConfig) {
	// Check if reporting interval changed and restart usage ticker if needed
	oldInterval := m.currentReportingInterval
	newInterval := int32(60) // Default
	if config != nil && config.ReportingInterval > 0 {
		newInterval = config.ReportingInterval
	}

	// Restart usage ticker if interval changed
	if oldInterval != newInterval {
		m.restartUsageTicker(newInterval)
	}
}

// OnBalanceUpdated is called when balance is updated
func (m *SmartMeter) OnBalanceUpdated(balance *BalanceMessage) {
}

// OnAuthorizationGranted is called when authorization is granted
func (m *SmartMeter) OnAuthorizationGranted(response *AuthorizeResponse) {
	// Device service will send RESUME control command to restore appliances
}

// OnAuthorizationActive is called when an existing authorization is found
func (m *SmartMeter) OnAuthorizationActive(response *AuthorizeResponse) {
}

// OnAuthorizationRejected is called when authorization is rejected
func (m *SmartMeter) OnAuthorizationRejected(response *AuthorizeResponse) {
	// Device service will send STOP control command to halt consumption
}

// OnInvoiceCreated is called when an invoice is created
// DeviceInterface has already updated the DeviceContext
func (m *SmartMeter) OnInvoiceCreated(invoice *InvoiceResponse) {
}

// ClearInvoice clears the current invoice
// DeviceInterface manages invoice state, so we delegate to it
func (m *SmartMeter) ClearInvoice() {
	m.device.ClearInvoice()
}

// RequestTopUp requests an invoice via device interface and updates local invoice state
// DeviceInterface handles the online check and logs the request
func (m *SmartMeter) RequestTopUp(amountMsat int64) {
	requestID := generateID()
	m.device.PublishInvoiceRequest(requestID, amountMsat, "USER_TOPUP")
}

// OnControlStop is called when STOP command is received
// DeviceInterface has already set default reason if empty and logged the command
// SmartMeter decides to halt consumption (keep device online) rather than shutdown
func (m *SmartMeter) OnControlStop(reason string) {
	m.withState(func(state *SmartMeterState) {
		// Save current appliance states before turning them off (only if not already saved)
		if len(m.savedApplianceStates) == 0 {
			for i := range state.Appliances {
				m.savedApplianceStates[state.Appliances[i].ID] = state.Appliances[i].IsOn
			}
		}

		// Turn off all appliances but keep connection
		for i := range state.Appliances {
			state.Appliances[i].IsOn = false
			state.Appliances[i].CurrentWatts = 0
		}
		state.InstantPower = 0
	})
}

// OnControlPause is called when PAUSE command is received
// DeviceInterface has already set default reason if empty and updated device status
func (m *SmartMeter) OnControlPause(reason string) {
	isOnline := m.device.GetDeviceStatus() == "ONLINE"
	if !isOnline {
		return
	}

	m.withState(func(state *SmartMeterState) {
		// Turn off all appliances but keep connection
		for i := range state.Appliances {
			state.Appliances[i].IsOn = false
			state.Appliances[i].CurrentWatts = 0
		}
		state.InstantPower = 0
	})
}

// OnControlResume is called when RESUME command is received
func (m *SmartMeter) OnControlResume() {
	// Restore previous appliance states that were saved when consumption was halted
	m.withState(func(state *SmartMeterState) {
		// Only restore previous states if we actually have any saved.
		if len(m.savedApplianceStates) > 0 {
			for i := range state.Appliances {
				prevOn, ok := m.savedApplianceStates[state.Appliances[i].ID]
				if ok && prevOn {
					state.Appliances[i].IsOn = true
				}
			}
		}
		// Clear saved states after restoring (or after a no-op RESUME)
		m.savedApplianceStates = make(map[string]bool)
	})

	m.Log("Appliances resumed", "info")
}

// ToggleAppliance toggles an appliance on or off
func (m *SmartMeter) ToggleAppliance(applianceID string) {
	logger.InfoWithFields(context.Background(), "ToggleAppliance called", map[string]interface{}{
		"appliance_id": applianceID,
	})
	deviceStatus := m.device.GetDeviceStatus()
	if deviceStatus != "ONLINE" {
		m.Log("Cannot toggle appliance: offline", "error")
		logger.WarnWithFields(context.Background(), "ToggleAppliance aborted: device not online", map[string]interface{}{
			"device_status": deviceStatus,
		})
		return
	}

	// Snapshot current appliances under read lock
	m.mu.RLock()
	appliancesSnapshot := make([]Appliance, len(m.meterState.Appliances))
	copy(appliancesSnapshot, m.meterState.Appliances)
	m.mu.RUnlock()

	// Work on the snapshot outside the lock
	var (
		name          string
		status        string
		turningOn     bool
		allOffBefore  bool
		applianceSeen bool
		newIsOn       bool
	)

	for i := range appliancesSnapshot {
		if appliancesSnapshot[i].ID == applianceID {
			applianceSeen = true
			turningOn = !appliancesSnapshot[i].IsOn

			// Check if this is the first appliance being turned on (all currently off)
			allOffBefore = true
			if turningOn {
				for j := range appliancesSnapshot {
					if appliancesSnapshot[j].IsOn {
						allOffBefore = false
						break
					}
				}
			}

			// Toggle in the snapshot
			appliancesSnapshot[i].IsOn = !appliancesSnapshot[i].IsOn
			newIsOn = appliancesSnapshot[i].IsOn
			status = "OFF"
			if newIsOn {
				status = "ON"
			}
			name = appliancesSnapshot[i].Name
			break
		}
	}

	if !applianceSeen {
		m.Log(fmt.Sprintf("Appliance not found: %s", applianceID), "error")
		logger.WarnWithFields(context.Background(), "ToggleAppliance aborted: appliance not found", map[string]interface{}{
			"appliance_id": applianceID,
		})
		return
	}

	// Short critical section: apply the new on/off state back into the real state
	m.withState(func(state *SmartMeterState) {
		for i := range state.Appliances {
			if state.Appliances[i].ID == applianceID {
				state.Appliances[i].IsOn = newIsOn
				break
			}
		}
	})

	needsAuth := turningOn && allOffBefore && !m.device.HasActiveAuthorization() && !m.device.IsPendingAuthorization()
	logger.DebugWithFields(context.Background(), "ToggleAppliance state after toggle", map[string]interface{}{
		"appliance_id":   applianceID,
		"appliance_name": name,
		"turned_on":      newIsOn,
		"all_off_before": allOffBefore,
		"needs_auth":     needsAuth,
	})
	var reason string
	if needsAuth {
		reason = "INITIATE_USAGE"
	}
	m.Log(name+" turned "+status, "info")

	if needsAuth {
		go func(r string) {
			m.Log("Initiating usage requesting authorization", "info")
			time.Sleep(1 * time.Second)
			m.device.PublishAuthorizeRequest(r)
		}(reason)
	}
}

// Shutdown shuts down the meter completely
func (m *SmartMeter) Shutdown() {
	// Set device status to OFFLINE first to prevent race conditions
	// This ensures any concurrent operations (like updatePowerReadings) see OFFLINE status immediately
	// Note: DeviceInterface should handle this, but for shutdown we do it directly
	// TODO: Add a Shutdown method to DeviceInterface that handles this
	m.withState(func(state *SmartMeterState) {
		for i := range state.Appliances {
			state.Appliances[i].IsOn = false
			state.Appliances[i].CurrentWatts = 0
		}
		state.InstantPower = 0
	})

	// Stop tickers (after setting status to prevent concurrent updates)
	if m.powerUpdateTicker != nil {
		m.powerUpdateTicker.Stop()
		m.powerUpdateTicker = nil
	}
	if m.usageTicker != nil {
		m.usageTicker.Stop()
		m.usageTicker = nil
	}
	// Heartbeat ticker is managed by DeviceInterface and will be stopped on disconnect

	// Publish offline and disconnect MQTT
	m.device.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_OFFLINE)
	m.device.Disconnect()

	// MQTT status will be updated by DeviceInterface on disconnect

	m.Log("Meter system shut down", "info")
}

// StartSimulation starts the meter simulation (power updates and usage reporting)
func (m *SmartMeter) startSimulationLoops() {
	// Power update ticker (1 second)
	m.powerUpdateTicker = time.NewTicker(1 * time.Second)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorWithFields(context.Background(), "panic in power update goroutine", nil, map[string]interface{}{
					"panic": r,
				})
			}
		}()

		for range m.powerUpdateTicker.C {
			logger.Debug(context.Background(), "updatePowerReadings tick")
			m.updatePowerReadings()
		}
	}()

	// Usage reporting ticker
	config := m.device.GetDeviceConfig()
	// Use default of 60 seconds if config is nil (e.g., when MQTT connection fails)
	defaultReportingInterval := int32(60)
	reportingInterval := defaultReportingInterval
	if config != nil && config.ReportingInterval > 0 {
		reportingInterval = config.ReportingInterval
	}
	m.currentReportingInterval = reportingInterval
	m.usageTicker = time.NewTicker(time.Duration(reportingInterval) * time.Second)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorWithFields(context.Background(), "panic in usage reporting goroutine", nil, map[string]interface{}{
					"panic": r,
				})
			}
		}()

		for range m.usageTicker.C {
			shouldReport, reportID, kWh := m.ReportUsage()
			if shouldReport {
				m.device.PublishUsageReport(reportID, kWh)
			}
		}
	}()
}

// restartUsageTicker restarts the usage ticker with a new interval
func (m *SmartMeter) restartUsageTicker(interval int32) {
	// Stop existing ticker if any and update stored interval
	if m.usageTicker != nil {
		m.usageTicker.Stop()
		m.usageTicker = nil
	}

	m.currentReportingInterval = interval

	// Start with new interval
	m.usageTicker = time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorWithFields(context.Background(), "panic in usage reporting goroutine (restart)", nil, map[string]interface{}{
					"panic": r,
				})
			}
		}()

		for range m.usageTicker.C {
			shouldReport, reportID, kWh := m.ReportUsage()
			if shouldReport {
				m.device.PublishUsageReport(reportID, kWh)
			}
		}
	}()

	m.Log(fmt.Sprintf("Usage reporting interval updated to %d seconds", interval), "info")
}

// ReportUsage generates and reports usage (should be called by the usage ticker)
func (m *SmartMeter) ReportUsage() (shouldReport bool, reportID string, kWhConsumed float64) {
	// Snapshot external config and status first (no SmartMeter lock involved)
	deviceStatus := m.device.GetDeviceStatus()
	config := m.device.GetDeviceConfig()

	if deviceStatus != "ONLINE" {
		return false, "", 0
	}

	// Snapshot instantaneous power under read lock, then calculate outside the lock
	m.mu.RLock()
	instantPower := m.meterState.InstantPower
	m.mu.RUnlock()

	if instantPower == 0 {
		return false, "", 0
	}

	// Calculate kWh consumed in this interval (outside lock)
	// Use default of 60 seconds if config is nil (e.g., when MQTT connection fails)
	defaultReportingInterval := float64(60)
	intervalSeconds := defaultReportingInterval
	if config != nil && config.ReportingInterval > 0 {
		intervalSeconds = float64(config.ReportingInterval)
	}
	kWhConsumed = (float64(instantPower) / 1000.0) * (intervalSeconds / 3600.0)

	// Now lock briefly just to update total consumption
	m.withState(func(state *SmartMeterState) {
		state.TotalConsumption += kWhConsumed
	})

	reportID = generateID()
	return true, reportID, kWhConsumed
}

// updatePowerReadings updates power consumption for all appliances
func (m *SmartMeter) updatePowerReadings() {
	// Skip if device is offline to prevent race conditions during shutdown
	if m.device.GetDeviceStatus() == "OFFLINE" {
		return
	}

	// Snapshot current appliances under read lock
	m.mu.RLock()
	appliancesSnapshot := make([]Appliance, len(m.meterState.Appliances))
	copy(appliancesSnapshot, m.meterState.Appliances)
	m.mu.RUnlock()

	// Calculate new power readings outside the lock
	newWatts := make([]int, len(appliancesSnapshot))
	totalPower := 0
	for i := range appliancesSnapshot {
		appliance := &appliancesSnapshot[i]
		if !appliance.IsOn {
			newWatts[i] = 0
			continue
		}

		// Simulate power variance
		powerRange := appliance.MaxWatts - appliance.MinWatts
		variance := (rand.Float64() - 0.5) * float64(powerRange) * 0.2
		baseWatts := float64(appliance.MinWatts+appliance.MaxWatts) / 2
		currentWatts := math.Max(float64(appliance.MinWatts),
			math.Min(float64(appliance.MaxWatts), baseWatts+variance))

		watts := int(math.Round(currentWatts))
		newWatts[i] = watts
		totalPower += watts
	}

	// Briefly lock just to apply the new readings
	m.withState(func(state *SmartMeterState) {
		for i := range state.Appliances {
			if i < len(newWatts) {
				state.Appliances[i].CurrentWatts = newWatts[i]
			}
		}
		state.InstantPower = totalPower
	})
}

// OnInvoiceSettled is called when an invoice is settled
func (m *SmartMeter) OnInvoiceSettled(invoiceID string, amountMsat int64) {
	m.ClearInvoice()
}

// OnInvoiceExpired is called when an invoice expires
func (m *SmartMeter) OnInvoiceExpired(invoiceID string) {
	m.ClearInvoice()
}

// OnInvoiceFailed is called when an invoice fails
func (m *SmartMeter) OnInvoiceFailed(invoiceID string) {
	m.ClearInvoice()
}

// OnControlReboot is called when REBOOT command is received
func (m *SmartMeter) OnControlReboot() {
	m.Shutdown()
	m.Start()
}

// OnLog is called when a log message should be recorded
func (m *SmartMeter) OnLog(message, logType string) {
	m.Log(message, logType)
}
