package main

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"sync"
	"time"

	"errors"

	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
)

var (
	errMQTTWaitTimeout = errors.New("mqtt wait timeout")
	errMQTTWaitAborted = errors.New("mqtt wait aborted")
)

// SmartMeter encapsulates all meter-specific logic and state
type SmartMeter struct {
	mu                   sync.RWMutex
	state                DeviceState
	southbound           *SouthboundInterface
	powerUpdateTicker    *time.Ticker
	usageTicker          *time.Ticker
	heartbeatTicker      *time.Ticker
	savedApplianceStates map[string]bool
	pendingAuthorization bool
	stateChangeCallback  func(DeviceState)
	logCallback          func(message, logType string)
	deviceSecret         string
	deviceID             string
}

// NewSmartMeter creates a new smart meter instance
func NewSmartMeter(deviceID, deviceSecret string, cfg *Config) *SmartMeter {
	// Make a copy of default appliances
	appliances := make([]Appliance, len(defaultAppliances))
	copy(appliances, defaultAppliances)

	// Default DeviceConfig values (will be overwritten by retained MQTT config)
	defaultDeviceConfig := &DeviceConfig{
		DeviceId:             deviceID,
		MeasurementUnit:      "kWh",
		UnitPriceMsat:        10,
		ReportingStrategy:    mqttmodel.ReportingStrategy_REPORTING_STRATEGY_INTERVAL,
		ReportingInterval:    30,
		HeartbeatInterval:    10,
		AuthorizeRequestMsat: 1000,
		Timestamp:            time.Now().Format(time.RFC3339),
	}

	m := &SmartMeter{
		deviceSecret: deviceSecret,
		deviceID:     deviceID,
		state: DeviceState{
			DeviceID:             deviceID,
			DeviceStatus:         "OFFLINE",
			Appliances:           appliances,
			Config:               defaultDeviceConfig,
			TotalConsumption:     0,
			InstantPower:         0,
			Logs:                 []LogEntry{},
			CurrentAuthorization: nil,
			MQTTStatus:           "disconnected",
		},
		savedApplianceStates: make(map[string]bool),
	}
	// attach southbound interface
	m.southbound = NewSouthboundInterface(m, cfg)
	return m
}

// SetStateChangeCallback sets the callback for state changes
func (m *SmartMeter) SetStateChangeCallback(cb func(DeviceState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateChangeCallback = cb
}

// SetLogCallback sets the callback for log messages
func (m *SmartMeter) SetLogCallback(cb func(message, logType string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logCallback = cb
}

// GetState returns a copy of the current state
func (m *SmartMeter) GetState() DeviceState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// GetStateJSON returns the state as JSON
func (m *SmartMeter) GetStateJSON() json.RawMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, _ := json.Marshal(&m.state)
	return data
}

// AddLog adds a log entry
func (m *SmartMeter) AddLog(message, logType string) {
	ctx := context.Background()

	m.mu.Lock()
	entry := LogEntry{
		ID:        generateID(),
		Timestamp: time.Now().Format(time.RFC3339),
		Message:   message,
		Type:      logType,
	}

	m.state.Logs = append([]LogEntry{entry}, m.state.Logs...)
	if len(m.state.Logs) > 50 {
		m.state.Logs = m.state.Logs[:50]
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

// notifyStateChange calls the state change callback if set
func (m *SmartMeter) notifyStateChange() {
	m.mu.RLock()
	if m.stateChangeCallback != nil {
		state := m.state
		m.mu.RUnlock()
		m.stateChangeCallback(state)
	} else {
		m.mu.RUnlock()
	}
}

// SetMQTTStatus updates the MQTT connection status
func (m *SmartMeter) SetMQTTStatus(status string) {
	m.mu.Lock()
	m.state.MQTTStatus = status
	m.mu.Unlock()
	m.notifyStateChange()
}

// SetDeviceStatus updates the device status
func (m *SmartMeter) SetDeviceStatus(status string) {
	m.mu.Lock()
	m.state.DeviceStatus = status
	m.mu.Unlock()
	m.notifyStateChange()
}

// Start boots the smart meter: connect MQTT, start simulation, and complete startup sequence
func (m *SmartMeter) Start() {
	if m.GetDeviceStatus() != "OFFLINE" {
		m.AddLog("Device is not offline, skipping start", "info")
		return
	}
	m.SetDeviceStatus("STARTING")
	m.AddLog("Starting meter system...", "info")

	// Connect to MQTT
	m.southbound.Connect()

	// Start simulation loops
	m.startSimulationLoops()

	// Complete startup async
	go m.completeStartupSequence()
}

func (m *SmartMeter) completeStartupSequence() {
	ctx := context.Background()
	const timeout = 15 * time.Second
	if err := m.waitForMQTTConnection(timeout); err != nil {
		if errors.Is(err, errMQTTWaitTimeout) {
			m.AddLog("MQTT connection timeout during startup - reverting to OFFLINE", "error")
			m.Shutdown()
		}
		return
	}
	if m.GetDeviceStatus() != "STARTING" {
		return
	}
	// Wait for subscriptions
	select {
	case <-m.southbound.GetSubscriptionsReady():
		logger.WithDeviceID(m.deviceID).
			Info(ctx, "Subscriptions ready, proceeding with startup sequence on southbound mqtt")
	case <-time.After(10 * time.Second):
		m.AddLog("Timeout waiting for subscriptions - reverting to OFFLINE", "error")
		m.Shutdown()
		return
	}

	m.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
	m.southbound.PublishAuthorizeRequest("STARTUP")
}

func (m *SmartMeter) waitForMQTTConnection(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.southbound.IsConnected() {
			return nil
		}
		if m.GetDeviceStatus() != "STARTING" {
			return errMQTTWaitAborted
		}
		select {
		case <-timer.C:
			return errMQTTWaitTimeout
		case <-ticker.C:
		}
	}
}

// GetDeviceStatus returns the current device status
func (m *SmartMeter) GetDeviceStatus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.DeviceStatus
}

// GetDeviceID returns the device ID
func (m *SmartMeter) GetDeviceID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.DeviceID
}

// GetConfig returns the current configuration
func (m *SmartMeter) GetDeviceConfig() *DeviceConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.Config
}

// UpdateConfig updates the device configuration
func (m *SmartMeter) UpdateDeviceConfig(config *DeviceConfig) {
	m.mu.Lock()
	m.state.Config = config
	m.mu.Unlock()
	m.AddLog("Configuration updated", "info")
	m.notifyStateChange()
}

// UpdateBalance updates the device balance
func (m *SmartMeter) UpdateBalance(balance *BalanceMessage) (shouldRequestAuth bool, reason string) {
	m.mu.Lock()
	m.state.Balance = balance
	available := balance.AvailableMsat
	lastStatus := ""
	if m.state.CurrentAuthorization != nil {
		lastStatus = m.state.CurrentAuthorization.Status
	}
	shouldRequestAuth = available > 0 && !m.pendingAuthorization && !m.hasActiveAuthorization() && lastStatus == "REJECTED"
	if shouldRequestAuth {
		m.pendingAuthorization = true
		reason = "FUNDS_AVAILABLE"
	}
	m.mu.Unlock()

	m.AddLog("Balance updated: "+formatMsat(balance.AvailableMsat)+" msat available", "info")
	m.notifyStateChange()
	return shouldRequestAuth, reason
}

// HandleAuthorizationGranted processes a granted authorization
func (m *SmartMeter) HandleAuthorizationGranted(response *AuthorizeResponse) {
	auth := &Authorization{
		AuthorizationID: response.AuthorizationId,
		RequestID:       response.RequestId,
		GrantedMsat:     response.GrantedMsat,
		RemainingMsat:   response.RemainingMsat,
		IssuedAt:        response.IssuedAt,
		ExpiresAt:       response.ExpiresAt,
		Status:          "ACTIVE",
		Reason:          response.Reason,
	}

	m.mu.Lock()
	m.state.CurrentAuthorization = auth
	m.state.DeviceStatus = "ONLINE"
	m.pendingAuthorization = false
	m.mu.Unlock()

	m.AddLog("Authorization granted: "+formatMsat(response.GrantedMsat)+" msat (reserved)", "success")
	m.notifyStateChange()

	// Restore previous appliance states
	m.restoreApplianceStates()
}

// HandleAuthorizationRejected processes a rejected authorization
func (m *SmartMeter) HandleAuthorizationRejected(response *AuthorizeResponse) (shouldHalt bool, haltReason string) {
	m.AddLog("Authorization rejected: "+response.RequestId, "error")

	m.mu.Lock()
	rejected := &Authorization{
		AuthorizationID: response.AuthorizationId,
		RequestID:       response.RequestId,
		GrantedMsat:     0,
		RemainingMsat:   0,
		IssuedAt:        response.IssuedAt,
		ExpiresAt:       response.ExpiresAt,
		Status:          "REJECTED",
		Reason:          response.Reason,
	}
	m.state.CurrentAuthorization = rejected
	m.pendingAuthorization = false

	// Move to ONLINE even on rejection so device isn't stuck in STARTING
	if m.state.DeviceStatus == "STARTING" {
		m.state.DeviceStatus = "ONLINE"
	}
	m.mu.Unlock()

	return true, response.Reason
}

// SetInvoice updates the invoice state
func (m *SmartMeter) SetInvoice(invoice *InvoiceResponse) {
	m.mu.Lock()
	m.state.Invoice = invoice
	m.mu.Unlock()
	m.notifyStateChange()
}

// ClearInvoice clears the current invoice
func (m *SmartMeter) ClearInvoice() {
	m.mu.Lock()
	m.state.Invoice = nil
	m.mu.Unlock()
	m.notifyStateChange()
}

// RequestTopUp requests an invoice via southbound and updates local invoice state
func (m *SmartMeter) RequestTopUp(amountMsat int64) {
	if m.GetDeviceStatus() != "ONLINE" {
		m.AddLog("Cannot request top-up - meter is offline", "error")
		return
	}
	requestID := generateID()
	m.southbound.PublishInvoiceRequest(requestID, amountMsat, "USER_TOPUP")
	m.AddLog("Invoice requested: "+formatMsat(amountMsat)+" msat", "info")
}

// HandleControlStop processes a stop control command
func (m *SmartMeter) HandleControlStop(reason string) (shouldHalt bool, haltReason string) {
	if reason == "" {
		reason = "REMOTE_COMMAND"
	}
	m.AddLog("Command STOP received: "+reason, "warning")
	return true, reason
}

// HandleControlPause processes a pause control command
func (m *SmartMeter) HandleControlPause(reason string) {
	if reason == "" {
		reason = "REMOTE_COMMAND"
	}
	m.AddLog("Command PAUSE received: "+reason, "info")

	m.mu.Lock()
	if m.state.DeviceStatus == "ONLINE" {
		m.state.DeviceStatus = "PAUSED"
		// Turn off all appliances but keep connection
		for i := range m.state.Appliances {
			m.state.Appliances[i].IsOn = false
			m.state.Appliances[i].CurrentWatts = 0
		}
		m.state.InstantPower = 0
	}
	m.mu.Unlock()

	m.notifyStateChange()
}

// HandleControlResume processes a resume control command
func (m *SmartMeter) HandleControlResume() {
	m.AddLog("Command RESUME received", "info")

	m.mu.Lock()
	if m.state.DeviceStatus == "PAUSED" || m.state.DeviceStatus == "OFFLINE" {
		m.state.DeviceStatus = "ONLINE"
	}
	m.mu.Unlock()

	m.notifyStateChange()
}

// ToggleAppliance toggles an appliance on or off
func (m *SmartMeter) ToggleAppliance(applianceID string) {
	m.mu.Lock()

	if m.state.DeviceStatus != "ONLINE" {
		m.mu.Unlock()
		m.AddLog("Cannot toggle appliance: offline", "error")
		return
	}

	var appliance *Appliance
	for i := range m.state.Appliances {
		if m.state.Appliances[i].ID == applianceID {
			appliance = &m.state.Appliances[i]
			break
		}
	}

	if appliance == nil {
		m.mu.Unlock()
		return
	}

	// Check if this is the first appliance being turned on (all currently off)
	turningOn := !appliance.IsOn
	allOff := true
	if turningOn {
		for i := range m.state.Appliances {
			if m.state.Appliances[i].IsOn {
				allOff = false
				break
			}
		}
	}

	// Toggle appliance
	appliance.IsOn = !appliance.IsOn
	status := "OFF"
	if appliance.IsOn {
		status = "ON"
	}
	name := appliance.Name
	needsAuth := turningOn && allOff && !m.hasActiveAuthorization() && !m.pendingAuthorization
	var reason string
	if needsAuth {
		m.pendingAuthorization = true
		reason = "INITIATE_USAGE"
	}
	m.mu.Unlock()

	m.AddLog(name+" turned "+status, "info")
	m.notifyStateChange()

	if needsAuth {
		go func(r string) {
			m.AddLog("Initiating usage requesting authorization", "info")
			time.Sleep(1 * time.Second)
			m.southbound.PublishAuthorizeRequest(r)
		}(reason)
	}
}

// HaltConsumption stops all appliances but keeps the device online
func (m *SmartMeter) HaltConsumption(reason string) {
	m.mu.Lock()
	// Save current appliance states before turning them off (only if not already saved)
	if len(m.savedApplianceStates) == 0 {
		for i := range m.state.Appliances {
			m.savedApplianceStates[m.state.Appliances[i].ID] = m.state.Appliances[i].IsOn
		}
	}

	// Turn off all appliances but keep connection
	for i := range m.state.Appliances {
		m.state.Appliances[i].IsOn = false
		m.state.Appliances[i].CurrentWatts = 0
	}
	m.state.InstantPower = 0
	m.mu.Unlock()

	m.AddLog("Consumption halted: "+reason, "warning")
	m.notifyStateChange()
}

// Shutdown shuts down the meter completely
func (m *SmartMeter) Shutdown() {
	// Stop tickers
	if m.powerUpdateTicker != nil {
		m.powerUpdateTicker.Stop()
		m.powerUpdateTicker = nil
	}
	if m.usageTicker != nil {
		m.usageTicker.Stop()
		m.usageTicker = nil
	}
	if m.heartbeatTicker != nil {
		m.heartbeatTicker.Stop()
		m.heartbeatTicker = nil
	}

	// Publish offline and disconnect MQTT
	m.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_OFFLINE)
	m.southbound.Disconnect()
	m.SetMQTTStatus("disconnected")

	// Reset device state
	m.mu.Lock()
	m.state.DeviceStatus = "OFFLINE"
	for i := range m.state.Appliances {
		m.state.Appliances[i].IsOn = false
		m.state.Appliances[i].CurrentWatts = 0
	}
	m.state.InstantPower = 0
	m.mu.Unlock()

	m.AddLog("Meter system shut down", "info")
}

// StartSimulation starts the meter simulation (power updates and usage reporting)
func (m *SmartMeter) startSimulationLoops() {
	// Power update ticker (1 second)
	m.powerUpdateTicker = time.NewTicker(1 * time.Second)
	go func() {
		for range m.powerUpdateTicker.C {
			m.updatePowerReadings()
		}
	}()

	// Heartbeat ticker
	m.mu.RLock()
	hb := time.Duration(m.state.Config.HeartbeatInterval) * time.Second
	m.mu.RUnlock()
	m.heartbeatTicker = time.NewTicker(hb)
	go func() {
		for range m.heartbeatTicker.C {
			m.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
		}
	}()

	// Usage reporting ticker
	m.mu.RLock()
	reportingInterval := time.Duration(m.state.Config.ReportingInterval) * time.Second
	m.mu.RUnlock()
	m.usageTicker = time.NewTicker(reportingInterval)
	go func() {
		for range m.usageTicker.C {
			shouldReport, reportID, kWh := m.ReportUsage()
			if shouldReport {
				m.southbound.PublishUsageReport(reportID, kWh)
			}
		}
	}()
}

// ReportUsage generates and reports usage (should be called by the usage ticker)
func (m *SmartMeter) ReportUsage() (shouldReport bool, reportID string, kWhConsumed float64) {
	m.mu.Lock()

	if m.state.DeviceStatus != "ONLINE" || m.state.InstantPower == 0 {
		m.mu.Unlock()
		return false, "", 0
	}

	// Calculate kWh consumed in this interval
	intervalSeconds := float64(m.state.Config.ReportingInterval)
	kWhConsumed = (float64(m.state.InstantPower) / 1000.0) * (intervalSeconds / 3600.0)

	// Update total consumption
	m.state.TotalConsumption += kWhConsumed

	reportID = generateID()
	m.mu.Unlock()

	m.notifyStateChange()
	return true, reportID, kWhConsumed
}

// GetInstantPower returns the current power consumption
func (m *SmartMeter) GetInstantPower() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.InstantPower
}

// GetUsageTicker returns the usage ticker for periodic reporting
func (m *SmartMeter) GetUsageTicker() *time.Ticker {
	return m.usageTicker
}

// IsPendingAuthorization returns whether an authorization is pending
func (m *SmartMeter) IsPendingAuthorization() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pendingAuthorization
}

// HasActiveAuthorization returns whether there is an active authorization
func (m *SmartMeter) HasActiveAuthorization() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hasActiveAuthorization()
}

// hasActiveAuthorization (internal) checks if there is an active authorization
func (m *SmartMeter) hasActiveAuthorization() bool {
	return m.state.CurrentAuthorization != nil && m.state.CurrentAuthorization.Status == "ACTIVE"
}

// updatePowerReadings updates power consumption for all appliances
func (m *SmartMeter) updatePowerReadings() {
	m.mu.Lock()

	totalPower := 0
	for i := range m.state.Appliances {
		appliance := &m.state.Appliances[i]
		if !appliance.IsOn {
			appliance.CurrentWatts = 0
			continue
		}

		// Simulate power variance
		powerRange := appliance.MaxWatts - appliance.MinWatts
		variance := (rand.Float64() - 0.5) * float64(powerRange) * 0.2
		baseWatts := float64(appliance.MinWatts+appliance.MaxWatts) / 2
		currentWatts := math.Max(float64(appliance.MinWatts),
			math.Min(float64(appliance.MaxWatts), baseWatts+variance))

		appliance.CurrentWatts = int(math.Round(currentWatts))
		totalPower += appliance.CurrentWatts
	}

	m.state.InstantPower = totalPower
	m.mu.Unlock()
	m.notifyStateChange()
}

// restoreApplianceStates restores previously saved appliance states
func (m *SmartMeter) restoreApplianceStates() {
	m.mu.Lock()
	if len(m.savedApplianceStates) == 0 {
		m.mu.Unlock()
		return
	}
	for i := range m.state.Appliances {
		prevOn, ok := m.savedApplianceStates[m.state.Appliances[i].ID]
		if ok && prevOn {
			m.state.Appliances[i].IsOn = true
		}
	}
	// Clear saved states after restoring
	m.savedApplianceStates = make(map[string]bool)
	m.mu.Unlock()
	m.AddLog("Appliances resumed", "info")
	m.notifyStateChange()
}
