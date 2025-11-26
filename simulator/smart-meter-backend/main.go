package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
	"google.golang.org/protobuf/encoding/protojson"
)

// Type aliases for proto types
type Config = mqttmodel.ConfigPayload
type BalanceMessage = mqttmodel.BalancePayload
type AuthorizeResponse = mqttmodel.AuthorizationResponsePayload
type InvoiceResponse = mqttmodel.InvoiceResponsePayload
type HeartbeatMessage = mqttmodel.HeartbeatPayload
type AuthorizeRequest = mqttmodel.AuthorizationRequestPayload
type UsageReport = mqttmodel.UsagePayload
type InvoiceRequest = mqttmodel.InvoiceRequestPayload

// Appliance represents a connected device/appliance
type Appliance struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Icon         string `json:"icon"`
	MinWatts     int    `json:"minWatts"`
	MaxWatts     int    `json:"maxWatts"`
	IsOn         bool   `json:"isOn"`
	CurrentWatts int    `json:"currentWatts"`
}

// DeviceState represents the complete state of the smart meter
type DeviceState struct {
	DeviceID         string           `json:"deviceId"`
	DeviceStatus     string           `json:"deviceStatus"`
	Appliances       []Appliance      `json:"appliances"`
	Balance          *BalanceMessage  `json:"balance"`
	Config           *Config          `json:"config"`
	TotalConsumption float64          `json:"totalConsumption"`
	InstantPower     int              `json:"instantPower"`
	Invoice          *InvoiceResponse `json:"invoice"`
	Authorizations   []Authorization  `json:"authorizations"`
	Logs             []LogEntry       `json:"logs"`
	MQTTStatus       string           `json:"mqttStatus"`
}

type Authorization struct {
	AuthorizationID string `json:"authorization_id"`
	RequestID       string `json:"request_id"`
	GrantedMsat     int64  `json:"granted_msat"`
	RemainingMsat   int64  `json:"remaining_msat"`
	IssuedAt        string `json:"issued_at"`
	ExpiresAt       string `json:"expires_at"`
	Status          string `json:"status"`
}
type LogEntry struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
	Type      string `json:"type"`
}

// WebSocket Message Types
type WSMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type WSCommand struct {
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// SmartMeterBackend manages the entire smart meter simulation
type SmartMeterBackend struct {
	mu                 sync.RWMutex
	state              DeviceState
	mqttClient         mqtt.Client
	southbound         *SouthboundInterface
	wsClients          map[*websocket.Conn]*sync.Mutex
	wsClientsMu        sync.RWMutex
	broadcast          chan interface{}
	stopChan           chan bool
	powerUpdateTicker  *time.Ticker
	heartbeatTicker    *time.Ticker
	usageTicker        *time.Ticker
	subscriptionsReady chan bool
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for development
		},
	}

	defaultAppliances = []Appliance{
		{ID: "fridge", Name: "Refrigerator", Icon: "fridge", MinWatts: 100, MaxWatts: 150, IsOn: false, CurrentWatts: 0},
		{ID: "microwave", Name: "Microwave", Icon: "microwave", MinWatts: 800, MaxWatts: 1200, IsOn: false, CurrentWatts: 0},
		{ID: "heater", Name: "Space Heater", Icon: "heater", MinWatts: 1000, MaxWatts: 1500, IsOn: false, CurrentWatts: 0},
		{ID: "oven", Name: "Electric Oven", Icon: "oven", MinWatts: 2000, MaxWatts: 2500, IsOn: false, CurrentWatts: 0},
		{ID: "computer", Name: "Computer", Icon: "computer", MinWatts: 150, MaxWatts: 300, IsOn: false, CurrentWatts: 0},
		{ID: "washer", Name: "Washing Machine", Icon: "washer", MinWatts: 300, MaxWatts: 500, IsOn: false, CurrentWatts: 0},
	}

	protoMarshalOpts   = protojson.MarshalOptions{UseProtoNames: true}
	protoUnmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

	errMQTTWaitTimeout = errors.New("mqtt wait timeout")
	errMQTTWaitAborted = errors.New("mqtt wait aborted")
)

func NewSmartMeterBackend() *SmartMeterBackend {
	deviceID := getEnv("DEVICE_ID", "smart-meter-001")

	// Make a copy of default appliances
	appliances := make([]Appliance, len(defaultAppliances))
	copy(appliances, defaultAppliances)

	backend := &SmartMeterBackend{
		state: DeviceState{
			DeviceID:     deviceID,
			DeviceStatus: "OFFLINE",
			Appliances:   appliances,
			Config: &Config{
				DeviceId:             deviceID,
				Unit:                 "kWh",
				UnitPrice:            "10",
				PricingUnit:          "msat",
				ReportingStrategy:    mqttmodel.ReportingStrategy_REPORTING_STRATEGY_INTERVAL,
				ReportingInterval:    30,
				HeartbeatInterval:    10,
				AuthorizeRequestMsat: 1000,
				Timestamp:            time.Now().Format(time.RFC3339),
			},
			TotalConsumption: 0,
			InstantPower:     0,
			Logs:             []LogEntry{},
			Authorizations:   []Authorization{},
			MQTTStatus:       "disconnected",
		},
		wsClients:          make(map[*websocket.Conn]*sync.Mutex),
		broadcast:          make(chan interface{}, 100),
		stopChan:           make(chan bool),
		subscriptionsReady: make(chan bool, 1),
	}

	// Initialize southbound interface
	backend.southbound = NewSouthboundInterface(backend)

	return backend
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	backend := NewSmartMeterBackend()

	// HTTP handlers
	http.HandleFunc("/ws", backend.handleWebSocket)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// Start broadcast goroutine
	go backend.broadcastLoop()

	port := getEnv("PORT", "8080")
	log.Printf("Smart Meter Backend starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// WebSocket handler
func (b *SmartMeterBackend) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	// Register client
	b.wsClientsMu.Lock()
	b.wsClients[conn] = &sync.Mutex{}
	b.wsClientsMu.Unlock()

	log.Printf("WebSocket client connected. Total clients: %d", len(b.wsClients))

	// Send initial state
	initialState := b.marshalState()
	log.Printf("Sending initial state with %d appliances, status: %s", len(b.state.Appliances), b.state.DeviceStatus)
	b.sendToClient(conn, WSMessage{
		Type:    "state",
		Payload: initialState,
	})

	// Cleanup on disconnect
	defer func() {
		b.wsClientsMu.Lock()
		delete(b.wsClients, conn)
		b.wsClientsMu.Unlock()
		log.Printf("WebSocket client disconnected. Total clients: %d", len(b.wsClients))
	}()

	// Read messages from client
	for {
		var cmd WSCommand
		err := conn.ReadJSON(&cmd)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		b.handleCommand(cmd)
	}
}

// Handle commands from WebSocket clients
func (b *SmartMeterBackend) handleCommand(cmd WSCommand) {
	log.Printf("Received command: %s", cmd.Action)
	switch cmd.Action {
	case "start":
		b.startMeter()
	case "stop":
		b.shutdownMeter()
	case "toggle_appliance":
		var data struct {
			ApplianceID string `json:"applianceId"`
		}
		if err := json.Unmarshal(cmd.Data, &data); err == nil {
			log.Printf("Toggling appliance: %s", data.ApplianceID)
			b.toggleAppliance(data.ApplianceID)
		} else {
			log.Printf("Error unmarshaling toggle_appliance data: %v", err)
		}
	case "request_topup":
		var data struct {
			AmountMsat int64 `json:"amountMsat"`
		}
		if err := json.Unmarshal(cmd.Data, &data); err == nil {
			b.requestTopUp(data.AmountMsat)
		}
	case "simulate_payment":
		b.simulatePayment()
	case "clear_invoice":
		b.clearInvoice()
	default:
		log.Printf("Unknown command: %s", cmd.Action)
	}
}

// Broadcast loop sends state updates to all connected clients
func (b *SmartMeterBackend) broadcastLoop() {
	for msg := range b.broadcast {
		// Take a snapshot of current clients to avoid locking upgrades on write errors
		b.wsClientsMu.RLock()
		type clientEntry struct {
			conn *websocket.Conn
			mu   *sync.Mutex
		}
		clients := make([]clientEntry, 0, len(b.wsClients))
		for client, mu := range b.wsClients {
			clients = append(clients, clientEntry{conn: client, mu: mu})
		}
		b.wsClientsMu.RUnlock()

		for _, client := range clients {
			client.mu.Lock()
			err := client.conn.WriteJSON(msg)
			client.mu.Unlock()

			if err != nil {
				log.Printf("WebSocket write error: %v", err)
				client.conn.Close()

				b.wsClientsMu.Lock()
				delete(b.wsClients, client.conn)
				b.wsClientsMu.Unlock()
			}
		}
	}
}

func (b *SmartMeterBackend) sendToClient(conn *websocket.Conn, msg WSMessage) {
	b.wsClientsMu.RLock()
	writeMu, ok := b.wsClients[conn]
	b.wsClientsMu.RUnlock()
	if !ok {
		return
	}

	writeMu.Lock()
	err := conn.WriteJSON(msg)
	writeMu.Unlock()

	if err != nil {
		log.Printf("Error sending to client: %v", err)
	}
}

func (b *SmartMeterBackend) marshalState() json.RawMessage {
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, _ := json.Marshal(&b.state)
	return data
}

func (b *SmartMeterBackend) broadcastState() {
	b.broadcast <- WSMessage{
		Type:    "state",
		Payload: b.marshalState(),
	}
}

// Logging
func (b *SmartMeterBackend) addLog(message, logType string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry := LogEntry{
		ID:        generateID(),
		Timestamp: time.Now().Format(time.RFC3339),
		Message:   message,
		Type:      logType,
	}

	b.state.Logs = append([]LogEntry{entry}, b.state.Logs...)
	if len(b.state.Logs) > 50 {
		b.state.Logs = b.state.Logs[:50]
	}

	log.Printf("[%s] %s", logType, message)
}

// Start meter system
func (b *SmartMeterBackend) startMeter() {
	b.mu.Lock()
	if b.state.DeviceStatus != "OFFLINE" {
		log.Printf("Device is not offline, skipping start")
		b.mu.Unlock()
		return
	}
	b.state.DeviceStatus = "STARTING"
	b.mu.Unlock()

	b.addLog("Starting meter system...", "info")
	b.broadcastState()

	// Connect to MQTT
	b.southbound.Connect()

	// Start simulation goroutines
	b.startSimulation()

	// Complete startup handshake asynchronously once MQTT is ready
	go b.completeStartupSequence()
}

// completeStartupSequence waits for the MQTT connection and subscriptions before sending the startup
// heartbeat and authorization request. This prevents the simulator from staying in
// STARTING state when the broker takes longer to accept the connection.
func (b *SmartMeterBackend) completeStartupSequence() {
	const timeout = 15 * time.Second

	if err := b.waitForMQTTConnection(timeout); err != nil {
		if errors.Is(err, errMQTTWaitTimeout) {
			b.addLog("MQTT connection timeout during startup - reverting to OFFLINE", "error")
			b.shutdownMeter()
		}
		return
	}

	b.mu.RLock()
	isStillStarting := b.state.DeviceStatus == "STARTING"
	b.mu.RUnlock()
	if !isStillStarting {
		return
	}

	// Wait for subscriptions to be fully established
	select {
	case <-b.subscriptionsReady:
		log.Printf("Subscriptions ready, proceeding with startup sequence")
	case <-time.After(10 * time.Second):
		b.addLog("Timeout waiting for subscriptions - reverting to OFFLINE", "error")
		b.shutdownMeter()
		return
	}

	b.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
	b.southbound.PublishAuthorizeRequest("STARTUP")
}

func (b *SmartMeterBackend) waitForMQTTConnection(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if b.mqttClient != nil && b.mqttClient.IsConnected() {
			return nil
		}

		b.mu.RLock()
		isStarting := b.state.DeviceStatus == "STARTING"
		b.mu.RUnlock()
		if !isStarting {
			return errMQTTWaitAborted
		}

		select {
		case <-timer.C:
			return errMQTTWaitTimeout
		case <-ticker.C:
		}
	}
}

// Shutdown meter system completely (disconnect from MQTT and stop all operations)
func (b *SmartMeterBackend) shutdownMeter() {
	b.mu.Lock()
	if b.state.DeviceStatus == "OFFLINE" {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	// Stop all tickers
	if b.powerUpdateTicker != nil {
		b.powerUpdateTicker.Stop()
	}
	if b.heartbeatTicker != nil {
		b.heartbeatTicker.Stop()
	}
	if b.usageTicker != nil {
		b.usageTicker.Stop()
	}

	b.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_OFFLINE)

	if b.mqttClient != nil && b.mqttClient.IsConnected() {
		b.mqttClient.Disconnect(250)
	}

	b.mu.Lock()
	b.state.DeviceStatus = "OFFLINE"
	b.state.MQTTStatus = "disconnected"
	for i := range b.state.Appliances {
		b.state.Appliances[i].IsOn = false
		b.state.Appliances[i].CurrentWatts = 0
	}
	b.state.InstantPower = 0
	b.mu.Unlock()

	b.addLog("Meter system shut down", "info")
	b.broadcastState()
}

// Halt consumption (stop all appliances but keep MQTT connection and device ONLINE)
func (b *SmartMeterBackend) haltConsumption(reason string) {
	b.mu.Lock()
	if b.state.DeviceStatus != "ONLINE" {
		b.mu.Unlock()
		return
	}

	// Turn off all appliances but keep connection
	for i := range b.state.Appliances {
		b.state.Appliances[i].IsOn = false
		b.state.Appliances[i].CurrentWatts = 0
	}
	b.state.InstantPower = 0
	b.mu.Unlock()

	b.addLog("Consumption halted: "+reason, "warning")
	b.broadcastState()
}

// Start simulation goroutines
func (b *SmartMeterBackend) startSimulation() {
	// Power update ticker (1 second)
	b.powerUpdateTicker = time.NewTicker(1 * time.Second)
	go func() {
		for range b.powerUpdateTicker.C {
			b.updatePowerReadings()
		}
	}()

	// Heartbeat ticker
	b.mu.RLock()
	heartbeatInterval := time.Duration(b.state.Config.HeartbeatInterval) * time.Second
	b.mu.RUnlock()

	b.heartbeatTicker = time.NewTicker(heartbeatInterval)
	go func() {
		for range b.heartbeatTicker.C {
			b.southbound.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
		}
	}()

	// Usage reporting ticker
	b.mu.RLock()
	reportingInterval := time.Duration(b.state.Config.ReportingInterval) * time.Second
	b.mu.RUnlock()

	b.usageTicker = time.NewTicker(reportingInterval)
	go func() {
		for range b.usageTicker.C {
			b.reportUsage()
		}
	}()
}

// Update power readings for all appliances
func (b *SmartMeterBackend) updatePowerReadings() {
	b.mu.Lock()

	totalPower := 0
	for i := range b.state.Appliances {
		appliance := &b.state.Appliances[i]
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

	b.state.InstantPower = totalPower
	b.mu.Unlock()
	b.broadcastState()
}

// Report usage and update balance
func (b *SmartMeterBackend) reportUsage() {
	b.mu.Lock()

	if b.state.DeviceStatus != "ONLINE" || b.state.InstantPower == 0 {
		b.mu.Unlock()
		return
	}

	// Calculate kWh consumed in this interval
	intervalSeconds := float64(b.state.Config.ReportingInterval)
	kWhConsumed := (float64(b.state.InstantPower) / 1000.0) * (intervalSeconds / 3600.0)

	// Update total consumption
	b.state.TotalConsumption += kWhConsumed

	reportID := generateID()
	b.mu.Unlock()

	// Publish usage report to MQTT - backend will handle balance deduction
	b.southbound.PublishUsageReport(reportID, kWhConsumed)
	b.broadcastState()
}

// Toggle appliance on/off
func (b *SmartMeterBackend) toggleAppliance(applianceID string) {
	b.mu.Lock()

	if b.state.DeviceStatus != "ONLINE" {
		b.mu.Unlock()
		b.addLog("Cannot toggle appliance - meter is offline", "error")
		return
	}

	var appliance *Appliance
	for i := range b.state.Appliances {
		if b.state.Appliances[i].ID == applianceID {
			appliance = &b.state.Appliances[i]
			break
		}
	}

	if appliance == nil {
		b.mu.Unlock()
		return
	}

	// Toggle appliance - backend will send STOP command if out of funds
	appliance.IsOn = !appliance.IsOn
	status := "OFF"
	if appliance.IsOn {
		status = "ON"
	}
	name := appliance.Name
	b.mu.Unlock()

	b.addLog(name+" turned "+status, "info")
	b.broadcastState()
}

// Request invoice for top-up
func (b *SmartMeterBackend) requestTopUp(amountMsat int64) {
	b.mu.RLock()
	if b.state.DeviceStatus != "ONLINE" {
		b.mu.RUnlock()
		b.addLog("Cannot request top-up - meter is offline", "error")
		return
	}
	deviceID := b.state.DeviceID
	b.mu.RUnlock()

	requestID := generateID()
	b.southbound.PublishInvoiceRequest(requestID, amountMsat, "USER_TOPUP")
	b.addLog("Invoice requested: "+formatMsat(amountMsat)+" msat", "info")

	// Simulate invoice response
	time.AfterFunc(500*time.Millisecond, func() {
		invoice := &InvoiceResponse{
			DeviceId:   deviceID,
			RequestId:  requestID,
			Status:     mqttmodel.InvoiceStatus_INVOICE_STATUS_CREATED,
			InvoiceId:  "inv-" + generateID(),
			Bolt11:     generateBolt11(amountMsat),
			AmountMsat: amountMsat,
			ExpiresAt:  time.Now().Add(10 * time.Minute).Format(time.RFC3339),
		}

		b.mu.Lock()
		b.state.Invoice = invoice
		b.mu.Unlock()

		b.addLog("Invoice created - scan QR to pay", "success")
		b.broadcastState()
	})
}

// Simulate payment received - in real scenario, backend will update balance via MQTT
func (b *SmartMeterBackend) simulatePayment() {
	b.mu.Lock()
	if b.state.Invoice == nil {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	b.addLog("Payment simulation - waiting for backend to confirm", "info")
	// In real implementation, payment would be detected by backend
	// and balance update would come via MQTT balance message
}

// Clear invoice
func (b *SmartMeterBackend) clearInvoice() {
	b.mu.Lock()
	b.state.Invoice = nil
	b.mu.Unlock()
	b.broadcastState()
}

// Utility functions
func generateID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(6)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func generateBolt11(amountMsat int64) string {
	return "lnbc" + formatMsat(amountMsat/1000) + "u1pjq" + randomString(40)
}

func formatMsat(msat int64) string {
	return fmt.Sprintf("%d", msat)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (b *SmartMeterBackend) hasActiveAuthorization() bool {
	for i := range b.state.Authorizations {
		if b.state.Authorizations[i].Status == "ACTIVE" {
			return true
		}
	}
	return false
}
