package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
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
	mu                sync.RWMutex
	state             DeviceState
	mqttClient        mqtt.Client
	wsClients         map[*websocket.Conn]*sync.Mutex
	wsClientsMu       sync.RWMutex
	broadcast         chan interface{}
	stopChan          chan bool
	powerUpdateTicker *time.Ticker
	heartbeatTicker   *time.Ticker
	usageTicker       *time.Ticker
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

	return &SmartMeterBackend{
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
		wsClients: make(map[*websocket.Conn]*sync.Mutex),
		broadcast: make(chan interface{}, 100),
		stopChan:  make(chan bool),
	}
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
		b.stopMeter()
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
	b.connectMQTT()

	// Start simulation goroutines
	b.startSimulation()

	// Complete startup handshake asynchronously once MQTT is ready
	go b.completeStartupSequence()
}

// completeStartupSequence waits for the MQTT connection before sending the startup
// heartbeat and authorization request. This prevents the simulator from staying in
// STARTING state when the broker takes longer to accept the connection.
func (b *SmartMeterBackend) completeStartupSequence() {
	const timeout = 15 * time.Second

	if err := b.waitForMQTTConnection(timeout); err != nil {
		if errors.Is(err, errMQTTWaitTimeout) {
			b.addLog("MQTT connection timeout during startup - reverting to OFFLINE", "error")
			b.stopMeter()
		}
		return
	}

	b.mu.RLock()
	isStillStarting := b.state.DeviceStatus == "STARTING"
	b.mu.RUnlock()
	if !isStillStarting {
		return
	}

	b.publishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
	b.publishAuthorizeRequest("STARTUP")
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

// Stop meter system
func (b *SmartMeterBackend) stopMeter() {
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

	b.publishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_OFFLINE)

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

	b.addLog("Meter system stopped", "info")
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
			b.publishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
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

	// Calculate cost in msat
	unitPrice := 10.0 // Parse from config if needed
	costMsat := int64(kWhConsumed * unitPrice)

	// Update total consumption
	b.state.TotalConsumption += kWhConsumed

	// Update balance (local simulation)
	outOfFunds := false
	if b.state.Balance != nil {
		b.state.Balance.AvailableMsat = maxInt64(0, b.state.Balance.AvailableMsat-costMsat)
		b.state.Balance.TotalMsat = maxInt64(0, b.state.Balance.TotalMsat-costMsat)
		b.state.Balance.Timestamp = time.Now().Format(time.RFC3339)

		// Check if out of funds
		if b.state.Balance.AvailableMsat <= 0 {
			outOfFunds = true
			for i := range b.state.Appliances {
				b.state.Appliances[i].IsOn = false
				b.state.Appliances[i].CurrentWatts = 0
			}
			b.state.InstantPower = 0
		}
	}

	reportID := generateID()
	b.mu.Unlock()

	if outOfFunds {
		b.addLog("OUT OF FUNDS - All appliances stopped", "error")
	}

	// Publish usage report to MQTT
	b.publishUsageReport(reportID, kWhConsumed)
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

	if !appliance.IsOn && b.state.Balance != nil && b.state.Balance.AvailableMsat <= 0 {
		name := appliance.Name
		b.mu.Unlock()
		b.addLog("Cannot turn on "+name+" - out of funds", "error")
		return
	}

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
	b.publishInvoiceRequest(requestID, amountMsat, "USER_TOPUP")
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

// Simulate payment received
func (b *SmartMeterBackend) simulatePayment() {
	b.mu.Lock()

	if b.state.Invoice == nil {
		b.mu.Unlock()
		return
	}

	amountMsat := b.state.Invoice.AmountMsat

	if b.state.Balance == nil {
		b.state.Balance = &BalanceMessage{
			DeviceId:      b.state.DeviceID,
			AvailableMsat: amountMsat,
			ReservedMsat:  0,
			TotalMsat:     amountMsat,
			Timestamp:     time.Now().Format(time.RFC3339),
		}
	} else {
		b.state.Balance.AvailableMsat += amountMsat
		b.state.Balance.TotalMsat += amountMsat
		b.state.Balance.Timestamp = time.Now().Format(time.RFC3339)
	}

	b.state.Invoice = nil
	b.mu.Unlock()

	b.addLog("Payment received!", "success")
	b.broadcastState()
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

// createTLSConfig creates TLS configuration for MQTT connection
func (b *SmartMeterBackend) createTLSConfig() (*tls.Config, error) {
	caFile := getEnv("MQTT_TLS_CA_CERT", "/certs/ca.crt")
	skipVerify := getEnv("MQTT_TLS_SKIP_VERIFY", "false") == "true"
	serverName := getEnv("MQTT_TLS_SERVER_NAME", "mosquitto")

	// Load CA cert
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: skipVerify,
		ServerName:         serverName,
	}

	return tlsConfig, nil
}

// MQTT Integration
func (b *SmartMeterBackend) connectMQTT() {
	// Determine broker URL based on TLS configuration
	useTLS := getEnv("MQTT_USE_TLS", "true") == "true"
	broker := getEnv("MQTT_BROKER", "mosquitto")
	var brokerURL string

	if useTLS {
		port := getEnv("MQTT_TLS_PORT", "8883")
		brokerURL = "ssl://" + broker + ":" + port
	} else {
		port := getEnv("MQTT_PORT", "1883")
		brokerURL = "tcp://" + broker + ":" + port
	}

	username := getEnv("MQTT_USERNAME", b.state.DeviceID)
	password := getEnv("MQTT_PASSWORD", b.state.DeviceID+"_password")

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(b.state.DeviceID + "_backend_" + generateID())
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)

	// Configure TLS if enabled
	if useTLS {
		tlsConfig, err := b.createTLSConfig()
		if err != nil {
			b.addLog("Failed to create TLS config: "+err.Error(), "error")
			return
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		b.mu.Lock()
		b.state.MQTTStatus = "connected"
		b.mu.Unlock()

		b.addLog("Connected to MQTT broker", "success")
		b.subscribeToTopics()
		b.broadcastState()
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		b.mu.Lock()
		b.state.MQTTStatus = "disconnected"
		b.mu.Unlock()

		b.addLog("MQTT connection lost: "+err.Error(), "error")
		b.broadcastState()
	})

	b.mqttClient = mqtt.NewClient(opts)

	b.mu.Lock()
	b.state.MQTTStatus = "connecting"
	b.mu.Unlock()
	b.broadcastState()

	if token := b.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		err := token.Error()
		errMsg := err.Error()
		b.mu.Lock()
		b.state.MQTTStatus = "error"
		b.mu.Unlock()
		b.addLog("MQTT connection failed: "+errMsg, "error")
		if isMQTTAuthError(errMsg) {
			b.addLog("MQTT credentials rejected - setting device OFFLINE", "error")
			b.stopMeter()
		}
		b.broadcastState()
	}
}

func isMQTTAuthError(errMsg string) bool {
	msg := strings.ToLower(errMsg)
	return strings.Contains(msg, "not authorized") || strings.Contains(msg, "not authorised")
}

func (b *SmartMeterBackend) subscribeToTopics() {
	deviceID := b.state.DeviceID

	topics := map[string]mqtt.MessageHandler{
		"/devices/" + deviceID + "/config":             b.handleConfigMessage,
		"/devices/" + deviceID + "/response/authorize": b.handleAuthorizeResponse,
		"/devices/" + deviceID + "/balance":            b.handleBalanceMessage,
		"/devices/" + deviceID + "/response/invoice":   b.handleInvoiceResponse,
		"/devices/" + deviceID + "/control":            b.handleControlMessage,
	}

	for topic, handler := range topics {
		if token := b.mqttClient.Subscribe(topic, 1, handler); token.Wait() && token.Error() != nil {
			b.addLog("Failed to subscribe to "+topic+": "+token.Error().Error(), "error")
		} else {
			log.Printf("Subscribed to %s", topic)
		}
	}
}

// MQTT Message Handlers
func (b *SmartMeterBackend) handleConfigMessage(client mqtt.Client, msg mqtt.Message) {
	log.Printf("DEBUG: Raw config payload: %s", string(msg.Payload()))

	var config Config
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &config); err != nil {
		b.addLog("Failed to parse config message: "+err.Error(), "error")
		return
	}

	b.mu.Lock()
	b.state.Config = &config
	b.mu.Unlock()

	b.addLog("Configuration updated", "info")
	b.broadcastState()
}

func (b *SmartMeterBackend) handleAuthorizeResponse(client mqtt.Client, msg mqtt.Message) {
	var response AuthorizeResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		b.addLog("Failed to parse authorize response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED:
		auth := Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     response.GrantedMsat,
			RemainingMsat:   response.RemainingMsat,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "ACTIVE",
		}

		b.mu.Lock()
		b.state.Authorizations = append(b.state.Authorizations, auth)
		b.state.DeviceStatus = "ONLINE"
		b.mu.Unlock()

		b.addLog("Authorization granted: "+formatMsat(response.GrantedMsat)+" msat (reserved)", "success")
		b.broadcastState()
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED:
		b.addLog("Authorization rejected: "+response.RequestId, "error")
		b.stopMeter()
	}
}

func (b *SmartMeterBackend) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	log.Printf("DEBUG: Raw balance payload: %s", string(msg.Payload()))

	var balance BalanceMessage
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		b.addLog("Failed to parse balance message: "+err.Error(), "error")
		return
	}

	b.mu.Lock()
	b.state.Balance = &balance
	b.mu.Unlock()

	b.addLog("Balance updated: "+formatMsat(balance.AvailableMsat)+" msat available", "info")
	b.broadcastState()
}

func (b *SmartMeterBackend) handleInvoiceResponse(client mqtt.Client, msg mqtt.Message) {
	var response InvoiceResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		b.addLog("Failed to parse invoice response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_CREATED:
		b.mu.Lock()
		b.state.Invoice = &response
		b.mu.Unlock()

		b.addLog("Invoice created - scan QR to pay", "success")
		b.broadcastState()
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_SETTLED:
		b.mu.Lock()
		b.state.Invoice = nil
		b.mu.Unlock()

		b.addLog("Payment received: "+formatMsat(response.AmountMsat)+" msat", "success")
		b.broadcastState()
	}
}

func (b *SmartMeterBackend) handleControlMessage(client mqtt.Client, msg mqtt.Message) {
	// Handle control commands from backend if needed
	log.Printf("Control message received: %s", string(msg.Payload()))
}

// MQTT Publishers
func (b *SmartMeterBackend) publishHeartbeat(status mqttmodel.DeviceStatus) {
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	heartbeat := HeartbeatMessage{
		DeviceId:  b.state.DeviceID,
		Status:    status,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&heartbeat)
	if err != nil {
		b.addLog("Failed to marshal heartbeat: "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/heartbeat"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish heartbeat: "+token.Error().Error(), "error")
	}
	b.broadcastState()
}

func (b *SmartMeterBackend) publishAuthorizeRequest(reason string) {
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	request := AuthorizeRequest{
		DeviceId:    b.state.DeviceID,
		RequestId:   generateID(),
		RequestMsat: b.state.Config.AuthorizeRequestMsat,
		Reason:      reason,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		b.addLog("Failed to marshal authorize request ("+request.RequestId+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/request/authorize"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish authorize request ("+request.RequestId+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Authorization requested (%s): %s msat for %s",
			request.RequestId,
			formatMsat(request.RequestMsat),
			reason,
		)
		b.addLog(msg, "info")
	}
}

func (b *SmartMeterBackend) publishUsageReport(reportID string, kWhConsumed float64) {
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}
	if b.state.Config == nil {
		return
	}

	report := UsageReport{
		DeviceId:  b.state.DeviceID,
		ReportId:  reportID,
		Strategy:  b.state.Config.ReportingStrategy,
		Measure:   kWhConsumed,
		Unit:      b.state.Config.Unit,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&report)
	if err != nil {
		b.addLog("Failed to marshal usage report ("+reportID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/usage"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish usage report ("+reportID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Usage report sent (%s): %.4f %s",
			reportID,
			kWhConsumed,
			report.Unit,
		)
		b.addLog(msg, "info")
	}
}

func (b *SmartMeterBackend) publishInvoiceRequest(requestID string, amountMsat int64, reason string) {
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	request := InvoiceRequest{
		DeviceId:   b.state.DeviceID,
		RequestId:  requestID,
		AmountMsat: amountMsat,
		Reason:     reason,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		b.addLog("Failed to marshal invoice request ("+requestID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/request/invoice"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish invoice request ("+requestID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Invoice request sent (%s): %s msat for %s",
			requestID,
			formatMsat(amountMsat),
			reason,
		)
		b.addLog(msg, "info")
	}
}
