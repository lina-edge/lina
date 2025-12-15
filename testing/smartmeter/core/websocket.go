package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// WebSocket message types and upgrader (scoped to websocket)
type WSMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type WSCommand struct {
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data,omitempty"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// WebSocketHandler manages WebSocket connections for the UI
type WebSocketHandler struct {
	meter     *SmartMeter
	clients   map[*websocket.Conn]*sync.Mutex
	clientsMu sync.RWMutex
	broadcast chan interface{}
}

// NewWebSocketHandler creates a new WebSocket handler
func NewWebSocketHandler(meter *SmartMeter) *WebSocketHandler {
	handler := &WebSocketHandler{
		meter:     meter,
		clients:   make(map[*websocket.Conn]*sync.Mutex),
		broadcast: make(chan interface{}, 100),
	}

	// Set the meter's state change callback to broadcast to all WS clients
	meter.SetStateChangeCallback(func(state DeviceState) {
		handler.BroadcastState()
	})

	// Start broadcast loop
	go handler.broadcastLoop()

	return handler
}

// Callbacks removed - WebSocketHandler calls meter methods directly

// HandleWebSocket handles WebSocket connections
func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error(ctx, "WebSocket upgrade error via northbound REST", err)
		return
	}
	defer conn.Close()

	// Register client
	h.clientsMu.Lock()
	h.clients[conn] = &sync.Mutex{}
	h.clientsMu.Unlock()

	logger.InfoWithFields(ctx, "WebSocket client connected via northbound REST", map[string]interface{}{
		"total_clients": len(h.clients),
	})

	// Send initial state
	state := h.meter.GetState()
	stateJSON := h.meter.GetStateJSON()

	// Parse JSON to verify appliances are included
	var verifyState map[string]interface{}
	if err := json.Unmarshal(stateJSON, &verifyState); err == nil {
		if appliances, ok := verifyState["appliances"].([]interface{}); ok {
			logger.InfoWithFields(ctx, "Sending initial state via northbound REST", map[string]interface{}{
				"appliance_count":         len(state.Appliances),
				"appliance_count_in_json": len(appliances),
				"device_status":           state.DeviceStatus,
			})
		} else {
			logger.WarnWithFields(ctx, "Appliances not found in JSON state", map[string]interface{}{
				"appliance_count": len(state.Appliances),
				"device_status":   state.DeviceStatus,
			})
		}
	} else {
		logger.InfoWithFields(ctx, "Sending initial state via northbound REST", map[string]interface{}{
			"appliance_count": len(state.Appliances),
			"device_status":   state.DeviceStatus,
		})
	}

	h.sendToClient(conn, WSMessage{
		Type:    "state",
		Payload: stateJSON,
	})

	// Cleanup on disconnect
	defer func() {
		h.clientsMu.Lock()
		delete(h.clients, conn)
		h.clientsMu.Unlock()
		logger.InfoWithFields(ctx, "WebSocket client disconnected via northbound REST", map[string]interface{}{
			"total_clients": len(h.clients),
		})
	}()

	// Read messages from client
	for {
		var cmd WSCommand
		err := conn.ReadJSON(&cmd)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Error(ctx, "WebSocket error via northbound REST", err)
			}
			break
		}

		h.handleCommand(ctx, cmd)
	}
}

// handleCommand processes commands from WebSocket clients
func (h *WebSocketHandler) handleCommand(ctx context.Context, cmd WSCommand) {
	logger.InfoWithFields(ctx, "Received command via northbound REST", map[string]interface{}{
		"action": cmd.Action,
	})

	switch cmd.Action {
	case "start":
		// Execute in goroutine to avoid blocking websocket handler
		go func() {
			h.meter.Start()
		}()

	case "stop":
		// Execute in goroutine to avoid blocking websocket handler
		go func() {
			h.meter.Shutdown()
		}()

	case "toggle_appliance":
		var data struct {
			ApplianceID string `json:"applianceId"`
		}
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			logger.ErrorWithFields(ctx, "Error unmarshaling toggle_appliance data via northbound REST", err, map[string]interface{}{
				"payload": string(cmd.Data),
			})
		} else if data.ApplianceID == "" {
			logger.WarnWithFields(ctx, "Invalid appliance ID for toggle_appliance request via northbound REST", map[string]interface{}{
				"applianceId": data.ApplianceID,
			})
		} else {
			h.meter.ToggleAppliance(data.ApplianceID)
		}

	case "request_topup":
		var data struct {
			AmountMsat int64 `json:"amountMsat"`
		}
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			logger.ErrorWithFields(ctx, "Error unmarshaling request_topup data via northbound REST", err, map[string]interface{}{
				"payload": string(cmd.Data),
			})
		} else if data.AmountMsat <= 0 {
			logger.WarnWithFields(ctx, "Invalid amount for topup request via northbound REST", map[string]interface{}{
				"amountMsat": data.AmountMsat,
			})
		} else {
			// Execute in goroutine to avoid blocking websocket handler
			go func(amount int64) {
				h.meter.RequestTopUp(amount)
			}(data.AmountMsat)
		}

	case "clear_invoice":
		h.meter.ClearInvoice()

	default:
		logger.WarnWithFields(ctx, "Unknown command via northbound REST", map[string]interface{}{
			"action": cmd.Action,
		})
	}
}

// broadcastLoop sends state updates to all connected clients
func (h *WebSocketHandler) broadcastLoop() {
	ctx := context.Background()

	for msg := range h.broadcast {
		// Take a snapshot of current clients to avoid locking upgrades on write errors
		h.clientsMu.RLock()
		type clientEntry struct {
			conn *websocket.Conn
			mu   *sync.Mutex
		}
		clients := make([]clientEntry, 0, len(h.clients))
		for client, mu := range h.clients {
			clients = append(clients, clientEntry{conn: client, mu: mu})
		}
		h.clientsMu.RUnlock()

		for _, client := range clients {
			client.mu.Lock()
			err := client.conn.WriteJSON(msg)
			client.mu.Unlock()

			if err != nil {
				logger.Error(ctx, "WebSocket write error via northbound REST", err)
				client.conn.Close()

				h.clientsMu.Lock()
				delete(h.clients, client.conn)
				h.clientsMu.Unlock()
			}
		}
	}
}

// sendToClient sends a message to a specific client
func (h *WebSocketHandler) sendToClient(conn *websocket.Conn, msg WSMessage) {
	ctx := context.Background()

	h.clientsMu.RLock()
	writeMu, ok := h.clients[conn]
	h.clientsMu.RUnlock()
	if !ok {
		return
	}

	writeMu.Lock()
	err := conn.WriteJSON(msg)
	writeMu.Unlock()

	if err != nil {
		logger.Error(ctx, "Error sending to client via northbound REST", err)
	}
}

// BroadcastState broadcasts the current state to all clients
// Uses non-blocking send to prevent blocking if channel is full
func (h *WebSocketHandler) BroadcastState() {
	ctx := context.Background()

	msg := WSMessage{
		Type:    "state",
		Payload: h.meter.GetStateJSON(),
	}

	select {
	case h.broadcast <- msg:
		// Successfully queued
	default:
		// Channel is full, log warning but don't block
		logger.WarnWithFields(ctx, "Broadcast channel full, dropping state update", map[string]interface{}{
			"channel_capacity": cap(h.broadcast),
		})
	}
}
