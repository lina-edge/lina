package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/robertodantas/lina/internal"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NorthboundInterface handles REST API endpoints
type NorthboundInterface struct {
	router    *gin.Engine
	repo      LedgerRepository
	publisher *EastWestStreamPublisher
	cfg       Config
	server    *http.Server
}

// NewNorthboundInterface creates a new northbound interface
func NewNorthboundInterface(repo LedgerRepository, cfg Config, publisher *EastWestStreamPublisher) *NorthboundInterface {
	router := gin.Default()

	router.Use(otelgin.Middleware("ledger-service"))

	nb := &NorthboundInterface{
		router:    router,
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
	}

	// Register routes
	nb.registerRoutes()

	return nb
}

// registerRoutes registers all REST API routes
func (nb *NorthboundInterface) registerRoutes() {
	// Health check
	nb.router.GET("/health", nb.health)

	// API v1 routes
	api := nb.router.Group("/api/v1")
	{
		// Device-specific routes
		devices := api.Group("/devices/:id")
		{
			devices.GET("/balance", nb.getDeviceBalance)
			devices.GET("/entries", nb.listDeviceEntries)
			devices.GET("/authorizations", nb.listDeviceAuthorizations)

			// Protected mutating endpoints
			authDevices := devices.Group("/", nb.authMiddleware())
			{
				authDevices.POST("/credit", nb.postDeviceCredit)
				authDevices.POST("/debit", nb.postDeviceDebit)
			}
		}
	}
}

// health handles GET /health
func (nb *NorthboundInterface) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

// getDeviceBalance handles GET /api/v1/devices/:id/balance
func (nb *NorthboundInterface) getDeviceBalance(c *gin.Context) {
	deviceID := c.Param("id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}

	tx, err := nb.repo.BeginTx(c, &LedgerTxOptions{ReadOnly: true})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	_ = nb.repo.EnsureBalanceRow(c, tx, deviceID)
	bal, err := nb.repo.GetBalance(c, tx, deviceID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	commitStart := time.Now()
	err = tx.Commit()
	RecordTxCommitLatency(c, "http.get_device_balance", time.Since(commitStart).Seconds(), err == nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}

	c.JSON(200, gin.H{"device_id": deviceID, "balance_msat": bal})
}

// listDeviceEntries handles GET /api/v1/devices/:id/entries
func (nb *NorthboundInterface) listDeviceEntries(c *gin.Context) {
	deviceID := c.Param("id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}

	limit := min(internal.IntEnv("DEFAULT_PAGE_SIZE", 50), nb.cfg.MaxPageSize)
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, nb.cfg.MaxPageSize)
		}
	}

	// cursor = created_at:id for keyset pagination (newest first)
	var cursorCreated int64 = 1<<62 - 1 // effectively +Inf
	var cursorID string = "zzzz"
	if cur := c.Query("cursor"); cur != "" {
		parts := strings.Split(cur, ":")
		if len(parts) == 2 {
			if t, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				cursorCreated = t
				cursorID = parts[1]
			}
		}
	}

	resp, err := nb.repo.ListLedgerEntries(c, deviceID, cursorCreated, cursorID, limit)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var lastCreated int64
	var lastID string
	if len(resp) > 0 {
		lastCreated = resp[len(resp)-1].CreatedAt
		lastID = resp[len(resp)-1].EntryID
	}

	nextCursor := ""
	if len(resp) == limit {
		nextCursor = fmt.Sprintf("%d:%s", lastCreated, lastID)
	}
	c.JSON(200, gin.H{"items": resp, "next_cursor": nextCursor})
}

// AuthorizationResponse represents an authorization in the API response
type AuthorizationResponse struct {
	AuthorizationID string `json:"authorization_id"`
	DeviceID        string `json:"device_id"`
	RequestID       string `json:"request_id"`
	GrantedMsat     int64  `json:"granted_msat"`
	RemainingMsat   int64  `json:"remaining_msat"`
	ConsumedMsat    int64  `json:"consumed_msat"`
	OverflowMsat    int64  `json:"overflow_msat"`
	IssuedAt        string `json:"issued_at"`
	ExpiresAt       string `json:"expires_at"`
	Status          string `json:"status"`
	CreatedAt       int64  `json:"created_at"`
}

// listDeviceAuthorizations handles GET /api/v1/devices/:id/authorizations
func (nb *NorthboundInterface) listDeviceAuthorizations(c *gin.Context) {
	deviceID := c.Param("id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}

	// Parse active query parameter
	activeFilter := c.Query("active")
	var statusFilter string
	if activeFilter != "" {
		active, err := strconv.ParseBool(activeFilter)
		if err != nil {
			c.JSON(400, gin.H{"error": "invalid active parameter, must be true or false"})
			return
		}
		if active {
			statusFilter = "active"
		} else {
			// For false, we want non-active (completed or expired)
			statusFilter = "non-active"
		}
	}

	resp, err := nb.repo.ListAuthorizations(c, deviceID, statusFilter)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"items": resp})
}

// DeviceCreditRequest represents the request body for crediting a device (device_id comes from URL)
type DeviceCreditRequest struct {
	AmountMsat     int64  `json:"amount_msat" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

// DeviceDebitRequest represents the request body for debiting a device (device_id comes from URL)
type DeviceDebitRequest struct {
	AmountMsat     int64  `json:"amount_msat" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	AllowNegative  bool   `json:"allow_negative,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

// postDeviceCredit handles POST /api/v1/devices/:id/credit
func (nb *NorthboundInterface) postDeviceCredit(c *gin.Context) {
	deviceID := c.Param("id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}

	var in DeviceCreditRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}

	// Convert to CreditRequest with device_id from URL
	creditReq := CreditRequest{
		DeviceID:       deviceID,
		AmountMsat:     in.AmountMsat,
		Reason:         in.Reason,
		CorrelationID:  in.CorrelationID,
		IdempotencyKey: in.IdempotencyKey,
	}

	// Idempotency short-circuit
	if kind, blob, ok, err := nb.repo.GetCachedIdem(c, creditReq.IdempotencyKey); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	} else if ok && kind == "credit" {
		var out EntryResponse
		_ = json.Unmarshal(blob, &out)
		c.JSON(http.StatusOK, out)
		return
	}

	ctx := c
	tx, err := nb.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	out, err := nb.repo.ApplyCredit(ctx, tx, creditReq)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := nb.repo.SaveIdem(ctx, tx, creditReq.IdempotencyKey, "credit", hashReq("credit", creditReq), out); err != nil {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	commitStart := time.Now()
	err = tx.Commit()
	RecordTxCommitLatency(c, "http.device_credit", time.Since(commitStart).Seconds(), err == nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}

	// Emit DeviceCreditedEvent to event.ledger
	if nb.publisher != nil {
		timestamp := time.Unix(out.CreatedAt, 0).UTC().Format(time.RFC3339)
		if err := nb.publisher.PublishDeviceCredited(c, out.DeviceID, out.AmountMsat, out.BalanceAfter, timestamp); err != nil {
			logger.WithDeviceID(out.DeviceID).
				WithStream(internal.StreamLedger, "produce").
				Error(c, "Failed to publish DeviceCreditedEvent via northbound REST", err)
		}
	}

	c.JSON(http.StatusOK, out)
}

// postDeviceDebit handles POST /api/v1/devices/:id/debit
func (nb *NorthboundInterface) postDeviceDebit(c *gin.Context) {
	deviceID := c.Param("id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}

	var in DeviceDebitRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		logger.WithDeviceID(deviceID).
			Error(c, "postDeviceDebit bind error via northbound REST", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}

	// Convert to DebitRequest with device_id from URL
	debitReq := DebitRequest{
		DeviceID:       deviceID,
		AmountMsat:     in.AmountMsat,
		Reason:         in.Reason,
		AllowNegative:  in.AllowNegative,
		CorrelationID:  in.CorrelationID,
		IdempotencyKey: in.IdempotencyKey,
	}

	if kind, blob, ok, err := nb.repo.GetCachedIdem(c, debitReq.IdempotencyKey); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	} else if ok && kind == "debit" {
		var out EntryResponse
		_ = json.Unmarshal(blob, &out)
		c.JSON(http.StatusOK, out)
		return
	}

	ctx := c
	tx, err := nb.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	out, err := nb.repo.ApplyDebit(ctx, tx, debitReq)
	if err != nil {
		// Do not persist idempotency for failed attempts
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := nb.repo.SaveIdem(ctx, tx, debitReq.IdempotencyKey, "debit", hashReq("debit", debitReq), out); err != nil {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	commitStart := time.Now()
	err = tx.Commit()
	RecordTxCommitLatency(c, "http.device_debit", time.Since(commitStart).Seconds(), err == nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}

	// Emit DeviceDebitedEvent to event.ledger
	if nb.publisher != nil {
		timestamp := time.Unix(out.CreatedAt, 0).UTC().Format(time.RFC3339)
		if err := nb.publisher.PublishDeviceDebited(
			c,
			out.DeviceID,
			debitReq.CorrelationID,
			out.AmountMsat,
			out.BalanceAfter,
			timestamp,
		); err != nil {
			logger.WithDeviceID(out.DeviceID).
				WithStream(internal.StreamLedger, "produce").
				Error(c, "Failed to publish DeviceDebitedEvent via northbound REST", err)
		}
	}

	c.JSON(http.StatusOK, out)
}

// Start starts the HTTP server
func (nb *NorthboundInterface) Start(ctx context.Context, addr string) error {
	nb.server = &http.Server{
		Addr:    addr,
		Handler: nb.router,
	}

	logger.Infof(context.Background(), "Starting northbound REST API server on %s", addr)
	return nb.server.ListenAndServe()
}

// Stop gracefully stops the HTTP server
func (nb *NorthboundInterface) Stop(ctx context.Context) error {
	if nb.server != nil {
		logger.Info(ctx, "Stopping northbound REST API server")
		return nb.server.Shutdown(ctx)
	}
	return nil
}

// authMiddleware provides authentication middleware for protected endpoints
func (nb *NorthboundInterface) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		got := c.GetHeader("X-Service-Token")
		if nb.cfg.ServiceToken == "" || got == nb.cfg.ServiceToken {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}
