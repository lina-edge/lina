package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

const northboundRequestTimeout = 5 * time.Second

// NorthboundInterface exposes a lightweight REST surface for LND data.
type NorthboundInterface struct {
	router    *gin.Engine
	lndClient *LNDClient
	cfg       *Config
	server    *http.Server
}

// NewNorthboundInterface wires the HTTP handlers.
func NewNorthboundInterface(lndClient *LNDClient, cfg *Config) *NorthboundInterface {
	router := gin.Default()

	// Add OpenTelemetry middleware for automatic route-based span naming
	router.Use(otelgin.Middleware("lightning-service"))

	nb := &NorthboundInterface{
		router:    router,
		lndClient: lndClient,
		cfg:       cfg,
	}

	nb.registerRoutes()
	return nb
}

func (nb *NorthboundInterface) registerRoutes() {
	nb.router.GET("/health", nb.health)

	api := nb.router.Group("/api/v1")
	{
		lndGroup := api.Group("/lnd", nb.authMiddleware())
		{
			lndGroup.GET("/info", nb.getInfo)
			lndGroup.GET("/wallet", nb.getWallet)
		}
	}
}

func (nb *NorthboundInterface) health(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoWithFields(ctx, "Health check requested via northbound REST", map[string]interface{}{
		"client_ip": c.ClientIP(),
	})
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (nb *NorthboundInterface) getInfo(c *gin.Context) {
	start := time.Now()
	ctx := c.Request.Context()
	logger.InfoWithFields(ctx, "Northbound getInfo request via northbound REST", map[string]interface{}{
		"client_ip": c.ClientIP(),
	})
	ctx, cancel := context.WithTimeout(ctx, northboundRequestTimeout)
	defer cancel()

	info, err := nb.lndClient.GetInfo(ctx)
	if err != nil {
		logger.Error(ctx, "Northbound getInfo failed via northbound REST", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	logger.InfoWithFields(ctx, "Northbound getInfo succeeded via northbound REST", map[string]interface{}{
		"duration":     time.Since(start).String(),
		"alias":        info.Alias,
		"block_height": info.BlockHeight,
	})
	c.JSON(http.StatusOK, info)
}

func (nb *NorthboundInterface) getWallet(c *gin.Context) {
	start := time.Now()
	ctx := c.Request.Context()
	logger.InfoWithFields(ctx, "Northbound getWallet request via northbound REST", map[string]interface{}{
		"client_ip": c.ClientIP(),
	})
	ctx, cancel := context.WithTimeout(ctx, northboundRequestTimeout)
	defer cancel()

	bal, err := nb.lndClient.GetWalletBalance(ctx)
	if err != nil {
		logger.Error(ctx, "Northbound getWallet failed via northbound REST", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	logger.InfoWithFields(ctx, "Northbound getWallet succeeded via northbound REST", map[string]interface{}{
		"duration":      time.Since(start).String(),
		"confirmed_sat": bal.ConfirmedBalance,
	})
	c.JSON(http.StatusOK, bal)
}

// Start boots the HTTP server.
func (nb *NorthboundInterface) Start(ctx context.Context, addr string) error {
	logger.Infof(ctx, "Starting northbound HTTP server on %s", addr)
	nb.server = &http.Server{
		Addr:    addr,
		Handler: nb.router,
	}

	return nb.server.ListenAndServe()
}

// Stop gracefully stops the HTTP server.
func (nb *NorthboundInterface) Stop(ctx context.Context) error {
	logger.Info(ctx, "Stopping northbound HTTP server")
	if nb.server == nil {
		return nil
	}
	return nb.server.Shutdown(ctx)
}

func (nb *NorthboundInterface) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if nb.cfg.ServiceToken == "" {
			c.Next()
			return
		}

		if c.GetHeader("X-Service-Token") == nb.cfg.ServiceToken {
			c.Next()
			return
		}

		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}
