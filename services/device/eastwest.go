package main

import (
	"context"
	"fmt"
	"time"

	internalpkg "github.com/robertodantas/lnpay/internal"
	ledgerpb "github.com/robertodantas/lnpay/proto/gen/interfaces/ledger"
	lightningpb "github.com/robertodantas/lnpay/proto/gen/interfaces/lightning"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
	lightningmodel "github.com/robertodantas/lnpay/proto/gen/model/lightning"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// LedgerClient wraps the gRPC client for the ledger service
type LedgerClient struct {
	client ledgerpb.LedgerServiceClient
	conn   *grpc.ClientConn
}

// LightningClient wraps the gRPC client for the lightning service
type LightningClient struct {
	client lightningpb.LightningServiceClient
	conn   *grpc.ClientConn
}

// NewLedgerClient creates a new gRPC client connection to the ledger service
func NewLedgerClient(cfg Config) (*LedgerClient, error) {
	host := cfg.LedgerGRPCHost
	port := cfg.LedgerGRPCPort

	addr := fmt.Sprintf("%s:%d", host, port)
	logger.Infof("Connecting to ledger gRPC service at %s via eastwest gRPC", addr)

	// Configure keepalive for long-lived connections
	// Time: 30s is a reasonable interval to avoid "too_many_pings" errors
	keepaliveParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}

	// Create gRPC connection (using insecure for now, can be upgraded to TLS later)
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(internalpkg.LoggingUnaryClientInterceptor("device-service")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	client := ledgerpb.NewLedgerServiceClient(conn)

	logger.Infof("Connected to ledger gRPC service at %s via eastwest gRPC", addr)

	return &LedgerClient{
		client: client,
		conn:   conn,
	}, nil
}

// Close closes the gRPC connection
func (c *LedgerClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Close closes the gRPC connection
func (c *LightningClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// CreateOrGetAuthorization creates a new authorization or returns the active one for the device
func (c *LedgerClient) CreateOrGetAuthorization(ctx context.Context, deviceID string, requestID string, requestMsat int64, reason string) (*ledgermodel.CreateAuthorizationResponse, error) {
	req := &ledgermodel.CreateAuthorizationRequest{
		DeviceId:    deviceID,
		RequestId:   requestID,
		RequestMsat: requestMsat,
		Reason:      reason,
	}

	resp, err := c.client.CreateOrGetAuthorization(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create or get authorization: %w", err)
	}

	return resp, nil
}

// NewLightningClient creates a new gRPC client connection to the lightning service
func NewLightningClient(cfg Config) (*LightningClient, error) {
	host := cfg.LightningGRPCHost
	port := cfg.LightningGRPCPort

	addr := fmt.Sprintf("%s:%d", host, port)
	logger.Infof("Connecting to lightning gRPC service at %s via eastwest gRPC", addr)

	keepaliveParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(internalpkg.LoggingUnaryClientInterceptor("device-service")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create lightning gRPC client: %w", err)
	}

	client := lightningpb.NewLightningServiceClient(conn)

	logger.Infof("Connected to lightning gRPC service at %s via eastwest gRPC", addr)

	return &LightningClient{
		client: client,
		conn:   conn,
	}, nil
}

// CreateInvoice requests a new invoice from the lightning service
func (c *LightningClient) CreateInvoice(ctx context.Context, deviceID string, amountMsat int64, reason string) (*lightningmodel.CreateInvoiceResponse, error) {
	req := &lightningmodel.CreateInvoiceRequest{
		DeviceId:   deviceID,
		AmountMsat: amountMsat,
		Reason:     reason,
	}

	resp, err := c.client.CreateInvoice(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create invoice: %w", err)
	}

	return resp, nil
}
