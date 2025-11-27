package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type LNDClient struct {
	conn           *grpc.ClientConn
	client         lnrpc.LightningClient
	invoicesClient invoicesrpc.InvoicesClient
}

// NewLNDClient creates a new LND client from hex-encoded credentials
func NewLNDClient(cfg Config) (*LNDClient, error) {
	// Decode hex TLS certificate
	tlsCertBytes, err := hex.DecodeString(cfg.LNDTLSCertHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode TLS cert: %w", err)
	}

	// Create certificate pool
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(tlsCertBytes) {
		return nil, fmt.Errorf("failed to append TLS certificate")
	}

	// Create TLS credentials
	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}
	creds := credentials.NewTLS(tlsConfig)

	// Decode hex macaroon
	macaroonBytes, err := hex.DecodeString(cfg.LNDMacaroonHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode macaroon: %w", err)
	}

	// Create macaroon credential
	mac := &macaroonCredential{macaroon: macaroonBytes}

	// Dial LND
	log.Printf("Dialing LND host %s", cfg.LNDHost)
	conn, err := grpc.Dial(
		cfg.LNDHost,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(mac),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Printf("Failed to dial LND host %s: %v", cfg.LNDHost, err)
		return nil, fmt.Errorf("failed to dial LND: %w", err)
	}
	log.Printf("Successfully connected to LND host %s", cfg.LNDHost)

	// Create clients
	client := lnrpc.NewLightningClient(conn)
	invoicesClient := invoicesrpc.NewInvoicesClient(conn)

	return &LNDClient{
		conn:           conn,
		client:         client,
		invoicesClient: invoicesClient,
	}, nil
}

// macaroonCredential implements grpc.PerRPCCredentials
type macaroonCredential struct {
	macaroon []byte
}

func (m *macaroonCredential) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"macaroon": hex.EncodeToString(m.macaroon),
	}, nil
}

func (m *macaroonCredential) RequireTransportSecurity() bool {
	return true
}

// GetInfo retrieves node information
func (c *LNDClient) GetInfo(ctx context.Context) (*lnrpc.GetInfoResponse, error) {
	return c.client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
}

// GetWalletBalance retrieves wallet balance
func (c *LNDClient) GetWalletBalance(ctx context.Context) (*lnrpc.WalletBalanceResponse, error) {
	return c.client.WalletBalance(ctx, &lnrpc.WalletBalanceRequest{})
}

// GetChannelBalance retrieves channel balance
func (c *LNDClient) GetChannelBalance(ctx context.Context) (*lnrpc.ChannelBalanceResponse, error) {
	return c.client.ChannelBalance(ctx, &lnrpc.ChannelBalanceRequest{})
}

// CreateInvoice creates a new invoice using milli-satoshi precision.
func (c *LNDClient) CreateInvoice(ctx context.Context, amountMsat int64, memo string, expirySeconds int64) (*lnrpc.AddInvoiceResponse, error) {
	if amountMsat <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	log.Printf("Creating invoice via LND: amount_msat=%d expiry=%ds memo_len=%d", amountMsat, expirySeconds, len(memo))
	invoice := &lnrpc.Invoice{
		Memo:      memo,
		ValueMsat: amountMsat,
		Expiry:    expirySeconds,
	}

	return c.client.AddInvoice(ctx, invoice)
}

// LookupInvoice retrieves invoice details by payment hash
func (c *LNDClient) LookupInvoice(ctx context.Context, paymentHash []byte) (*lnrpc.Invoice, error) {
	return c.client.LookupInvoice(ctx, &lnrpc.PaymentHash{
		RHash: paymentHash,
	})
}

// SubscribeInvoices creates a subscription stream for invoice updates
func (c *LNDClient) SubscribeInvoices(ctx context.Context, addIndex, settleIndex uint64) (lnrpc.Lightning_SubscribeInvoicesClient, error) {
	log.Printf("Subscribing to LND invoices stream (add_index=%d settle_index=%d)", addIndex, settleIndex)
	return c.client.SubscribeInvoices(ctx, &lnrpc.InvoiceSubscription{
		AddIndex:    addIndex,
		SettleIndex: settleIndex,
	})
}

// Close closes the connection
func (c *LNDClient) Close() error {
	if c.conn != nil {
		log.Println("Closing LND gRPC connection")
		return c.conn.Close()
	}
	return nil
}
