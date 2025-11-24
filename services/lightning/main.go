package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type LNDConfig struct {
	Host        string
	TLSCertHex  string
	MacaroonHex string
	Network     string // "mainnet", "testnet", or "regtest"
}

type LNDClient struct {
	conn           *grpc.ClientConn
	client         lnrpc.LightningClient
	invoicesClient invoicesrpc.InvoicesClient
	lndServices    *lndclient.GrpcLndServices
}

// NewLNDClient creates a new LND client from hex-encoded credentials
func NewLNDClient(cfg LNDConfig) (*LNDClient, error) {
	// Decode hex TLS certificate
	tlsCertBytes, err := hex.DecodeString(cfg.TLSCertHex)
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
	macaroonBytes, err := hex.DecodeString(cfg.MacaroonHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode macaroon: %w", err)
	}

	// Create macaroon credential
	mac := &macaroonCredential{macaroon: macaroonBytes}

	// Dial LND
	conn, err := grpc.Dial(
		cfg.Host,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(mac),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial LND: %w", err)
	}

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

// GetBalance retrieves wallet and channel balances
func (c *LNDClient) GetBalance(ctx context.Context) error {
	// Get wallet balance
	walletBalance, err := c.client.WalletBalance(ctx, &lnrpc.WalletBalanceRequest{})
	if err != nil {
		return fmt.Errorf("wallet balance error: %w", err)
	}

	fmt.Printf("\n=== Wallet Balance ===\n")
	fmt.Printf("Confirmed: %d sats\n", walletBalance.ConfirmedBalance)
	fmt.Printf("Unconfirmed: %d sats\n", walletBalance.UnconfirmedBalance)
	fmt.Printf("Total: %d sats\n", walletBalance.TotalBalance)

	// Get channel balance
	channelBalance, err := c.client.ChannelBalance(ctx, &lnrpc.ChannelBalanceRequest{})
	if err != nil {
		return fmt.Errorf("channel balance error: %w", err)
	}

	fmt.Printf("\n=== Channel Balance ===\n")
	fmt.Printf("Local Balance: %d sats\n", channelBalance.LocalBalance.Sat)
	fmt.Printf("Remote Balance: %d sats\n", channelBalance.RemoteBalance.Sat)
	fmt.Printf("Pending Open: %d sats\n", channelBalance.PendingOpenLocalBalance.Sat)

	return nil
}

// CreateInvoice creates a new invoice
func (c *LNDClient) CreateInvoice(ctx context.Context, amountSats int64, memo string) (*lnrpc.AddInvoiceResponse, error) {
	invoice := &lnrpc.Invoice{
		Memo:   memo,
		Value:  amountSats,
		Expiry: 3600, // 1 hour
	}

	response, err := c.client.AddInvoice(ctx, invoice)
	if err != nil {
		return nil, fmt.Errorf("create invoice error: %w", err)
	}

	fmt.Printf("\n=== Invoice Created ===\n")
	fmt.Printf("Payment Request: %s\n", response.PaymentRequest)
	fmt.Printf("Payment Hash: %x\n", response.RHash)
	fmt.Printf("Add Index: %d\n", response.AddIndex)

	return response, nil
}

// SubscribeInvoices listens for invoice updates
func (c *LNDClient) SubscribeInvoices(ctx context.Context) error {
	stream, err := c.client.SubscribeInvoices(ctx, &lnrpc.InvoiceSubscription{
		AddIndex:    0,
		SettleIndex: 0,
	})
	if err != nil {
		return fmt.Errorf("subscribe invoices error: %w", err)
	}

	fmt.Println("\n=== Listening for Invoice Updates ===")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			invoice, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("receive invoice error: %w", err)
			}

			switch invoice.State {
			case lnrpc.Invoice_SETTLED:
				fmt.Printf("\n✅ PAYMENT RECEIVED\n")
				fmt.Printf("Memo: %s\n", invoice.Memo)
				fmt.Printf("Amount: %d sats\n", invoice.AmtPaidSat)
				fmt.Printf("Payment Hash: %x\n", invoice.RHash)
				fmt.Printf("Settled At: %v\n", time.Unix(invoice.SettleDate, 0))

				// Process payment based on memo
				c.processPayment(invoice)

			case lnrpc.Invoice_OPEN:
				fmt.Printf("\n📝 Invoice Created: %s (%d sats)\n", invoice.Memo, invoice.Value)

			case lnrpc.Invoice_CANCELED:
				fmt.Printf("\n❌ Invoice Canceled: %s\n", invoice.Memo)

			case lnrpc.Invoice_ACCEPTED:
				fmt.Printf("\n⏳ Payment Accepted: %s\n", invoice.Memo)
			}
		}
	}
}

// processPayment handles payment logic based on memo
func (c *LNDClient) processPayment(invoice *lnrpc.Invoice) {
	switch invoice.Memo {
	case "subscription":
		fmt.Println("→ Processing subscription payment...")
	case "order":
		fmt.Println("→ Processing order payment...")
	default:
		fmt.Printf("→ Processing payment: %s\n", invoice.Memo)
	}
}

// LookupInvoice retrieves invoice details by payment hash
func (c *LNDClient) LookupInvoice(ctx context.Context, paymentHash []byte) (*lnrpc.Invoice, error) {
	return c.client.LookupInvoice(ctx, &lnrpc.PaymentHash{
		RHash: paymentHash,
	})
}

// Close closes the connection
func (c *LNDClient) Close() error {
	return c.conn.Close()
}

func main() {
	// Configuration - replace with your values or use environment variables
	cfg := LNDConfig{
		Host:        os.Getenv("LND_HOST"),         // e.g., "localhost:10009"
		TLSCertHex:  os.Getenv("LND_TLS_CERT_HEX"), // hex-encoded TLS cert
		MacaroonHex: os.Getenv("LND_MACAROON_HEX"), // hex-encoded macaroon
		Network:     "mainnet",                     // or "testnet", "regtest"
	}

	// Validate configuration
	if cfg.Host == "" {
		log.Fatal("LND_HOST environment variable required")
	}
	if cfg.TLSCertHex == "" {
		log.Fatal("LND_TLS_CERT_HEX environment variable required")
	}
	if cfg.MacaroonHex == "" {
		log.Fatal("LND_MACAROON_HEX environment variable required")
	}

	// Log environment variables (masked for security)
	fmt.Printf("\n=== Configuration ===\n")
	fmt.Printf("LND_HOST: %s\n", cfg.Host)
	fmt.Printf("LND_TLS_CERT_HEX: %s...\n", cfg.TLSCertHex[:min(20, len(cfg.TLSCertHex))])
	fmt.Printf("LND_MACAROON_HEX: %s...\n", cfg.MacaroonHex[:min(20, len(cfg.MacaroonHex))])
	fmt.Printf("Network: %s\n", cfg.Network)

	// Create client
	client, err := NewLNDClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create LND client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get node info
	info, err := client.GetInfo(ctx)
	if err != nil {
		log.Fatalf("Failed to get info: %v", err)
	}

	fmt.Printf("\n=== Node Info ===\n")
	fmt.Printf("Alias: %s\n", info.Alias)
	fmt.Printf("Identity: %s\n", info.IdentityPubkey)
	fmt.Printf("Version: %s\n", info.Version)
	fmt.Printf("Block Height: %d\n", info.BlockHeight)
	fmt.Printf("Synced to Chain: %v\n", info.SyncedToChain)
	fmt.Printf("Num Active Channels: %d\n", info.NumActiveChannels)

	// Check balances
	if err := client.GetBalance(ctx); err != nil {
		log.Printf("Balance check error: %v", err)
	}

	// Create a test invoice
	_, err = client.CreateInvoice(ctx, 1000, "test payment")
	if err != nil {
		log.Printf("Invoice creation error: %v", err)
	}

	// Subscribe to invoice updates (blocking)
	if err := client.SubscribeInvoices(ctx); err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}
}
