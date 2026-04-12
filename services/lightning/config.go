package main

import (
	"context"

	"github.com/robertodantas/lina/internal"
)

type Config struct {
	LNDHost          string
	LNDTLSCertHex    string
	LNDTLSServerName string
	LNDMacaroonHex   string
	Network          string
	ListenAddr       string
	GRPCAddr         string
	ServiceToken     string
	RedisAddr        string
	RedisPassword    string

	// OpenTelemetry / Jaeger
	OTELExporterOTLPEndpoint string
	OTELServiceName          string
}

func LoadConfig() *Config {
	cfg := &Config{
		LNDHost:          internal.GetEnv("LND_HOST", ""),
		LNDTLSCertHex:    internal.GetEnv("LND_TLS_CERT_HEX", ""),
		LNDTLSServerName: internal.GetEnv("LND_TLS_SERVER_NAME", "localhost"),
		LNDMacaroonHex:   internal.GetEnv("LND_MACAROON_HEX", ""),
		Network:          internal.GetEnv("NETWORK", "testnet"),
		GRPCAddr:         internal.GetEnv("GRPC_ADDR", ":9090"),
		ListenAddr:       internal.GetEnv("LISTEN_ADDR", ":8080"),
		ServiceToken:     internal.GetEnv("SERVICE_TOKEN", "dev-token"),
		RedisAddr:        internal.GetEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    internal.GetEnv("REDIS_PASSWORD", ""),

		// OpenTelemetry / Jaeger
		OTELExporterOTLPEndpoint: internal.GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		OTELServiceName:          internal.GetEnv("OTEL_SERVICE_NAME", "lightning-service"),
	}

	ctx := context.Background()

	// Validate configuration
	if cfg.LNDHost == "" {
		logger.Fatal(ctx, "LND_HOST environment variable required", nil)
	}
	if cfg.LNDTLSCertHex == "" {
		logger.Fatal(ctx, "LND_TLS_CERT_HEX environment variable required", nil)
	}
	if cfg.LNDMacaroonHex == "" {
		logger.Fatal(ctx, "LND_MACAROON_HEX environment variable required", nil)
	}

	return cfg
}
