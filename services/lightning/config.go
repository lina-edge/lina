package main

import (
	"log"

	"github.com/robertodantas/lnpay/internal"
)

type Config struct {
	LNDHost        string
	LNDTLSCertHex  string
	LNDMacaroonHex string
	Network        string
	ListenAddr     string
	GRPCAddr       string
	ServiceToken   string
	RedisAddr      string
	RedisPassword  string

	// OpenTelemetry / Jaeger
	OTELExporterOTLPEndpoint string
	OTELServiceName          string
}

func LoadConfig() *Config {
	cfg := &Config{
		LNDHost:        internal.GetEnv("LND_HOST", "lightning.db"),
		LNDTLSCertHex:  internal.GetEnv("LND_TLS_CERT_HEX", ""),
		LNDMacaroonHex: internal.GetEnv("LND_MACAROON_HEX", ""),
		Network:        internal.GetEnv("NETWORK", "testnet"),
		GRPCAddr:       internal.GetEnv("GRPC_ADDR", ":9090"),
		ListenAddr:     internal.GetEnv("LISTEN_ADDR", ":8080"),
		ServiceToken:   internal.GetEnv("SERVICE_TOKEN", "dev-token"),
		RedisAddr:      internal.GetEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  internal.GetEnv("REDIS_PASSWORD", ""),

		// OpenTelemetry / Jaeger
		OTELExporterOTLPEndpoint: internal.GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4317"),
		OTELServiceName:          internal.GetEnv("OTEL_SERVICE_NAME", "lightning-service"),
	}

	// Validate configuration
	if cfg.LNDHost == "" {
		log.Fatal("LND_HOST environment variable required")
	}
	if cfg.LNDTLSCertHex == "" {
		log.Fatal("LND_TLS_CERT_HEX environment variable required")
	}
	if cfg.LNDMacaroonHex == "" {
		log.Fatal("LND_MACAROON_HEX environment variable required")
	}

	return cfg
}
