package main

import (
	"github.com/robertodantas/lnpay/internal"
)

type Config struct {
	DBPath        string
	ServiceToken  string
	ListenAddr    string
	GRPCAddr      string
	MaxPageSize   int
	BusyTimeoutMS int

	// OpenTelemetry / Jaeger
	OTELExporterOTLPEndpoint string
	OTELServiceName          string
}

func LoadConfig() Config {
	return Config{
		DBPath:        internal.GetEnv("DB_PATH", "ledger.db"),
		ServiceToken:  internal.GetEnv("SERVICE_TOKEN", "dev-token"),
		ListenAddr:    internal.GetEnv("LISTEN_ADDR", ":8080"),
		GRPCAddr:      internal.GetEnv("GRPC_ADDR", ":9090"),
		MaxPageSize:   internal.IntEnv("MAX_PAGE_SIZE", 200),
		BusyTimeoutMS: internal.IntEnv("BUSY_TIMEOUT_MS", 5000),

		// OpenTelemetry / Jaeger
		OTELExporterOTLPEndpoint: internal.GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4317"),
		OTELServiceName:          internal.GetEnv("OTEL_SERVICE_NAME", "ledger-service"),
	}
}

