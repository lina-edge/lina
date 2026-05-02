package main

import (
	"time"

	"github.com/robertodantas/lina/internal"
)

type Config struct {
	RepositoryType string // pebble (default) or sqlite
	DBPath         string
	BusyTimeoutMS  int // SQLite busy_timeout pragma (ignored for pebble)
	ServiceToken   string
	ListenAddr     string
	GRPCAddr       string
	GRPCUseTLS     bool
	GRPCTLSCACert  string
	GRPCServerCert string
	GRPCServerKey  string
	MaxPageSize    int

	// Redis streams: REDIS_STREAM_CONSUMER_NAME; STREAM_PARALLELISM / STREAM_READ_COUNT (or map from LEDGER_STREAM_* in compose/ansible).
	StreamConsumerName string
	ConsumeParallelism int
	StreamReadCount    int
	ConsumeBatchSize   int // SQLite only: events per transaction (0/1 = off)

	// OpenTelemetry / Jaeger
	OTELExporterOTLPEndpoint string
	OTELServiceName          string

	// event.lightning retention: XTRIM … ACKED (Redis 8.2+). MAXLEN only trims when length > threshold — keep this modest (see stream_janitor.go).
	LightningJanitorEnabled     bool
	LightningJanitorInterval    time.Duration
	LightningJanitorMaxLen      int64
	LightningJanitorApprox      bool
	LightningJanitorApproxLimit int64
}

func LoadConfig() Config {
	return Config{
		RepositoryType: internal.GetEnv("REPOSITORY_TYPE", "pebble"),
		DBPath:         internal.GetEnv("DB_PATH", "ledger-pebble"),
		BusyTimeoutMS:  internal.IntEnv("BUSY_TIMEOUT_MS", 5000),
		ServiceToken:   internal.GetEnv("SERVICE_TOKEN", "dev-token"),
		ListenAddr:     internal.GetEnv("LISTEN_ADDR", ":8080"),
		GRPCAddr:       internal.GetEnv("GRPC_ADDR", ":9090"),
		GRPCUseTLS:     internal.BoolEnv("GRPC_USE_TLS", false),
		GRPCTLSCACert:  internal.GetEnv("GRPC_TLS_CA_CERT", "/certs/ca.crt"),
		GRPCServerCert: internal.GetEnv("GRPC_TLS_SERVER_CERT", "/certs/server.crt"),
		GRPCServerKey:  internal.GetEnv("GRPC_TLS_SERVER_KEY", "/certs/server.key"),
		MaxPageSize:    internal.IntEnv("MAX_PAGE_SIZE", 200),

		StreamConsumerName: internal.GetEnv("REDIS_STREAM_CONSUMER_NAME", "ledger-service"),
		// Default of 2 suits Pebble (concurrent writes). For SQLite set LEDGER_STREAM_PARALLELISM=1
		// to avoid goroutines spinning on the single connection pool while waiting to write.
		ConsumeParallelism: internal.StreamParallelismFromEnv("LEDGER_STREAM_PARALLELISM", 2),
		StreamReadCount:    internal.StreamReadCountFromEnv("LEDGER_STREAM_READ_COUNT", 100),
		// SQLite batch: number of consumption events to commit in one transaction (amortises the WAL
		// commit cost). Only active when REPOSITORY_TYPE=sqlite. 0/1 = per-event (off). Default 10.
		ConsumeBatchSize: internal.IntEnv("LEDGER_CONSUME_BATCH_SIZE", 10),

		// OpenTelemetry / Jaeger
		OTELExporterOTLPEndpoint: internal.GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		OTELServiceName:          internal.GetEnv("OTEL_SERVICE_NAME", "ledger-service"),

		LightningJanitorEnabled:     internal.BoolEnv("LEDGER_LIGHTNING_JANITOR_ENABLED", true),
		LightningJanitorInterval:    time.Duration(internal.IntEnv("LEDGER_LIGHTNING_JANITOR_INTERVAL_SEC", 30)) * time.Second,
		LightningJanitorMaxLen:      int64(internal.IntEnv("LEDGER_LIGHTNING_JANITOR_MAXLEN", 2000)),
		LightningJanitorApprox:      internal.BoolEnv("LEDGER_LIGHTNING_JANITOR_APPROX", true),
		LightningJanitorApproxLimit: int64(internal.IntEnv("LEDGER_LIGHTNING_JANITOR_APPROX_LIMIT", 10_000)),
	}
}
