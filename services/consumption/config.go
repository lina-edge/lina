package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBPath             string
	RegistryBaseURL    string
	ServiceToken       string
	RegistryCacheTTL   time.Duration
	LedgerURL          string
	WorkerBatchSize    int
	WorkerPollInterval time.Duration
	DispatcherEvery    time.Duration
	MaxAttempts        int
}

func loadConfig() Config {
	return Config{
		DBPath:             getenv("DB_PATH", "consumption.db"),
		RegistryBaseURL:    getenv("REGISTRY_URL", "http://localhost:8080"),
		ServiceToken:       getenv("SERVICE_TOKEN", "dev-token"),
		RegistryCacheTTL:   durationEnv("REGISTRY_CACHE_TTL", 2*time.Minute),
		LedgerURL:          getenv("LEDGER_URL", "http://localhost:8080"),
		WorkerBatchSize:    intEnv("WORKER_BATCH_SIZE", 50),
		WorkerPollInterval: durationEnv("WORKER_POLL_INTERVAL", 500*time.Millisecond),
		DispatcherEvery:    durationEnv("DISPATCHER_EVERY", 2*time.Second),
		MaxAttempts:        intEnv("MAX_ATTEMPTS", 10),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func intEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
func durationEnv(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
