package main

import (
	"github.com/robertodantas/lnpay/internal"
)

type Config struct {
	// Database
	DBPath string

	// API
	APIAddr string

	// MQTT Configuration
	MQTTBroker             string
	MQTTUseTLS             bool
	MQTTPort               int
	MQTTTLSPort            int
	MQTTTLSProtocol        string
	MQTTClientID           string
	MQTTUsername           string
	MQTTPassword           string
	MQTTTLSSkipVerify      bool
	MQTTTLSServerName      string
	MQTTTLSCACert          string
	MQTTTLSRequireEdgeCert bool
	MQTTTLSEdgeCert        string
	MQTTTLSEdgeKey         string

	// MQTT Dynamic Security
	MQTTDynSecAdminUser     string
	MQTTDynSecAdminPassword string

	// Ledger gRPC
	LedgerGRPCHost string
	LedgerGRPCPort int
}

func LoadConfig() Config {
	return Config{
		// Database
		DBPath: internal.GetEnv("DB_PATH", "devices.db"),

		// API
		APIAddr: internal.GetEnv("API_ADDR", ":8080"),

		// MQTT Configuration
		MQTTBroker:             internal.GetEnv("MQTT_BROKER", "mosquitto"),
		MQTTUseTLS:             internal.BoolEnv("MQTT_USE_TLS", true),
		MQTTPort:               internal.IntEnv("MQTT_PORT", 1883),
		MQTTTLSPort:            internal.IntEnv("MQTT_TLS_PORT", 8883),
		MQTTTLSProtocol:        internal.GetEnv("MQTT_TLS_PROTOCOL", "tls"),
		MQTTClientID:           internal.GetEnv("MQTT_CLIENT_ID", "device-service"),
		MQTTUsername:           internal.GetEnv("MQTT_USERNAME", ""),
		MQTTPassword:           internal.GetEnv("MQTT_PASSWORD", ""),
		MQTTTLSSkipVerify:      internal.BoolEnv("MQTT_TLS_SKIP_VERIFY", false),
		MQTTTLSServerName:      internal.GetEnv("MQTT_TLS_SERVER_NAME", ""),
		MQTTTLSCACert:          internal.GetEnv("MQTT_TLS_CA_CERT", "/certs/ca.crt"),
		MQTTTLSRequireEdgeCert: internal.BoolEnv("MQTT_TLS_REQUIRE_EDGE_CERT", false),
		MQTTTLSEdgeCert:        internal.GetEnv("MQTT_TLS_EDGE_CERT", ""),
		MQTTTLSEdgeKey:         internal.GetEnv("MQTT_TLS_EDGE_KEY", ""),

		// MQTT Dynamic Security
		MQTTDynSecAdminUser:     internal.GetEnv("MQTT_DYNSEC_ADMIN_USER", "admin"),
		MQTTDynSecAdminPassword: internal.GetEnv("MQTT_DYNSEC_ADMIN_PASSWORD", "admin"),

		// Ledger gRPC
		LedgerGRPCHost: internal.GetEnv("LEDGER_GRPC_HOST", "ledger"),
		LedgerGRPCPort: internal.IntEnv("LEDGER_GRPC_PORT", 9090),
	}
}
