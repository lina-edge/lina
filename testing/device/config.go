package device

import "github.com/robertodantas/lina/internal"

// LoadConfig loads HTTP + MQTT settings using the same environment variable names and defaults
// as services/device/config.go (PORT for HTTP; MQTT_* for broker and TLS).
// It does not load MQTT_USERNAME/MQTT_PASSWORD: simulators authenticate with Connect(deviceID, deviceSecret).
func LoadConfig() Config {
	return Config{
		HTTPPort: internal.GetEnv("PORT", "8080"),

		MQTTBroker:             internal.GetEnv("MQTT_BROKER", "nanomq"),
		MQTTUseTLS:             internal.BoolEnv("MQTT_USE_TLS", true),
		MQTTPort:               internal.IntEnv("MQTT_PORT", 1883),
		MQTTTLSPort:            internal.IntEnv("MQTT_TLS_PORT", 8883),
		MQTTTLSProtocol:        internal.GetEnv("MQTT_TLS_PROTOCOL", "tls"),
		MQTTClientID:           internal.GetEnv("MQTT_CLIENT_ID", "device-service"),
		MQTTTLSSkipVerify:      internal.BoolEnv("MQTT_TLS_SKIP_VERIFY", false),
		MQTTTLSServerName:      internal.GetEnv("MQTT_TLS_SERVER_NAME", "nanomq"),
		MQTTTLSCACert:          internal.GetEnv("MQTT_TLS_CA_CERT", "/certs/ca.crt"),
		MQTTTLSRequireEdgeCert: internal.BoolEnv("MQTT_TLS_REQUIRE_EDGE_CERT", false),
		MQTTTLSEdgeCert:        internal.GetEnv("MQTT_TLS_EDGE_CERT", ""),
		MQTTTLSEdgeKey:         internal.GetEnv("MQTT_TLS_EDGE_KEY", ""),
	}
}
