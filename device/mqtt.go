package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTClient wraps the MQTT client and connection logic
type MQTTClient struct {
	client mqtt.Client
	broker string
	port   int
}

// MQTTConnectionOptions holds options for MQTT connection
type MQTTConnectionOptions struct {
	ClientID  string
	Username  string
	Password  string
	UseTLS    bool
	Broker    string
	Port      int
	Protocol  string
	Timeout   time.Duration
	KeepAlive time.Duration
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// createTLSConfig creates a TLS configuration from environment variables
func createTLSConfig() (*tls.Config, error) {
	// Check if we should skip certificate verification (for testing only)
	skipVerify := getEnvBool("MQTT_TLS_SKIP_VERIFY", false)

	broker := getEnv("MQTT_BROKER", "mosquitto")
	// Allow custom server name for certificate validation (useful when CN doesn't match hostname)
	serverName := getEnv("MQTT_TLS_SERVER_NAME", broker)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify,
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName, // Set server name for SNI and hostname verification
	}

	if serverName != broker {
		log.Printf("Using custom TLS server name: %s (broker hostname: %s)", serverName, broker)
	}

	if skipVerify {
		log.Println("WARNING: TLS certificate verification is disabled (for testing only)")
	}

	// Load CA certificate
	caCertPath := getEnv("MQTT_TLS_CA_CERT", "/certs/ca.crt")
	log.Printf("Loading CA certificate from: %s", caCertPath)
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}
	tlsConfig.RootCAs = caCertPool
	log.Println("CA certificate loaded successfully")

	// Load edge node certificate and key if provided and required
	requireEdgeCert := getEnvBool("MQTT_TLS_REQUIRE_EDGE_CERT", false)
	edgeCertPath := getEnv("MQTT_TLS_EDGE_CERT", "")
	edgeKeyPath := getEnv("MQTT_TLS_EDGE_KEY", "")

	if requireEdgeCert && edgeCertPath != "" && edgeKeyPath != "" {
		log.Printf("Loading edge node certificate from: %s and key from: %s", edgeCertPath, edgeKeyPath)
		cert, err := tls.LoadX509KeyPair(edgeCertPath, edgeKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load edge node certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		log.Println("Edge node certificate loaded for client authentication")
	} else {
		log.Println("No edge node certificate required - using CA-only server verification")
	}

	return tlsConfig, nil
}

// buildMQTTOptions creates MQTT client options from connection options
func buildMQTTOptions(opts *MQTTConnectionOptions) (*mqtt.ClientOptions, error) {
	brokerURL := fmt.Sprintf("%s://%s:%d", opts.Protocol, opts.Broker, opts.Port)

	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(brokerURL)
	mqttOpts.SetClientID(opts.ClientID)
	mqttOpts.SetCleanSession(true)
	mqttOpts.SetAutoReconnect(true)
	mqttOpts.SetConnectRetry(true)
	mqttOpts.SetConnectRetryInterval(5 * time.Second)
	mqttOpts.SetKeepAlive(opts.KeepAlive)
	mqttOpts.SetPingTimeout(10 * time.Second)
	mqttOpts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})
	mqttOpts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("MQTT OnConnect handler called")
	})

	// Set username/password if provided
	if opts.Username != "" {
		mqttOpts.SetUsername(opts.Username)
		if opts.Password != "" {
			mqttOpts.SetPassword(opts.Password)
		}
	}

	// Configure TLS if enabled
	if opts.UseTLS {
		log.Println("Configuring TLS for MQTT connection...")
		tlsConfig, err := createTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		mqttOpts.SetTLSConfig(tlsConfig)
		log.Println("TLS configuration created successfully")
	}

	return mqttOpts, nil
}

// ConnectMQTT connects to MQTT broker with the given options and returns the client
func ConnectMQTT(opts *MQTTConnectionOptions) (mqtt.Client, error) {
	mqttOpts, err := buildMQTTOptions(opts)
	if err != nil {
		return nil, err
	}

	brokerURL := fmt.Sprintf("%s://%s:%d", opts.Protocol, opts.Broker, opts.Port)
	log.Printf("Attempting to connect to MQTT broker at %s...", brokerURL)

	client := mqtt.NewClient(mqttOpts)
	token := client.Connect()

	// Wait for connection with timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	connected := token.WaitTimeout(timeout)
	if !connected {
		if token.Error() != nil {
			errMsg := token.Error().Error()
			log.Printf("MQTT connection error (timeout): %s", errMsg)
			return nil, fmt.Errorf("connection timeout after %v: %w", timeout, token.Error())
		}
		return nil, fmt.Errorf("connection timeout after %v - broker may not be accepting connections or certificate validation failed", timeout)
	}

	if token.Error() != nil {
		errMsg := token.Error().Error()
		log.Printf("MQTT connection error details: %s", errMsg)
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	log.Printf("Connected to MQTT broker at %s", brokerURL)
	return client, nil
}

// NewMQTTClient creates a new MQTT client with TLS support using default options
func NewMQTTClient() (*MQTTClient, error) {
	broker := getEnv("MQTT_BROKER", "mosquitto")
	useTLS := getEnvBool("MQTT_USE_TLS", true)

	var port int
	var protocol string
	if useTLS {
		port = getEnvInt("MQTT_TLS_PORT", 8883)
		protocol = getEnv("MQTT_TLS_PROTOCOL", "tls")
	} else {
		port = getEnvInt("MQTT_PORT", 1883)
		protocol = "tcp"
	}

	clientID := getEnv("MQTT_CLIENT_ID", "device-service")
	username := getEnv("MQTT_USERNAME", "")
	password := getEnv("MQTT_PASSWORD", "")

	opts := &MQTTConnectionOptions{
		ClientID:  clientID,
		Username:  username,
		Password:  password,
		UseTLS:    useTLS,
		Broker:    broker,
		Port:      port,
		Protocol:  protocol,
		Timeout:   30 * time.Second,
		KeepAlive: 60 * time.Second,
	}

	client, err := ConnectMQTT(opts)
	if err != nil {
		return nil, err
	}

	return &MQTTClient{
		client: client,
		broker: broker,
		port:   port,
	}, nil
}

// Publish publishes a message to the specified topic
func (m *MQTTClient) Publish(topic string, qos byte, retained bool, payload []byte) error {
	token := m.client.Publish(topic, qos, retained, payload)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish message: %w", token.Error())
	}
	log.Printf("Published message to topic: %s", topic)
	return nil
}

// Subscribe subscribes to a topic with a message handler
func (m *MQTTClient) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	token := m.client.Subscribe(topic, qos, handler)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}
	log.Printf("Subscribed to topic: %s", topic)
	return nil
}

// Disconnect disconnects from the MQTT broker
func (m *MQTTClient) Disconnect() {
	m.client.Disconnect(250)
	log.Println("Disconnected from MQTT broker")
}

// GetClient returns the underlying MQTT client
func (m *MQTTClient) GetClient() mqtt.Client {
	return m.client
}

