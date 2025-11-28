package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	mqttPublishTracer = otel.Tracer("mqtt.publish")
	mqttReceiveTracer = otel.Tracer("mqtt.receive")
)

// MQTTMessageHandlerWithContext is a custom handler that receives context
type MQTTMessageHandlerWithContext func(ctx context.Context, client mqtt.Client, msg mqtt.Message)

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

// createTLSConfig creates a TLS configuration from config
func createTLSConfig(ctx context.Context, cfg Config) (*tls.Config, error) {
	// Check if we should skip certificate verification (for testing only)
	skipVerify := cfg.MQTTTLSSkipVerify

	broker := cfg.MQTTBroker
	// Allow custom server name for certificate validation (useful when CN doesn't match hostname)
	serverName := cfg.MQTTTLSServerName
	if serverName == "" {
		serverName = broker
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify,
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName, // Set server name for SNI and hostname verification
	}

	if serverName != broker {
		logger.InfoWithFields(ctx, "Using custom TLS server name on southbound mqtt", map[string]interface{}{
			"server_name": serverName,
			"broker":      broker,
		})
	}

	if skipVerify {
		logger.Warn(ctx, "TLS certificate verification is disabled on southbound mqtt (for testing only)")
	}

	// Load CA certificate
	caCertPath := cfg.MQTTTLSCACert
	logger.Infof(ctx, "Loading CA certificate from %s on southbound mqtt", caCertPath)
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}
	tlsConfig.RootCAs = caCertPool
	logger.Info(ctx, "CA certificate loaded successfully on southbound mqtt")

	// Load edge node certificate and key if provided and required
	if cfg.MQTTTLSRequireEdgeCert && cfg.MQTTTLSEdgeCert != "" && cfg.MQTTTLSEdgeKey != "" {
		logger.InfoWithFields(ctx, "Loading edge node certificate on southbound mqtt", map[string]interface{}{
			"cert_path": cfg.MQTTTLSEdgeCert,
			"key_path":  cfg.MQTTTLSEdgeKey,
		})
		cert, err := tls.LoadX509KeyPair(cfg.MQTTTLSEdgeCert, cfg.MQTTTLSEdgeKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load edge node certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		logger.Info(ctx, "Edge node certificate loaded for client authentication on southbound mqtt")
	} else {
		logger.Info(ctx, "No edge node certificate required on southbound mqtt - using CA-only server verification")
	}

	return tlsConfig, nil
}

// buildMQTTOptions creates MQTT client options from connection options
func buildMQTTOptions(ctx context.Context, opts *MQTTConnectionOptions, cfg Config) (*mqtt.ClientOptions, error) {
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
		logger.Error(ctx, "MQTT connection lost on southbound mqtt", err)
	})
	mqttOpts.SetOnConnectHandler(func(client mqtt.Client) {
		logger.Info(ctx, "MQTT OnConnect handler called on southbound mqtt")
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
		logger.Info(ctx, "Configuring TLS for MQTT connection on southbound mqtt")
		tlsConfig, err := createTLSConfig(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		mqttOpts.SetTLSConfig(tlsConfig)
		logger.Info(ctx, "TLS configuration created successfully on southbound mqtt")
	}

	return mqttOpts, nil
}

// ConnectMQTT connects to MQTT broker with the given options and returns the client
func ConnectMQTT(ctx context.Context, opts *MQTTConnectionOptions, cfg Config) (mqtt.Client, error) {
	mqttOpts, err := buildMQTTOptions(ctx, opts, cfg)
	if err != nil {
		return nil, err
	}

	brokerURL := fmt.Sprintf("%s://%s:%d", opts.Protocol, opts.Broker, opts.Port)
	logger.Infof(ctx, "Attempting to connect to MQTT broker at %s on southbound mqtt", brokerURL)

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
			logger.Errorf(ctx, "MQTT connection error (timeout) on southbound mqtt: %s", errMsg)
			return nil, fmt.Errorf("connection timeout after %v: %w", timeout, token.Error())
		}
		return nil, fmt.Errorf("connection timeout after %v - broker may not be accepting connections or certificate validation failed", timeout)
	}

	if token.Error() != nil {
		errMsg := token.Error().Error()
		logger.Errorf(ctx, "MQTT connection error details on southbound mqtt: %s", errMsg)
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	logger.Infof(ctx, "Connected to MQTT broker at %s on southbound mqtt", brokerURL)
	return client, nil
}

// NewMQTTClient creates a new MQTT client with TLS support using config
func NewMQTTClient(ctx context.Context, cfg Config) (*MQTTClient, error) {
	broker := cfg.MQTTBroker
	useTLS := cfg.MQTTUseTLS

	var port int
	var protocol string
	if useTLS {
		port = cfg.MQTTTLSPort
		protocol = cfg.MQTTTLSProtocol
	} else {
		port = cfg.MQTTPort
		protocol = "tcp"
	}

	clientID := cfg.MQTTClientID
	username := cfg.MQTTUsername
	password := cfg.MQTTPassword

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

	client, err := ConnectMQTT(ctx, opts, cfg)
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
func (m *MQTTClient) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	spanName := fmt.Sprintf("[mqtt] %s publish", topic)
	ctx, span := mqttPublishTracer.Start(ctx, spanName,
		trace.WithAttributes(
			attribute.String("mqtt.topic", topic),
			attribute.Int("mqtt.qos", int(qos)),
			attribute.Bool("mqtt.retained", retained),
			attribute.Int("mqtt.payload.size", len(payload)),
			attribute.String("mqtt.operation", "PUBLISH"),
		),
	)
	defer span.End()

	// Check if client is connected before attempting to publish
	if !m.client.IsConnected() {
		err := fmt.Errorf("MQTT client is not connected")
		span.RecordError(err)
		span.SetStatus(codes.Error, "client not connected")
		return err
	}

	token := m.client.Publish(topic, qos, retained, payload)

	// Wait for publish to complete with timeout (important for QoS 1/2 to get PUBACK/PUBREC)
	if !token.WaitTimeout(10 * time.Second) {
		err := fmt.Errorf("timeout waiting for publish acknowledgment")
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish timeout")
		return err
	}

	// Check for errors after waiting
	if token.Error() != nil {
		span.RecordError(token.Error())
		span.SetStatus(codes.Error, token.Error().Error())
		return fmt.Errorf("failed to publish message: %w", token.Error())
	}

	// For QoS 1, verify client is still connected (broker might disconnect on denial)
	if qos >= 1 && !m.client.IsConnected() {
		err := fmt.Errorf("client disconnected after publish - broker may have denied the publish")
		span.RecordError(err)
		span.SetStatus(codes.Error, "client disconnected after publish")
		return err
	}

	logger.InfoWithFields(ctx, "Published message on southbound mqtt", map[string]interface{}{
		"topic": topic,
	})
	span.SetStatus(codes.Ok, "success")
	return nil
}

// wrapHandlerWithTracing wraps a context-aware handler with OpenTelemetry tracing
func wrapHandlerWithTracing(handler MQTTMessageHandlerWithContext) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		deviceID := extractDeviceIDFromTopic(topic)

		ctx, span := mqttReceiveTracer.Start(context.Background(), fmt.Sprintf("[mqtt] %s received", topic),
			trace.WithAttributes(
				attribute.String("mqtt.topic", topic),
				attribute.String("mqtt.device_id", deviceID),
				attribute.Int("mqtt.payload.size", len(msg.Payload())),
				attribute.String("mqtt.operation", "RECEIVE"),
			),
		)
		defer span.End()

		// Call the custom handler with context
		handler(ctx, client, msg)

		span.SetStatus(codes.Ok, "processed")
	}
}

// extractDeviceIDFromTopic extracts device ID from MQTT topic
func extractDeviceIDFromTopic(topic string) string {
	parts := strings.Split(strings.TrimPrefix(topic, "/"), "/")
	if len(parts) >= 2 && parts[0] == "devices" {
		return parts[1]
	}
	return ""
}

// Subscribe subscribes to a topic with a context-aware message handler
func (m *MQTTClient) Subscribe(ctx context.Context, topic string, qos byte, handler MQTTMessageHandlerWithContext) error {
	spanName := fmt.Sprintf("[mqtt] %s subscribe", topic)
	ctx, span := mqttPublishTracer.Start(ctx, spanName,
		trace.WithAttributes(
			attribute.String("mqtt.topic", topic),
			attribute.Int("mqtt.qos", int(qos)),
			attribute.String("mqtt.operation", "SUBSCRIBE"),
		),
	)
	defer span.End()

	// Wrap the context-aware handler with tracing
	wrappedHandler := wrapHandlerWithTracing(handler)

	token := m.client.Subscribe(topic, qos, wrappedHandler)
	if token.Wait() && token.Error() != nil {
		span.RecordError(token.Error())
		span.SetStatus(codes.Error, token.Error().Error())
		return fmt.Errorf("failed to subscribe to topic: %w", token.Error())
	}

	logger.InfoWithFields(ctx, "Subscribed to topic on southbound mqtt", map[string]interface{}{
		"topic": topic,
		"qos":   qos,
	})
	span.SetStatus(codes.Ok, "success")
	return nil
}

// Disconnect disconnects from the MQTT broker
func (m *MQTTClient) Disconnect() {
	m.client.Disconnect(250)
	logger.Info(context.Background(), "Disconnected from MQTT broker on southbound mqtt")
}

// GetClient returns the underlying MQTT client
func (m *MQTTClient) GetClient() mqtt.Client {
	return m.client
}
