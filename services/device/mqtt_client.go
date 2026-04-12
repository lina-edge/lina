package main

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	lina "github.com/robertodantas/lina/internal"
	"go.opentelemetry.io/otel"
)

var (
	mqttPublishTracer = otel.Tracer("mqtt.publish")
	mqttReceiveTracer = otel.Tracer("mqtt.receive")
)

// MQTTClient and MQTTMessageHandlerWithContext are aliases to the shared internal MQTT wrapper.
type MQTTClient = lina.MQTTClient
type MQTTMessageHandlerWithContext = lina.MQTTMessageHandlerWithContext

// NewMQTTClient creates a new MQTT client with TLS support using config.
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

	if cfg.MQTTTLSServerName != "" && cfg.MQTTTLSServerName != broker {
		logger.InfoWithFields(ctx, "Using custom TLS server name on southbound mqtt", map[string]interface{}{
			"server_name": cfg.MQTTTLSServerName,
			"broker":      broker,
		})
	}
	if useTLS && cfg.MQTTTLSSkipVerify {
		logger.Warn(ctx, "TLS certificate verification is disabled on southbound mqtt (for testing only)")
	}
	if useTLS {
		logger.Info(ctx, "Configuring TLS for MQTT connection on southbound mqtt")
		logger.Infof(ctx, "Loading CA certificate from %s on southbound mqtt", cfg.MQTTTLSCACert)
	}

	brokerURL := fmt.Sprintf("%s://%s:%d", protocol, broker, port)
	logger.Infof(ctx, "Connecting to MQTT broker at %s", brokerURL)

	dial := lina.MQTTConnectConfig{
		Connection: lina.MQTTConnectionSpec{
			ClientID:       cfg.MQTTClientID,
			Username:       cfg.MQTTUsername,
			Password:       cfg.MQTTPassword,
			UseTLS:         useTLS,
			Broker:         broker,
			Port:           port,
			Protocol:       protocol,
			ConnectTimeout: 30 * time.Second,
			KeepAlive:      60 * time.Second,
		},
		Hooks: &lina.MQTTSessionHooks{
			OnConnectionLost: func(client mqtt.Client, err error) {
				logger.Error(ctx, "MQTT connection lost on southbound mqtt", err)
			},
			OnConnect: func(client mqtt.Client) {
				logger.Debug(ctx, "MQTT OnConnect handler called on southbound mqtt")
			},
		},
	}
	if useTLS {
		dial.TLS = &lina.MQTTTLSParams{
			BrokerHost:      broker,
			SkipVerify:      cfg.MQTTTLSSkipVerify,
			ServerName:      cfg.MQTTTLSServerName,
			CACertPath:      cfg.MQTTTLSCACert,
			RequireEdgeCert: cfg.MQTTTLSRequireEdgeCert,
			EdgeCertPath:    cfg.MQTTTLSEdgeCert,
			EdgeKeyPath:     cfg.MQTTTLSEdgeKey,
		}
	}

	mc, err := lina.ConnectMQTT(dial, &lina.MQTTClientBehavior{
		Tracing: lina.MQTTTracing{
			Enabled:       true,
			PublishTracer: mqttPublishTracer,
			ReceiveTracer: mqttReceiveTracer,
		},
		Receive: &lina.MQTTReceiveHooks{
			OnReceived: func(c context.Context, topic, deviceID string) {
				RecordMQTTMessageReceived(c, topic, deviceID)
			},
			OnProcessed: func(c context.Context, topic, deviceID string) {
				RecordMQTTMessageProcessed(c, topic, deviceID)
			},
			OnFailed: func(c context.Context, topic, deviceID string) {
				RecordMQTTMessageFailed(c, topic, deviceID)
			},
		},
		OnPublishSuccess: func(c context.Context, topic string) {
			logger.DebugWithFields(c, "Published message on southbound mqtt", map[string]interface{}{
				"topic": topic,
			})
		},
		OnSubscribeSuccess: func(c context.Context, topic string, qos byte) {
			logger.InfoWithFields(c, "Subscribed to topic on southbound mqtt", map[string]interface{}{
				"topic": topic,
				"qos":   qos,
			})
		},
		OnDisconnect: func() {
			logger.Debug(context.Background(), "Disconnected from MQTT broker on southbound mqtt")
		},
	})
	if err != nil {
		errMsg := err.Error()
		logger.Errorf(ctx, "MQTT connection error on southbound mqtt: %s", errMsg)
		return nil, err
	}

	if useTLS {
		logger.Info(ctx, "TLS configuration created successfully on southbound mqtt")
		logger.Info(ctx, "CA certificate loaded successfully on southbound mqtt")
		if cfg.MQTTTLSRequireEdgeCert && cfg.MQTTTLSEdgeCert != "" && cfg.MQTTTLSEdgeKey != "" {
			logger.Info(ctx, "Edge node certificate loaded for client authentication on southbound mqtt")
		} else {
			logger.Info(ctx, "No edge node certificate required on southbound mqtt - using CA-only server verification")
		}
	}
	logger.Info(ctx, "Connected to MQTT broker")
	return mc, nil
}
