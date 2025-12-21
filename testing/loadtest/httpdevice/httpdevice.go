package main

import (
	"context"
	"strings"
	"sync"

	devicepkg "github.com/robertodantas/lnpay/testing/device"
)

// HTTPDevice implements DeviceCallback and manages device state for HTTP load testing
type HTTPDevice struct {
	mu sync.RWMutex

	// Device identity
	DeviceID string
	Secret   string

	// DeviceInterface handles MQTT communication and authorization state
	device devicepkg.DeviceInterface

	// Minimal state (DeviceInterface handles authorization, balance, etc.)
	ReportingEnabled  bool // Controls whether usage reports should be sent
	InvoiceAmountMsat int64
}

// NewHTTPDevice creates a new HTTP device instance
func NewHTTPDevice(deviceID, secret string, config *Config) *HTTPDevice {
	// Create device interface config from runtime config
	deviceCfg := &devicepkg.Config{
		MQTTBroker:        config.MQTTBroker,
		MQTTUseTLS:        config.MQTTUseTLS,
		MQTTPort:          config.MQTTPort,
		MQTTTLSPort:       config.MQTTTLSPort,
		MQTTTLSCACert:     config.MQTTTLSCACert,
		MQTTTLSSkipVerify: config.MQTTTLSSkipVerify,
		MQTTTLSServerName: config.MQTTTLSServerName,
	}

	device := &HTTPDevice{
		DeviceID:          deviceID,
		Secret:            secret,
		InvoiceAmountMsat: 250000, // Default 250k msat
		ReportingEnabled:  true,
	}

	// Create device interface with this device as the callback
	device.device = devicepkg.NewDeviceInterface(device, deviceCfg, deviceID)

	return device
}

// Connect establishes MQTT connection using DeviceInterface
func (d *HTTPDevice) Connect() error {
	d.device.Connect(d.DeviceID, d.Secret)
	return nil
}

// Disconnect disconnects from MQTT broker
func (d *HTTPDevice) Disconnect() {
	if d.device != nil {
		d.device.Disconnect()
	}
}

// IsConnected returns whether the device is connected
func (d *HTTPDevice) IsConnected() bool {
	return d.device != nil && d.device.IsConnected()
}

// GetDeviceInterface returns the underlying DeviceInterface
func (d *HTTPDevice) GetDeviceInterface() devicepkg.DeviceInterface {
	return d.device
}

// IsReportingEnabled returns whether reporting is enabled
func (d *HTTPDevice) IsReportingEnabled() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.ReportingEnabled
}

func (d *HTTPDevice) OnConfigUpdated(config *devicepkg.DeviceConfig) {

}

// OnBalanceUpdated is called when balance is updated
func (d *HTTPDevice) OnBalanceUpdated(balance *devicepkg.BalanceMessage) {

}

// OnAuthorizationGranted is called when authorization is granted
func (d *HTTPDevice) OnAuthorizationGranted(response *devicepkg.AuthorizeResponse) {

}

// OnAuthorizationActive is called when an existing authorization is found
func (d *HTTPDevice) OnAuthorizationActive(response *devicepkg.AuthorizeResponse) {
}

// OnAuthorizationRejected is called when authorization is rejected
func (d *HTTPDevice) OnAuthorizationRejected(response *devicepkg.AuthorizeResponse) {
	logger.WithDeviceID(d.DeviceID).Info(context.Background(), "Requesting invoice")
	requestID := devicepkg.GenerateID()
	d.device.PublishInvoiceRequest(requestID, d.InvoiceAmountMsat, "AUTHORIZATION_REJECTED")
}

// OnInvoiceCreated is called when an invoice is created
func (d *HTTPDevice) OnInvoiceCreated(response *devicepkg.InvoiceResponse) {

}

// OnInvoiceSettled is called when an invoice is settled
func (d *HTTPDevice) OnInvoiceSettled(invoiceID string, amountMsat int64) {

}

// OnInvoiceExpired is called when an invoice expires
func (d *HTTPDevice) OnInvoiceExpired(invoiceID string) {
	logger.WithDeviceID(d.DeviceID).Warnf(context.Background(), "Invoice expired: %s", invoiceID)
}

// OnInvoiceFailed is called when an invoice fails
func (d *HTTPDevice) OnInvoiceFailed(invoiceID string) {
}

// OnControlStop is called when STOP command is received
func (d *HTTPDevice) OnControlStop(reason string) {
	d.mu.Lock()
	d.ReportingEnabled = false
	d.mu.Unlock()
}

// OnControlPause is called when PAUSE command is received
func (d *HTTPDevice) OnControlPause(reason string) {
	d.mu.Lock()
	d.ReportingEnabled = false
	d.mu.Unlock()
}

// OnControlResume is called when RESUME command is received
func (d *HTTPDevice) OnControlResume() {
	d.mu.Lock()
	d.ReportingEnabled = true
	d.mu.Unlock()
}

// OnControlReboot is called when REBOOT command is received
func (d *HTTPDevice) OnControlReboot() {
	logger.WithDeviceID(d.DeviceID).Info(context.Background(), "Received REBOOT command")
	// For HTTP device, we don't actually reboot, just log it
}

// OnConnected is called when the device has successfully connected to MQTT
func (d *HTTPDevice) OnConnected() {
	logger.WithDeviceID(d.DeviceID).Info(context.Background(), "Device connected and ready")
}

// OnMQTTStatus is called when MQTT connection status changes
func (d *HTTPDevice) OnMQTTStatus(status string) {
	logger.WithDeviceID(d.DeviceID).Infof(context.Background(), "MQTT status: %s", status)
}

// OnDeviceStatus is called when device status changes
func (d *HTTPDevice) OnDeviceStatus(status string) {
	logger.WithDeviceID(d.DeviceID).Infof(context.Background(), "Device status: %s", status)
}

// OnLog is called when a log message should be recorded
func (d *HTTPDevice) OnLog(message, logType string) {
	deviceLogger := logger.WithDeviceID(d.DeviceID)
	switch strings.ToLower(logType) {
	case "debug":
		deviceLogger.Debug(context.Background(), message)
	case "info":
		deviceLogger.Info(context.Background(), message)
	case "warn", "warning":
		deviceLogger.Warn(context.Background(), message)
	case "error":
		deviceLogger.Error(context.Background(), message, nil)
	default:
		deviceLogger.Info(context.Background(), message)
	}
}
