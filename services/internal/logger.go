package internal

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// LogLevel represents the minimum log level to output
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

var (
	globalLogLevel LogLevel = LogLevelInfo // Default to INFO in production
	logLevelMu     sync.RWMutex
)

// SetLogLevel sets the global log level from a string (case-insensitive)
// Valid values: "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
func SetLogLevel(level string) {
	logLevelMu.Lock()
	defer logLevelMu.Unlock()

	switch strings.ToUpper(level) {
	case "DEBUG":
		globalLogLevel = LogLevelDebug
	case "INFO":
		globalLogLevel = LogLevelInfo
	case "WARN", "WARNING":
		globalLogLevel = LogLevelWarn
	case "ERROR":
		globalLogLevel = LogLevelError
	case "FATAL":
		globalLogLevel = LogLevelFatal
	default:
		globalLogLevel = LogLevelInfo // Default to INFO if invalid
	}
}

// InitLogLevel initializes the log level from the LOG_LEVEL environment variable
// This should be called early in main() or init()
func InitLogLevel() {
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		SetLogLevel(level)
	}
}

// shouldLog checks if a message at the given level should be logged
func shouldLog(level string) bool {
	logLevelMu.RLock()
	defer logLevelMu.RUnlock()

	var messageLevel LogLevel
	switch strings.ToLower(level) {
	case "debug":
		messageLevel = LogLevelDebug
	case "info":
		messageLevel = LogLevelInfo
	case "warn", "warning":
		messageLevel = LogLevelWarn
	case "error":
		messageLevel = LogLevelError
	case "fatal":
		messageLevel = LogLevelFatal
	default:
		// Unknown level, always log it
		return true
	}

	return messageLevel >= globalLogLevel
}

// Logger provides structured logfmt logging with context
type Logger struct {
	serviceName string
	fields      map[string]interface{}
	ctx         context.Context // Optional context for trace extraction
}

// NewLogger creates a new logger instance for a service
func NewLogger(serviceName string) *Logger {
	return &Logger{
		serviceName: serviceName,
		fields:      make(map[string]interface{}),
	}
}

// WithDeviceID adds device_id to the logger context
func (l *Logger) WithDeviceID(deviceID string) *Logger {
	return l.withField("device_id", deviceID)
}

// WithStream adds stream information to the logger
func (l *Logger) WithStream(streamName string, action string) *Logger {
	logger := l.withField("stream_name", streamName)
	return logger.withField("stream_action", action)
}

// WithField adds a custom field to the logger context
func (l *Logger) WithField(key string, value interface{}) *Logger {
	return l.withField(key, value)
}

// WithFields adds multiple fields to the logger context
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	newLogger := &Logger{
		serviceName: l.serviceName,
		fields:      make(map[string]interface{}),
	}
	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	// Add new fields
	for k, v := range fields {
		newLogger.fields[k] = v
	}
	return newLogger
}

// withField creates a new logger instance with an additional field
func (l *Logger) withField(key string, value interface{}) *Logger {
	newLogger := &Logger{
		serviceName: l.serviceName,
		fields:      make(map[string]interface{}),
	}
	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	// Add new field
	newLogger.fields[key] = value
	return newLogger
}

// formatLogfmtValue formats a value for logfmt output
func formatLogfmtValue(v interface{}) string {
	if v == nil {
		return "null"
	}

	switch val := v.(type) {
	case string:
		// Quote strings that contain spaces, special characters, or are empty
		if val == "" || strings.ContainsAny(val, " =\"\n\t") {
			return strconv.Quote(val)
		}
		return val
	case bool:
		return strconv.FormatBool(val)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case time.Duration:
		return val.String()
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		// For complex types, convert to string and quote if needed
		str := fmt.Sprintf("%v", val)
		if strings.ContainsAny(str, " =\"\n\t") {
			return strconv.Quote(str)
		}
		return str
	}
}

// log writes a structured logfmt log entry
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) log(ctx context.Context, level, message string, err error, duration time.Duration, additionalFields map[string]interface{}) {
	// Skip logging if message level is below the configured global log level
	if !shouldLog(level) {
		return
	}

	var parts []string

	// Always include timestamp, level, service, and message
	parts = append(parts, fmt.Sprintf("timestamp=%s", time.Now().UTC().Format(time.RFC3339Nano)))
	parts = append(parts, fmt.Sprintf("level=%s", level))
	parts = append(parts, fmt.Sprintf("service=%s", l.serviceName))
	parts = append(parts, fmt.Sprintf("message=%s", formatLogfmtValue(message)))

	// Extract trace ID and span ID from context if available
	// Priority: explicit ctx parameter > stored ctx in logger
	var traceCtx context.Context
	if ctx != nil {
		traceCtx = ctx
	} else if l.ctx != nil {
		traceCtx = l.ctx
	}

	if traceCtx != nil {
		span := trace.SpanFromContext(traceCtx)
		if span.SpanContext().IsValid() {
			traceID := span.SpanContext().TraceID().String()
			spanID := span.SpanContext().SpanID().String()
			if traceID != "" {
				parts = append(parts, fmt.Sprintf("trace_id=%s", traceID))
			}
			if spanID != "" {
				parts = append(parts, fmt.Sprintf("span_id=%s", spanID))
			}
		}
	}

	// Extract common fields from context
	if deviceID, ok := l.fields["device_id"].(string); ok && deviceID != "" {
		parts = append(parts, fmt.Sprintf("device_id=%s", formatLogfmtValue(deviceID)))
	}
	if streamName, ok := l.fields["stream_name"].(string); ok && streamName != "" {
		parts = append(parts, fmt.Sprintf("stream_name=%s", formatLogfmtValue(streamName)))
	}
	if streamAction, ok := l.fields["stream_action"].(string); ok && streamAction != "" {
		parts = append(parts, fmt.Sprintf("stream_action=%s", formatLogfmtValue(streamAction)))
	}

	// Add error if present
	if err != nil {
		parts = append(parts, fmt.Sprintf("error=%s", formatLogfmtValue(err.Error())))
	}

	// Add duration if present
	if duration > 0 {
		parts = append(parts, fmt.Sprintf("duration=%s", formatLogfmtValue(duration.String())))
	}

	// Add other context fields (excluding already processed ones)
	for k, v := range l.fields {
		if k != "device_id" && k != "stream_name" && k != "stream_action" {
			parts = append(parts, fmt.Sprintf("%s=%s", k, formatLogfmtValue(v)))
		}
	}

	// Add additional fields
	for k, v := range additionalFields {
		parts = append(parts, fmt.Sprintf("%s=%s", k, formatLogfmtValue(v)))
	}

	// Write to stdout in logfmt format
	fmt.Fprintln(os.Stdout, strings.Join(parts, " "))
}

// Info logs an info level message
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Info(ctx context.Context, message string) {
	l.log(ctx, "info", message, nil, 0, nil)
}

// Infof logs an info level message with formatting
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.Info(ctx, fmt.Sprintf(format, args...))
}

// InfoWithFields logs an info level message with additional fields
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) InfoWithFields(ctx context.Context, message string, fields map[string]interface{}) {
	l.log(ctx, "info", message, nil, 0, fields)
}

// Error logs an error level message
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Error(ctx context.Context, message string, err error) {
	l.log(ctx, "error", message, err, 0, nil)
}

// Errorf logs an error level message with formatting
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Errorf(ctx context.Context, format string, args ...interface{}) {
	var err error
	if len(args) > 0 {
		if e, ok := args[len(args)-1].(error); ok {
			err = e
		}
	}
	message := fmt.Sprintf(format, args...)
	l.log(ctx, "error", message, err, 0, nil)
}

// ErrorWithFields logs an error level message with additional fields
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) ErrorWithFields(ctx context.Context, message string, err error, fields map[string]interface{}) {
	l.log(ctx, "error", message, err, 0, fields)
}

// Warn logs a warning level message
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Warn(ctx context.Context, message string) {
	l.log(ctx, "warn", message, nil, 0, nil)
}

// Warnf logs a warning level message with formatting
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.Warn(ctx, fmt.Sprintf(format, args...))
}

// WarnWithFields logs a warning level message with additional fields
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) WarnWithFields(ctx context.Context, message string, fields map[string]interface{}) {
	l.log(ctx, "warn", message, nil, 0, fields)
}

// Debug logs a debug level message
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Debug(ctx context.Context, message string) {
	l.log(ctx, "debug", message, nil, 0, nil)
}

// Debugf logs a debug level message with formatting
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.Debug(ctx, fmt.Sprintf(format, args...))
}

// DebugWithFields logs a debug level message with additional fields
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) DebugWithFields(ctx context.Context, message string, fields map[string]interface{}) {
	l.log(ctx, "debug", message, nil, 0, fields)
}

// WithDuration logs a message with duration information
func (l *Logger) WithDuration(duration time.Duration) *Logger {
	return l.withField("duration", duration.String())
}

// WithContext adds a context to the logger for trace ID extraction
func (l *Logger) WithContext(ctx context.Context) *Logger {
	newLogger := &Logger{
		serviceName: l.serviceName,
		fields:      make(map[string]interface{}),
		ctx:         ctx,
	}
	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	return newLogger
}

// LogOperation logs the start/end of an operation with duration
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) LogOperation(ctx context.Context, operation string, fn func() error) error {
	start := time.Now()
	opLogger := l.withField("operation", operation)
	opLogger.Info(ctx, fmt.Sprintf("Operation %s started", strings.ReplaceAll(operation, "_", " ")))

	err := fn()
	duration := time.Since(start)

	if err != nil {
		opLogger.ErrorWithFields(ctx, fmt.Sprintf("Operation %s failed", strings.ReplaceAll(operation, "_", " ")), err, map[string]interface{}{
			"duration": duration.String(),
		})
	} else {
		opLogger.InfoWithFields(ctx, fmt.Sprintf("Operation %s completed", strings.ReplaceAll(operation, "_", " ")), map[string]interface{}{
			"duration": duration.String(),
		})
	}

	return err
}

// Fatal logs a fatal error and exits
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Fatal(ctx context.Context, message string, err error) {
	l.log(ctx, "fatal", message, err, 0, nil)
	os.Exit(1)
}

// Fatalf logs a fatal error with formatting and exits
// ctx is optional - if provided, trace context will be extracted from it
func (l *Logger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	var err error
	if len(args) > 0 {
		if e, ok := args[len(args)-1].(error); ok {
			err = e
			args = args[:len(args)-1]
		}
	}
	message := fmt.Sprintf(format, args...)
	l.Fatal(ctx, message, err)
}
