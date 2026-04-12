package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// SimplifyMethodName extracts a compact service/method identifier from a full gRPC method path.
// Example: /foo.bar.Service/Method -> Service/Method.
func SimplifyMethodName(method string) string {
	method = strings.TrimPrefix(method, "/")

	parts := strings.Split(method, "/")
	if len(parts) != 2 {
		return method
	}

	serviceParts := strings.Split(parts[0], ".")
	if len(serviceParts) == 0 {
		return method
	}

	serviceName := serviceParts[len(serviceParts)-1]
	return fmt.Sprintf("%s/%s", serviceName, parts[1])
}

// LoggingUnaryClientInterceptor logs outgoing unary gRPC calls, their responses, and durations.
// The service name should be passed via context using WithServiceName or extracted from the method.
func LoggingUnaryClientInterceptor(serviceName string) func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	logger := NewLogger(serviceName)

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		simpleMethod := SimplifyMethodName(method)

		// Extract device_id from request if possible
		deviceID := extractDeviceIDFromRequest(req)

		opLogger := logger

		if deviceID != "" {
			opLogger = opLogger.WithDeviceID(deviceID)
		}

		opLogger.DebugWithFields(ctx, fmt.Sprintf("gRPC call started: %s via eastwest gRPC", simpleMethod), map[string]interface{}{
			"method":  simpleMethod,
			"request": req,
		})

		err := invoker(ctx, method, req, reply, cc, opts...)
		duration := time.Since(start)

		if err != nil {
			if st, ok := status.FromError(err); ok {
				opLogger.ErrorWithFields(ctx, fmt.Sprintf("gRPC call failed: %s via eastwest gRPC", simpleMethod), err, map[string]interface{}{
					"grpc_code":    st.Code().String(),
					"grpc_message": st.Message(),
					"duration":     duration.String(),
				})
			} else {
				opLogger.ErrorWithFields(ctx, fmt.Sprintf("gRPC call failed: %s via eastwest gRPC", simpleMethod), err, map[string]interface{}{
					"duration": duration.String(),
				})
			}
		} else {
			opLogger.DebugWithFields(ctx, fmt.Sprintf("gRPC call succeeded: %s via eastwest gRPC", simpleMethod), map[string]interface{}{
				"response": reply,
				"duration": duration.String(),
			})
		}

		return err
	}
}

// extractDeviceIDFromRequest attempts to extract device_id from common request types
func extractDeviceIDFromRequest(req interface{}) string {
	// Try to extract device_id using type assertion or reflection
	// This is a simple implementation - can be extended for specific types
	if reqMap, ok := req.(map[string]interface{}); ok {
		if deviceID, ok := reqMap["device_id"].(string); ok {
			return deviceID
		}
		if deviceID, ok := reqMap["DeviceId"].(string); ok {
			return deviceID
		}
	}

	// Try common struct field names via fmt.Sprintf
	reqStr := fmt.Sprintf("%+v", req)
	if strings.Contains(reqStr, "device_id") || strings.Contains(reqStr, "DeviceId") {
		// Extract using simple string parsing (not perfect but works for most cases)
		parts := strings.Fields(reqStr)
		for i, part := range parts {
			if (part == "device_id:" || part == "DeviceId:") && i+1 < len(parts) {
				return strings.Trim(parts[i+1], "{}")
			}
		}
	}

	return ""
}
