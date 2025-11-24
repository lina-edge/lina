package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
)

// Custom JSON marshaling for enums to use shorter names in JSON
// while keeping full names in proto to prevent collisions

// DeviceStatus JSON marshaling
func (x DeviceStatus) MarshalJSON() ([]byte, error) {
	// Remove the "DEVICE_STATUS_" prefix for JSON
	name := DeviceStatus_name[int32(x)]
	shortName := strings.TrimPrefix(name, "DEVICE_STATUS_")
	return json.Marshal(shortName)
}

func (x *DeviceStatus) UnmarshalJSON(data []byte) error {
	var shortName string
	if err := json.Unmarshal(data, &shortName); err != nil {
		return err
	}
	// Add the prefix back to match proto enum name
	fullName := "DEVICE_STATUS_" + shortName
	value, ok := DeviceStatus_value[fullName]
	if !ok {
		return fmt.Errorf("unknown DeviceStatus value: %s", shortName)
	}
	*x = DeviceStatus(value)
	return nil
}

// ReportingStrategy JSON marshaling
func (x ReportingStrategy) MarshalJSON() ([]byte, error) {
	// Remove the "REPORTING_STRATEGY_" prefix for JSON
	name := ReportingStrategy_name[int32(x)]
	shortName := strings.TrimPrefix(name, "REPORTING_STRATEGY_")
	return json.Marshal(shortName)
}

func (x *ReportingStrategy) UnmarshalJSON(data []byte) error {
	var shortName string
	if err := json.Unmarshal(data, &shortName); err != nil {
		return err
	}
	// Add the prefix back to match proto enum name
	fullName := "REPORTING_STRATEGY_" + shortName
	value, ok := ReportingStrategy_value[fullName]
	if !ok {
		return fmt.Errorf("unknown ReportingStrategy value: %s", shortName)
	}
	*x = ReportingStrategy(value)
	return nil
}

// ControlCommand JSON marshaling
func (x ControlCommand) MarshalJSON() ([]byte, error) {
	// Remove the "CONTROL_COMMAND_" prefix for JSON
	name := ControlCommand_name[int32(x)]
	shortName := strings.TrimPrefix(name, "CONTROL_COMMAND_")
	return json.Marshal(shortName)
}

func (x *ControlCommand) UnmarshalJSON(data []byte) error {
	var shortName string
	if err := json.Unmarshal(data, &shortName); err != nil {
		return err
	}
	// Add the prefix back to match proto enum name
	fullName := "CONTROL_COMMAND_" + shortName
	value, ok := ControlCommand_value[fullName]
	if !ok {
		return fmt.Errorf("unknown ControlCommand value: %s", shortName)
	}
	*x = ControlCommand(value)
	return nil
}

// Message-level UnmarshalJSON methods
// These transform short enum names to full names, then use protojson.Unmarshal
// Usage: json.Unmarshal(data, &payload) or payload.UnmarshalJSON(data)

func (x *HeartbeatPayload) UnmarshalJSON(data []byte) error {
	// Transform enum values in JSON (add prefixes)
	transformed, err := transformEnumInJSON(data, "DEVICE_STATUS_", "status")
	if err != nil {
		return err
	}
	// Use protojson to unmarshal with UseProtoNames to preserve snake_case
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *UsagePayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "REPORTING_STRATEGY_", "strategy")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *ConfigPayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "REPORTING_STRATEGY_", "reporting_strategy")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *ControlPayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "CONTROL_COMMAND_", "command")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *AuthorizationResponsePayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "AUTHORIZATION_STATUS_", "status")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *InvoiceResponsePayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "INVOICE_STATUS_", "status")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

func (x *InvoiceEventPayload) UnmarshalJSON(data []byte) error {
	transformed, err := transformEnumInJSON(data, "INVOICE_STATUS_", "status")
	if err != nil {
		return err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(transformed, x)
}

// transformEnumInJSON adds the prefix to enum values in JSON if they don't already have it
func transformEnumInJSON(data []byte, prefix, fieldName string) ([]byte, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// Transform the enum field if it exists
	if value, exists := jsonData[fieldName]; exists {
		if str, ok := value.(string); ok {
			// Add prefix if not already present
			if !strings.HasPrefix(str, prefix) {
				jsonData[fieldName] = prefix + str
			}
		}
	}

	return json.Marshal(jsonData)
}

// Message-level MarshalJSON methods
// These use protojson to marshal, then transform enum names to short versions
// Usage: json.Marshal(payload) or payload.MarshalJSON()

func (x *HeartbeatPayload) MarshalJSON() ([]byte, error) {
	// Marshal with protojson first, using UseProtoNames to preserve snake_case
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	// Transform enum names to short versions
	return transformEnumOutJSON(data, "DEVICE_STATUS_", "status")
}

func (x *UsagePayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "REPORTING_STRATEGY_", "strategy")
}

func (x *ConfigPayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "REPORTING_STRATEGY_", "reporting_strategy")
}

func (x *ControlPayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "CONTROL_COMMAND_", "command")
}

func (x *AuthorizationResponsePayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "AUTHORIZATION_STATUS_", "status")
}

func (x *InvoiceResponsePayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "INVOICE_STATUS_", "status")
}

func (x *InvoiceEventPayload) MarshalJSON() ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	data, err := opts.Marshal(x)
	if err != nil {
		return nil, err
	}
	return transformEnumOutJSON(data, "INVOICE_STATUS_", "status")
}

// transformEnumOutJSON removes the prefix from enum values in JSON
func transformEnumOutJSON(data []byte, prefix, fieldName string) ([]byte, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// Transform the enum field if it exists
	if value, exists := jsonData[fieldName]; exists {
		if str, ok := value.(string); ok {
			// Remove prefix if present
			jsonData[fieldName] = strings.TrimPrefix(str, prefix)
		}
	}

	return json.Marshal(jsonData)
}

// AuthorizationStatus JSON marshaling
func (x AuthorizationStatus) MarshalJSON() ([]byte, error) {
	// Remove the "AUTHORIZATION_STATUS_" prefix for JSON
	name := AuthorizationStatus_name[int32(x)]
	shortName := strings.TrimPrefix(name, "AUTHORIZATION_STATUS_")
	return json.Marshal(shortName)
}

func (x *AuthorizationStatus) UnmarshalJSON(data []byte) error {
	var shortName string
	if err := json.Unmarshal(data, &shortName); err != nil {
		return err
	}
	// Add the prefix back to match proto enum name
	fullName := "AUTHORIZATION_STATUS_" + shortName
	value, ok := AuthorizationStatus_value[fullName]
	if !ok {
		return fmt.Errorf("unknown AuthorizationStatus value: %s", shortName)
	}
	*x = AuthorizationStatus(value)
	return nil
}

// InvoiceStatus JSON marshaling
func (x InvoiceStatus) MarshalJSON() ([]byte, error) {
	// Remove the "INVOICE_STATUS_" prefix for JSON
	name := InvoiceStatus_name[int32(x)]
	shortName := strings.TrimPrefix(name, "INVOICE_STATUS_")
	return json.Marshal(shortName)
}

func (x *InvoiceStatus) UnmarshalJSON(data []byte) error {
	var shortName string
	if err := json.Unmarshal(data, &shortName); err != nil {
		return err
	}
	// Add the prefix back to match proto enum name
	fullName := "INVOICE_STATUS_" + shortName
	value, ok := InvoiceStatus_value[fullName]
	if !ok {
		return fmt.Errorf("unknown InvoiceStatus value: %s", shortName)
	}
	*x = InvoiceStatus(value)
	return nil
}
