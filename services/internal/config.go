package internal

import (
	"os"
	"strconv"
)

// GetEnv retrieves an environment variable or returns the default value
func GetEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// IntEnv retrieves an integer environment variable or returns the default value
func IntEnv(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultValue
}

// BoolEnv retrieves a boolean environment variable or returns the default value
func BoolEnv(key string, defaultValue bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultValue
}
