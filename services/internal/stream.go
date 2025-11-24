package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// StreamClient wraps the Redis client for stream operations
type StreamClient struct {
	client *redis.Client
	ctx    context.Context
}

// StreamConfig holds configuration for creating a StreamClient
type StreamConfig struct {
	Host     string
	Port     string
	Password string
	DB       int
}

// NewStreamClient creates a new Redis stream client with the provided configuration
func NewStreamClient(config StreamConfig) (*StreamClient, error) {
	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	log.Printf("Connecting to Redis at %s (db: %d)...", addr, config.DB)

	opts := &redis.Options{
		Addr:     addr,
		Password: config.Password,
		DB:       config.DB,
	}

	client := redis.NewClient(opts)
	ctx := context.Background()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Println("Connected to Redis successfully")

	return &StreamClient{
		client: client,
		ctx:    ctx,
	}, nil
}

// NewStreamClientFromEnv creates a new Redis stream client reading configuration from environment variables
func NewStreamClientFromEnv() (*StreamClient, error) {
	host := getEnv("REDIS_HOST", "redis")
	port := getEnv("REDIS_PORT", "6379")
	password := getEnv("REDIS_PASSWORD", "")
	dbStr := getEnv("REDIS_DB", "0")

	db, err := strconv.Atoi(dbStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_DB value: %w", err)
	}

	config := StreamConfig{
		Host:     host,
		Port:     port,
		Password: password,
		DB:       db,
	}

	return NewStreamClient(config)
}

// Client returns the underlying Redis client (useful for advanced operations)
func (sc *StreamClient) Client() *redis.Client {
	return sc.client
}

// Context returns the context used by the stream client
func (sc *StreamClient) Context() context.Context {
	return sc.ctx
}

// Close closes the Redis client connection
func (sc *StreamClient) Close() error {
	if err := sc.client.Close(); err != nil {
		return fmt.Errorf("failed to close Redis client: %w", err)
	}
	log.Println("Redis client closed")
	return nil
}

// getEnv is a helper function to get environment variables with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

