package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type DeviceConfig struct {
	DeviceID        string  `json:"id"`
	Unit            string  `json:"unit"`
	PricePerUnit    float64 `json:"price_per_unit"`
	PublicKey       string  `json:"public_key"`
	AggregationMode string  `json:"aggregation_mode"`
	SecretKey       string  `json:"secret_key,omitempty"`
	ReportingMode   string  `json:"reporting_mode,omitempty"`
	WindowSeconds   int     `json:"window_seconds,omitempty"`
	ValueThreshold  float64 `json:"value_threshold,omitempty"`
	MeterMax        float64 `json:"meter_max,omitempty"`
	MaxDeltaAbs     float64 `json:"max_delta_abs,omitempty"`
	BillFromFirst   bool    `json:"bill_from_first,omitempty"`
}

type registryClient struct {
	baseURL string
	token   string
	ttl     time.Duration
	cache   sync.Map
}
type cachedCfg struct {
	cfg     DeviceConfig
	expires time.Time
}

func newRegistryClient(base, token string, ttl time.Duration) *registryClient {
	return &registryClient{baseURL: base, token: token, ttl: ttl}
}

func (rc *registryClient) GetConfig(ctx context.Context, deviceID string) (DeviceConfig, error) {
	if v, ok := rc.cache.Load(deviceID); ok {
		c := v.(cachedCfg)
		if time.Now().Before(c.expires) {
			return c.cfg, nil
		}
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", rc.baseURL+"/internal/devices/config?deviceId="+deviceID, nil)
	req.Header.Set("X-Service-Token", rc.token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return DeviceConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return DeviceConfig{}, fmt.Errorf("device not found in registry")
	}
	if resp.StatusCode != 200 {
		return DeviceConfig{}, fmt.Errorf("registry error: %s", resp.Status)
	}

	var out DeviceConfig
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return DeviceConfig{}, err
	}
	if out.AggregationMode == "" {
		out.AggregationMode = "per-unit"
	}
	if out.ReportingMode == "" {
		out.ReportingMode = "delta"
	}

	rc.cache.Store(deviceID, cachedCfg{cfg: out, expires: time.Now().Add(rc.ttl)})
	return out, nil
}
