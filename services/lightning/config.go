package main

import (
	"github.com/robertodantas/lnpay/internal"
)

type Config struct {
	LNDHost       string
	LNDTLSCertHex string
	LNDMacaroonHex string
	Network       string
}

func LoadConfig() Config {
	return Config{
		LNDHost:        internal.GetEnv("LND_HOST", ""),
		LNDTLSCertHex:   internal.GetEnv("LND_TLS_CERT_HEX", ""),
		LNDMacaroonHex:  internal.GetEnv("LND_MACAROON_HEX", ""),
		Network:         internal.GetEnv("LND_NETWORK", "mainnet"),
	}
}

