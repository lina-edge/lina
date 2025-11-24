package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
)

func verifySignature(secret, deviceID string, ts int64, seq int64, qty sql.NullFloat64, cnt sql.NullFloat64, signature string) bool {
	var valStr string
	switch {
	case cnt.Valid:
		valStr = fmt.Sprintf("%.6f", cnt.Float64)
	case qty.Valid:
		valStr = fmt.Sprintf("%.6f", qty.Float64)
	default:
		return false
	}
	msg := fmt.Sprintf("%d|%s|%d|%s", ts, deviceID, seq, valStr)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(msg))
	expected := hex.EncodeToString(h.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}
