package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type postConsumptionIn struct {
	DeviceID  string   `json:"device_id" binding:"required"`
	TS        int64    `json:"ts" binding:"required"`
	Seq       int64    `json:"seq" binding:"required"`
	Quantity  *float64 `json:"quantity,omitempty"`
	Counter   *float64 `json:"counter,omitempty"`
	Signature string   `json:"signature"`
}

func nullInt64ToNative(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}

func nullFloat64ToNative(v sql.NullFloat64) any {
	if v.Valid {
		return v.Float64
	}
	return nil
}

func nullStringToNative(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}

func (s *Service) postConsumptions(c *gin.Context) {
	var in postConsumptionIn
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	if (in.Quantity == nil && in.Counter == nil) || (in.Quantity != nil && in.Counter != nil) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "must send exactly one of quantity or counter"})
		return
	}
	now := time.Now().Unix()

	var qtyVal any
	var cntVal any
	if in.Quantity != nil {
		qtyVal = *in.Quantity
	} else {
		qtyVal = nil
	}
	if in.Counter != nil {
		cntVal = *in.Counter
	} else {
		cntVal = nil
	}

	res, err := s.db.Exec(`
        INSERT INTO raw_events(device_id, seq, ts, quantity, counter, signature, created_at)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(device_id, seq) DO NOTHING
    `, in.DeviceID, in.Seq, in.TS, qtyVal, cntVal, in.Signature, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db insert raw_events"})
		return
	}

	var rawID int64
	affected, _ := res.RowsAffected()
	if affected == 0 {
		row := s.db.QueryRow(`SELECT id FROM raw_events WHERE device_id=? AND seq=?`, in.DeviceID, in.Seq)
		if err := row.Scan(&rawID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db select raw_event"})
			return
		}
	} else {
		row := s.db.QueryRow(`SELECT last_insert_rowid()`)
		if err := row.Scan(&rawID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "db last_insert_rowid"})
			return
		}
	}

	_, err = s.db.Exec(`INSERT INTO queue_events(raw_event_id, status, created_at) VALUES(?,?,?)`,
		rawID, "pending", now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db enqueue"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "queued", "raw_event_id": rawID})
}

func (s *Service) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *Service) getDeviceState(c *gin.Context) {
	deviceID := c.Query("device_id")
	if deviceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing device_id"})
		return
	}
	var st deviceState
	row := s.db.QueryRow(`SELECT partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at FROM device_state WHERE device_id=?`, deviceID)
	err := row.Scan(&st.PartialRemainder, &st.CurrentBucket, &st.SumInBucket, &st.SumForThreshold, &st.LastCounter, &st.LastTS, &st.UpdatedAt)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"partial_remainder": st.PartialRemainder,
		"current_bucket":    st.CurrentBucket,
		"sum_in_bucket":     st.SumInBucket,
		"sum_for_threshold": st.SumForThreshold,
		"last_counter":      nullFloat64ToNative(st.LastCounter),
		"last_ts":           nullInt64ToNative(st.LastTS),
		"updated_at":        st.UpdatedAt,
	})
}

func (s *Service) getQueue(c *gin.Context) {
	deviceID := c.Query("device_id")
	rows, err := s.db.Query(`SELECT q.id, q.status, q.attempts, q.worker_id, q.claimed_at, q.last_error, q.created_at, re.seq, re.ts, re.quantity, re.counter FROM queue_events q JOIN raw_events re ON re.id = q.raw_event_id WHERE re.device_id=? ORDER BY q.id DESC LIMIT 100`, deviceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, attempts, claimedAt, createdAt, seq, ts int64
		var status string
		var workerID, lastError sql.NullString
		var quantity, counter sql.NullFloat64
		err := rows.Scan(&id, &status, &attempts, &workerID, &claimedAt, &lastError, &createdAt, &seq, &ts, &quantity, &counter)
		if err != nil {
			continue
		}
		out = append(out, map[string]any{
			"id":         id,
			"status":     status,
			"attempts":   attempts,
			"worker_id":  nullStringToNative(workerID),
			"claimed_at": claimedAt,
			"last_error": nullStringToNative(lastError),
			"created_at": createdAt,
			"seq":        seq,
			"ts":         ts,
			"quantity":   nullFloat64ToNative(quantity),
			"counter":    nullFloat64ToNative(counter),
		})
	}
	c.JSON(http.StatusOK, out)
}

func (s *Service) getBatches(c *gin.Context) {
	deviceID := c.Query("device_id")
	rows, err := s.db.Query(`SELECT id, device_id, window_start, window_end, units, unit, unit_price_sats, total_sats, status, created_at FROM consumption_batches WHERE device_id=? ORDER BY created_at DESC LIMIT 100`, deviceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var id, deviceID string
		var windowStart, windowEnd sql.NullInt64
		var createdAt int64
		var units, unitPrice, totalSats float64
		var unit, status string
		err := rows.Scan(&id, &deviceID, &windowStart, &windowEnd, &units, &unit, &unitPrice, &totalSats, &status, &createdAt)
		if err != nil {
			log.Printf("[getBatches] scan error: %v", err)
			continue
		}
		out = append(out, map[string]any{
			"id":              id,
			"device_id":       deviceID,
			"window_start":    nullInt64ToNative(windowStart),
			"window_end":      nullInt64ToNative(windowEnd),
			"units":           units,
			"unit":            unit,
			"unit_price_sats": unitPrice,
			"total_sats":      totalSats,
			"status":          status,
			"created_at":      createdAt,
		})
	}
	c.JSON(http.StatusOK, out)
}

func (s *Service) getQueueAll(c *gin.Context) {
	rows, err := s.db.Query(`SELECT q.id, q.status, q.attempts, q.worker_id, q.claimed_at, q.last_error, q.created_at, re.device_id, re.seq, re.ts, re.quantity, re.counter FROM queue_events q JOIN raw_events re ON re.id = q.raw_event_id ORDER BY q.id DESC LIMIT 200`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, attempts, claimedAt, createdAt, seq, ts int64
		var status, deviceID string
		var workerID, lastError sql.NullString
		var quantity, counter sql.NullFloat64
		err := rows.Scan(&id, &status, &attempts, &workerID, &claimedAt, &lastError, &createdAt, &deviceID, &seq, &ts, &quantity, &counter)
		if err != nil {
			log.Printf("[getQueueAll] scan error: %v", err)
			continue
		}
		log.Printf("[getQueueAll] row: id=%d status=%s attempts=%d worker_id=%s claimed_at=%d last_error=%s created_at=%d device_id=%s seq=%d ts=%d quantity=%v counter=%v",
			id, status, attempts, workerID.String, claimedAt, lastError.String, createdAt, deviceID, seq, ts, quantity, counter)
		out = append(out, map[string]any{
			"id":         id,
			"status":     status,
			"attempts":   attempts,
			"worker_id":  nullStringToNative(workerID),
			"claimed_at": claimedAt,
			"last_error": nullStringToNative(lastError),
			"created_at": createdAt,
			"device_id":  deviceID,
			"seq":        seq,
			"ts":         ts,
			"quantity":   nullFloat64ToNative(quantity),
			"counter":    nullFloat64ToNative(counter),
		})
	}
	c.JSON(http.StatusOK, out)
}
