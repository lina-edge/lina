package main

import (
	"context"
	"database/sql"
	"math"
	"time"

	"github.com/google/uuid"
)

type deviceState struct {
	PartialRemainder float64
	CurrentBucket    int64
	SumInBucket      float64
	SumForThreshold  float64
	LastCounter      sql.NullFloat64
	LastTS           sql.NullInt64
	UpdatedAt        int64
}

type Aggregator struct {
	db *sql.DB
}

func NewAggregator(db *sql.DB) *Aggregator { return &Aggregator{db: db} }

func (a *Aggregator) loadState(ctx context.Context, deviceID string) (deviceState, error) {
	var st deviceState
	row := a.db.QueryRowContext(ctx, `
        SELECT partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at
          FROM device_state WHERE device_id = ?`, deviceID)
	switch err := row.Scan(&st.PartialRemainder, &st.CurrentBucket, &st.SumInBucket, &st.SumForThreshold, &st.LastCounter, &st.LastTS, &st.UpdatedAt); err {
	case sql.ErrNoRows:
		return deviceState{UpdatedAt: time.Now().Unix()}, nil
	case nil:
		return st, nil
	default:
		return deviceState{}, err
	}
}

func (a *Aggregator) saveState(ctx context.Context, deviceID string, st deviceState) error {
	_, err := a.db.ExecContext(ctx, `
        INSERT INTO device_state(device_id, partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(device_id) DO UPDATE SET
            partial_remainder=excluded.partial_remainder,
            current_bucket=excluded.current_bucket,
            sum_in_bucket=excluded.sum_in_bucket,
            sum_for_threshold=excluded.sum_for_threshold,
            last_counter=excluded.last_counter,
            last_ts=excluded.last_ts,
            updated_at=excluded.updated_at
    `, deviceID, st.PartialRemainder, st.CurrentBucket, st.SumInBucket, st.SumForThreshold, st.LastCounter, st.LastTS, time.Now().Unix())
	return err
}

type AggEvent struct {
	DeviceID string
	TS       int64
	Quantity float64
}

type Batch struct {
	ID          string
	DeviceID    string
	WindowStart int64
	WindowEnd   int64
	Units       float64
	Unit        string
	UnitPrice   float64
	TotalSats   float64
	CreatedAt   int64
}

func (a *Aggregator) OnEvent(ctx context.Context, cfg DeviceConfig, ev AggEvent) ([]Batch, error) {
	st, err := a.loadState(ctx, ev.DeviceID)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	var out []Batch

	switch cfg.AggregationMode {
	case "per-unit":
		total := st.PartialRemainder + ev.Quantity
		units := math.Floor(total)
		if units >= 1 {
			out = append(out, Batch{
				ID:        uuid.NewString(),
				DeviceID:  ev.DeviceID,
				Units:     units,
				Unit:      cfg.Unit,
				UnitPrice: cfg.PricePerUnit,
				TotalSats: units * cfg.PricePerUnit,
				CreatedAt: now,
			})
			st.PartialRemainder = total - units
		} else {
			st.PartialRemainder = total
		}

	case "time-window":
		win := cfg.WindowSeconds
		if win <= 0 {
			win = 60
		}
		bucket := ev.TS / int64(win)
		if st.CurrentBucket == 0 {
			st.CurrentBucket = bucket
		}
		if bucket != st.CurrentBucket {
			if st.SumInBucket > 0 {
				start := st.CurrentBucket * int64(win)
				end := (st.CurrentBucket+1)*int64(win) - 1
				out = append(out, Batch{
					ID:          uuid.NewString(),
					DeviceID:    ev.DeviceID,
					WindowStart: start,
					WindowEnd:   end,
					Units:       st.SumInBucket,
					Unit:        cfg.Unit,
					UnitPrice:   cfg.PricePerUnit,
					TotalSats:   st.SumInBucket * cfg.PricePerUnit,
					CreatedAt:   now,
				})
			}
			st.CurrentBucket = bucket
			st.SumInBucket = 0
		}
		st.SumInBucket += ev.Quantity

	case "value-threshold":
		thr := cfg.ValueThreshold
		if thr <= 0 {
			thr = 1
		}
		st.SumForThreshold += ev.Quantity
		if st.SumForThreshold >= thr {
			units := st.SumForThreshold
			out = append(out, Batch{
				ID:        uuid.NewString(),
				DeviceID:  ev.DeviceID,
				Units:     units,
				Unit:      cfg.Unit,
				UnitPrice: cfg.PricePerUnit,
				TotalSats: units * cfg.PricePerUnit,
				CreatedAt: now,
			})
			st.SumForThreshold = 0
		}

	default:
		total := st.PartialRemainder + ev.Quantity
		units := math.Floor(total)
		if units >= 1 {
			out = append(out, Batch{
				ID:        uuid.NewString(),
				DeviceID:  ev.DeviceID,
				Units:     units,
				Unit:      cfg.Unit,
				UnitPrice: cfg.PricePerUnit,
				TotalSats: units * cfg.PricePerUnit,
				CreatedAt: now,
			})
			st.PartialRemainder = total - units
		} else {
			st.PartialRemainder = total
		}
	}

	if err := a.saveState(ctx, ev.DeviceID, st); err != nil {
		return nil, err
	}
	return out, nil
}
