package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "modernc.org/sqlite"
)

func initDB(path string) *sql.DB {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout=5000&_pragma=journal_mode(WAL)")
	if err != nil {
		log.Fatalf("db open: %v", err)
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            quantity REAL,
            counter REAL,
            signature TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(device_id, seq)
        );`,
		`CREATE TABLE IF NOT EXISTS queue_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_event_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            worker_id TEXT,
            claimed_at INTEGER,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(raw_event_id) REFERENCES raw_events(id)
        );`,
		`CREATE TABLE IF NOT EXISTS device_state (
            device_id TEXT PRIMARY KEY,
            partial_remainder REAL NOT NULL DEFAULT 0,
            current_bucket INTEGER NOT NULL DEFAULT 0,
            sum_in_bucket REAL NOT NULL DEFAULT 0,
            sum_for_threshold REAL NOT NULL DEFAULT 0,
            last_counter REAL,
            last_ts INTEGER DEFAULT 0,
            updated_at INTEGER NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS consumption_batches (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            window_start INTEGER,
            window_end INTEGER,
            units REAL NOT NULL,
            unit TEXT NOT NULL,
            unit_price_sats REAL NOT NULL,
            total_sats REAL NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );`,
		`CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            ref_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            sent_at INTEGER
        );`,
		`CREATE INDEX IF NOT EXISTS idx_raw_device_ts ON raw_events(device_id, ts);`,
		`CREATE INDEX IF NOT EXISTS idx_queue_status ON queue_events(status, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox(kind, sent_at);`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			log.Fatalf("db schema: %v", err)
		}
	}

	migrateAddColumnIfMissing(db, "raw_events", "counter", "REAL")
	migrateAddColumnIfMissing(db, "device_state", "last_counter", "REAL")
	migrateAddColumnIfMissing(db, "device_state", "last_ts", "INTEGER DEFAULT 0")

	return db
}

func migrateAddColumnIfMissing(db *sql.DB, table, col, typ string) {
	var found bool
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var cid int
			var name, ctype string
			var notnull, pk int
			var dflt sql.NullString
			_ = rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk)
			if strings.EqualFold(name, col) {
				found = true
				break
			}
		}
	}
	if !found {
		_, _ = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, col, typ))
	}
}
