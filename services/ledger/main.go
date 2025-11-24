package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

/*
   =========================================
   Config & bootstrap
   =========================================
*/

type Config struct {
	DBPath        string
	ServiceToken  string
	ListenAddr    string
	MaxPageSize   int
	BusyTimeoutMS int
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func intEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func loadConfig() Config {
	return Config{
		DBPath:        getenv("DB_PATH", "ledger.db"),
		ServiceToken:  getenv("SERVICE_TOKEN", "dev-token"),
		ListenAddr:    getenv("LISTEN_ADDR", ":8080"),
		MaxPageSize:   intEnv("MAX_PAGE_SIZE", 200),
		BusyTimeoutMS: intEnv("BUSY_TIMEOUT_MS", 5000),
	}
}

/*
   =========================================
   SQLite init (WAL + schema)
   =========================================
*/

func initDB(cfg Config) *sql.DB {
	// WAL + busy_timeout for concurrent writers on edge devices.
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", cfg.DBPath, cfg.BusyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}

	stmts := []string{
		// Accounts / balances
		`CREATE TABLE IF NOT EXISTS balances(
			device_id TEXT PRIMARY KEY,
			balance_sats INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL
		);`,
		// Ledger entries (append-only)
		`CREATE TABLE IF NOT EXISTS ledger_entries(
			id TEXT PRIMARY KEY,
			device_id TEXT NOT NULL,
			entry_type TEXT NOT NULL,        -- credit|debit|transfer_in|transfer_out
			amount_sats INTEGER NOT NULL,    -- positive for credits & transfer_in; positive value also stored for debit, semantics defined by entry_type
			balance_after INTEGER NOT NULL,  -- balance after applying this entry
			reason TEXT,
			correlation_id TEXT,             -- optional foreign corr id from caller
			created_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_ledger_device_time ON ledger_entries(device_id, created_at DESC);`,

		// Idempotency registry: one row per unique client request
		`CREATE TABLE IF NOT EXISTS idempotency(
			idempotency_key TEXT PRIMARY KEY,
			kind TEXT NOT NULL,              -- credit|debit|transfer
			request_hash TEXT NOT NULL,      -- lightweight dedupe guard (payload hash)
			response_json TEXT NOT NULL,     -- cached successful response
			created_at INTEGER NOT NULL
		);`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			log.Fatalf("schema: %v", err)
		}
	}
	return db
}

/*
   =========================================
   Service state
   =========================================
*/

type Service struct {
	cfg Config
	db  *sql.DB
}

func NewService(cfg Config, db *sql.DB) *Service {
	return &Service{cfg: cfg, db: db}
}

/*
   =========================================
   Models & helpers
   =========================================
*/

type CreditRequest struct {
	DeviceID       string `json:"device_id" binding:"required"`
	AmountSats     int64  `json:"amount_sats" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type DebitRequest struct {
	DeviceID       string `json:"device_id" binding:"required"`
	AmountSats     int64  `json:"amount_sats" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	AllowNegative  bool   `json:"allow_negative,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type TransferRequest struct {
	FromDeviceID   string `json:"from_device_id" binding:"required"`
	ToDeviceID     string `json:"to_device_id" binding:"required"`
	AmountSats     int64  `json:"amount_sats" binding:"required"`
	Reason         string `json:"reason"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type EntryResponse struct {
	EntryID       string `json:"entry_id"`
	DeviceID      string `json:"device_id"`
	EntryType     string `json:"entry_type"`
	AmountSats    int64  `json:"amount_sats"`
	BalanceAfter  int64  `json:"balance_after"`
	CreatedAt     int64  `json:"created_at"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

type TransferResponse struct {
	Out EntryResponse `json:"debit_out"`
	In  EntryResponse `json:"credit_in"`
}

func now() int64 { return time.Now().Unix() }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func hashReq(kind string, body any) string {
	// cheap & deterministic fingerprint (no crypto requirement here)
	b, _ := json.Marshal(body)
	return fmt.Sprintf("%s:%d:%x", kind, len(b), djb2(b))
}

func djb2(b []byte) uint64 {
	var h uint64 = 5381
	for _, c := range b {
		h = ((h << 5) + h) + uint64(c)
	}
	return h
}

/*
   =========================================
   Auth (simple service token)
   =========================================
*/

func (s *Service) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		got := c.GetHeader("X-Service-Token")
		if s.cfg.ServiceToken == "" || got == s.cfg.ServiceToken {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}

/*
   =========================================
   Core ledger operations (transactional)
   =========================================
*/

func (s *Service) ensureBalanceRow(ctx context.Context, tx *sql.Tx, deviceID string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO balances(device_id, balance_sats, updated_at)
		 VALUES(?,?,?)
		 ON CONFLICT(device_id) DO NOTHING`,
		deviceID, 0, now(),
	)
	return err
}

func (s *Service) getBalance(ctx context.Context, tx *sql.Tx, deviceID string) (int64, error) {
	var bal int64
	row := tx.QueryRowContext(ctx, `SELECT balance_sats FROM balances WHERE device_id=?`, deviceID)
	switch err := row.Scan(&bal); err {
	case nil:
		return bal, nil
	case sql.ErrNoRows:
		return 0, nil
	default:
		return 0, err
	}
}

func (s *Service) applyCredit(ctx context.Context, tx *sql.Tx, in CreditRequest) (EntryResponse, error) {
	if in.AmountSats <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	if err := s.ensureBalanceRow(ctx, tx, in.DeviceID); err != nil {
		return EntryResponse{}, err
	}
	// Add funds
	_, err := tx.ExecContext(ctx, `
		UPDATE balances SET balance_sats = balance_sats + ?, updated_at=?
		 WHERE device_id=?`, in.AmountSats, now(), in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	// Read new balance
	bal, err := s.getBalance(ctx, tx, in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	entry := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.DeviceID,
		EntryType:     "credit",
		AmountSats:    in.AmountSats,
		BalanceAfter:  bal,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries(id, device_id, entry_type, amount_sats, balance_after, reason, correlation_id, created_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		entry.EntryID, entry.DeviceID, entry.EntryType, entry.AmountSats, entry.BalanceAfter, in.Reason, in.CorrelationID, entry.CreatedAt,
	)
	return entry, err
}

func (s *Service) applyDebit(ctx context.Context, tx *sql.Tx, in DebitRequest) (EntryResponse, error) {
	if in.AmountSats <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	if err := s.ensureBalanceRow(ctx, tx, in.DeviceID); err != nil {
		return EntryResponse{}, err
	}
	// Funds check
	if !in.AllowNegative {
		bal, err := s.getBalance(ctx, tx, in.DeviceID)
		if err != nil {
			return EntryResponse{}, err
		}
		if bal < in.AmountSats {
			return EntryResponse{}, fmt.Errorf("insufficient funds: have %d need %d", bal, in.AmountSats)
		}
	}
	// Subtract
	_, err := tx.ExecContext(ctx, `
		UPDATE balances SET balance_sats = balance_sats - ?, updated_at=?
		 WHERE device_id=?`, in.AmountSats, now(), in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	// Read new balance
	bal, err := s.getBalance(ctx, tx, in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	entry := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.DeviceID,
		EntryType:     "debit",
		AmountSats:    in.AmountSats,
		BalanceAfter:  bal,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries(id, device_id, entry_type, amount_sats, balance_after, reason, correlation_id, created_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		entry.EntryID, entry.DeviceID, entry.EntryType, entry.AmountSats, entry.BalanceAfter, in.Reason, in.CorrelationID, entry.CreatedAt,
	)
	return entry, err
}

func (s *Service) applyTransfer(ctx context.Context, tx *sql.Tx, in TransferRequest) (TransferResponse, error) {
	if in.AmountSats <= 0 {
		return TransferResponse{}, errors.New("amount must be > 0")
	}
	if strings.EqualFold(in.FromDeviceID, in.ToDeviceID) {
		return TransferResponse{}, errors.New("from and to must differ")
	}
	// Ensure both rows
	if err := s.ensureBalanceRow(ctx, tx, in.FromDeviceID); err != nil {
		return TransferResponse{}, err
	}
	if err := s.ensureBalanceRow(ctx, tx, in.ToDeviceID); err != nil {
		return TransferResponse{}, err
	}
	// Funds check
	fromBal, err := s.getBalance(ctx, tx, in.FromDeviceID)
	if err != nil {
		return TransferResponse{}, err
	}
	if fromBal < in.AmountSats {
		return TransferResponse{}, fmt.Errorf("insufficient funds on source: have %d need %d", fromBal, in.AmountSats)
	}

	// Move amounts atomically
	if _, err := tx.ExecContext(ctx,
		`UPDATE balances SET balance_sats = balance_sats - ?, updated_at=? WHERE device_id=?`,
		in.AmountSats, now(), in.FromDeviceID); err != nil {
		return TransferResponse{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE balances SET balance_sats = balance_sats + ?, updated_at=? WHERE device_id=?`,
		in.AmountSats, now(), in.ToDeviceID); err != nil {
		return TransferResponse{}, err
	}

	// Read new balances
	fromAfter, err := s.getBalance(ctx, tx, in.FromDeviceID)
	if err != nil {
		return TransferResponse{}, err
	}
	toAfter, err := s.getBalance(ctx, tx, in.ToDeviceID)
	if err != nil {
		return TransferResponse{}, err
	}

	out := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.FromDeviceID,
		EntryType:     "transfer_out",
		AmountSats:    in.AmountSats,
		BalanceAfter:  fromAfter,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}
	inE := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.ToDeviceID,
		EntryType:     "transfer_in",
		AmountSats:    in.AmountSats,
		BalanceAfter:  toAfter,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}

	// Append both entries
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ledger_entries(id, device_id, entry_type, amount_sats, balance_after, reason, correlation_id, created_at)
		VALUES(?,?,?,?,?,?,?,?),
		      (?,?,?,?,?,?,?,?)`,
		out.EntryID, out.DeviceID, out.EntryType, out.AmountSats, out.BalanceAfter, in.Reason, out.CorrelationID, out.CreatedAt,
		inE.EntryID, inE.DeviceID, inE.EntryType, inE.AmountSats, inE.BalanceAfter, in.Reason, inE.CorrelationID, inE.CreatedAt,
	); err != nil {
		return TransferResponse{}, err
	}

	return TransferResponse{Out: out, In: inE}, nil
}

/*
   =========================================
   Idempotency helpers
   =========================================
*/

func (s *Service) getCachedIdem(ctx context.Context, key string) (kind string, resp []byte, ok bool, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT kind, response_json FROM idempotency WHERE idempotency_key=?`, key)
	var k string
	var r string
	if e := row.Scan(&k, &r); e == sql.ErrNoRows {
		return "", nil, false, nil
	} else if e != nil {
		return "", nil, false, e
	}
	return k, []byte(r), true, nil
}

func (s *Service) saveIdem(ctx context.Context, tx *sql.Tx, key, kind, reqHash string, response any) error {
	js, _ := json.Marshal(response)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency(idempotency_key, kind, request_hash, response_json, created_at)
		VALUES(?,?,?,?,?)`,
		key, kind, reqHash, string(js), now(),
	)
	return err
}

/*
   =========================================
   HTTP Handlers
   =========================================
*/

func (s *Service) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *Service) postCredit(c *gin.Context) {
	var in CreditRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	// Idempotency short-circuit
	if kind, blob, ok, err := s.getCachedIdem(c, in.IdempotencyKey); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	} else if ok && kind == "credit" {
		var out EntryResponse
		_ = json.Unmarshal(blob, &out)
		c.JSON(http.StatusOK, out)
		return
	}

	ctx := c
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	out, err := s.applyCredit(ctx, tx, in)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := s.saveIdem(ctx, tx, in.IdempotencyKey, "credit", hashReq("credit", in), out); err != nil {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (s *Service) postDebit(c *gin.Context) {
	var in DebitRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		log.Printf("postDebit bind error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	if kind, blob, ok, err := s.getCachedIdem(c, in.IdempotencyKey); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	} else if ok && kind == "debit" {
		var out EntryResponse
		_ = json.Unmarshal(blob, &out)
		c.JSON(http.StatusOK, out)
		return
	}

	ctx := c
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	out, err := s.applyDebit(ctx, tx, in)
	if err != nil {
		// Do not persist idempotency for failed attempts
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := s.saveIdem(ctx, tx, in.IdempotencyKey, "debit", hashReq("debit", in), out); err != nil {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (s *Service) postTransfer(c *gin.Context) {
	var in TransferRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	if kind, blob, ok, err := s.getCachedIdem(c, in.IdempotencyKey); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	} else if ok && kind == "transfer" {
		var out TransferResponse
		_ = json.Unmarshal(blob, &out)
		c.JSON(http.StatusOK, out)
		return
	}

	ctx := c
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()

	out, err := s.applyTransfer(ctx, tx, in)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := s.saveIdem(ctx, tx, in.IdempotencyKey, "transfer", hashReq("transfer", in), out); err != nil {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}
	c.JSON(http.StatusOK, out)
}

func (s *Service) getBalanceHandler(c *gin.Context) {
	deviceID := c.Query("device_id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}
	tx, err := s.db.BeginTx(c, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		c.JSON(500, gin.H{"error": "begin"})
		return
	}
	defer func() { _ = tx.Rollback() }()
	_ = s.ensureBalanceRow(c, tx, deviceID)
	bal, err := s.getBalance(c, tx, deviceID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}
	c.JSON(200, gin.H{"device_id": deviceID, "balance_sats": bal})
}

func (s *Service) listEntries(c *gin.Context) {
	deviceID := c.Query("device_id")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "missing device_id"})
		return
	}
	limit := min(intEnv("DEFAULT_PAGE_SIZE", 50), s.cfg.MaxPageSize)
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, s.cfg.MaxPageSize)
		}
	}
	// cursor = created_at:id for keyset pagination (newest first)
	var cursorCreated int64 = 1<<62 - 1 // effectively +Inf
	var cursorID string = "zzzz"
	if cur := c.Query("cursor"); cur != "" {
		parts := strings.Split(cur, ":")
		if len(parts) == 2 {
			if t, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				cursorCreated = t
				cursorID = parts[1]
			}
		}
	}

	rows, err := s.db.Query(`
		SELECT id, entry_type, amount_sats, balance_after, reason, correlation_id, created_at
		  FROM ledger_entries
		 WHERE device_id = ?
		   AND (created_at < ? OR (created_at = ? AND id < ?))
		 ORDER BY created_at DESC, id DESC
		 LIMIT ?`,
		deviceID, cursorCreated, cursorCreated, cursorID, limit,
	)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var resp []EntryResponse
	var lastCreated int64
	var lastID string
	for rows.Next() {
		var e EntryResponse
		var reason, corr sql.NullString
		if err := rows.Scan(&e.EntryID, &e.EntryType, &e.AmountSats, &e.BalanceAfter, &reason, &corr, &e.CreatedAt); err != nil {
			continue
		}
		e.DeviceID = deviceID
		if corr.Valid {
			e.CorrelationID = corr.String
		}
		resp = append(resp, e)
		lastCreated = e.CreatedAt
		lastID = e.EntryID
	}

	nextCursor := ""
	if len(resp) == limit {
		nextCursor = fmt.Sprintf("%d:%s", lastCreated, lastID)
	}
	c.JSON(200, gin.H{"items": resp, "next_cursor": nextCursor})
}

/*
   =========================================
   main
   =========================================
*/

func main() {
	cfg := loadConfig()
	db := initDB(cfg)
	defer db.Close()

	svc := NewService(cfg, db)

	r := gin.Default()
	r.GET("/health", svc.health)

	// Protect mutating endpoints with service token
	auth := r.Group("/", svc.authMiddleware())
	auth.POST("/credit", svc.postCredit)
	auth.POST("/debit", svc.postDebit)
	auth.POST("/transfer", svc.postTransfer)

	// Read endpoints
	r.GET("/balance", svc.getBalanceHandler)
	r.GET("/entries", svc.listEntries)

	log.Printf("Ledger Service listening on %s (DB=%s)", cfg.ListenAddr, cfg.DBPath)
	if err := r.Run(cfg.ListenAddr); err != nil {
		log.Fatal(err)
	}
}
