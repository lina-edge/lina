package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robertodantas/lnpay/internal"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"
)

// LedgerRepository manages database operations for the ledger
type LedgerRepository struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer
}

// NewLedgerRepository creates and initializes the SQLite database with schema
func NewLedgerRepository(dbPath string, busyTimeoutMS int) (*LedgerRepository, error) {
	// WAL + busy_timeout for concurrent writers on edge devices.
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath, busyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	// Create tables and indexes
	stmts := []string{
		// Accounts / balances
		`CREATE TABLE IF NOT EXISTS balances(
			device_id TEXT PRIMARY KEY,
			balance_msat INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL
		);`,
		// Ledger entries (append-only)
		`CREATE TABLE IF NOT EXISTS ledger_entries(
			id TEXT PRIMARY KEY,
			device_id TEXT NOT NULL,
			entry_type TEXT NOT NULL,        -- credit|debit
			amount_msat INTEGER NOT NULL,    -- positive for credits & transfer_in; positive value also stored for debit, semantics defined by entry_type
			balance_after INTEGER NOT NULL,  -- balance after applying this entry (in msat)
			reason TEXT,
			correlation_id TEXT,             -- optional foreign corr id from caller
			created_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_ledger_device_time ON ledger_entries(device_id, created_at DESC);`,

		// Idempotency registry: one row per unique client request
		`CREATE TABLE IF NOT EXISTS idempotency(
			idempotency_key TEXT PRIMARY KEY,
			kind TEXT NOT NULL,              -- credit|debit
			request_hash TEXT NOT NULL,      -- lightweight dedupe guard (payload hash)
			response_json TEXT NOT NULL,     -- cached successful response
			created_at INTEGER NOT NULL
		);`,

		// Authorizations: holds for device spending
		`CREATE TABLE IF NOT EXISTS authorizations(
			authorization_id TEXT PRIMARY KEY,
			device_id TEXT NOT NULL,
			request_id TEXT NOT NULL,        -- unique request identifier for idempotency
			granted_msat INTEGER NOT NULL,
			remaining_msat INTEGER NOT NULL,
			consumed_msat INTEGER NOT NULL DEFAULT 0,
			overflow_msat INTEGER NOT NULL DEFAULT 0,
			issued_at TEXT NOT NULL,          -- ISO-8601 timestamp
			expires_at TEXT NOT NULL,         -- ISO-8601 timestamp
			status TEXT NOT NULL,            -- active|completed|expired
			created_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_auth_device_status ON authorizations(device_id, status, expires_at);`,
		`CREATE INDEX IF NOT EXISTS idx_auth_request_id ON authorizations(request_id);`,
	}

	repo := &LedgerRepository{
		db:        db,
		sqlTracer: internal.NewSQLTracer("repository.ledger"),
	}

	ctx := context.Background()
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "CREATE TABLE/INDEX"),
	}
	for _, s := range stmts {
		if _, err := repo.sqlTracer.ExecWithSpan(ctx, "[repository] create schema", attrs, db, s); err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return repo, nil
}

// now returns the current Unix timestamp
func now() int64 { return time.Now().Unix() }

/*
   =========================================
   Balance operations
   =========================================
*/

// EnsureBalanceRow ensures a balance row exists for a device
func (r *LedgerRepository) EnsureBalanceRow(ctx context.Context, tx *sql.Tx, deviceID string) error {
	query := `INSERT INTO balances(device_id, balance_msat, updated_at)
		 VALUES(?,?,?)
		 ON CONFLICT(device_id) DO NOTHING`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] ensure balance row", attrs, tx, query, deviceID, 0, now())
	return err
}

// GetBalance retrieves the balance for a device
func (r *LedgerRepository) GetBalance(ctx context.Context, tx *sql.Tx, deviceID string) (int64, error) {
	query := `SELECT balance_msat FROM balances WHERE device_id=?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get balance", attrs, tx, query, deviceID)

	var bal int64
	err := row.Scan(&bal)
	switch err {
	case nil:
		return bal, nil
	case sql.ErrNoRows:
		return 0, nil
	default:
		return 0, err
	}
}

// UpdateBalance adds or subtracts from a device's balance
func (r *LedgerRepository) UpdateBalance(ctx context.Context, tx *sql.Tx, deviceID string, amountMsat int64) error {
	query := `UPDATE balances SET balance_msat = balance_msat + ?, updated_at=? WHERE device_id=?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
		attribute.Int64("amount_msat", amountMsat),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] update balance", attrs, tx, query, amountMsat, now(), deviceID)
	return err
}

/*
   =========================================
   Ledger entry operations
   =========================================
*/

// CreateLedgerEntry creates a new ledger entry
func (r *LedgerRepository) CreateLedgerEntry(ctx context.Context, tx *sql.Tx, entry EntryResponse) error {
	query := `INSERT INTO ledger_entries(id, device_id, entry_type, amount_msat, balance_after, reason, correlation_id, created_at)
		VALUES(?,?,?,?,?,?,?,?)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "ledger_entries"),
		attribute.String("entry.id", entry.EntryID),
		attribute.String("device.id", entry.DeviceID),
		attribute.String("entry.type", entry.EntryType),
		attribute.Int64("amount_msat", entry.AmountMsat),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] create ledger entry", attrs, tx, query,
		entry.EntryID, entry.DeviceID, entry.EntryType, entry.AmountMsat, entry.BalanceAfter, entry.Reason, entry.CorrelationID, entry.CreatedAt,
	)
	return err
}

// ListLedgerEntries retrieves ledger entries for a device with pagination
func (r *LedgerRepository) ListLedgerEntries(ctx context.Context, deviceID string, cursorCreated int64, cursorID string, limit int) ([]EntryResponse, error) {
	query := `
		SELECT id, entry_type, amount_msat, balance_after, reason, correlation_id, created_at
		  FROM ledger_entries
		 WHERE device_id = ?
		   AND (created_at < ? OR (created_at = ? AND id < ?))
		 ORDER BY created_at DESC, id DESC
		 LIMIT ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "ledger_entries"),
		attribute.String("device.id", deviceID),
		attribute.Int("limit", limit),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list ledger entries", attrs, r.db, query,
		deviceID, cursorCreated, cursorCreated, cursorID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []EntryResponse
	for rows.Next() {
		var e EntryResponse
		var reason, corr sql.NullString
		if err := rows.Scan(&e.EntryID, &e.EntryType, &e.AmountMsat, &e.BalanceAfter, &reason, &corr, &e.CreatedAt); err != nil {
			continue
		}
		e.DeviceID = deviceID
		if reason.Valid {
			e.Reason = reason.String
		}
		if corr.Valid {
			e.CorrelationID = corr.String
		}
		resp = append(resp, e)
	}

	return resp, nil
}

/*
   =========================================
   Credit/Debit operations
   =========================================
*/

// ApplyCredit applies a credit to a device's balance and creates a ledger entry
func (r *LedgerRepository) ApplyCredit(ctx context.Context, tx *sql.Tx, in CreditRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	if err := r.EnsureBalanceRow(ctx, tx, in.DeviceID); err != nil {
		return EntryResponse{}, err
	}
	// Add funds
	if err := r.UpdateBalance(ctx, tx, in.DeviceID, in.AmountMsat); err != nil {
		return EntryResponse{}, err
	}
	// Read new balance
	bal, err := r.GetBalance(ctx, tx, in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	entry := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.DeviceID,
		EntryType:     "credit",
		AmountMsat:    in.AmountMsat,
		BalanceAfter:  bal,
		Reason:        in.Reason,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}
	if err := r.CreateLedgerEntry(ctx, tx, entry); err != nil {
		return EntryResponse{}, err
	}
	return entry, nil
}

// ApplyDebit applies a debit to a device's balance and creates a ledger entry
func (r *LedgerRepository) ApplyDebit(ctx context.Context, tx *sql.Tx, in DebitRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	if err := r.EnsureBalanceRow(ctx, tx, in.DeviceID); err != nil {
		return EntryResponse{}, err
	}
	// Funds check
	if !in.AllowNegative {
		bal, err := r.GetBalance(ctx, tx, in.DeviceID)
		if err != nil {
			return EntryResponse{}, err
		}
		if bal < in.AmountMsat {
			return EntryResponse{}, fmt.Errorf("insufficient funds: have %d need %d", bal, in.AmountMsat)
		}
	}
	// Subtract
	if err := r.UpdateBalance(ctx, tx, in.DeviceID, -in.AmountMsat); err != nil {
		return EntryResponse{}, err
	}
	// Read new balance
	bal, err := r.GetBalance(ctx, tx, in.DeviceID)
	if err != nil {
		return EntryResponse{}, err
	}
	entry := EntryResponse{
		EntryID:       uuid.NewString(),
		DeviceID:      in.DeviceID,
		EntryType:     "debit",
		AmountMsat:    in.AmountMsat,
		BalanceAfter:  bal,
		Reason:        in.Reason,
		CreatedAt:     now(),
		CorrelationID: in.CorrelationID,
	}
	if err := r.CreateLedgerEntry(ctx, tx, entry); err != nil {
		return EntryResponse{}, err
	}
	return entry, nil
}

/*
   =========================================
   Idempotency operations
   =========================================
*/

// GetCachedIdem retrieves a cached idempotency response
func (r *LedgerRepository) GetCachedIdem(ctx context.Context, key string) (kind string, resp []byte, ok bool, err error) {
	query := `SELECT kind, response_json FROM idempotency WHERE idempotency_key=?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "idempotency"),
		attribute.String("idempotency.key", key),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get cached idempotency", attrs, r.db, query, key)
	var k string
	var rStr string
	if e := row.Scan(&k, &rStr); e == sql.ErrNoRows {
		return "", nil, false, nil
	} else if e != nil {
		return "", nil, false, e
	}
	return k, []byte(rStr), true, nil
}

// SaveIdem saves an idempotency response
func (r *LedgerRepository) SaveIdem(ctx context.Context, tx *sql.Tx, key, kind, reqHash string, response any) error {
	js, _ := json.Marshal(response)
	query := `INSERT INTO idempotency(idempotency_key, kind, request_hash, response_json, created_at)
		VALUES(?,?,?,?,?)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "idempotency"),
		attribute.String("idempotency.key", key),
		attribute.String("idempotency.kind", kind),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] save idempotency", attrs, tx, query, key, kind, reqHash, string(js), now())
	return err
}

/*
   =========================================
   Authorization operations
   =========================================
*/

// CreateAuthorization creates a new authorization
func (r *LedgerRepository) CreateAuthorization(ctx context.Context, tx *sql.Tx, authID, deviceID, requestID string, grantedMsat int64, issuedAt, expiresAt string) error {
	query := `
		INSERT INTO authorizations(
			authorization_id, device_id, request_id, granted_msat, remaining_msat,
			consumed_msat, overflow_msat, issued_at, expires_at, status, created_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authID),
		attribute.String("device.id", deviceID),
		attribute.String("request.id", requestID),
		attribute.Int64("granted_msat", grantedMsat),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] create authorization", attrs, tx, query,
		authID, deviceID, requestID, grantedMsat, grantedMsat,
		0, 0, issuedAt, expiresAt, "active", time.Now().Unix(),
	)
	return err
}

// GetAuthorizationByRequestID retrieves an authorization by request_id
func (r *LedgerRepository) GetAuthorizationByRequestID(ctx context.Context, tx *sql.Tx, requestID string) (*ledgermodel.Authorization, string, error) {
	query := `
		SELECT authorization_id, device_id, granted_msat, remaining_msat, issued_at, expires_at, status
		FROM authorizations
		WHERE request_id = ?
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("request.id", requestID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get authorization by request id", attrs, tx, query, requestID)

	var authID, deviceID, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64

	err := row.Scan(&authID, &deviceID, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus)
	if err != nil {
		return nil, "", err
	}

	auth := &ledgermodel.Authorization{
		DeviceId:        deviceID,
		AuthorizationId: authID,
		GrantedMsat:     grantedMsat,
		RemainingMsat:   remainingMsat,
		IssuedAt:        issuedAt,
		ExpiresAt:       expiresAt,
	}

	return auth, authStatus, nil
}

// GetActiveAuthorization retrieves the most recent active authorization for a device
func (r *LedgerRepository) GetActiveAuthorization(ctx context.Context, tx *sql.Tx, deviceID string, expiresAfter string) (string, int64, int64, int64, string, string, error) {
	query := `
		SELECT authorization_id, remaining_msat, granted_msat, overflow_msat, expires_at, status
		FROM authorizations
		WHERE device_id = ? AND status = 'active' AND expires_at > ?
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization", attrs, tx, query, deviceID, expiresAfter)

	var authorizationID string
	var remainingMsat int64
	var grantedMsat int64
	var overflowMsat int64
	var expiresAt string
	var status string

	err := row.Scan(&authorizationID, &remainingMsat, &grantedMsat, &overflowMsat, &expiresAt, &status)
	if err != nil {
		return "", 0, 0, 0, "", "", err
	}

	return authorizationID, remainingMsat, grantedMsat, overflowMsat, expiresAt, status, nil
}

// GetActiveAuthorizationForDevice retrieves the most recent active authorization for a device
// Returns the authorization and its status, or sql.ErrNoRows if none exists
func (r *LedgerRepository) GetActiveAuthorizationForDevice(ctx context.Context, tx *sql.Tx, deviceID string) (*ledgermodel.Authorization, string, error) {
	now := time.Now().Format(time.RFC3339)
	query := `
		SELECT authorization_id, device_id, granted_msat, remaining_msat, issued_at, expires_at, status
		FROM authorizations
		WHERE device_id = ? AND status = 'active' AND expires_at > ?
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization for device", attrs, tx, query, deviceID, now)

	var authID, deviceIDResult, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64

	err := row.Scan(&authID, &deviceIDResult, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus)
	if err != nil {
		return nil, "", err
	}

	auth := &ledgermodel.Authorization{
		DeviceId:        deviceIDResult,
		AuthorizationId: authID,
		GrantedMsat:     grantedMsat,
		RemainingMsat:   remainingMsat,
		IssuedAt:        issuedAt,
		ExpiresAt:       expiresAt,
	}

	return auth, authStatus, nil
}

// UpdateAuthorization updates an authorization's remaining amount, consumed amount, overflow amount, and status
func (r *LedgerRepository) UpdateAuthorization(ctx context.Context, tx *sql.Tx, authorizationID string, remainingMsat int64, consumedMsat int64, overflowMsat int64, status string) error {
	query := `
		UPDATE authorizations
		SET remaining_msat = ?, consumed_msat = ?, overflow_msat = ?, status = ?
		WHERE authorization_id = ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
		attribute.String("authorization.status", status),
		attribute.Int64("remaining_msat", remainingMsat),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] update authorization", attrs, tx, query,
		remainingMsat, consumedMsat, overflowMsat, status, authorizationID,
	)
	return err
}

// ExpiredAuthorization represents an expired authorization
type ExpiredAuthorization struct {
	AuthorizationID string
	DeviceID        string
	ExpiresAt       string
}

// GetExpiredAuthorizations retrieves all expired active authorizations
func (r *LedgerRepository) GetExpiredAuthorizations(ctx context.Context, expiresBefore string) ([]ExpiredAuthorization, error) {
	query := `
		SELECT authorization_id, device_id, expires_at
		FROM authorizations
		WHERE status = 'active' AND expires_at < ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] get expired authorizations", attrs, r.db, query, expiresBefore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expired []ExpiredAuthorization
	for rows.Next() {
		var auth ExpiredAuthorization
		if err := rows.Scan(&auth.AuthorizationID, &auth.DeviceID, &auth.ExpiresAt); err != nil {
			continue
		}
		expired = append(expired, auth)
	}

	return expired, nil
}

// GetActiveAuthorizationByID retrieves an active authorization's device ID and remaining amount
func (r *LedgerRepository) GetActiveAuthorizationByID(ctx context.Context, tx *sql.Tx, authorizationID string) (deviceID string, remainingMsat int64, err error) {
	query := `
		SELECT device_id, remaining_msat
		FROM authorizations
		WHERE authorization_id = ? AND status = 'active'`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization by id", attrs, tx, query, authorizationID)

	if err := row.Scan(&deviceID, &remainingMsat); err != nil {
		return "", 0, err
	}

	return deviceID, remainingMsat, nil
}

// MarkAuthorizationExpired marks an authorization as expired
func (r *LedgerRepository) MarkAuthorizationExpired(ctx context.Context, tx *sql.Tx, authorizationID string) error {
	query := `
		UPDATE authorizations
		SET status = 'expired',
		    remaining_msat = 0
		WHERE authorization_id = ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] mark authorization expired", attrs, tx, query, authorizationID)
	return err
}

// ListAuthorizations retrieves authorizations for a device with optional status filter
func (r *LedgerRepository) ListAuthorizations(ctx context.Context, deviceID string, statusFilter string) ([]AuthorizationResponse, error) {
	var query string
	var args []interface{}

	if statusFilter == "active" {
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = ? AND status = 'active'
			ORDER BY created_at DESC`
		args = []interface{}{deviceID}
	} else if statusFilter == "non-active" {
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = ? AND status IN ('completed', 'expired')
			ORDER BY created_at DESC`
		args = []interface{}{deviceID}
	} else {
		// No filter - return all
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = ?
			ORDER BY created_at DESC`
		args = []interface{}{deviceID}
	}

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
		attribute.String("status.filter", statusFilter),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list authorizations", attrs, r.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []AuthorizationResponse
	for rows.Next() {
		var auth AuthorizationResponse
		if err := rows.Scan(
			&auth.AuthorizationID,
			&auth.DeviceID,
			&auth.RequestID,
			&auth.GrantedMsat,
			&auth.RemainingMsat,
			&auth.ConsumedMsat,
			&auth.OverflowMsat,
			&auth.IssuedAt,
			&auth.ExpiresAt,
			&auth.Status,
			&auth.CreatedAt,
		); err != nil {
			continue
		}
		resp = append(resp, auth)
	}

	return resp, nil
}

// BeginTx starts a new transaction
func (r *LedgerRepository) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, opts)
}

// Close closes the database connection
func (r *LedgerRepository) Close() error {
	return r.db.Close()
}
