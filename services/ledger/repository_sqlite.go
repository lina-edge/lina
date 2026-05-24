package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robertodantas/lina/internal"
	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"
)

type sqliteLedgerTx struct {
	tx        *sql.Tx
	ctx       context.Context
	sqlTracer *internal.SQLTracer
}

func (t *sqliteLedgerTx) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		t.sqlTracer.LogSQLError(t.ctx, "[repository] commit tx", []attribute.KeyValue{
			attribute.String("db.operation", "COMMIT"),
		}, err)
	}
	return err
}
func (t *sqliteLedgerTx) Rollback() error { return t.tx.Rollback() }

func expectSqliteTx(tx LedgerTx) (*sql.Tx, error) {
	if tx == nil {
		return nil, errors.New("ledger: nil transaction")
	}
	st, ok := tx.(*sqliteLedgerTx)
	if !ok {
		return nil, fmt.Errorf("ledger: expected sqlite transaction, got %T", tx)
	}
	return st.tx, nil
}

func mapSQLRowErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// ledgerRepoSQLite is the SQLite implementation of LedgerRepository.
type ledgerRepoSQLite struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer
}

// openLedgerRepoSQLite creates and initializes the SQLite database with schema.
func openLedgerRepoSQLite(dbPath string, busyTimeoutMS int) (LedgerRepository, error) {
	// WAL + busy_timeout + performance optimizations for high load
	// - WAL mode: allows concurrent readers and one writer
	// - busy_timeout: how long to wait when database is locked (in ms)
	// - synchronous(NORMAL): good balance between safety and performance with WAL
	// - cache_size: increase cache to 8MB (negative = KB, so -8192 = 8MB, default is -2000 = 2MB)
	// - temp_store: use memory for temporary tables/indexes (2 = memory)
	// - mmap_size: use memory-mapped I/O for better performance (268435456 = 256MB)
	// - foreign_keys: enable foreign key constraints
	dsn := fmt.Sprintf(
		"%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-8192)&_pragma=temp_store(2)&_pragma=mmap_size(268435456)&_pragma=foreign_keys(1)",
		dbPath, busyTimeoutMS,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	// Configure connection pool for SQLite
	// WAL allows concurrent readers but still a single writer. Multiple open
	// connections each trying to write cause SQLITE_BUSY under load. Serialize
	// access through one connection so callers wait on the pool instead of
	// failing after busy_timeout.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

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
			kind TEXT NOT NULL,              -- credit|debit|consumption
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

	repo := &ledgerRepoSQLite{
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

/*
   =========================================
   Balance operations
   =========================================
*/

// EnsureBalanceRow ensures a balance row exists for a device
func (r *ledgerRepoSQLite) EnsureBalanceRow(ctx context.Context, tx LedgerTx, deviceID string) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
	query := `INSERT INTO balances(device_id, balance_msat, updated_at)
		 VALUES(?,?,?)
		 ON CONFLICT(device_id) DO NOTHING`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] ensure balance row", attrs, stx, query, deviceID, 0, now())
	return err
}

// GetBalance retrieves the balance for a device
func (r *ledgerRepoSQLite) GetBalance(ctx context.Context, tx LedgerTx, deviceID string) (int64, error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return 0, err
	}
	query := `SELECT balance_msat FROM balances WHERE device_id=?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get balance", attrs, stx, query, deviceID)

	var bal int64
	err = row.Scan(&bal)
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
func (r *ledgerRepoSQLite) UpdateBalance(ctx context.Context, tx LedgerTx, deviceID string, amountMsat int64) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
	query := `UPDATE balances SET balance_msat = balance_msat + ?, updated_at=? WHERE device_id=?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
		attribute.Int64("amount_msat", amountMsat),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] update balance", attrs, stx, query, amountMsat, now(), deviceID)
	return err
}

/*
   =========================================
   Ledger entry operations
   =========================================
*/

// CreateLedgerEntry creates a new ledger entry
func (r *ledgerRepoSQLite) CreateLedgerEntry(ctx context.Context, tx LedgerTx, entry EntryResponse) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
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
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] create ledger entry", attrs, stx, query,
		entry.EntryID, entry.DeviceID, entry.EntryType, entry.AmountMsat, entry.BalanceAfter, entry.Reason, entry.CorrelationID, entry.CreatedAt,
	)
	return err
}

// ListLedgerEntries retrieves ledger entries for a device with pagination
func (r *ledgerRepoSQLite) ListLedgerEntries(ctx context.Context, deviceID string, cursorCreated int64, cursorID string, limit int) ([]EntryResponse, error) {
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
	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list ledger entries rows", attrs, err)
		return nil, err
	}

	return resp, nil
}

/*
   =========================================
   Credit/Debit operations
   =========================================
*/

// upsertBalanceReturning inserts or updates the balance row in a single statement and returns the new balance.
// Uses INSERT ON CONFLICT to handle first-time devices without a separate EnsureBalanceRow call.
func (r *ledgerRepoSQLite) upsertBalanceReturning(ctx context.Context, stx *sql.Tx, deviceID string, deltaMsat int64) (int64, error) {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPSERT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
		attribute.Int64("delta_msat", deltaMsat),
	}
	// excluded.balance_msat is the delta from the VALUES clause.
	// On conflict the existing balance is updated by adding the delta (negative for debits).
	query := `
		INSERT INTO balances(device_id, balance_msat, updated_at) VALUES(?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			balance_msat = balance_msat + excluded.balance_msat,
			updated_at   = excluded.updated_at
		RETURNING balance_msat`
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] upsert balance", attrs, stx, query, deviceID, deltaMsat, now())
	var bal int64
	if err := row.Scan(&bal); err != nil {
		return 0, err
	}
	return bal, nil
}

// ApplyCredit applies a credit to a device's balance and creates a ledger entry
func (r *ledgerRepoSQLite) ApplyCredit(ctx context.Context, tx LedgerTx, in CreditRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return EntryResponse{}, err
	}
	// Single UPSERT: creates the row if missing, adds amount, returns new balance.
	bal, err := r.upsertBalanceReturning(ctx, stx, in.DeviceID, in.AmountMsat)
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
func (r *ledgerRepoSQLite) ApplyDebit(ctx context.Context, tx LedgerTx, in DebitRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return EntryResponse{}, err
	}
	if !in.AllowNegative {
		// Funds check: ensure row exists and read balance before subtracting.
		if err := r.EnsureBalanceRow(ctx, tx, in.DeviceID); err != nil {
			return EntryResponse{}, err
		}
		bal, err := r.GetBalance(ctx, tx, in.DeviceID)
		if err != nil {
			return EntryResponse{}, err
		}
		if bal < in.AmountMsat {
			return EntryResponse{}, fmt.Errorf("insufficient funds: have %d need %d", bal, in.AmountMsat)
		}
	}
	// Single UPSERT: creates the row if missing (AllowNegative path), subtracts amount, returns new balance.
	bal, err := r.upsertBalanceReturning(ctx, stx, in.DeviceID, -in.AmountMsat)
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
func (r *ledgerRepoSQLite) GetCachedIdem(ctx context.Context, key string) (kind string, resp []byte, ok bool, err error) {
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
func (r *ledgerRepoSQLite) SaveIdem(ctx context.Context, tx LedgerTx, key, kind, reqHash string, response any) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
	js, _ := json.Marshal(response)
	query := `INSERT INTO idempotency(idempotency_key, kind, request_hash, response_json, created_at)
		VALUES(?,?,?,?,?)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "idempotency"),
		attribute.String("idempotency.key", key),
		attribute.String("idempotency.kind", kind),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] save idempotency", attrs, stx, query, key, kind, reqHash, string(js), now())
	return err
}

/*
   =========================================
   Authorization operations
   =========================================
*/

// CreateAuthorization creates a new authorization
func (r *ledgerRepoSQLite) CreateAuthorization(ctx context.Context, tx LedgerTx, authID, deviceID, requestID string, grantedMsat int64, issuedAt, expiresAt string) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
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
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] create authorization", attrs, stx, query,
		authID, deviceID, requestID, grantedMsat, grantedMsat,
		0, 0, issuedAt, expiresAt, "active", time.Now().Unix(),
	)
	return err
}

// GetAuthorizationByRequestID retrieves an authorization by request_id
func (r *ledgerRepoSQLite) GetAuthorizationByRequestID(ctx context.Context, tx LedgerTx, requestID string) (*ledgermodel.Authorization, string, error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return nil, "", err
	}
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
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get authorization by request id", attrs, stx, query, requestID)

	var authID, deviceID, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64

	err = row.Scan(&authID, &deviceID, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus)
	if err != nil {
		return nil, "", mapSQLRowErr(err)
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
func (r *ledgerRepoSQLite) GetActiveAuthorization(ctx context.Context, tx LedgerTx, deviceID string, expiresAfter string) (string, int64, int64, int64, string, string, error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return "", 0, 0, 0, "", "", err
	}
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
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization", attrs, stx, query, deviceID, expiresAfter)

	var authorizationID string
	var remainingMsat int64
	var grantedMsat int64
	var overflowMsat int64
	var expiresAt string
	var status string

	err = row.Scan(&authorizationID, &remainingMsat, &grantedMsat, &overflowMsat, &expiresAt, &status)
	if err != nil {
		return "", 0, 0, 0, "", "", mapSQLRowErr(err)
	}

	return authorizationID, remainingMsat, grantedMsat, overflowMsat, expiresAt, status, nil
}

// GetActiveAuthorizationForDevice retrieves the most recent active authorization for a device
// Returns the authorization and its status, or ErrNotFound if none exists
func (r *ledgerRepoSQLite) GetActiveAuthorizationForDevice(ctx context.Context, tx LedgerTx, deviceID string) (*ledgermodel.Authorization, string, error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return nil, "", err
	}
	nowStr := time.Now().Format(time.RFC3339)
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
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization for device", attrs, stx, query, deviceID, nowStr)

	var authID, deviceIDResult, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64

	err = row.Scan(&authID, &deviceIDResult, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus)
	if err != nil {
		return nil, "", mapSQLRowErr(err)
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
func (r *ledgerRepoSQLite) UpdateAuthorization(ctx context.Context, tx LedgerTx, authorizationID string, remainingMsat int64, consumedMsat int64, overflowMsat int64, status string) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
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
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] update authorization", attrs, stx, query,
		remainingMsat, consumedMsat, overflowMsat, status, authorizationID,
	)
	return err
}

// ConsumeAuthorization atomically consumes from an authorization in a single SQL statement.
// Uses scalar MIN/MAX expressions so all calculation and the write happen in one round-trip.
// Returns the new remaining_msat, consumed_msat, overflow_msat, and status.
func (r *ledgerRepoSQLite) ConsumeAuthorization(ctx context.Context, tx LedgerTx, authorizationID string, debitAmount int64) (newRemaining int64, newConsumed int64, newOverflow int64, newStatus string, err error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return 0, 0, 0, "", err
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
		attribute.Int64("debit_amount", debitAmount),
	}
	// Column references inside SET expressions refer to the OLD row values, so:
	//   actualDebit  = MIN(remaining_msat, debitAmount)
	//   newRemaining = MAX(0, remaining_msat - debitAmount)
	//   newConsumed  = MIN(granted_msat, consumed_msat + actualDebit)
	//   newOverflow  = overflow_msat + MAX(0, debitAmount - remaining_msat)
	//   newStatus    = 'completed' when remaining_msat <= debitAmount (newRemaining would be 0)
	// RETURNING gives the post-update values so the caller gets them without a second round-trip.
	query := `
		UPDATE authorizations SET
			remaining_msat = MAX(0, remaining_msat - ?),
			consumed_msat  = MIN(granted_msat, consumed_msat + MIN(remaining_msat, ?)),
			overflow_msat  = overflow_msat + MAX(0, ? - remaining_msat),
			status         = CASE WHEN remaining_msat <= ? THEN 'completed' ELSE 'active' END
		WHERE authorization_id = ?
		RETURNING remaining_msat, consumed_msat, overflow_msat, status`
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] consume authorization", attrs, stx, query,
		debitAmount, debitAmount, debitAmount, debitAmount, authorizationID,
	)
	if err = row.Scan(&newRemaining, &newConsumed, &newOverflow, &newStatus); err != nil {
		return 0, 0, 0, "", mapSQLRowErr(err)
	}
	return newRemaining, newConsumed, newOverflow, newStatus, nil
}

// GetExpiredAuthorizations retrieves all expired active authorizations
func (r *ledgerRepoSQLite) GetExpiredAuthorizations(ctx context.Context, expiresBefore string) ([]ExpiredAuthorization, error) {
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
	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] get expired authorizations rows", attrs, err)
		return nil, err
	}

	return expired, nil
}

// GetActiveAuthorizationByID retrieves an active authorization's device ID and remaining amount
func (r *ledgerRepoSQLite) GetActiveAuthorizationByID(ctx context.Context, tx LedgerTx, authorizationID string) (deviceID string, remainingMsat int64, err error) {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return "", 0, err
	}
	query := `
		SELECT device_id, remaining_msat
		FROM authorizations
		WHERE authorization_id = ? AND status = 'active'`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization by id", attrs, stx, query, authorizationID)

	if err = row.Scan(&deviceID, &remainingMsat); err != nil {
		return "", 0, mapSQLRowErr(err)
	}

	return deviceID, remainingMsat, nil
}

// MarkAuthorizationExpired marks an authorization as expired
func (r *ledgerRepoSQLite) MarkAuthorizationExpired(ctx context.Context, tx LedgerTx, authorizationID string) error {
	stx, err := expectSqliteTx(tx)
	if err != nil {
		return err
	}
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
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] mark authorization expired", attrs, stx, query, authorizationID)
	return err
}

// ListAuthorizations retrieves authorizations for a device with optional status filter
func (r *ledgerRepoSQLite) ListAuthorizations(ctx context.Context, deviceID string, statusFilter string) ([]AuthorizationResponse, error) {
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
	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list authorizations rows", attrs, err)
		return nil, err
	}

	return resp, nil
}

// BeginTx starts a new transaction.
func (r *ledgerRepoSQLite) BeginTx(ctx context.Context, opts *LedgerTxOptions) (LedgerTx, error) {
	var o *sql.TxOptions
	if opts != nil && opts.ReadOnly {
		o = &sql.TxOptions{ReadOnly: true}
	}
	tx, err := r.db.BeginTx(ctx, o)
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] begin tx", []attribute.KeyValue{
			attribute.String("db.operation", "BEGIN"),
			attribute.Bool("db.transaction.read_only", opts != nil && opts.ReadOnly),
		}, err)
		return nil, err
	}
	return &sqliteLedgerTx{tx: tx, ctx: ctx, sqlTracer: r.sqlTracer}, nil
}

// Close closes the database connection.
func (r *ledgerRepoSQLite) Close() error {
	return r.db.Close()
}
