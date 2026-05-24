package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/robertodantas/lina/internal"
	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
	"go.opentelemetry.io/otel/attribute"
)

type pgLedgerTx struct {
	tx        *sql.Tx
	ctx       context.Context
	sqlTracer *internal.SQLTracer
}

func (t *pgLedgerTx) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		t.sqlTracer.LogSQLError(t.ctx, "[repository] commit tx", []attribute.KeyValue{
			attribute.String("db.operation", "COMMIT"),
		}, err)
	}
	return err
}
func (t *pgLedgerTx) Rollback() error { return t.tx.Rollback() }

func expectPgTx(tx LedgerTx) (*sql.Tx, error) {
	if tx == nil {
		return nil, errors.New("ledger: nil transaction")
	}
	pt, ok := tx.(*pgLedgerTx)
	if !ok {
		return nil, fmt.Errorf("ledger: expected postgres transaction, got %T", tx)
	}
	return pt.tx, nil
}

type ledgerRepoPG struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer
}

// openLedgerRepoPostgres connects to PostgreSQL and creates the schema if absent.
// maxOpenConns controls the connection pool size — unlike SQLite, Postgres supports
// concurrent writers so this should be > 1 (typically 8–16 for a Pi workload).
func openLedgerRepoPostgres(dsn string, maxOpenConns int) (LedgerRepository, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns / 2)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS balances(
			device_id    TEXT PRIMARY KEY,
			balance_msat BIGINT NOT NULL DEFAULT 0,
			updated_at   BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ledger_entries(
			id             TEXT PRIMARY KEY,
			device_id      TEXT NOT NULL,
			entry_type     TEXT NOT NULL,
			amount_msat    BIGINT NOT NULL,
			balance_after  BIGINT NOT NULL,
			reason         TEXT,
			correlation_id TEXT,
			created_at     BIGINT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ledger_device_time ON ledger_entries(device_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS idempotency(
			idempotency_key TEXT PRIMARY KEY,
			kind            TEXT NOT NULL,
			request_hash    TEXT NOT NULL,
			response_json   TEXT NOT NULL,
			created_at      BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS authorizations(
			authorization_id TEXT PRIMARY KEY,
			device_id        TEXT NOT NULL,
			request_id       TEXT NOT NULL,
			granted_msat     BIGINT NOT NULL,
			remaining_msat   BIGINT NOT NULL,
			consumed_msat    BIGINT NOT NULL DEFAULT 0,
			overflow_msat    BIGINT NOT NULL DEFAULT 0,
			issued_at        TEXT NOT NULL,
			expires_at       TEXT NOT NULL,
			status           TEXT NOT NULL,
			created_at       BIGINT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_auth_device_status ON authorizations(device_id, status, expires_at)`,
		`CREATE INDEX IF NOT EXISTS idx_auth_request_id ON authorizations(request_id)`,
	}

	repo := &ledgerRepoPG{
		db:        db,
		sqlTracer: internal.NewSQLTracer("repository.ledger"),
	}

	ctx := context.Background()
	attrs := []attribute.KeyValue{attribute.String("db.operation", "CREATE TABLE/INDEX")}
	for _, s := range stmts {
		if _, err := repo.sqlTracer.ExecWithSpan(ctx, "[repository] create schema", attrs, db, s); err != nil {
			return nil, fmt.Errorf("failed to create postgres schema: %w", err)
		}
	}

	return repo, nil
}

/*
   =========================================
   Balance operations
   =========================================
*/

func (r *ledgerRepoPG) EnsureBalanceRow(ctx context.Context, tx LedgerTx, deviceID string) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `INSERT INTO balances(device_id, balance_msat, updated_at) VALUES($1, $2, $3) ON CONFLICT(device_id) DO NOTHING`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] ensure balance row", attrs, ptx, query, deviceID, 0, now())
	return err
}

func (r *ledgerRepoPG) GetBalance(ctx context.Context, tx LedgerTx, deviceID string) (int64, error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return 0, err
	}
	query := `SELECT balance_msat FROM balances WHERE device_id=$1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get balance", attrs, ptx, query, deviceID)
	var bal int64
	if err := row.Scan(&bal); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return bal, nil
}

func (r *ledgerRepoPG) UpdateBalance(ctx context.Context, tx LedgerTx, deviceID string, amountMsat int64) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `UPDATE balances SET balance_msat = balance_msat + $1, updated_at=$2 WHERE device_id=$3`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
		attribute.Int64("amount_msat", amountMsat),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] update balance", attrs, ptx, query, amountMsat, now(), deviceID)
	return err
}

/*
   =========================================
   Ledger entry operations
   =========================================
*/

func (r *ledgerRepoPG) CreateLedgerEntry(ctx context.Context, tx LedgerTx, entry EntryResponse) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `INSERT INTO ledger_entries(id, device_id, entry_type, amount_msat, balance_after, reason, correlation_id, created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "ledger_entries"),
		attribute.String("entry.id", entry.EntryID),
		attribute.String("device.id", entry.DeviceID),
		attribute.String("entry.type", entry.EntryType),
		attribute.Int64("amount_msat", entry.AmountMsat),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] create ledger entry", attrs, ptx, query,
		entry.EntryID, entry.DeviceID, entry.EntryType, entry.AmountMsat, entry.BalanceAfter, entry.Reason, entry.CorrelationID, entry.CreatedAt,
	)
	return err
}

func (r *ledgerRepoPG) ListLedgerEntries(ctx context.Context, deviceID string, cursorCreated int64, cursorID string, limit int) ([]EntryResponse, error) {
	query := `
		SELECT id, entry_type, amount_msat, balance_after, reason, correlation_id, created_at
		  FROM ledger_entries
		 WHERE device_id = $1
		   AND (created_at < $2 OR (created_at = $3 AND id < $4))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $5`
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

// upsertBalanceReturning inserts or updates the balance in one round-trip and returns the new balance.
// PostgreSQL's ON CONFLICT DO UPDATE SET references existing row values without table qualification
// (same semantics as SQLite's INSERT OR REPLACE variant).
func (r *ledgerRepoPG) upsertBalanceReturning(ctx context.Context, ptx *sql.Tx, deviceID string, deltaMsat int64) (int64, error) {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPSERT"),
		attribute.String("db.table", "balances"),
		attribute.String("device.id", deviceID),
		attribute.Int64("delta_msat", deltaMsat),
	}
	query := `
		INSERT INTO balances(device_id, balance_msat, updated_at) VALUES($1, $2, $3)
		ON CONFLICT(device_id) DO UPDATE SET
			balance_msat = balances.balance_msat + excluded.balance_msat,
			updated_at   = excluded.updated_at
		RETURNING balance_msat`
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] upsert balance", attrs, ptx, query, deviceID, deltaMsat, now())
	var bal int64
	if err := row.Scan(&bal); err != nil {
		return 0, err
	}
	return bal, nil
}

func (r *ledgerRepoPG) ApplyCredit(ctx context.Context, tx LedgerTx, in CreditRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	ptx, err := expectPgTx(tx)
	if err != nil {
		return EntryResponse{}, err
	}
	bal, err := r.upsertBalanceReturning(ctx, ptx, in.DeviceID, in.AmountMsat)
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

func (r *ledgerRepoPG) ApplyDebit(ctx context.Context, tx LedgerTx, in DebitRequest) (EntryResponse, error) {
	if in.AmountMsat <= 0 {
		return EntryResponse{}, errors.New("amount must be > 0")
	}
	ptx, err := expectPgTx(tx)
	if err != nil {
		return EntryResponse{}, err
	}
	if !in.AllowNegative {
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
	bal, err := r.upsertBalanceReturning(ctx, ptx, in.DeviceID, -in.AmountMsat)
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

func (r *ledgerRepoPG) GetCachedIdem(ctx context.Context, key string) (kind string, resp []byte, ok bool, err error) {
	query := `SELECT kind, response_json FROM idempotency WHERE idempotency_key=$1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "idempotency"),
		attribute.String("idempotency.key", key),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get cached idempotency", attrs, r.db, query, key)
	var k, rStr string
	if e := row.Scan(&k, &rStr); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return "", nil, false, nil
		}
		return "", nil, false, e
	}
	return k, []byte(rStr), true, nil
}

func (r *ledgerRepoPG) SaveIdem(ctx context.Context, tx LedgerTx, key, kind, reqHash string, response any) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	js, _ := json.Marshal(response)
	query := `INSERT INTO idempotency(idempotency_key, kind, request_hash, response_json, created_at)
		VALUES($1,$2,$3,$4,$5)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "idempotency"),
		attribute.String("idempotency.key", key),
		attribute.String("idempotency.kind", kind),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] save idempotency", attrs, ptx, query, key, kind, reqHash, string(js), now())
	return err
}

/*
   =========================================
   Authorization operations
   =========================================
*/

func (r *ledgerRepoPG) CreateAuthorization(ctx context.Context, tx LedgerTx, authID, deviceID, requestID string, grantedMsat int64, issuedAt, expiresAt string) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO authorizations(
			authorization_id, device_id, request_id, granted_msat, remaining_msat,
			consumed_msat, overflow_msat, issued_at, expires_at, status, created_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authID),
		attribute.String("device.id", deviceID),
		attribute.String("request.id", requestID),
		attribute.Int64("granted_msat", grantedMsat),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] create authorization", attrs, ptx, query,
		authID, deviceID, requestID, grantedMsat, grantedMsat,
		0, 0, issuedAt, expiresAt, "active", time.Now().Unix(),
	)
	return err
}

func (r *ledgerRepoPG) GetAuthorizationByRequestID(ctx context.Context, tx LedgerTx, requestID string) (*ledgermodel.Authorization, string, error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return nil, "", err
	}
	query := `
		SELECT authorization_id, device_id, granted_msat, remaining_msat, issued_at, expires_at, status
		FROM authorizations
		WHERE request_id = $1
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("request.id", requestID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get authorization by request id", attrs, ptx, query, requestID)

	var authID, deviceID, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64
	if err := row.Scan(&authID, &deviceID, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus); err != nil {
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

func (r *ledgerRepoPG) GetActiveAuthorization(ctx context.Context, tx LedgerTx, deviceID string, expiresAfter string) (string, int64, int64, int64, string, string, error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return "", 0, 0, 0, "", "", err
	}
	query := `
		SELECT authorization_id, remaining_msat, granted_msat, overflow_msat, expires_at, status
		FROM authorizations
		WHERE device_id = $1 AND status = 'active' AND expires_at > $2
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization", attrs, ptx, query, deviceID, expiresAfter)

	var authorizationID, expiresAt, status string
	var remainingMsat, grantedMsat, overflowMsat int64
	if err := row.Scan(&authorizationID, &remainingMsat, &grantedMsat, &overflowMsat, &expiresAt, &status); err != nil {
		return "", 0, 0, 0, "", "", mapSQLRowErr(err)
	}
	return authorizationID, remainingMsat, grantedMsat, overflowMsat, expiresAt, status, nil
}

func (r *ledgerRepoPG) GetActiveAuthorizationForDevice(ctx context.Context, tx LedgerTx, deviceID string) (*ledgermodel.Authorization, string, error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return nil, "", err
	}
	nowStr := time.Now().Format(time.RFC3339)
	query := `
		SELECT authorization_id, device_id, granted_msat, remaining_msat, issued_at, expires_at, status
		FROM authorizations
		WHERE device_id = $1 AND status = 'active' AND expires_at > $2
		ORDER BY created_at DESC
		LIMIT 1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization for device", attrs, ptx, query, deviceID, nowStr)

	var authID, deviceIDResult, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64
	if err := row.Scan(&authID, &deviceIDResult, &grantedMsat, &remainingMsat, &issuedAt, &expiresAt, &authStatus); err != nil {
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

func (r *ledgerRepoPG) UpdateAuthorization(ctx context.Context, tx LedgerTx, authorizationID string, remainingMsat int64, consumedMsat int64, overflowMsat int64, status string) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `
		UPDATE authorizations
		SET remaining_msat = $1, consumed_msat = $2, overflow_msat = $3, status = $4
		WHERE authorization_id = $5`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
		attribute.String("authorization.status", status),
		attribute.Int64("remaining_msat", remainingMsat),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] update authorization", attrs, ptx, query,
		remainingMsat, consumedMsat, overflowMsat, status, authorizationID,
	)
	return err
}

// ConsumeAuthorization atomically consumes from an authorization in a single SQL statement.
// PostgreSQL uses GREATEST/LEAST for scalar min/max (unlike SQLite which uses MAX/MIN).
// Column references in SET refer to the row's values before this UPDATE began, so all
// calculations see consistent pre-update state — identical semantics to the SQLite version.
func (r *ledgerRepoPG) ConsumeAuthorization(ctx context.Context, tx LedgerTx, authorizationID string, debitAmount int64) (newRemaining int64, newConsumed int64, newOverflow int64, newStatus string, err error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return 0, 0, 0, "", err
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
		attribute.Int64("debit_amount", debitAmount),
	}
	query := `
		UPDATE authorizations SET
			remaining_msat = GREATEST(0, remaining_msat - $1),
			consumed_msat  = LEAST(granted_msat, consumed_msat + LEAST(remaining_msat, $2)),
			overflow_msat  = overflow_msat + GREATEST(0, $3 - remaining_msat),
			status         = CASE WHEN remaining_msat <= $4 THEN 'completed' ELSE 'active' END
		WHERE authorization_id = $5
		RETURNING remaining_msat, consumed_msat, overflow_msat, status`
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] consume authorization", attrs, ptx, query,
		debitAmount, debitAmount, debitAmount, debitAmount, authorizationID,
	)
	if err = row.Scan(&newRemaining, &newConsumed, &newOverflow, &newStatus); err != nil {
		return 0, 0, 0, "", mapSQLRowErr(err)
	}
	return newRemaining, newConsumed, newOverflow, newStatus, nil
}

func (r *ledgerRepoPG) GetExpiredAuthorizations(ctx context.Context, expiresBefore string) ([]ExpiredAuthorization, error) {
	query := `
		SELECT authorization_id, device_id, expires_at
		FROM authorizations
		WHERE status = 'active' AND expires_at < $1`
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

func (r *ledgerRepoPG) GetActiveAuthorizationByID(ctx context.Context, tx LedgerTx, authorizationID string) (deviceID string, remainingMsat int64, err error) {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return "", 0, err
	}
	query := `
		SELECT device_id, remaining_msat
		FROM authorizations
		WHERE authorization_id = $1 AND status = 'active'`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get active authorization by id", attrs, ptx, query, authorizationID)
	if err = row.Scan(&deviceID, &remainingMsat); err != nil {
		return "", 0, mapSQLRowErr(err)
	}
	return deviceID, remainingMsat, nil
}

func (r *ledgerRepoPG) MarkAuthorizationExpired(ctx context.Context, tx LedgerTx, authorizationID string) error {
	ptx, err := expectPgTx(tx)
	if err != nil {
		return err
	}
	query := `
		UPDATE authorizations
		SET status = 'expired', remaining_msat = 0
		WHERE authorization_id = $1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "authorizations"),
		attribute.String("authorization.id", authorizationID),
	}
	_, err = r.sqlTracer.ExecWithSpan(ctx, "[repository] mark authorization expired", attrs, ptx, query, authorizationID)
	return err
}

func (r *ledgerRepoPG) ListAuthorizations(ctx context.Context, deviceID string, statusFilter string) ([]AuthorizationResponse, error) {
	var query string
	switch statusFilter {
	case "active":
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = $1 AND status = 'active'
			ORDER BY created_at DESC`
	case "non-active":
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = $1 AND status IN ('completed', 'expired')
			ORDER BY created_at DESC`
	default:
		query = `
			SELECT authorization_id, device_id, request_id, granted_msat, remaining_msat, consumed_msat, overflow_msat,
			       issued_at, expires_at, status, created_at
			FROM authorizations
			WHERE device_id = $1
			ORDER BY created_at DESC`
	}

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "authorizations"),
		attribute.String("device.id", deviceID),
		attribute.String("status.filter", statusFilter),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list authorizations", attrs, r.db, query, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []AuthorizationResponse
	for rows.Next() {
		var auth AuthorizationResponse
		if err := rows.Scan(
			&auth.AuthorizationID, &auth.DeviceID, &auth.RequestID,
			&auth.GrantedMsat, &auth.RemainingMsat, &auth.ConsumedMsat, &auth.OverflowMsat,
			&auth.IssuedAt, &auth.ExpiresAt, &auth.Status, &auth.CreatedAt,
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

func (r *ledgerRepoPG) BeginTx(ctx context.Context, opts *LedgerTxOptions) (LedgerTx, error) {
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
	return &pgLedgerTx{tx: tx, ctx: ctx, sqlTracer: r.sqlTracer}, nil
}

func (r *ledgerRepoPG) Close() error {
	return r.db.Close()
}
