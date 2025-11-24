package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/robertodantas/lnpay/proto/gen/interfaces/ledger"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
)

// EastWestServer implements the LedgerService gRPC server
type EastWestServer struct {
	ledgerpb.UnimplementedLedgerServiceServer
	svc           *Service
	streamHandler *StreamHandler
}

// NewEastWestServer creates a new east-west gRPC server
func NewEastWestServer(svc *Service, streamHandler *StreamHandler) *EastWestServer {
	return &EastWestServer{
		svc:           svc,
		streamHandler: streamHandler,
	}
}

// CreateOrGetAuthorization creates a new authorization or returns the existing one based on request_id
func (s *EastWestServer) CreateOrGetAuthorization(ctx context.Context, req *ledgermodel.CreateAuthorizationRequest) (*ledgermodel.CreateAuthorizationResponse, error) {
	if req.DeviceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "device_id is required")
	}
	if req.RequestId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "request_id is required")
	}
	if req.RequestMsat <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "request_msat must be > 0")
	}

	tx, err := s.svc.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Ensure balance row exists
	if err := s.svc.ensureBalanceRow(ctx, tx, req.DeviceId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to ensure balance: %v", err)
	}

	// Get current balance (already in msat)
	balanceMsat, err := s.svc.getBalance(ctx, tx, req.DeviceId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get balance: %v", err)
	}

	// Check if authorization with this request_id already exists (idempotency)
	existingAuth, authStatus, err := s.getAuthorizationByRequestID(ctx, tx, req.RequestId)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, status.Errorf(codes.Internal, "failed to check authorization: %v", err)
	}

	// If authorization with this request_id exists, return it
	if existingAuth != nil {
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to commit: %v", err)
		}
		// Determine status: if still active, return ACTIVE, otherwise return GRANTED (for completed/expired)
		respStatus := ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_ACTIVE
		if authStatus != "active" {
			respStatus = ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED
		}
		return &ledgermodel.CreateAuthorizationResponse{
			DeviceId:      req.DeviceId,
			RequestId:     req.RequestId,
			Status:        respStatus,
			Authorization: existingAuth,
			Reason:        "ALREADY_ACTIVE",
		}, nil
	}

	// Check if we have sufficient balance
	if balanceMsat < req.RequestMsat {
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to commit: %v", err)
		}
		return &ledgermodel.CreateAuthorizationResponse{
			DeviceId:      req.DeviceId,
			RequestId:     req.RequestId,
			Status:        ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED,
			AvailableMsat: balanceMsat,
			Reason:        "INSUFFICIENT_FUNDS",
		}, nil
	}

	// Create new authorization
	nanos := time.Now().UnixNano()
	hexID := fmt.Sprintf("%x", nanos)
	authID := hexID
	now := time.Now()
	issuedAt := now.Format(time.RFC3339)
	expiresAt := now.Add(10 * time.Minute).Format(time.RFC3339) // 10 minute expiry

	// Insert authorization
	_, err = tx.ExecContext(ctx, `
		INSERT INTO authorizations(
			authorization_id, device_id, request_id, granted_msat, remaining_msat,
			issued_at, expires_at, status, created_at
		) VALUES(?,?,?,?,?,?,?,?,?)`,
		authID, req.DeviceId, req.RequestId, req.RequestMsat, req.RequestMsat,
		issuedAt, expiresAt, "active", time.Now().Unix(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create authorization: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit: %v", err)
	}

	// Build response
	auth := &ledgermodel.Authorization{
		DeviceId:        req.DeviceId,
		AuthorizationId: authID,
		GrantedMsat:     req.RequestMsat,
		RemainingMsat:   req.RequestMsat,
		IssuedAt:        issuedAt,
		ExpiresAt:       expiresAt,
	}

	// Publish AuthorizationCreated event
	if s.streamHandler != nil {
		if err := s.streamHandler.PublishAuthorizationCreated(ctx, auth); err != nil {
			// Log error but don't fail the request
			// TODO: consider adding retry logic or dead letter queue
		}
	}

	return &ledgermodel.CreateAuthorizationResponse{
		DeviceId:      req.DeviceId,
		RequestId:     req.RequestId,
		Status:        ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED,
		Authorization: auth,
	}, nil
}

// getAuthorizationByRequestID retrieves an authorization by request_id
// Returns: authorization, status string, error
func (s *EastWestServer) getAuthorizationByRequestID(ctx context.Context, tx *sql.Tx, requestID string) (*ledgermodel.Authorization, string, error) {
	var authID, deviceID, issuedAt, expiresAt, authStatus string
	var grantedMsat, remainingMsat int64

	row := tx.QueryRowContext(ctx, `
		SELECT authorization_id, device_id, granted_msat, remaining_msat, issued_at, expires_at, status
		FROM authorizations
		WHERE request_id = ?
		ORDER BY created_at DESC
		LIMIT 1`,
		requestID,
	)

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
