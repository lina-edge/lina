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
	repo          *LedgerRepository
	streamHandler *StreamHandler
}

// NewEastWestServer creates a new east-west gRPC server
func NewEastWestServer(repo *LedgerRepository, streamHandler *StreamHandler) *EastWestServer {
	return &EastWestServer{
		repo:          repo,
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

	tx, err := s.repo.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Ensure balance row exists
	if err := s.repo.EnsureBalanceRow(ctx, tx, req.DeviceId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to ensure balance: %v", err)
	}

	// Get current balance (already in msat)
	balanceMsat, err := s.repo.GetBalance(ctx, tx, req.DeviceId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get balance: %v", err)
	}

	// Check if authorization with this request_id already exists (idempotency)
	existingAuth, authStatus, err := s.repo.GetAuthorizationByRequestID(ctx, tx, req.RequestId)
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

	// Check if there's an active authorization for this device (even with different request_id)
	// This handles the case where the device reconnects after service restart with a new request_id
	activeAuth, activeAuthStatus, err := s.repo.GetActiveAuthorizationForDevice(ctx, tx, req.DeviceId)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, status.Errorf(codes.Internal, "failed to check active authorization: %v", err)
	}

	// If an active authorization exists for this device, return it
	if activeAuth != nil {
		if err := tx.Commit(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to commit: %v", err)
		}
		// Determine status: if still active, return ACTIVE, otherwise return GRANTED (for completed/expired)
		respStatus := ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_ACTIVE
		if activeAuthStatus != "active" {
			respStatus = ledgermodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED
		}
		return &ledgermodel.CreateAuthorizationResponse{
			DeviceId:      req.DeviceId,
			RequestId:     req.RequestId,
			Status:        respStatus,
			Authorization: activeAuth,
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
	if err := s.repo.CreateAuthorization(ctx, tx, authID, req.DeviceId, req.RequestId, req.RequestMsat, issuedAt, expiresAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create authorization: %v", err)
	}

	// Create debit ledger entry for the authorization
	logger.WithDeviceID(req.DeviceId).
		InfoWithFields(ctx, "Creating authorization hold debit via eastwest gRPC", map[string]interface{}{
			"authorization_id": authID,
			"amount_msat":      req.RequestMsat,
			"reason":           req.Reason,
		})
	debitReq := DebitRequest{
		DeviceID:      req.DeviceId,
		AmountMsat:    req.RequestMsat,
		Reason:        "AUTHORIZATION_HOLD",
		AllowNegative: false,  // We already checked balance above
		CorrelationID: authID, // Use authorization_id as correlation_id
	}
	entry, err := s.repo.ApplyDebit(ctx, tx, debitReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create debit entry: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit: %v", err)
	}

	// Emit DeviceDebited event for the authorization hold
	if s.streamHandler != nil {
		timestamp := time.Unix(entry.CreatedAt, 0).UTC().Format(time.RFC3339)
		if err := s.streamHandler.PublishDeviceDebited(ctx, req.DeviceId, authID, entry.AmountMsat, entry.BalanceAfter, timestamp); err != nil {
			logger.WithDeviceID(req.DeviceId).
				WithStream("event.ledger", "produce").
				Errorf(ctx, "Failed to publish DeviceDebitedEvent for authorization %s via eastwest gRPC: %v", authID, err)
		}
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
