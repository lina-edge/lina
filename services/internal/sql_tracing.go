package internal

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// SQLExecutor is an interface that both *sql.DB and *sql.Tx implement
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// SQLTracer provides methods for tracing SQL operations
type SQLTracer struct {
	tracer     trace.Tracer
	logger     *Logger
	tracerName string
}

// NewSQLTracer creates a new SQL tracer with the given tracer name
func NewSQLTracer(tracerName string) *SQLTracer {
	return &SQLTracer{
		tracer:     otel.Tracer(tracerName),
		logger:     NewLogger(sqlTracerServiceName(tracerName)),
		tracerName: tracerName,
	}
}

func sqlTracerServiceName(tracerName string) string {
	if serviceName, ok := strings.CutPrefix(tracerName, "repository."); ok && serviceName != "" {
		return serviceName
	}
	return tracerName
}

func sqlErrorKind(err error) string {
	if err == nil {
		return ""
	}
	errText := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errText, "database is locked"),
		strings.Contains(errText, "database table is locked"),
		strings.Contains(errText, "database schema is locked"),
		strings.Contains(errText, "sqlite_busy"),
		strings.Contains(errText, "sqlite_locked"),
		strings.Contains(errText, "busy timeout"):
		return "sqlite_lock"
	case strings.Contains(errText, "sqlite"):
		return "sqlite"
	default:
		return "sql"
	}
}

func (st *SQLTracer) logSQLError(ctx context.Context, spanName string, attrs []attribute.KeyValue, err error) {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return
	}
	fields := map[string]interface{}{
		"component":      st.tracerName,
		"sql_span":       spanName,
		"sql_error_kind": sqlErrorKind(err),
	}
	for _, attr := range attrs {
		fields[string(attr.Key)] = attr.Value.AsInterface()
	}
	st.logger.ErrorWithFields(ctx, "SQL operation failed", err, fields)
}

// LogSQLError records a SQL error at ERROR level for database work that cannot
// be wrapped by ExecWithSpan, QueryWithSpan, or QueryRowWithSpan.
func (st *SQLTracer) LogSQLError(ctx context.Context, spanName string, attrs []attribute.KeyValue, err error) {
	st.logSQLError(ctx, spanName, attrs, err)
}

// ExecWithSpan executes a SQL command with automatic tracing
func (st *SQLTracer) ExecWithSpan(ctx context.Context, spanName string, attrs []attribute.KeyValue, exec SQLExecutor, query string, args ...interface{}) (sql.Result, error) {
	ctx, span := st.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	defer span.End()

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		st.logSQLError(ctx, spanName, attrs, err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return result, nil
}

// ExecStmtWithSpan executes a prepared statement with automatic tracing
func (st *SQLTracer) ExecStmtWithSpan(ctx context.Context, spanName string, attrs []attribute.KeyValue, stmt *sql.Stmt, args ...interface{}) (sql.Result, error) {
	ctx, span := st.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	defer span.End()

	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		st.logSQLError(ctx, spanName, attrs, err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return result, nil
}

// QueryWithSpan executes a SQL query with automatic tracing
func (st *SQLTracer) QueryWithSpan(ctx context.Context, spanName string, attrs []attribute.KeyValue, exec SQLExecutor, query string, args ...interface{}) (*sql.Rows, error) {
	ctx, span := st.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	defer span.End()

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		st.logSQLError(ctx, spanName, attrs, err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "success")
	return rows, nil
}

// QueryRowResult wraps a sql.Row with span handling
type QueryRowResult struct {
	row      *sql.Row
	span     trace.Span
	ctx      context.Context
	tracer   *SQLTracer
	spanName string
	attrs    []attribute.KeyValue
}

// Scan scans the row and updates the span based on the result
// sql.ErrNoRows is treated as a successful "not found" case, not an error
func (qr *QueryRowResult) Scan(dest ...interface{}) error {
	err := qr.row.Scan(dest...)
	if err == sql.ErrNoRows || errors.Is(err, sql.ErrNoRows) {
		qr.span.SetStatus(codes.Ok, "no rows found")
	} else if err != nil {
		qr.span.RecordError(err)
		qr.span.SetStatus(codes.Error, err.Error())
		qr.tracer.logSQLError(qr.ctx, qr.spanName, qr.attrs, err)
	} else {
		qr.span.SetStatus(codes.Ok, "success")
	}
	qr.span.End()
	return err
}

// QueryRowWithSpan executes a SQL query that returns a single row with automatic tracing
func (st *SQLTracer) QueryRowWithSpan(ctx context.Context, spanName string, attrs []attribute.KeyValue, exec SQLExecutor, query string, args ...interface{}) *QueryRowResult {
	ctx, span := st.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	row := exec.QueryRowContext(ctx, query, args...)
	return &QueryRowResult{row: row, span: span, ctx: ctx, tracer: st, spanName: spanName, attrs: attrs}
}
