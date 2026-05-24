package internal

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

type failingSQLExecutor struct {
	err error
}

func (f failingSQLExecutor) ExecContext(context.Context, string, ...interface{}) (sql.Result, error) {
	return nil, f.err
}

func (f failingSQLExecutor) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, f.err
}

func (f failingSQLExecutor) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return nil
}

func TestSQLTracerLogsExecErrorsAtErrorLevel(t *testing.T) {
	errSQLiteLocked := errors.New("SQLITE_BUSY: database is locked")
	output := captureStdout(t, func() {
		tracer := NewSQLTracer("repository.device")
		_, err := tracer.ExecWithSpan(
			context.Background(),
			"[repository] create device",
			[]attribute.KeyValue{
				attribute.String("db.operation", "INSERT"),
				attribute.String("db.table", "devices"),
			},
			failingSQLExecutor{err: errSQLiteLocked},
			"INSERT INTO devices(device_id) VALUES (?)",
			"device-1",
		)
		if !errors.Is(err, errSQLiteLocked) {
			t.Fatalf("ExecWithSpan() error = %v, want %v", err, errSQLiteLocked)
		}
	})

	for _, want := range []string{
		"level=error",
		"service=device",
		"message=\"SQL operation failed\"",
		"error=\"SQLITE_BUSY: database is locked\"",
		"component=repository.device",
		"sql_span=\"[repository] create device\"",
		"sql_error_kind=sqlite_lock",
		"db.operation=INSERT",
		"db.table=devices",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected log output to contain %q, got %q", want, output)
		}
	}
}

func TestSQLTracerDoesNotLogNoRows(t *testing.T) {
	output := captureStdout(t, func() {
		tracer := NewSQLTracer("repository.ledger")
		tracer.logSQLError(context.Background(), "[repository] get balance", nil, sql.ErrNoRows)
	})

	if output != "" {
		t.Fatalf("expected no log output for sql.ErrNoRows, got %q", output)
	}
}

func TestSQLErrorKind(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "sqlite locked", err: errors.New("sqlite: database is locked"), want: "sqlite_lock"},
		{name: "sqlite busy", err: errors.New("SQLITE_BUSY"), want: "sqlite_lock"},
		{name: "sqlite generic", err: errors.New("sqlite: constraint failed"), want: "sqlite"},
		{name: "sql generic", err: errors.New("driver: bad connection"), want: "sql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlErrorKind(tt.err); got != tt.want {
				t.Fatalf("sqlErrorKind(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(): %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
		_ = reader.Close()
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return string(output)
}
