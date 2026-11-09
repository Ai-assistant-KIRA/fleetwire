package store

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes the services react to.
const (
	codeUniqueViolation    = "23505"
	codeExclusionViolation = "23P01"
	codeInsufficientPriv   = "42501"
)

// IsTransient reports whether err is worth retrying with the same input:
// the database is unreachable, restarting, out of connections, or the
// transaction lost a serialization race. Everything else is treated as a bug
// or bad input, and retrying it would loop forever.
func IsTransient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"): // connection exception
			return true
		case pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03": // shutdown, crash, cannot connect now
			return true
		case pgErr.Code == "40001", pgErr.Code == "40P01": // serialization failure, deadlock
			return true
		case pgErr.Code == "53300": // too many connections
			return true
		}
		return false
	}
	var connErr *pgconn.ConnectError
	var netErr net.Error
	switch {
	case errors.As(err, &connErr), errors.As(err, &netErr):
		return true
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case pgconn.Timeout(err), pgconn.SafeToRetry(err):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		return true
	}
	// pgx reports a connection that died under it as "conn closed".
	return strings.Contains(err.Error(), "conn closed") || strings.Contains(err.Error(), "closed pool")
}

func hasCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// IsUniqueViolation reports a unique constraint violation.
func IsUniqueViolation(err error) bool { return hasCode(err, codeUniqueViolation) }

// IsExclusionViolation reports a violated exclusion constraint. For chunks,
// that means the application tried to store a seq twice: a bug signal.
func IsExclusionViolation(err error) bool { return hasCode(err, codeExclusionViolation) }

// IsPermissionDenied reports a privilege or row-level security violation.
func IsPermissionDenied(err error) bool { return hasCode(err, codeInsufficientPriv) }
