package store

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// AppRole is the runtime role the services connect as. The migrations
// grant it exactly what the services use and write the row-level security
// policies for it.
const AppRole = "fleetwire_app"

// EnsureLoginRole creates a login role, or resets its password, over the
// owner connection; fleetwire-api migrate calls it for AppRole. Managed
// PostgreSQL such as Charmed PostgreSQL hands the deployment one relation
// user, with no superuser and no init script, so the deployment's migrate
// step creates the runtime role instead. Run it before Migrate, whose
// policies name the role.
//
// The password travels as a SCRAM-SHA-256 verifier, never in clear, so it
// cannot end up in the server's statement log.
func EnsureLoginRole(ctx context.Context, ownerURL, role, password string) error {
	if err := checkAppPassword(password); err != nil {
		return err
	}
	verifier, err := scramVerifier(password)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return fmt.Errorf("connect as owner: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// Charmed PostgreSQL switches a relation user to the database owner role
	// at login. That role cannot create roles; the login role can.
	if _, err := conn.Exec(ctx, "SET ROLE NONE"); err != nil {
		return fmt.Errorf("reset role: %w", err)
	}
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
		return fmt.Errorf("look up role %s: %w", role, err)
	}
	// Role statements take no bind parameters. The verifier is base64 and
	// '$', ':' only, so quoting it as a literal is safe. Unlisted attributes
	// keep their defaults (no superuser, CREATEDB, CREATEROLE or BYPASSRLS);
	// the services refuse to start if the role has any of them anyway.
	verb := "ALTER"
	if !exists {
		verb = "CREATE"
	}
	ident := pgx.Identifier{role}.Sanitize()
	if _, err := conn.Exec(ctx, fmt.Sprintf("%s ROLE %s LOGIN NOINHERIT PASSWORD '%s'", verb, ident, verifier)); err != nil {
		return fmt.Errorf("%s role %s: %w", strings.ToLower(verb), role, err)
	}
	var db string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		return fmt.Errorf("current database: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pgx.Identifier{db}.Sanitize(), ident)); err != nil {
		return fmt.Errorf("grant connect to %s: %w", role, err)
	}
	return nil
}

// checkAppPassword keeps passwords to printable ASCII, where SASLprep (the
// normalization SCRAM applies) changes nothing, so the verifier computed
// here matches what the server checks at login.
func checkAppPassword(p string) error {
	if len(p) < 16 {
		return errors.New("the role password must be at least 16 characters")
	}
	for i := range len(p) {
		if p[i] < 0x21 || p[i] > 0x7e {
			return errors.New("the role password must be printable ASCII without spaces")
		}
	}
	return nil
}

// scramVerifier builds the SCRAM-SHA-256 secret PostgreSQL stores for a
// password (RFC 5802, RFC 7677), in PostgreSQL's text form.
func scramVerifier(password string) (string, error) {
	const iterations = 4096
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("scram salt: %w", err)
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return "", fmt.Errorf("scram key: %w", err)
	}
	mac := func(key []byte, msg string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(msg))
		return h.Sum(nil)
	}
	stored := sha256.Sum256(mac(salted, "Client Key"))
	server := mac(salted, "Server Key")
	b64 := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iterations, b64(salt), b64(stored[:]), b64(server)), nil
}
