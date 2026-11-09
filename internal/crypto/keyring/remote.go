package keyring

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// The remote backend lets ingest run apart from the API (separate pods in
// Kubernetes) while the KEKs stay in one place. The API serves the keyring
// over mutual TLS on its internal listener and ingest calls it, the way a
// service calls a cloud KMS: only data keys cross the wire, never KEKs, and
// creating or destroying a tenant's KEK stays with the API.

// Errors of the remote backend.
var (
	// ErrUnavailable wraps failures that may pass: the API is unreachable,
	// restarting or overloaded. Callers retry instead of giving up on data.
	ErrUnavailable = errors.New("keyring: remote keyring unavailable")
	// ErrRemoteOnly is returned by Create and Destroy on the remote client.
	ErrRemoteOnly = errors.New("keyring: the remote keyring only wraps and unwraps; the API creates and destroys tenant keys")
)

const (
	wrapPath   = "/internal/v1/keyring/wrap"
	unwrapPath = "/internal/v1/keyring/unwrap"
	// maxBody bounds requests and answers; real ones are under 200 bytes.
	maxBody = 4 << 10
)

type wrapRequest struct {
	TenantID uuid.UUID `json:"tenant_id"`
	Key      []byte    `json:"key"`
}

type wrapResponse struct {
	Wrapped []byte `json:"wrapped"`
	KEKID   string `json:"kek_id"`
}

type unwrapRequest struct {
	TenantID uuid.UUID `json:"tenant_id"`
	KEKID    string    `json:"kek_id"`
	Wrapped  []byte    `json:"wrapped"`
}

type unwrapResponse struct {
	Key []byte `json:"key"`
}

type errorResponse struct {
	Code string `json:"code"`
}

// Error codes on the wire, mapped back to the package errors.
const (
	codeNotFound = "not_found"
	codeWrongKEK = "wrong_kek"
	codeBad      = "bad_request"
	codeInternal = "internal"
)

// Remote is the client side. It is safe for concurrent use.
type Remote struct {
	base   string
	client *http.Client
}

// NewRemote returns a client for the keyring at baseURL (https://host:port).
// tlsCfg must carry the client certificate and the CA that signed the API's
// certificate.
func NewRemote(baseURL string, tlsCfg *tls.Config) (*Remote, error) {
	if !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("keyring URL %q: must start with https://", baseURL)
	}
	if tlsCfg == nil || len(tlsCfg.Certificates) == 0 {
		return nil, errors.New("keyring: the remote keyring needs a client certificate")
	}
	return &Remote{
		base: strings.TrimSuffix(baseURL, "/"),
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute},
		},
	}, nil
}

// Create implements Keyring; only the API creates KEKs.
func (r *Remote) Create(context.Context, uuid.UUID) error { return ErrRemoteOnly }

// Destroy implements Keyring; only the API destroys KEKs.
func (r *Remote) Destroy(context.Context, uuid.UUID) error { return ErrRemoteOnly }

// Wrap implements Keyring.
func (r *Remote) Wrap(ctx context.Context, tenantID uuid.UUID, dek []byte) ([]byte, string, error) {
	var out wrapResponse
	if err := r.call(ctx, wrapPath, wrapRequest{TenantID: tenantID, Key: dek}, &out); err != nil {
		return nil, "", err
	}
	return out.Wrapped, out.KEKID, nil
}

// Unwrap implements Keyring.
func (r *Remote) Unwrap(ctx context.Context, tenantID uuid.UUID, kekID string, wrapped []byte) ([]byte, error) {
	var out unwrapResponse
	if err := r.call(ctx, unwrapPath, unwrapRequest{TenantID: tenantID, KEKID: kekID, Wrapped: wrapped}, &out); err != nil {
		return nil, err
	}
	return out.Key, nil
}

func (r *Remote) call(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("keyring: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+path, bytes.NewReader(body)) //nolint:gosec // G704: the configured keyring address plus a fixed path
	if err != nil {
		return fmt.Errorf("keyring: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req) //nolint:gosec // G704: the configured keyring address plus a fixed path
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("%w: read answer: %w", ErrUnavailable, err)
	}
	if resp.StatusCode == http.StatusOK {
		// A cut-off answer comes from an API going away; the next try works.
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("%w: decode answer: %w", ErrUnavailable, err)
		}
		return nil
	}
	var e errorResponse
	_ = json.Unmarshal(data, &e) // a proxy's error page has no code; the status decides then
	switch {
	case e.Code == codeNotFound:
		return ErrNotFound
	case e.Code == codeWrongKEK:
		return ErrWrongKEK
	case resp.StatusCode >= 500, resp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: %s", ErrUnavailable, resp.Status)
	default:
		return fmt.Errorf("keyring: %s %s (%s)", path, resp.Status, e.Code)
	}
}

// NewServer returns the handler the API serves on its internal listener.
// The listener verifies client certificates against the service CA; the
// handler then admits only the common names in allowed.
func NewServer(kr Keyring, log *slog.Logger, allowed ...string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+wrapPath, func(w http.ResponseWriter, r *http.Request) {
		var in wrapRequest
		if !decode(w, r, &in) {
			return
		}
		wrapped, kekID, err := kr.Wrap(r.Context(), in.TenantID, in.Key)
		if err != nil {
			fail(w, log, "wrap", in.TenantID, err)
			return
		}
		reply(w, wrapResponse{Wrapped: wrapped, KEKID: kekID})
	})
	mux.HandleFunc("POST "+unwrapPath, func(w http.ResponseWriter, r *http.Request) {
		var in unwrapRequest
		if !decode(w, r, &in) {
			return
		}
		key, err := kr.Unwrap(r.Context(), in.TenantID, in.KEKID, in.Wrapped)
		if err != nil {
			fail(w, log, "unwrap", in.TenantID, err)
			return
		}
		reply(w, unwrapResponse{Key: key})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			writeCode(w, http.StatusUnauthorized, "client_certificate_required")
			return
		}
		if cn := r.TLS.VerifiedChains[0][0].Subject.CommonName; !slices.Contains(allowed, cn) {
			log.Warn("keyring request refused", "client_cn", cn, "path", r.URL.Path)
			writeCode(w, http.StatusForbidden, "forbidden")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeCode(w, http.StatusBadRequest, codeBad)
		return false
	}
	return true
}

func fail(w http.ResponseWriter, log *slog.Logger, op string, tenant uuid.UUID, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeCode(w, http.StatusNotFound, codeNotFound)
	case errors.Is(err, ErrWrongKEK):
		writeCode(w, http.StatusConflict, codeWrongKEK)
	default:
		log.Error("keyring operation failed", "op", op, "tenant_id", tenant, "err", err)
		writeCode(w, http.StatusInternalServerError, codeInternal)
	}
}

func reply(w http.ResponseWriter, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeCode(w, http.StatusInternalServerError, codeInternal)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	// A failed write means the client is gone; it sees an unavailable
	// keyring and retries.
	_, _ = w.Write(body)
}

func writeCode(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The code is one of the constants above, so the body is fixed JSON.
	_, _ = w.Write([]byte(`{"code":"` + code + `"}`))
}
