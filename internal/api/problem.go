package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
)

// problem is an RFC 9457 problem details body. Code is an extension member
// with a stable, machine-readable reason.
type problem struct {
	Type     string   `json:"type"`
	Title    string   `json:"title"`
	Status   int      `json:"status"`
	Detail   string   `json:"detail,omitempty"`
	Instance string   `json:"instance,omitempty"`
	Code     string   `json:"code"`
	Errors   []string `json:"errors,omitempty"`
}

// apiError is an error a handler returns to the client as a problem.
type apiError struct {
	status int
	code   string
	detail string
	errs   []string
}

func (e *apiError) Error() string { return fmt.Sprintf("%d %s: %s", e.status, e.code, e.detail) }

func errBadRequest(detail string, errs ...string) *apiError {
	return &apiError{status: http.StatusBadRequest, code: "invalid_request", detail: detail, errs: errs}
}

func errNotFound(what string) *apiError {
	return &apiError{status: http.StatusNotFound, code: "not_found", detail: what + " not found"}
}

func errConflict(detail string) *apiError {
	return &apiError{status: http.StatusConflict, code: "conflict", detail: detail}
}

var (
	errUnauthorized = &apiError{status: http.StatusUnauthorized, code: "unauthorized", detail: "a valid bearer token is required"}
	errForbidden    = &apiError{status: http.StatusForbidden, code: "forbidden", detail: "the token does not allow this action"}
)

// writeProblem sends p with the request ID as the instance. Internal error
// details never reach the client; they are logged with the same request ID.
func writeProblem(w http.ResponseWriter, r *http.Request, e *apiError) {
	p := problem{
		Type:     "about:blank",
		Title:    http.StatusText(e.status),
		Status:   e.status,
		Detail:   e.detail,
		Instance: "urn:fleetwire:request:" + requestID(r.Context()),
		Code:     e.code,
		Errors:   e.errs,
	}
	if e.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="fleetwire"`)
	}
	writeBody(w, e.status, "application/problem+json", p)
}

// writeBody encodes v before sending any header, so an encoding failure
// becomes a clean 500 instead of a half-written 200.
func writeBody(w http.ResponseWriter, status int, contentType string, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status, contentType = http.StatusInternalServerError, "application/problem+json"
		body = []byte(`{"type":"about:blank","title":"Internal Server Error","status":500,"code":"internal"}`)
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	// A failed write means the client is gone; there is no one to tell.
	_, _ = w.Write(append(body, '\n'))
}

func internalError() *apiError {
	return &apiError{status: http.StatusInternalServerError, code: "internal", detail: "internal error; see the server log for this request ID"}
}

// maxBody bounds every request body.
const maxBody = 1 << 20

// decodeJSON reads exactly one JSON object with no unknown fields.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) *apiError {
	return decodeBody(w, r, v, false)
}

// decodeOptionalJSON is decodeJSON for a body the client may leave out.
// An empty body counts as none even without Content-Length: 0, since
// chunked HTTP/1.1 and HTTP/2 requests arrive with ContentLength -1.
func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, v any) *apiError {
	return decodeBody(w, r, v, true)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any, optional bool) *apiError {
	if optional && r.ContentLength == 0 {
		return nil
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			return &apiError{status: http.StatusUnsupportedMediaType, code: "unsupported_media_type", detail: "send application/json"}
		}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		// Decode returns io.EOF only when the body holds no JSON at all.
		if optional && errors.Is(err, io.EOF) {
			return nil
		}
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return &apiError{status: http.StatusRequestEntityTooLarge, code: "too_large", detail: "request body exceeds 1 MiB"}
		}
		return errBadRequest("malformed JSON body: " + err.Error())
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errBadRequest("body must contain a single JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	writeBody(w, status, "application/json", v)
}
