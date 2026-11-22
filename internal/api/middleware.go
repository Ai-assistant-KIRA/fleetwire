package api

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

type ctxKey int

const (
	keyRequestID ctxKey = iota
	keyReqInfo
)

// reqInfo carries what inner handlers learn (who called) back out to the
// access log.
type reqInfo struct {
	actor string
}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{8,64}$`)

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(keyRequestID).(string)
	return id
}

func setActor(ctx context.Context, actor string) {
	if ri, ok := ctx.Value(keyReqInfo).(*reqInfo); ok {
		ri.actor = actor
	}
}

// recorder captures the status for logs and metrics. It also turns the
// plain-text 404 and 405 replies of http.ServeMux (no route matched, so
// r.Pattern is empty) into problem details, so every error the API sends
// is RFC 9457 (R8.6). Handlers' own 404s have a pattern and pass through.
type recorder struct {
	http.ResponseWriter
	req     *http.Request
	status  int
	bytes   int64
	swallow bool
}

func (r *recorder) WriteHeader(code int) {
	if r.status != 0 {
		return
	}
	r.status = code
	if (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) && r.req.Pattern == "" {
		r.swallow = true
		detail, reason := "no such endpoint", "not_found"
		if code == http.StatusMethodNotAllowed {
			detail, reason = "method not allowed; see the Allow header", "method_not_allowed"
		}
		writeBody(r.ResponseWriter, code, "application/problem+json", problem{Type: "about:blank",
			Title: http.StatusText(code), Status: code, Detail: detail,
			Instance: "urn:fleetwire:request:" + requestID(r.req.Context()), Code: reason})
		return
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	if r.swallow {
		return len(b), nil
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach Flush and SetWriteDeadline.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

type httpMetrics struct {
	requests       *prometheus.CounterVec
	seconds        *prometheus.HistogramVec
	enrollRefusals *prometheus.CounterVec
}

func newHTTPMetrics(reg prometheus.Registerer) *httpMetrics {
	m := &httpMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetwire_api_requests_total", Help: "API requests by route pattern and status code.",
		}, []string{"listener", "route", "code"}),
		seconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "fleetwire_api_request_seconds", Help: "API request duration by route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"listener", "route"}),
		// The audit log keeps a sample of refusals; this counts all of them.
		enrollRefusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetwire_api_enroll_refusals_total", Help: "Refused enrollments by outcome (unknown, expired, used, tenant_inactive).",
		}, []string{"outcome"}),
	}
	reg.MustRegister(m.requests, m.seconds, m.enrollRefusals)
	return m
}

// wrap applies, outermost first: request ID, panic recovery, access log and
// metrics. Authentication and rate limiting are per route (see auth.go).
func wrap(listener string, next http.Handler, log *slog.Logger, m *httpMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-ID")
		if !validRequestID.MatchString(id) {
			id = uuid.Must(uuid.NewV7()).String()
		}
		info := &reqInfo{}
		ctx := context.WithValue(context.WithValue(r.Context(), keyRequestID, id), keyReqInfo, info)
		r = r.WithContext(ctx)
		h := w.Header()
		h.Set("X-Request-ID", id)
		// Responses carry tenant data and tokens: no MIME sniffing, and no
		// copies in caches. Handlers may relax Cache-Control (the CRL does).
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		rec := &recorder{ResponseWriter: w, req: r}

		defer func() {
			if v := recover(); v != nil {
				log.Error("panic in handler", "request_id", id, "panic", v, "stack", string(debug.Stack()))
				if rec.status == 0 {
					writeProblem(rec, r, internalError())
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			d := time.Since(start)
			m.requests.WithLabelValues(listener, route, strconv.Itoa(status)).Inc()
			m.seconds.WithLabelValues(listener, route).Observe(d.Seconds())
			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelError
			}
			log.LogAttrs(ctx, level, "http request",
				slog.String("request_id", id), slog.String("listener", listener),
				slog.String("method", r.Method), slog.String("route", route),
				slog.Int("status", status), slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", d), slog.String("remote", r.RemoteAddr),
				slog.String("actor", info.actor))
		}()
		next.ServeHTTP(rec, r)
	})
}
