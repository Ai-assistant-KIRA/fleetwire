// Package api is fleetwire's HTTP API: the tenant and operator API on the
// public listener, and enrollment and renewal on the device listener.
//
// Routing uses the standard library's method patterns (ADR-0011). Every
// error is an RFC 9457 problem; every list is paged with signed cursors.
package api

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/lru"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

// Config is the API's own settings.
type Config struct {
	PublicAddr string
	DeviceAddr string
	PublicTLS  *tls.Config
	// DeviceTLS must request (not require) client certificates: enrollment
	// has none yet, renewal needs one.
	DeviceTLS *tls.Config
	// CABundlePEM is what devices trust for the broker and the API.
	CABundlePEM  []byte
	BrokerURL    string
	DeviceAPIURL string

	RatePerSecond float64
	RateBurst     int
	// AuthFailPerSecond and AuthFailBurst bound failed token lookups per
	// client network. A network over the budget is refused before its
	// tokens reach the database.
	AuthFailPerSecond float64
	AuthFailBurst     int
	// EnrollPerSecond and EnrollBurst limit enrollment and CRL requests
	// per client network.
	EnrollPerSecond float64
	EnrollBurst     int
	EnrollTokenTTL  time.Duration
	ExportDir       string
	// MaxActiveExports caps a tenant's pending and running exports.
	MaxActiveExports int
}

// Deps are the API's collaborators.
type Deps struct {
	DB        *store.DB
	Keyring   keyring.Keyring
	Keys      *tenantkey.Manager
	DeviceCA  *pki.CA
	CRL       *crl.Publisher
	CursorKey []byte
	Log       *slog.Logger
	Registry  prometheus.Registerer
}

// Server serves the API.
type Server struct {
	cfg         Config
	db          *store.DB
	kr          keyring.Keyring
	keys        *tenantkey.Manager
	ca          *pki.CA
	crl         *crl.Publisher
	cur         cursors
	log         *slog.Logger
	metrics     *httpMetrics
	limiter     *limiter
	authFailLim *limiter
	enrollLim   *limiter
	// refusals and fleetRefusals bound audit entries for refused
	// enrollments (see auditRefusal).
	refusals      *lru.Cache[string, time.Time]
	fleetRefusals *rate.Limiter
	lastTouch     *lru.Cache[uuid.UUID, time.Time]
	touches       chan *principal
	now           func() time.Time
}

// New builds a server.
func New(cfg Config, d Deps) *Server {
	if cfg.RatePerSecond <= 0 {
		cfg.RatePerSecond, cfg.RateBurst = 20, 40
	}
	if cfg.AuthFailPerSecond <= 0 {
		cfg.AuthFailPerSecond, cfg.AuthFailBurst = 5, 50
	}
	if cfg.EnrollPerSecond <= 0 {
		cfg.EnrollPerSecond, cfg.EnrollBurst = 50, 200
	}
	// A bucket with no room refuses every request, whatever the rate.
	cfg.RateBurst, cfg.AuthFailBurst, cfg.EnrollBurst = max(cfg.RateBurst, 1), max(cfg.AuthFailBurst, 1), max(cfg.EnrollBurst, 1)
	if cfg.EnrollTokenTTL <= 0 {
		cfg.EnrollTokenTTL = 15 * time.Minute
	}
	if cfg.MaxActiveExports <= 0 {
		cfg.MaxActiveExports = 4
	}
	return &Server{
		cfg:           cfg,
		db:            d.DB,
		kr:            d.Keyring,
		keys:          d.Keys,
		ca:            d.DeviceCA,
		crl:           d.CRL,
		cur:           cursors{key: d.CursorKey},
		log:           d.Log,
		metrics:       newHTTPMetrics(d.Registry),
		limiter:       newLimiter(cfg.RatePerSecond, cfg.RateBurst),
		authFailLim:   newLimiter(cfg.AuthFailPerSecond, cfg.AuthFailBurst),
		enrollLim:     newLimiter(cfg.EnrollPerSecond, cfg.EnrollBurst),
		refusals:      lru.New[string, time.Time](100_000, 0),
		fleetRefusals: rate.NewLimiter(rate.Every(fleetRefusalEvery), fleetRefusalBurst),
		lastTouch:     lru.New[uuid.UUID, time.Time](100_000, 0),
		touches:       make(chan *principal, 1024),
		now:           time.Now,
	}
}

// route registers h with a span named after the pattern.
func route(mux *http.ServeMux, pattern string, h http.Handler) {
	mux.Handle(pattern, otelhttp.NewHandler(h, pattern))
}

// PublicHandler serves the tenant and operator API.
func (s *Server) PublicHandler() http.Handler {
	mux := http.NewServeMux()

	// Operator API (fleet-wide).
	route(mux, "POST /v1/admin/tenants", s.operatorRoute(s.createTenant))
	route(mux, "GET /v1/admin/tenants", s.operatorRoute(s.listTenants))
	route(mux, "DELETE /v1/admin/tenants/{id}", s.operatorRoute(s.destroyTenant))
	route(mux, "POST /v1/admin/tenants/{id}/tokens", s.operatorRoute(s.adminCreateToken))
	route(mux, "POST /v1/admin/tenants/{id}/enrollment-tokens", s.operatorRoute(s.adminEnrollmentTokens))
	route(mux, "POST /v1/admin/devices/{id}/revoke", s.operatorRoute(s.adminRevokeDevice))
	route(mux, "GET /v1/admin/audit", s.operatorRoute(s.adminAudit))

	// Tenant API.
	route(mux, "GET /v1/tenant", s.tenantRoute(tokens.ScopeTenantAdmin, s.getTenant))
	route(mux, "PATCH /v1/tenant", s.tenantRoute(tokens.ScopeTenantAdmin, s.patchTenant))
	route(mux, "POST /v1/tokens", s.tenantRoute(tokens.ScopeTenantAdmin, s.createToken))
	route(mux, "GET /v1/tokens", s.tenantRoute(tokens.ScopeTenantAdmin, s.listTokens))
	route(mux, "GET /v1/tokens/{id}", s.tenantRoute(tokens.ScopeTenantAdmin, s.getToken))
	route(mux, "DELETE /v1/tokens/{id}", s.tenantRoute(tokens.ScopeTenantAdmin, s.revokeToken))
	route(mux, "POST /v1/enrollment-tokens", s.tenantRoute(tokens.ScopeDevicesAdmin, s.createEnrollmentTokens))
	route(mux, "GET /v1/devices", s.tenantRoute(tokens.ScopeDevicesRead, s.listDevices))
	route(mux, "GET /v1/devices/{id}", s.tenantRoute(tokens.ScopeDevicesRead, s.getDevice))
	route(mux, "POST /v1/devices/{id}/revoke", s.tenantRoute(tokens.ScopeDevicesAdmin, s.tenantRevokeDevice))
	route(mux, "DELETE /v1/devices/{id}", s.tenantRoute(tokens.ScopeDevicesAdmin, s.deleteDevice))
	route(mux, "GET /v1/devices/{id}/readings", s.tenantRoute(tokens.ScopeReadingsRead, s.listReadings))
	route(mux, "GET /v1/devices/{id}/gaps", s.tenantRoute(tokens.ScopeReadingsRead, s.listGaps))
	route(mux, "POST /v1/exports", s.tenantRoute(tokens.ScopeExportsWrite, s.createExport))
	route(mux, "GET /v1/exports/{id}", s.tenantRoute(tokens.ScopeExportsRead, s.getExport))
	route(mux, "GET /v1/exports/{id}/download", s.tenantRoute(tokens.ScopeExportsRead, s.downloadExport))
	route(mux, "GET /v1/audit", s.tenantRoute(tokens.ScopeAuditRead, s.listAudit))
	route(mux, "GET /v1/jobs/{id}", s.tenantRoute(scopeByJobKind, s.getJob))

	return wrap("public", mux, s.log, s.metrics)
}

// DeviceHandler serves enrollment and renewal.
func (s *Server) DeviceHandler() http.Handler {
	mux := http.NewServeMux()
	route(mux, "POST /v1/enroll", http.HandlerFunc(s.enroll))
	route(mux, "POST /v1/renew", http.HandlerFunc(s.renew))
	route(mux, "GET /v1/crl", http.HandlerFunc(s.getCRL))
	return wrap("device", mux, s.log, s.metrics)
}

func (s *Server) httpServer(h http.Handler, tlsCfg *tls.Config) *http.Server {
	return &http.Server{
		Handler:           h,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Export downloads extend their own write deadline.
		WriteTimeout:   60 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 64 << 10,
		ErrorLog:       slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
}

// Serve runs both listeners until ctx ends, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context) error {
	var lc net.ListenConfig
	pubLn, err := lc.Listen(ctx, "tcp", s.cfg.PublicAddr)
	if err != nil {
		return fmt.Errorf("public listener: %w", err)
	}
	devLn, err := lc.Listen(ctx, "tcp", s.cfg.DeviceAddr)
	if err != nil {
		_ = pubLn.Close()
		return fmt.Errorf("device listener: %w", err)
	}
	pub := s.httpServer(s.PublicHandler(), s.cfg.PublicTLS)
	dev := s.httpServer(s.DeviceHandler(), s.cfg.DeviceTLS)
	s.log.Info("api listening", "public", pubLn.Addr().String(), "device", devLn.Addr().String())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { s.runTouches(gctx); return nil })
	for _, p := range []struct {
		srv *http.Server
		ln  net.Listener
	}{{pub, pubLn}, {dev, devLn}} {
		g.Go(func() error {
			if err := p.srv.ServeTLS(p.ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve %s: %w", p.ln.Addr(), err)
			}
			return nil
		})
	}
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 30*time.Second)
		defer cancel()
		return errors.Join(pub.Shutdown(sctx), dev.Shutdown(sctx))
	})
	return g.Wait()
}
