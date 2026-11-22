package main

import (
	"errors"
	"net/url"
	"time"
)

// maxCRLInterval is the daily republish that ADR-0010 and threat-model D4
// rely on. Publisher.Run waits the interval after each publish finishes, so
// a longer interval would stretch the gap past a day.
const maxCRLInterval = 24 * time.Hour

// apiConfig is every setting of fleetwire-api. `fleetwire-api config-docs`
// renders it; `make docs-reference` puts it in docs/reference/configuration.md.
type apiConfig struct {
	cmd string

	DatabaseURL      string `env:"FLEETWIRE_DATABASE_URL" flag:"database-url" secret:"true" help:"PostgreSQL URL for the service login: the runtime role, or a member of it, never the schema owner."`
	OwnerDatabaseURL string `env:"FLEETWIRE_DATABASE_OWNER_URL" flag:"database-owner-url" secret:"true" help:"PostgreSQL URL for the schema owner. Used only by migrate and create-operator-token."`
	RuntimeRole      string `env:"FLEETWIRE_DATABASE_RUNTIME_ROLE" flag:"database-runtime-role" default:"fleetwire_app" help:"Role that migrate gives the row-level security policies and grants: the services' login role, or a group it belongs to (charmed_dml on Charmed PostgreSQL)."`
	DBMaxConns       int32  `env:"FLEETWIRE_DATABASE_MAX_CONNS" flag:"database-max-conns" default:"20" help:"Connection pool size."`

	PublicListen string `env:"FLEETWIRE_API_PUBLIC_LISTEN" flag:"public-listen" default:":8443" help:"Tenant and operator API listener (TLS)."`
	DeviceListen string `env:"FLEETWIRE_API_DEVICE_LISTEN" flag:"device-listen" default:":8444" help:"Enrollment and renewal listener (TLS, client certificates requested)."`
	AdminListen  string `env:"FLEETWIRE_API_ADMIN_LISTEN" flag:"admin-listen" default:"127.0.0.1:9090" help:"Health and metrics listener (plain HTTP; keep it private)."`
	// The keyring listener exists for deployments where ingest cannot share
	// the keyring directory (separate pods): ingest wraps and unwraps data
	// keys through it, and the KEKs never leave the API.
	InternalListen   string `env:"FLEETWIRE_API_INTERNAL_LISTEN" flag:"internal-listen" help:"Keyring listener for ingest, mutual TLS with service certificates, for example :8445. Empty disables it."`
	ServiceTrustFile string `env:"FLEETWIRE_SERVICE_TRUST_FILE" flag:"service-trust" help:"Service CA chain that verifies client certificates on the keyring listener."`
	TLSCertFile      string `env:"FLEETWIRE_API_TLS_CERT_FILE" flag:"tls-cert" help:"Server certificate chain (PEM)."`
	TLSKeyFile       string `env:"FLEETWIRE_API_TLS_KEY_FILE" flag:"tls-key" help:"Server private key (PEM)."`

	DeviceTrustFile  string `env:"FLEETWIRE_DEVICE_TRUST_FILE" flag:"device-trust" help:"Device CA chain used to verify device client certificates."`
	DeviceCACertFile string `env:"FLEETWIRE_DEVICE_CA_CERT_FILE" flag:"device-ca-cert" help:"Device Issuing CA certificate (signs device certificates and the CRL)."`
	DeviceCAKeyFile  string `env:"FLEETWIRE_DEVICE_CA_KEY_FILE" flag:"device-ca-key" help:"Device Issuing CA private key."`
	CABundleFile     string `env:"FLEETWIRE_CA_BUNDLE_FILE" flag:"ca-bundle" help:"CA bundle sent to devices to verify the broker and the API."`
	DeviceBrokerURL  string `env:"FLEETWIRE_DEVICE_BROKER_URL" flag:"device-broker-url" help:"Broker URL sent to devices at enrollment, for example tls://broker.example:8883. Must be tls://, mqtts:// or ssl:// with a port."`
	DeviceAPIURL     string `env:"FLEETWIRE_DEVICE_API_URL" flag:"device-api-url" help:"Device listener URL sent to devices, used for renewal, for example https://api.example:8444. Must be https://."`

	KeyringDir         string `env:"FLEETWIRE_KEYRING_DIR" flag:"keyring-dir" help:"Directory of per-tenant KEK files."`
	KeyringRootKeyFile string `env:"FLEETWIRE_KEYRING_ROOT_KEY_FILE" flag:"keyring-root-key" help:"32-byte root key that seals the KEKs. Refused if group or others have any access (use 0400 or 0600)."`

	CRLFile          string        `env:"FLEETWIRE_CRL_FILE" flag:"crl-file" help:"Where the device CRL is written for the broker."`
	CRLInterval      time.Duration `env:"FLEETWIRE_CRL_INTERVAL" flag:"crl-interval" default:"24h" help:"How often the CRL is rebuilt without a revocation. At most 24h: the stale-CRL alert at 48h and the CRL's 7-day validity assume a daily republish."`
	ExportDir        string        `env:"FLEETWIRE_EXPORT_DIR" flag:"export-dir" help:"Directory for export files. Must be on an encrypted volume."`
	MaxActiveExports int           `env:"FLEETWIRE_API_MAX_ACTIVE_EXPORTS" flag:"max-active-exports" default:"4" help:"Exports one tenant can have pending or running at once. Further POST /v1/exports requests get 409 until one finishes."`

	RateLimit         float64       `env:"FLEETWIRE_API_RATE_LIMIT" flag:"rate-limit" default:"20" help:"Requests per second per token."`
	RateBurst         int           `env:"FLEETWIRE_API_RATE_BURST" flag:"rate-burst" default:"40" help:"Burst size per token."`
	AuthFailRateLimit float64       `env:"FLEETWIRE_API_AUTH_FAILURE_RATE_LIMIT" flag:"auth-failure-rate-limit" default:"5" help:"Failed token lookups per second per client network (IPv4 address or IPv6 /64). A network over this budget gets 429 before its tokens are looked up."`
	AuthFailRateBurst int           `env:"FLEETWIRE_API_AUTH_FAILURE_RATE_BURST" flag:"auth-failure-rate-burst" default:"50" help:"Failed token lookup burst per client network."`
	EnrollRateLimit   float64       `env:"FLEETWIRE_API_ENROLL_RATE_LIMIT" flag:"enroll-rate-limit" default:"50" help:"Enrollment and CRL requests per second per client network (IPv4 address or IPv6 /64), counted separately."`
	EnrollRateBurst   int           `env:"FLEETWIRE_API_ENROLL_RATE_BURST" flag:"enroll-rate-burst" default:"200" help:"Enrollment and CRL burst per client network."`
	EnrollTokenTTL    time.Duration `env:"FLEETWIRE_ENROLL_TOKEN_TTL" flag:"enroll-token-ttl" default:"15m" help:"Default lifetime of enrollment tokens."`
	JobWorkers        int           `env:"FLEETWIRE_JOB_WORKERS" flag:"job-workers" default:"2" help:"Background jobs run in parallel by this replica."`

	LogLevel         string  `env:"FLEETWIRE_LOG_LEVEL" flag:"log-level" default:"info" help:"debug, info, warn or error."`
	OTLPEndpoint     string  `env:"FLEETWIRE_OTLP_ENDPOINT" flag:"otlp-endpoint" help:"OTLP/HTTP endpoint for traces, for example http://jaeger:4318. Empty disables tracing."`
	TraceSampleRatio float64 `env:"FLEETWIRE_TRACE_SAMPLE_RATIO" flag:"trace-sample-ratio" default:"0.1" help:"Fraction of new traces sampled."`
}

// Validate checks what the chosen subcommand needs.
func (c *apiConfig) Validate() []error {
	var errs []error
	need := func(v, name string) {
		if v == "" {
			errs = append(errs, errors.New(name+" is required"))
		}
	}
	switch c.cmd {
	case "migrate", "create-operator-token":
		need(c.OwnerDatabaseURL, "FLEETWIRE_DATABASE_OWNER_URL")
	case "serve":
		need(c.DatabaseURL, "FLEETWIRE_DATABASE_URL")
		need(c.TLSCertFile, "FLEETWIRE_API_TLS_CERT_FILE")
		need(c.TLSKeyFile, "FLEETWIRE_API_TLS_KEY_FILE")
		need(c.DeviceTrustFile, "FLEETWIRE_DEVICE_TRUST_FILE")
		need(c.DeviceCACertFile, "FLEETWIRE_DEVICE_CA_CERT_FILE")
		need(c.DeviceCAKeyFile, "FLEETWIRE_DEVICE_CA_KEY_FILE")
		need(c.CABundleFile, "FLEETWIRE_CA_BUNDLE_FILE")
		need(c.DeviceBrokerURL, "FLEETWIRE_DEVICE_BROKER_URL")
		need(c.DeviceAPIURL, "FLEETWIRE_DEVICE_API_URL")
		need(c.KeyringDir, "FLEETWIRE_KEYRING_DIR")
		need(c.KeyringRootKeyFile, "FLEETWIRE_KEYRING_ROOT_KEY_FILE")
		need(c.CRLFile, "FLEETWIRE_CRL_FILE")
		need(c.ExportDir, "FLEETWIRE_EXPORT_DIR")
		if c.InternalListen != "" {
			need(c.ServiceTrustFile, "FLEETWIRE_SERVICE_TRUST_FILE (with FLEETWIRE_API_INTERNAL_LISTEN)")
		}
		// Devices use these URLs as sent at enrollment, so a plaintext
		// scheme here would reach every device. The agent applies the same
		// rules to its own overrides.
		if u, err := url.Parse(c.DeviceBrokerURL); c.DeviceBrokerURL != "" &&
			(err != nil || (u.Scheme != "tls" && u.Scheme != "mqtts" && u.Scheme != "ssl") || u.Port() == "") {
			errs = append(errs, errors.New("FLEETWIRE_DEVICE_BROKER_URL must look like tls://host:port"))
		}
		if u, err := url.Parse(c.DeviceAPIURL); c.DeviceAPIURL != "" && (err != nil || u.Scheme != "https" || u.Host == "") {
			errs = append(errs, errors.New("FLEETWIRE_DEVICE_API_URL must be an https:// URL"))
		}
		if c.CRLInterval <= 0 || c.CRLInterval > maxCRLInterval {
			errs = append(errs, errors.New("FLEETWIRE_CRL_INTERVAL must be positive and at most 24h"))
		}
		// A bucket with no room refuses every request, whatever the rate.
		if c.RateLimit <= 0 || c.RateBurst < 1 {
			errs = append(errs, errors.New("FLEETWIRE_API_RATE_LIMIT must be positive and FLEETWIRE_API_RATE_BURST at least 1"))
		}
		if c.AuthFailRateLimit <= 0 || c.AuthFailRateBurst < 1 {
			errs = append(errs, errors.New("FLEETWIRE_API_AUTH_FAILURE_RATE_LIMIT must be positive and FLEETWIRE_API_AUTH_FAILURE_RATE_BURST at least 1"))
		}
		if c.EnrollRateLimit <= 0 || c.EnrollRateBurst < 1 {
			errs = append(errs, errors.New("FLEETWIRE_API_ENROLL_RATE_LIMIT must be positive and FLEETWIRE_API_ENROLL_RATE_BURST at least 1"))
		}
		// The enrollment-token endpoints refuse lifetimes outside this
		// range, so a default outside it would fail every request without
		// ttl_seconds.
		if c.EnrollTokenTTL < time.Minute || c.EnrollTokenTTL > 24*time.Hour {
			errs = append(errs, errors.New("FLEETWIRE_ENROLL_TOKEN_TTL must be between 1m and 24h"))
		}
		if c.MaxActiveExports < 1 {
			errs = append(errs, errors.New("FLEETWIRE_API_MAX_ACTIVE_EXPORTS must be at least 1"))
		}
		if c.JobWorkers < 0 {
			errs = append(errs, errors.New("FLEETWIRE_JOB_WORKERS must not be negative"))
		}
		if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
			errs = append(errs, errors.New("FLEETWIRE_TRACE_SAMPLE_RATIO must be between 0 and 1"))
		}
	}
	return errs
}
