package main

import (
	"errors"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
)

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
	DeviceBrokerURL  string `env:"FLEETWIRE_DEVICE_BROKER_URL" flag:"device-broker-url" help:"Broker URL sent to devices at enrollment, for example tls://broker.example:8883."`
	DeviceAPIURL     string `env:"FLEETWIRE_DEVICE_API_URL" flag:"device-api-url" help:"Device listener URL sent to devices, used for renewal."`

	KeyringDir         string `env:"FLEETWIRE_KEYRING_DIR" flag:"keyring-dir" help:"Directory of per-tenant KEK files."`
	KeyringRootKeyFile string `env:"FLEETWIRE_KEYRING_ROOT_KEY_FILE" flag:"keyring-root-key" help:"32-byte root key that seals the KEKs. Refused if group or others have any access (use 0400 or 0600)."`

	CRLFile     string        `env:"FLEETWIRE_CRL_FILE" flag:"crl-file" help:"Where the device CRL is written for the broker."`
	CRLInterval time.Duration `env:"FLEETWIRE_CRL_INTERVAL" flag:"crl-interval" default:"24h" help:"How often the CRL is rebuilt without a revocation. Must be shorter than the CRL's 7-day validity."`
	ExportDir   string        `env:"FLEETWIRE_EXPORT_DIR" flag:"export-dir" help:"Directory for export files. Must be on an encrypted volume."`

	RateLimit       float64       `env:"FLEETWIRE_API_RATE_LIMIT" flag:"rate-limit" default:"20" help:"Requests per second per token."`
	RateBurst       int           `env:"FLEETWIRE_API_RATE_BURST" flag:"rate-burst" default:"40" help:"Burst size per token."`
	EnrollRateLimit float64       `env:"FLEETWIRE_API_ENROLL_RATE_LIMIT" flag:"enroll-rate-limit" default:"50" help:"Enrollments per second per client address."`
	EnrollRateBurst int           `env:"FLEETWIRE_API_ENROLL_RATE_BURST" flag:"enroll-rate-burst" default:"200" help:"Enrollment burst per client address."`
	EnrollTokenTTL  time.Duration `env:"FLEETWIRE_ENROLL_TOKEN_TTL" flag:"enroll-token-ttl" default:"15m" help:"Default lifetime of enrollment tokens."`
	JobWorkers      int           `env:"FLEETWIRE_JOB_WORKERS" flag:"job-workers" default:"2" help:"Background jobs run in parallel by this replica."`

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
		if c.CRLInterval <= 0 || c.CRLInterval >= crl.Validity {
			errs = append(errs, errors.New("FLEETWIRE_CRL_INTERVAL must be positive and shorter than the CRL's validity of "+crl.Validity.String()))
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
