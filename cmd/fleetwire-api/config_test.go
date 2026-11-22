package main

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/config"
)

func serveConfig() apiConfig {
	return apiConfig{
		cmd: "serve", DatabaseURL: "postgres://app@db/fleetwire", TLSCertFile: "api.pem", TLSKeyFile: "api-key.pem",
		DeviceTrustFile: "trust.pem", DeviceCACertFile: "ca.pem", DeviceCAKeyFile: "ca-key.pem", CABundleFile: "bundle.pem",
		DeviceBrokerURL: "tls://broker:8883", DeviceAPIURL: "https://api:8444", KeyringDir: "keys",
		KeyringRootKeyFile: "root.key", CRLFile: "crl.pem", ExportDir: "exports", CRLInterval: 24 * time.Hour,
		RateLimit: 20, RateBurst: 40, AuthFailRateLimit: 5, AuthFailRateBurst: 50, EnrollRateLimit: 50, EnrollRateBurst: 200,
		EnrollTokenTTL: 15 * time.Minute,
	}
}

func TestServeConfigIsValid(t *testing.T) {
	c := serveConfig()
	if errs := c.Validate(); len(errs) != 0 {
		t.Fatalf("serveConfig: %v", errs)
	}
}

// Settings that would make the API refuse every request are configuration
// errors, not a silent outage.
func TestRateLimitsAndEnrollTTLMustBeUsable(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*apiConfig)
		want   string
	}{
		"rate burst 0":         {func(c *apiConfig) { c.RateBurst = 0 }, "FLEETWIRE_API_RATE_BURST"},
		"rate limit 0":         {func(c *apiConfig) { c.RateLimit = 0 }, "FLEETWIRE_API_RATE_LIMIT"},
		"negative rate":        {func(c *apiConfig) { c.RateLimit = -1 }, "FLEETWIRE_API_RATE_LIMIT"},
		"auth failure burst 0": {func(c *apiConfig) { c.AuthFailRateBurst = 0 }, "FLEETWIRE_API_AUTH_FAILURE_RATE_BURST"},
		"auth failure rate 0":  {func(c *apiConfig) { c.AuthFailRateLimit = 0 }, "FLEETWIRE_API_AUTH_FAILURE_RATE_LIMIT"},
		"enroll burst 0":       {func(c *apiConfig) { c.EnrollRateBurst = 0 }, "FLEETWIRE_API_ENROLL_RATE_BURST"},
		"enroll rate 0":        {func(c *apiConfig) { c.EnrollRateLimit = 0 }, "FLEETWIRE_API_ENROLL_RATE_LIMIT"},
		"enroll ttl too short": {func(c *apiConfig) { c.EnrollTokenTTL = 59 * time.Second }, "FLEETWIRE_ENROLL_TOKEN_TTL"},
		"enroll ttl too long":  {func(c *apiConfig) { c.EnrollTokenTTL = 25 * time.Hour }, "FLEETWIRE_ENROLL_TOKEN_TTL"},
	} {
		c := serveConfig()
		tc.change(&c)
		errs := c.Validate()
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), tc.want) {
			t.Errorf("%s: errors %v, want one about %s", name, errs, tc.want)
		}
	}
}

// NaN passes every range check, so the loader refuses it first.
func TestNonFiniteNumbersAreRefused(t *testing.T) {
	t.Setenv("FLEETWIRE_TRACE_SAMPLE_RATIO", "NaN")
	t.Setenv("FLEETWIRE_API_RATE_LIMIT", "+Inf")
	err := config.Load(&apiConfig{cmd: "serve"}, flag.NewFlagSet("x", flag.ContinueOnError), nil)
	if err == nil || !strings.Contains(err.Error(), "FLEETWIRE_TRACE_SAMPLE_RATIO") || !strings.Contains(err.Error(), "FLEETWIRE_API_RATE_LIMIT") {
		t.Fatalf("Load = %v, want errors for both settings", err)
	}
}

// ADR-0010 and threat-model D4: the CRL is republished at least every 24
// hours. A zero interval would rebuild it in a tight loop.
func TestCRLIntervalIsPositiveAndAtMostADay(t *testing.T) {
	for _, d := range []time.Duration{time.Nanosecond, time.Hour, 24 * time.Hour} {
		c := serveConfig()
		c.CRLInterval = d
		if errs := c.Validate(); len(errs) != 0 {
			t.Errorf("CRLInterval %v: %v, want no errors", d, errs)
		}
	}
	for _, d := range []time.Duration{0, -time.Hour, 24*time.Hour + time.Nanosecond, 7*24*time.Hour - time.Nanosecond} {
		c := serveConfig()
		c.CRLInterval = d
		errs := c.Validate()
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), "FLEETWIRE_CRL_INTERVAL") {
			t.Errorf("CRLInterval %v: errors %v, want one about FLEETWIRE_CRL_INTERVAL", d, errs)
		}
	}
}
