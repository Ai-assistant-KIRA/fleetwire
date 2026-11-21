package main

import (
	"strings"
	"testing"
	"time"
)

func serveConfig() apiConfig {
	return apiConfig{
		cmd: "serve", DatabaseURL: "postgres://app@db/fleetwire", TLSCertFile: "api.pem", TLSKeyFile: "api-key.pem",
		DeviceTrustFile: "trust.pem", DeviceCACertFile: "ca.pem", DeviceCAKeyFile: "ca-key.pem", CABundleFile: "bundle.pem",
		DeviceBrokerURL: "tls://broker:8883", DeviceAPIURL: "https://api:8444", KeyringDir: "keys",
		KeyringRootKeyFile: "root.key", CRLFile: "crl.pem", ExportDir: "exports", CRLInterval: 24 * time.Hour,
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
