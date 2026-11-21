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

// The broker rejects every device once the CRL's nextUpdate (7 days) passes,
// and a zero interval rebuilds the CRL in a tight loop.
func TestCRLIntervalMustBePositiveAndUnderTheValidity(t *testing.T) {
	valid := serveConfig()
	if errs := valid.Validate(); len(errs) != 0 {
		t.Fatalf("valid config: %v", errs)
	}
	for _, d := range []time.Duration{0, -time.Hour, 7 * 24 * time.Hour, 240 * time.Hour} {
		c := serveConfig()
		c.CRLInterval = d
		errs := c.Validate()
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), "FLEETWIRE_CRL_INTERVAL") {
			t.Errorf("CRLInterval %v: errors %v, want one about FLEETWIRE_CRL_INTERVAL", d, errs)
		}
	}
}
