package svc

import (
	"strings"
	"testing"
)

// With mqtt:// autopaho ignores the TLS config and connects in plain
// text, so the service URL is refused before any certificate is loaded.
func TestBrokerTLSRequiresATLSURL(t *testing.T) {
	if _, err := BrokerTLS("mqtt://mosquitto:8884", "cert.pem", "key.pem", "ca.pem"); err == nil || !strings.Contains(err.Error(), "TLS URL") {
		t.Fatalf("BrokerTLS(mqtt://) = %v, want a TLS URL error", err)
	}
	if _, err := BrokerTLS("tls://mosquitto:8884", "missing.pem", "missing.key", "ca.pem"); err == nil || !strings.Contains(err.Error(), "client certificate") {
		t.Fatalf("BrokerTLS(tls://) = %v, want it to get as far as loading the certificate", err)
	}
}
