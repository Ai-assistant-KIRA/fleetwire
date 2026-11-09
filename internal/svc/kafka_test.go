package svc_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/svc"
)

func TestKafkaOpts(t *testing.T) {
	dir := t.TempDir()
	if _, err := devpki.Generate(dir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(dir, devpki.CABundle)
	brokers := []string{"kafka-1:9092", "kafka-2:9092"}
	for _, tc := range []struct {
		name    string
		k       svc.Kafka
		wantErr string
	}{
		{"plaintext", svc.Kafka{Brokers: brokers}, ""},
		{"scram-512", svc.Kafka{Brokers: brokers, SASL: "SCRAM-SHA-512", Username: "relation-7", Password: "pw"}, ""},
		{"scram-256 lower case", svc.Kafka{Brokers: brokers, SASL: "scram-sha-256", Username: "u", Password: "pw"}, ""},
		{"tls with CA", svc.Kafka{Brokers: brokers, SASL: "SCRAM-SHA-512", Username: "u", Password: "pw", TLS: true, TLSCAFile: ca}, ""},
		{"tls with system roots", svc.Kafka{Brokers: brokers, TLS: true}, ""},
		{"no brokers", svc.Kafka{}, "FLEETWIRE_KAFKA_BROKERS"},
		{"unknown mechanism", svc.Kafka{Brokers: brokers, SASL: "PLAIN", Username: "u", Password: "p"}, "want SCRAM-SHA-512"},
		{"scram without password", svc.Kafka{Brokers: brokers, SASL: "SCRAM-SHA-512", Username: "u"}, "needs FLEETWIRE_KAFKA_USERNAME"},
		{"credentials without mechanism", svc.Kafka{Brokers: brokers, Username: "u", Password: "p"}, "MECHANISM is empty"},
		{"CA without TLS", svc.Kafka{Brokers: brokers, TLSCAFile: ca}, "FLEETWIRE_KAFKA_TLS is false"},
		{"unreadable CA", svc.Kafka{Brokers: brokers, TLS: true, TLSCAFile: filepath.Join(dir, "missing.pem")}, "kafka CA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := tc.k.KafkaOpts()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The options must form a valid client; NewClient does not dial.
			cl, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			cl.Close()
		})
	}
}
