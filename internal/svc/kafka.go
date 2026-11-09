package svc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

// Kafka is the Kafka connection of the bridge and ingest. The Compose stack
// uses plaintext without authentication; Charmed Kafka requires SASL/SCRAM
// and adds TLS when it is related to a certificates provider.
type Kafka struct {
	Brokers   []string `env:"FLEETWIRE_KAFKA_BROKERS" flag:"kafka-brokers" required:"true" help:"Comma-separated Kafka bootstrap brokers."`
	SASL      string   `env:"FLEETWIRE_KAFKA_SASL_MECHANISM" flag:"kafka-sasl-mechanism" help:"SCRAM-SHA-512 (what Charmed Kafka uses), SCRAM-SHA-256, or empty for no authentication."`
	Username  string   `env:"FLEETWIRE_KAFKA_USERNAME" flag:"kafka-username" help:"SASL user name."`
	Password  string   `env:"FLEETWIRE_KAFKA_PASSWORD" flag:"kafka-password" secret:"true" help:"SASL password."`
	TLS       bool     `env:"FLEETWIRE_KAFKA_TLS" flag:"kafka-tls" help:"Connect to the brokers over TLS."`
	TLSCAFile string   `env:"FLEETWIRE_KAFKA_TLS_CA_FILE" flag:"kafka-tls-ca" help:"CA bundle for the brokers' certificates. Empty means the system roots."`
}

// KafkaOpts returns the client options that reach and authenticate to the
// brokers. Callers add their producer or consumer options after them.
func (k Kafka) KafkaOpts() ([]kgo.Opt, error) {
	if len(k.Brokers) == 0 {
		return nil, errors.New("FLEETWIRE_KAFKA_BROKERS is empty")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(k.Brokers...)}
	switch mech := strings.ToUpper(k.SASL); mech {
	case "":
		if k.Username != "" || k.Password != "" {
			return nil, errors.New("FLEETWIRE_KAFKA_USERNAME/PASSWORD are set but FLEETWIRE_KAFKA_SASL_MECHANISM is empty")
		}
	case "SCRAM-SHA-512", "SCRAM-SHA-256":
		if k.Username == "" || k.Password == "" {
			return nil, fmt.Errorf("FLEETWIRE_KAFKA_SASL_MECHANISM %s needs FLEETWIRE_KAFKA_USERNAME and FLEETWIRE_KAFKA_PASSWORD", mech)
		}
		auth := scram.Auth{User: k.Username, Pass: k.Password}
		if mech == "SCRAM-SHA-512" {
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		} else {
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		}
	default:
		return nil, fmt.Errorf("FLEETWIRE_KAFKA_SASL_MECHANISM %q: want SCRAM-SHA-512, SCRAM-SHA-256 or empty", k.SASL)
	}
	switch {
	case k.TLS:
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if k.TLSCAFile != "" {
			roots, err := pki.CertPool(k.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("kafka CA: %w", err)
			}
			cfg.RootCAs = roots
		}
		// franz-go sets ServerName to each broker's host when it is empty.
		opts = append(opts, kgo.DialTLSConfig(cfg))
	case k.TLSCAFile != "":
		return nil, errors.New("FLEETWIRE_KAFKA_TLS_CA_FILE is set but FLEETWIRE_KAFKA_TLS is false")
	}
	return opts, nil
}
