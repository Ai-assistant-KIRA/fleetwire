// Command fleetwire-ingest stores uplink batches from Kafka in PostgreSQL
// exactly once and publishes end-to-end acks.
//
//	fleetwire-ingest [run]        consume until stopped
//	fleetwire-ingest ensure-topics create the Kafka topics if missing
//	fleetwire-ingest config-docs  print the configuration reference
//	fleetwire-ingest version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/ingest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/svc"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
)

type ingestConfig struct {
	svc.Common
	svc.Kafka
	DatabaseURL  string `env:"FLEETWIRE_DATABASE_URL" flag:"database-url" secret:"true" help:"PostgreSQL URL for the service login: the runtime role, or a member of it, never the schema owner."`
	DBMaxConns   int32  `env:"FLEETWIRE_DATABASE_MAX_CONNS" flag:"database-max-conns" default:"20" help:"Connection pool size."`
	Replication  int    `env:"FLEETWIRE_KAFKA_REPLICATION" flag:"kafka-replication" default:"3" help:"Replication factor for ensure-topics."`
	Group        string `env:"FLEETWIRE_KAFKA_GROUP" flag:"kafka-group" default:"fleetwire-ingest" help:"Consumer group. With Charmed Kafka it must start with the relation's consumer-group-prefix."`
	Workers      int    `env:"FLEETWIRE_INGEST_WORKERS" flag:"workers" default:"16" help:"Partitions processed in parallel."`
	MQTTURL      string `env:"FLEETWIRE_MQTT_URL" flag:"mqtt-url" help:"Broker service listener for acks, for example tls://mosquitto:8884."`
	MQTTCertFile string `env:"FLEETWIRE_MQTT_CERT_FILE" flag:"mqtt-cert" help:"Client certificate chain (CN fleetwire-ingest)."`
	MQTTKeyFile  string `env:"FLEETWIRE_MQTT_KEY_FILE" flag:"mqtt-key" help:"Client private key."`
	MQTTCAFile   string `env:"FLEETWIRE_MQTT_CA_FILE" flag:"mqtt-ca" help:"CA bundle that signed the broker certificate."`
	// One of the two keyring backends: the directory shared with the API on
	// one host, or the API's keyring listener when they run apart.
	KeyringDir         string `env:"FLEETWIRE_KEYRING_DIR" flag:"keyring-dir" help:"Directory of per-tenant KEK files, shared with the API. Set this or FLEETWIRE_KEYRING_URL."`
	KeyringRootKeyFile string `env:"FLEETWIRE_KEYRING_ROOT_KEY_FILE" flag:"keyring-root-key" help:"32-byte root key that seals the KEKs. Refused if group or others have any access (use 0400 or 0600). Only with FLEETWIRE_KEYRING_DIR."`
	KeyringURL         string `env:"FLEETWIRE_KEYRING_URL" flag:"keyring-url" help:"The API's keyring listener, for example https://api:8445. Set this or FLEETWIRE_KEYRING_DIR."`
	KeyringCertFile    string `env:"FLEETWIRE_KEYRING_CERT_FILE" flag:"keyring-cert" help:"Client certificate for the keyring listener. Default: FLEETWIRE_MQTT_CERT_FILE."`
	KeyringKeyFile     string `env:"FLEETWIRE_KEYRING_KEY_FILE" flag:"keyring-key" help:"Its private key. Default: FLEETWIRE_MQTT_KEY_FILE."`
	KeyringCAFile      string `env:"FLEETWIRE_KEYRING_CA_FILE" flag:"keyring-ca" help:"CA bundle that signed the API certificate. Default: FLEETWIRE_MQTT_CA_FILE."`
	Instance           string `env:"FLEETWIRE_INSTANCE_ID" flag:"instance-id" help:"Stable instance name. Default: host name."`
	AckWorkers         int    `env:"FLEETWIRE_INGEST_ACK_WORKERS" flag:"ack-workers" default:"8" help:"Concurrent ack publishers."`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-ingest:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var cfg ingestConfig
	switch cmd {
	case "version":
		fmt.Fprintln(stdout, buildinfo.Get())
		return nil
	case "healthcheck":
		addr := os.Getenv("FLEETWIRE_ADMIN_LISTEN")
		if addr == "" {
			addr = "127.0.0.1:9090"
		}
		return svc.Healthcheck(ctx, addr)
	case "config-docs":
		md, err := config.Markdown(&cfg)
		if err != nil {
			return err
		}
		fmt.Fprint(stdout, md)
		return nil
	case "run", "ensure-topics":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	fs := flag.NewFlagSet("fleetwire-ingest "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := config.Load(&cfg, fs, args); err != nil {
		return err
	}
	rt, err := svc.Start(ctx, "fleetwire-ingest", cfg.Common, stderr)
	if err != nil {
		return err
	}
	conn, err := cfg.KafkaOpts()
	if err != nil {
		return err
	}
	if cmd == "ensure-topics" {
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		// Brokers may still be electing a controller right after start.
		return backoff.Retry(ctx, backoff.Backoff{Base: time.Second, Max: 10 * time.Second}, backoff.RetryOptions{MaxAttempts: 30},
			func(ctx context.Context) error {
				return ingest.EnsureTopics(ctx, conn, int16(cfg.Replication), rt.Log)
			})
	}
	var missing []string
	for name, v := range map[string]string{"FLEETWIRE_DATABASE_URL": cfg.DatabaseURL, "FLEETWIRE_MQTT_URL": cfg.MQTTURL,
		"FLEETWIRE_MQTT_CERT_FILE": cfg.MQTTCertFile, "FLEETWIRE_MQTT_KEY_FILE": cfg.MQTTKeyFile, "FLEETWIRE_MQTT_CA_FILE": cfg.MQTTCAFile} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return fmt.Errorf("required for run: %s", strings.Join(missing, ", "))
	}
	db, err := store.Connect(ctx, cfg.DatabaseURL, store.Options{MaxConns: cfg.DBMaxConns, ApplicationName: "fleetwire-ingest", Tracer: otelpgx.NewTracer()})
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.CheckRuntimeRole(ctx); err != nil {
		return err
	}
	kr, err := openKeyring(&cfg)
	if err != nil {
		return err
	}
	tlsCfg, err := svc.BrokerTLS(cfg.MQTTURL, cfg.MQTTCertFile, cfg.MQTTKeyFile, cfg.MQTTCAFile)
	if err != nil {
		return err
	}
	keys := tenantkey.New(db, kr, rt.Log)
	s, err := ingest.New(ingest.Config{
		Kafka: conn, Group: cfg.Group, InstanceID: svc.InstanceID(cfg.Instance), Workers: cfg.Workers,
		MQTTURL: cfg.MQTTURL, TLS: tlsCfg, AckWorkers: cfg.AckWorkers,
	}, db, keys, rt.Log, rt.Registry)
	if err != nil {
		return err
	}
	return rt.Serve(ctx, cfg.AdminListen, map[string]telemetry.Probe{"database": db.Ping, "ingest": s.Ready}, s.Run)
}

// openKeyring picks the keyring backend: the directory shared with the API,
// or the API's keyring listener.
func openKeyring(cfg *ingestConfig) (keyring.Keyring, error) {
	switch {
	case cfg.KeyringURL != "" && cfg.KeyringDir != "":
		return nil, errors.New("set FLEETWIRE_KEYRING_URL or FLEETWIRE_KEYRING_DIR, not both")
	case cfg.KeyringURL != "":
		pick := func(v, fallback string) string {
			if v != "" {
				return v
			}
			return fallback
		}
		tlsCfg, err := svc.ClientTLS(pick(cfg.KeyringCertFile, cfg.MQTTCertFile), pick(cfg.KeyringKeyFile, cfg.MQTTKeyFile),
			pick(cfg.KeyringCAFile, cfg.MQTTCAFile))
		if err != nil {
			return nil, fmt.Errorf("keyring client: %w", err)
		}
		return keyring.NewRemote(cfg.KeyringURL, tlsCfg)
	case cfg.KeyringDir != "":
		if cfg.KeyringRootKeyFile == "" {
			return nil, errors.New("FLEETWIRE_KEYRING_ROOT_KEY_FILE is required with FLEETWIRE_KEYRING_DIR")
		}
		root, err := keyring.LoadRootKey(cfg.KeyringRootKeyFile)
		if err != nil {
			return nil, err
		}
		return keyring.NewLocal(cfg.KeyringDir, root)
	default:
		return nil, errors.New("set FLEETWIRE_KEYRING_DIR (shared with the API) or FLEETWIRE_KEYRING_URL (the API's keyring listener)")
	}
}
