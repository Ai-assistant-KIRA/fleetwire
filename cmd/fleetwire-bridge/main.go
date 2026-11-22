// Command fleetwire-bridge moves device messages from Mosquitto to Kafka.
//
//	fleetwire-bridge [run]       bridge until stopped
//	fleetwire-bridge config-docs print the configuration reference
//	fleetwire-bridge version
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/bridge"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/ingest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/svc"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
)

type bridgeConfig struct {
	svc.Common
	svc.Kafka
	MQTTURL      string `env:"FLEETWIRE_MQTT_URL" flag:"mqtt-url" required:"true" help:"Broker service listener, for example tls://mosquitto:8884."`
	MQTTCertFile string `env:"FLEETWIRE_MQTT_CERT_FILE" flag:"mqtt-cert" required:"true" help:"Client certificate chain (CN fleetwire-bridge)."`
	MQTTKeyFile  string `env:"FLEETWIRE_MQTT_KEY_FILE" flag:"mqtt-key" required:"true" help:"Client private key."`
	MQTTCAFile   string `env:"FLEETWIRE_MQTT_CA_FILE" flag:"mqtt-ca" required:"true" help:"CA bundle that signed the broker certificate."`
	Instance     string `env:"FLEETWIRE_INSTANCE_ID" flag:"instance-id" help:"Stable instance name; the MQTT client ID is fleetwire-bridge-<instance>. Default: host name."`
	Receive      int    `env:"FLEETWIRE_BRIDGE_RECEIVE_MAXIMUM" flag:"receive-maximum" default:"1000" help:"Messages the broker may send before the bridge acks."`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-bridge:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var cfg bridgeConfig
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
	case "run":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	fs := flag.NewFlagSet("fleetwire-bridge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := config.Load(&cfg, fs, args); err != nil {
		return err
	}
	if cfg.Receive < 1 || cfg.Receive > 65535 {
		return fmt.Errorf("FLEETWIRE_BRIDGE_RECEIVE_MAXIMUM must be 1 to 65535")
	}
	rt, err := svc.Start(ctx, "fleetwire-bridge", cfg.Common, stderr)
	if err != nil {
		return err
	}
	defer rt.Shutdown(ctx)
	tlsCfg, err := svc.BrokerTLS(cfg.MQTTURL, cfg.MQTTCertFile, cfg.MQTTKeyFile, cfg.MQTTCAFile)
	if err != nil {
		return err
	}
	conn, err := cfg.KafkaOpts()
	if err != nil {
		return err
	}
	b, err := bridge.New(bridge.Config{
		MQTTURL: cfg.MQTTURL, ClientID: "fleetwire-bridge-" + svc.InstanceID(cfg.Instance), TLS: tlsCfg,
		ReceiveMaximum: uint16(cfg.Receive), Kafka: conn, Topic: ingest.UplinkTopic,
	}, rt.Log, rt.Registry)
	if err != nil {
		return err
	}
	defer b.Close()
	return rt.Serve(ctx, cfg.AdminListen, map[string]telemetry.Probe{"bridge": b.Ready}, b.Run)
}
