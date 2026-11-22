// Package mqttconn sets up MQTT v5 connections the same way for the agent,
// the bridge and the ingest service: TLS client certificates, a persistent
// session, reconnects with full-jitter backoff, and connection metrics.
package mqttconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
)

// Metrics can be shared by many connections (the load generator runs
// thousands), so none of them carries a per-connection label.
type Metrics struct {
	Connects      prometheus.Counter
	ConnectErrors prometheus.Counter
	Disconnects   prometheus.Counter
	Connected     prometheus.Gauge
}

// NewMetrics registers connection metrics with prefix (for example
// "fleetwire_agent").
func NewMetrics(reg prometheus.Registerer, prefix string) *Metrics {
	m := &Metrics{
		Connects:      prometheus.NewCounter(prometheus.CounterOpts{Name: prefix + "_mqtt_connects_total", Help: "Successful MQTT connections."}),
		ConnectErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: prefix + "_mqtt_connect_errors_total", Help: "Failed MQTT connection attempts."}),
		Disconnects:   prometheus.NewCounter(prometheus.CounterOpts{Name: prefix + "_mqtt_disconnects_total", Help: "MQTT connections lost."}),
		Connected:     prometheus.NewGauge(prometheus.GaugeOpts{Name: prefix + "_mqtt_connected", Help: "MQTT connections currently up."}),
	}
	reg.MustRegister(m.Connects, m.ConnectErrors, m.Disconnects, m.Connected)
	return m
}

// Config describes one connection.
type Config struct {
	URL      string
	ClientID string
	TLS      *tls.Config
	// SessionExpiry keeps the broker-side session (queued QoS 1 messages)
	// for this long after a disconnect. Clean start is used only when it
	// is zero. MQTT counts it in whole seconds, so a fraction rounds up.
	SessionExpiry time.Duration
	// KeepAlive is the MQTT keep alive (default 30 s), in whole seconds
	// like SessionExpiry.
	KeepAlive time.Duration
	// ReceiveMaximum caps unacknowledged messages the broker sends us.
	// Zero means DefaultReceiveMaximum, never the protocol's 65,535: paho
	// allocates a channel of that many packets per connection.
	ReceiveMaximum uint16
	// ManualAck: messages are acknowledged only when the caller acks them
	// (paho sends PUBACKs strictly in arrival order).
	ManualAck bool
	Backoff   backoff.Backoff
	// OnUp runs after every (re)connection. It must not block for long; it
	// may subscribe.
	OnUp func(cm *autopaho.ConnectionManager, ack *paho.Connack)
	// OnDown runs after a connection is lost.
	OnDown func()
	// OnPublish receives messages.
	OnPublish func(paho.PublishReceived) (bool, error)
	Logger    *slog.Logger
	Metrics   *Metrics
}

// DefaultReceiveMaximum is sent when a Config sets none. paho.golang sizes
// a per-connection channel by the receive maximum, and the protocol default
// of 65,535 costs 512 KiB per connection: 5 GB for the load generator's
// 10,000 devices, which receive only a few acks at a time.
const DefaultReceiveMaximum = 64

// tlsSchemes are the URL schemes autopaho v0.23.0 dials with the TLS
// config (autopaho/net.go); mqtt://, tcp:// and ws:// ignore it.
var tlsSchemes = []string{"tls", "ssl", "mqtts", "mqtt+ssl", "tcps"}

// RequireTLS parses a broker URL and checks that autopaho reaches it over
// TLS. The services authenticate to the broker with client certificates:
// with mqtt:// autopaho would drop the TLS config without a word and
// connect in plain text.
func RequireTLS(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("broker url %q: %w", rawURL, err)
	}
	if !slices.Contains(tlsSchemes, strings.ToLower(u.Scheme)) || u.Host == "" {
		return nil, fmt.Errorf("broker url %q: want a TLS URL such as tls://host:port (schemes %s)", rawURL, strings.Join(tlsSchemes, ", "))
	}
	return u, nil
}

// seconds converts d to the whole seconds of an MQTT field that holds at
// most limit. A fraction of a second rounds up, never down to 0: for keep
// alive and session expiry 0 means off.
func seconds(name string, d time.Duration, limit uint64) (uint64, error) {
	if d < 0 {
		return 0, fmt.Errorf("mqtt %s %v is negative", name, d)
	}
	n := uint64(d / time.Second)
	if d%time.Second != 0 {
		n++
	}
	if n > limit {
		return 0, fmt.Errorf("mqtt %s %v is above the protocol limit of %d s", name, d, limit)
	}
	return n, nil
}

// Start begins connecting and keeps reconnecting until ctx ends.
func Start(ctx context.Context, cfg Config) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("broker url %q: %w", cfg.URL, err)
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	keepAlive, kerr := seconds("keep alive", cfg.KeepAlive, math.MaxUint16)
	expiry, eerr := seconds("session expiry", cfg.SessionExpiry, math.MaxUint32)
	if err := errors.Join(kerr, eerr); err != nil {
		return nil, err
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = backoff.Backoff{Base: time.Second, Max: time.Minute}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := cfg.Metrics
	ac := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		TlsCfg:                        cfg.TLS,
		KeepAlive:                     uint16(keepAlive),
		CleanStartOnInitialConnection: expiry == 0,
		SessionExpiryInterval:         uint32(expiry),
		ConnectTimeout:                20 * time.Second,
		ReconnectBackoff:              cfg.Backoff.Delay,
		ConnectPacketBuilder:          connectPacket(cfg.ReceiveMaximum),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ack *paho.Connack) {
			if m != nil {
				m.Connects.Inc()
				m.Connected.Inc()
			}
			log.Debug("mqtt connected", "client_id", cfg.ClientID, "session_present", ack.SessionPresent)
			if cfg.OnUp != nil {
				cfg.OnUp(cm, ack)
			}
		},
		OnConnectionDown: func() bool {
			if m != nil {
				m.Disconnects.Inc()
				m.Connected.Dec()
			}
			log.Debug("mqtt connection lost", "client_id", cfg.ClientID)
			if cfg.OnDown != nil {
				cfg.OnDown()
			}
			return true // keep reconnecting
		},
		OnConnectError: func(err error) {
			if m != nil {
				m.ConnectErrors.Inc()
			}
			log.Debug("mqtt connect failed", "client_id", cfg.ClientID, "err", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID:                   cfg.ClientID,
			EnableManualAcknowledgment: cfg.ManualAck,
			PacketTimeout:              30 * time.Second,
			OnClientError: func(err error) {
				log.Debug("mqtt client error", "client_id", cfg.ClientID, "err", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Warn("mqtt server disconnected us", "client_id", cfg.ClientID, "reason", d.ReasonCode)
			},
		},
	}
	if cfg.OnPublish != nil {
		ac.OnPublishReceived = []func(paho.PublishReceived) (bool, error){cfg.OnPublish}
	}
	cm, err := autopaho.NewConnection(ctx, ac)
	if err != nil {
		return nil, fmt.Errorf("mqtt %s: %w", cfg.ClientID, err)
	}
	return cm, nil
}

// connectPacket returns the CONNECT builder: every connection states its
// receive maximum, DefaultReceiveMaximum when the Config sets none.
func connectPacket(recvMax uint16) func(*paho.Connect, *url.URL) (*paho.Connect, error) {
	if recvMax == 0 {
		recvMax = DefaultReceiveMaximum
	}
	return func(c *paho.Connect, _ *url.URL) (*paho.Connect, error) {
		if c.Properties == nil {
			c.Properties = &paho.ConnectProperties{}
		}
		c.Properties.ReceiveMaximum = &recvMax
		return c, nil
	}
}
