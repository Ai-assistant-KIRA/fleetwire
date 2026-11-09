// Package mqttconn sets up MQTT v5 connections the same way for the agent,
// the bridge and the ingest service: TLS client certificates, a persistent
// session, reconnects with full-jitter backoff, and connection metrics.
package mqttconn

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
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
	// is zero.
	SessionExpiry time.Duration
	KeepAlive     time.Duration
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

// Start begins connecting and keeps reconnecting until ctx ends.
func Start(ctx context.Context, cfg Config) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("broker url %q: %w", cfg.URL, err)
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = backoff.Backoff{Base: time.Second, Max: time.Minute}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := cfg.Metrics
	expiry := uint32(cfg.SessionExpiry / time.Second)
	ac := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		TlsCfg:                        cfg.TLS,
		KeepAlive:                     uint16(cfg.KeepAlive / time.Second),
		CleanStartOnInitialConnection: expiry == 0,
		SessionExpiryInterval:         expiry,
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
