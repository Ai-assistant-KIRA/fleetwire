package mqttconn

import (
	"context"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
)

// MQTT carries keep alive and session expiry in whole seconds, and 0 turns
// either off. Truncating made 500 ms of keep alive no keep alive at all
// and 500 ms of session expiry a clean session, and a value above the
// field wrapped around.
func TestDurationsBecomeWholeSeconds(t *testing.T) {
	for _, tc := range []struct {
		d     time.Duration
		limit uint64
		want  uint64
		err   bool
	}{
		{0, math.MaxUint16, 0, false},
		{time.Nanosecond, math.MaxUint16, 1, false},
		{500 * time.Millisecond, math.MaxUint16, 1, false},
		{30 * time.Second, math.MaxUint16, 30, false},
		{1500 * time.Millisecond, math.MaxUint16, 2, false},
		{math.MaxUint16 * time.Second, math.MaxUint16, math.MaxUint16, false},
		{math.MaxUint16*time.Second + 1, math.MaxUint16, 0, true},
		{7 * 24 * time.Hour, math.MaxUint32, 604800, false},
		{math.MaxUint32 * time.Second, math.MaxUint32, math.MaxUint32, false},
		{(math.MaxUint32 + 1) * time.Second, math.MaxUint32, 0, true},
		{math.MaxInt64, math.MaxUint32, 0, true},
		{-time.Second, math.MaxUint32, 0, true},
	} {
		got, err := seconds("test", tc.d, tc.limit)
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("seconds(%v, %d) = %d, %v; want %d, error %v", tc.d, tc.limit, got, err, tc.want, tc.err)
		}
	}
}

func TestStartRefusesDurationsMQTTCannotCarry(t *testing.T) {
	for _, cfg := range []Config{
		{URL: "tls://broker:8884", KeepAlive: 20 * time.Hour},
		{URL: "tls://broker:8884", SessionExpiry: 200 * 365 * 24 * time.Hour},
		{URL: "tls://broker:8884", KeepAlive: -time.Second},
	} {
		if cm, err := Start(t.Context(), cfg); err == nil {
			_ = cm.Disconnect(t.Context())
			t.Errorf("Start accepted keep alive %v, session expiry %v", cfg.KeepAlive, cfg.SessionExpiry)
		}
	}
}

// serveMQTT is just enough of a broker for a client to connect and stay
// connected: CONNACK, PINGRESP, nothing else.
func serveMQTT(c net.Conn) {
	defer c.Close()
	for {
		p, err := packets.ReadPacket(c)
		if err != nil {
			return
		}
		var reply *packets.ControlPacket
		switch p.Type {
		case packets.CONNECT:
			reply = packets.NewControlPacket(packets.CONNACK)
		case packets.PINGREQ:
			reply = packets.NewControlPacket(packets.PINGRESP)
		case packets.DISCONNECT:
			return
		}
		if reply != nil {
			if _, err := reply.WriteTo(c); err != nil {
				return
			}
		}
	}
}

// Drop closes the live connection, and autopaho connects again on its own:
// a new TCP connection, and with TLS a new handshake.
func TestRedialerDropsTheConnectionAndAutopahoReconnects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan struct{}, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			go serveMQTT(c)
		}
	}()
	up, down := make(chan struct{}, 8), make(chan struct{}, 8)
	r := &Redialer{}
	if r.Drop() {
		t.Fatal("Drop reported a connection before any was made")
	}
	cm, err := Start(t.Context(), Config{
		URL: "mqtt://" + ln.Addr().String(), ClientID: "redial-test", Redialer: r,
		Backoff: backoff.Backoff{Base: time.Millisecond, Max: time.Millisecond},
		OnUp:    func(*autopaho.ConnectionManager, *paho.Connack) { up <- struct{}{} },
		OnDown:  func() { down <- struct{}{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Disconnect(context.Background()) }()
	wait := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	wait(accepted, "the first connection")
	wait(up, "the first connection to come up")
	if !r.Drop() {
		t.Fatal("Drop found no connection to close")
	}
	wait(down, "the connection to go down")
	wait(accepted, "a new connection")
	wait(up, "the new connection to come up")
}

// A service certificate must not be offered over a connection autopaho
// would make in plain text.
func TestRequireTLS(t *testing.T) {
	for raw, ok := range map[string]bool{
		"tls://mosquitto:8884":   true,
		"TLS://mosquitto:8884":   true,
		"mqtts://mosquitto:8884": true,
		"ssl://mosquitto:8884":   true,
		"mqtt://mosquitto:8884":  false,
		"tcp://mosquitto:8884":   false,
		"ws://mosquitto:8884":    false,
		"mosquitto:8884":         false,
		"tls://":                 false,
		"://bad":                 false,
	} {
		u, err := RequireTLS(raw)
		if (err == nil) != ok {
			t.Errorf("RequireTLS(%q) = %v, want ok %v", raw, err, ok)
		}
		if ok && err == nil && u.Hostname() != "mosquitto" {
			t.Errorf("RequireTLS(%q) host %q", raw, u.Hostname())
		}
		if !ok && err != nil && !strings.Contains(err.Error(), raw) {
			t.Errorf("RequireTLS(%q) error %q does not name the URL", raw, err)
		}
	}
}

// The receive maximum must always be sent: without it paho sizes a
// per-connection channel for 65,535 packets (512 KiB), which is what made
// 10,000 load-generator devices run out of memory.
func TestConnectAlwaysStatesAReceiveMaximum(t *testing.T) {
	for _, tc := range []struct {
		configured, want uint16
	}{
		{0, DefaultReceiveMaximum},
		{1000, 1000},
		{1, 1},
	} {
		for _, in := range []*paho.Connect{{}, {Properties: &paho.ConnectProperties{}}} {
			out, err := connectPacket(tc.configured)(in, nil)
			if err != nil {
				t.Fatal(err)
			}
			if out.Properties == nil || out.Properties.ReceiveMaximum == nil || *out.Properties.ReceiveMaximum != tc.want {
				t.Fatalf("configured %d: CONNECT properties %+v, want receive maximum %d", tc.configured, out.Properties, tc.want)
			}
		}
	}
}
