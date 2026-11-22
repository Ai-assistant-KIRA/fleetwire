package mqttconn

import (
	"math"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
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
