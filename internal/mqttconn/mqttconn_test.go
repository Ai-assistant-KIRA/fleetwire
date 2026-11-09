package mqttconn

import (
	"testing"

	"github.com/eclipse/paho.golang/paho"
)

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
