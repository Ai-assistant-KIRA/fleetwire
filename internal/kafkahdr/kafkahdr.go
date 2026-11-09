// Package kafkahdr names the Kafka record headers shared by the bridge and
// ingest, and adapts them for OpenTelemetry context propagation.
package kafkahdr

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// Header keys.
const (
	DeviceID   = "fw-device-id"
	ReceivedAt = "fw-received-at" // bridge receive time, Unix nanoseconds
	DLQReason  = "fw-dlq-reason"
	DLQDetail  = "fw-dlq-detail"
)

// Get returns the first header with key, or nil.
func Get(r *kgo.Record, key string) []byte {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}

// Carrier lets propagation.TextMapPropagator read and write record headers.
type Carrier struct{ Record *kgo.Record }

// Get implements propagation.TextMapCarrier.
func (c Carrier) Get(key string) string { return string(Get(c.Record, key)) }

// Set implements propagation.TextMapCarrier, replacing an existing key.
func (c Carrier) Set(key, value string) {
	for i, h := range c.Record.Headers {
		if h.Key == key {
			c.Record.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Record.Headers = append(c.Record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys implements propagation.TextMapCarrier.
func (c Carrier) Keys() []string {
	out := make([]string, len(c.Record.Headers))
	for i, h := range c.Record.Headers {
		out[i] = h.Key
	}
	return out
}
