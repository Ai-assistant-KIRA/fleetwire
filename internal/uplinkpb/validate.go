// Package uplinkpb decodes and validates uplink batches.
//
// Ingest runs every Kafka record through DecodeAndValidate before it touches
// the database. A batch that fails gets a reason code, which becomes the
// fw-dlq-reason header on the dead-letter topic, so an operator can tell a
// buggy agent from a malicious one without decoding anything.
package uplinkpb

import (
	"fmt"
	"math"
	"regexp"

	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
)

// Limits on one uplink batch.
const (
	SchemaVersion   = 1
	MaxMessageBytes = 256 << 10
	MaxReadings     = 1000
	MaxGaps         = 1000
	MaxLabels       = 16
	MaxLabelBytes   = 128
	// MaxSeq is the highest seq ingest can store. Seqs are PostgreSQL
	// bigint columns, and the overlap constraints build
	// int8range(lo, hi, '[]'), which PostgreSQL stores as [lo, hi+1), so
	// hi+1 must fit in a bigint too. A larger seq is refused here, so it
	// goes to the dead-letter topic without a database round trip.
	MaxSeq = math.MaxInt64 - 1
)

var nameRE = regexp.MustCompile(`^[a-z0-9_.]{1,64}$`)

// Reason is a stable, machine-readable validation failure code.
type Reason string

// Reasons. They appear in DLQ headers and metrics; do not rename them.
const (
	ReasonTooLarge        Reason = "too_large"
	ReasonDecode          Reason = "decode"
	ReasonSchemaVersion   Reason = "schema_version"
	ReasonEmpty           Reason = "empty"
	ReasonFirstSeq        Reason = "first_seq"
	ReasonTooManyReadings Reason = "too_many_readings"
	ReasonTooManyGaps     Reason = "too_many_gaps"
	ReasonSeqOverflow     Reason = "seq_overflow"
	ReasonGapRange        Reason = "gap_range"
	ReasonName            Reason = "name"
	ReasonLabels          Reason = "labels"
	ReasonValue           Reason = "value"
	ReasonTimestamp       Reason = "timestamp"
)

// ValidationError is returned for every invalid batch.
type ValidationError struct {
	Reason Reason
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid batch (%s): %s", e.Reason, e.Detail)
}

func invalid(r Reason, format string, args ...any) error {
	return &ValidationError{Reason: r, Detail: fmt.Sprintf(format, args...)}
}

// DecodeAndValidate parses a raw MQTT payload and validates it.
func DecodeAndValidate(raw []byte) (*fleetwirev1.Batch, error) {
	if len(raw) > MaxMessageBytes {
		return nil, invalid(ReasonTooLarge, "%d bytes, limit %d", len(raw), MaxMessageBytes)
	}
	var b fleetwirev1.Batch
	if err := proto.Unmarshal(raw, &b); err != nil {
		return nil, invalid(ReasonDecode, "%v", err)
	}
	if err := Validate(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

// Validate checks every rule that does not need the database.
func Validate(b *fleetwirev1.Batch) error {
	if b.GetSchemaVersion() != SchemaVersion {
		return invalid(ReasonSchemaVersion, "version %d, want %d", b.GetSchemaVersion(), SchemaVersion)
	}
	n := len(b.GetReadings())
	if n == 0 && len(b.GetGaps()) == 0 {
		return invalid(ReasonEmpty, "no readings and no gaps")
	}
	if n > MaxReadings {
		return invalid(ReasonTooManyReadings, "%d readings, limit %d", n, MaxReadings)
	}
	if len(b.GetGaps()) > MaxGaps {
		return invalid(ReasonTooManyGaps, "%d gaps, limit %d", len(b.GetGaps()), MaxGaps)
	}
	if n > 0 {
		if b.GetFirstSeq() < 1 {
			return invalid(ReasonFirstSeq, "first_seq must be at least 1")
		}
		if b.GetFirstSeq() > MaxSeq-uint64(n)+1 {
			return invalid(ReasonSeqOverflow, "first_seq %d + %d readings goes past seq %d", b.GetFirstSeq(), n, uint64(MaxSeq))
		}
	}
	for i, g := range b.GetGaps() {
		if g.GetFromSeq() < 1 || g.GetFromSeq() > g.GetToSeq() {
			return invalid(ReasonGapRange, "gap %d: %d-%d", i, g.GetFromSeq(), g.GetToSeq())
		}
		if g.GetToSeq() > MaxSeq {
			return invalid(ReasonSeqOverflow, "gap %d: %d-%d goes past seq %d", i, g.GetFromSeq(), g.GetToSeq(), uint64(MaxSeq))
		}
	}
	for i, r := range b.GetReadings() {
		if err := validateReading(i, r); err != nil {
			return err
		}
	}
	return nil
}

func validateReading(i int, r *fleetwirev1.Reading) error {
	if !nameRE.MatchString(r.GetCollector()) {
		return invalid(ReasonName, "reading %d: collector %q", i, r.GetCollector())
	}
	if !nameRE.MatchString(r.GetMetric()) {
		return invalid(ReasonName, "reading %d: metric %q", i, r.GetMetric())
	}
	if v := r.GetValue(); math.IsNaN(v) || math.IsInf(v, 0) {
		return invalid(ReasonValue, "reading %d: value %v is not finite", i, v)
	}
	if r.GetCollectedUnixNano() <= 0 {
		return invalid(ReasonTimestamp, "reading %d: collected_unix_nano %d", i, r.GetCollectedUnixNano())
	}
	if len(r.GetLabels()) > MaxLabels {
		return invalid(ReasonLabels, "reading %d: %d labels, limit %d", i, len(r.GetLabels()), MaxLabels)
	}
	for k, v := range r.GetLabels() {
		if k == "" || len(k) > MaxLabelBytes || len(v) > MaxLabelBytes {
			return invalid(ReasonLabels, "reading %d: label %q too long or empty", i, k)
		}
	}
	return nil
}

// ValidateReading applies the per-reading rules. The agent checks every
// reading with it before writing it to the local log: one bad reading would
// otherwise make ingest reject every batch that carries it, and the device
// would resend that batch forever.
func ValidateReading(r *fleetwirev1.Reading) error { return validateReading(0, r) }

// LastSeq is the seq of the last reading. Call it only on a valid batch with
// readings.
func LastSeq(b *fleetwirev1.Batch) uint64 {
	return b.GetFirstSeq() + uint64(len(b.GetReadings())) - 1
}
