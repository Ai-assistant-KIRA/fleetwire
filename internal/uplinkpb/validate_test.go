package uplinkpb

import (
	"errors"
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
)

func validBatch() *fleetwirev1.Batch {
	return &fleetwirev1.Batch{
		SchemaVersion: 1,
		FirstSeq:      7,
		Readings: []*fleetwirev1.Reading{
			{CollectedUnixNano: 1, Collector: "cpu", Metric: "cpu.utilization", Value: 0.5},
			{CollectedUnixNano: 2, Collector: "thermal", Metric: "thermal.celsius", Value: 41, Labels: map[string]string{"zone": "x86_pkg_temp"}},
		},
		Gaps: []*fleetwirev1.Gap{{FromSeq: 1, ToSeq: 3, Reason: fleetwirev1.GapReason_GAP_REASON_DISK_BUDGET}},
	}
}

func TestValidBatch(t *testing.T) {
	raw, err := proto.Marshal(validBatch())
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecodeAndValidate(raw)
	if err != nil {
		t.Fatalf("DecodeAndValidate = %v", err)
	}
	if LastSeq(b) != 8 {
		t.Fatalf("LastSeq = %d, want 8", LastSeq(b))
	}
}

func TestEveryRule(t *testing.T) {
	manyLabels := map[string]string{}
	for i := 0; i < MaxLabels+1; i++ {
		manyLabels[strings.Repeat("k", i+1)] = "v"
	}
	tests := []struct {
		reason Reason
		mutate func(*fleetwirev1.Batch)
	}{
		{ReasonSchemaVersion, func(b *fleetwirev1.Batch) { b.SchemaVersion = 2 }},
		{ReasonEmpty, func(b *fleetwirev1.Batch) { b.Readings, b.Gaps = nil, nil }},
		{ReasonFirstSeq, func(b *fleetwirev1.Batch) { b.FirstSeq = 0 }},
		{ReasonSeqOverflow, func(b *fleetwirev1.Batch) { b.FirstSeq = math.MaxUint64 }},
		{ReasonTooManyReadings, func(b *fleetwirev1.Batch) {
			r := b.Readings[0]
			b.Readings = nil
			for i := 0; i <= MaxReadings; i++ {
				b.Readings = append(b.Readings, r)
			}
		}},
		{ReasonTooManyGaps, func(b *fleetwirev1.Batch) {
			for i := 0; i <= MaxGaps; i++ {
				b.Gaps = append(b.Gaps, &fleetwirev1.Gap{FromSeq: 1, ToSeq: 1})
			}
		}},
		{ReasonGapRange, func(b *fleetwirev1.Batch) { b.Gaps[0].FromSeq, b.Gaps[0].ToSeq = 5, 4 }},
		{ReasonGapRange, func(b *fleetwirev1.Batch) { b.Gaps[0].FromSeq = 0 }},
		{ReasonName, func(b *fleetwirev1.Batch) { b.Readings[0].Metric = "CPU Util" }},
		{ReasonName, func(b *fleetwirev1.Batch) { b.Readings[0].Collector = "" }},
		{ReasonName, func(b *fleetwirev1.Batch) { b.Readings[0].Metric = strings.Repeat("a", 65) }},
		{ReasonValue, func(b *fleetwirev1.Batch) { b.Readings[0].Value = math.NaN() }},
		{ReasonValue, func(b *fleetwirev1.Batch) { b.Readings[0].Value = math.Inf(-1) }},
		{ReasonTimestamp, func(b *fleetwirev1.Batch) { b.Readings[0].CollectedUnixNano = 0 }},
		{ReasonLabels, func(b *fleetwirev1.Batch) { b.Readings[0].Labels = manyLabels }},
		{ReasonLabels, func(b *fleetwirev1.Batch) { b.Readings[0].Labels = map[string]string{"k": strings.Repeat("v", 129)} }},
		{ReasonLabels, func(b *fleetwirev1.Batch) { b.Readings[0].Labels = map[string]string{"": "v"} }},
	}
	for _, tt := range tests {
		t.Run(string(tt.reason), func(t *testing.T) {
			b := validBatch()
			tt.mutate(b)
			var ve *ValidationError
			if err := Validate(b); !errors.As(err, &ve) || ve.Reason != tt.reason {
				t.Fatalf("Validate = %v, want reason %s", err, tt.reason)
			}
		})
	}
}

func TestGapOnlyBatchIsValid(t *testing.T) {
	b := validBatch()
	b.Readings, b.FirstSeq = nil, 0
	if err := Validate(b); err != nil {
		t.Fatalf("gap-only batch rejected: %v", err)
	}
}

func TestDecodeErrors(t *testing.T) {
	var ve *ValidationError
	if _, err := DecodeAndValidate(make([]byte, MaxMessageBytes+1)); !errors.As(err, &ve) || ve.Reason != ReasonTooLarge {
		t.Fatalf("oversized: %v", err)
	}
	if _, err := DecodeAndValidate([]byte{0xff, 0xff, 0xff}); !errors.As(err, &ve) || ve.Reason != ReasonDecode {
		t.Fatalf("garbage: %v", err)
	}
}

func FuzzDecodeAndValidate(f *testing.F) {
	raw, _ := proto.Marshal(validBatch())
	f.Add(raw)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01, 0x10, 0x00})
	f.Fuzz(func(t *testing.T, raw []byte) {
		b, err := DecodeAndValidate(raw)
		if err != nil {
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Reason == "" {
				t.Fatalf("error without a reason: %v", err)
			}
			return
		}
		// Whatever passes must be safe for ingest to use as is.
		if len(b.GetReadings()) > 0 {
			if b.GetFirstSeq() < 1 || LastSeq(b) < b.GetFirstSeq() {
				t.Fatalf("bad seq range accepted: first %d, n %d", b.GetFirstSeq(), len(b.GetReadings()))
			}
		}
		for _, g := range b.GetGaps() {
			if g.GetFromSeq() < 1 || g.GetFromSeq() > g.GetToSeq() {
				t.Fatalf("bad gap accepted: %v", g)
			}
		}
		for _, r := range b.GetReadings() {
			if math.IsNaN(r.GetValue()) || math.IsInf(r.GetValue(), 0) {
				t.Fatal("non-finite value accepted")
			}
		}
	})
}
