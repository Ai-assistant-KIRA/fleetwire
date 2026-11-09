package ingest

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Records of one device must stay in arrival order (it keeps the device's
// row lock uncontended), and nothing may be lost or repeated by the split.
func TestByDeviceKeepsEachDevicesOrder(t *testing.T) {
	var records []*kgo.Record
	for i, key := range []string{"a", "b", "a", "c", "b", "a", ""} {
		records = append(records, &kgo.Record{Key: []byte(key), Offset: int64(i)})
	}
	var got [][]int64
	for _, g := range byDevice(records) {
		var offsets []int64
		for _, r := range g {
			offsets = append(offsets, r.Offset)
		}
		got = append(got, offsets)
	}
	want := [][]int64{{0, 2, 5}, {1, 4}, {3}, {6}}
	if !slices.EqualFunc(got, want, slices.Equal[[]int64]) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	if byDevice(nil) != nil {
		t.Fatal("no records should give no groups")
	}
}
