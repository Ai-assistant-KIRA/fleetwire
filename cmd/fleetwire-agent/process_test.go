package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func descriptors(c prometheus.Collector) []string {
	ch := make(chan *prometheus.Desc, 32)
	c.Describe(ch)
	close(ch)
	var out []string
	for d := range ch {
		out = append(out, d.String())
	}
	slices.Sort(out)
	return out
}

// The snap's process metrics keep the standard names and help, so
// dashboards and docs/reference/metrics.md stay right; only the network
// counters are left out.
func TestSnapProcessMetricsAreTheStandardOnesWithoutNetwork(t *testing.T) {
	var want []string
	for _, d := range descriptors(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})) {
		if !strings.Contains(d, `fqName: "process_network_`) {
			want = append(want, d)
		}
	}
	if got := descriptors(newProcessCollector()); !slices.Equal(got, want) {
		t.Fatalf("descriptors:\ngot  %v\nwant %v", got, want)
	}
}

func gather(t *testing.T, g prometheus.Gatherer) map[string]float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		m := mf.GetMetric()[0]
		out[mf.GetName()] = m.GetGauge().GetValue() + m.GetCounter().GetValue()
	}
	return out
}

func TestSnapRegistryReplacesTheProcessCollector(t *testing.T) {
	got := gather(t, newRegistry(true))
	for name := range got {
		if strings.HasPrefix(name, "process_network_") {
			t.Errorf("the snap exports %s", name)
		}
	}
	std := prometheus.NewRegistry()
	std.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	want := gather(t, std)
	for _, name := range []string{"process_max_fds", "process_start_time_seconds", "process_virtual_memory_max_bytes"} {
		if got[name] != want[name] || got[name] == 0 {
			t.Errorf("%s = %v, the standard collector says %v", name, got[name], want[name])
		}
	}
	for _, name := range []string{"process_cpu_seconds_total", "process_open_fds", "process_resident_memory_bytes", "process_virtual_memory_bytes", "fleetwire_build_info", "go_goroutines"} {
		if _, ok := got[name]; !ok {
			t.Errorf("the snap does not export %s", name)
		}
	}
	// Outside the snap the registry keeps the standard collector.
	if !newRegistry(false).Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})) {
		t.Error("the plain binary lost the standard process collector")
	}
}
