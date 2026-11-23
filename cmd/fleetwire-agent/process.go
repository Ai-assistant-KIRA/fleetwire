package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/procfs"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
)

// newRegistry returns the agent's metrics registry. In the snap, the
// standard process collector is replaced: it also reads
// /proc/<pid>/net/netstat for process_network_receive_bytes_total and
// process_network_transmit_bytes_total, the snap's AppArmor profile denies
// that read, and the kernel logged the denial on every scrape. The two
// metrics were never exported from the snap, since the read failed.
func newRegistry(inSnap bool) *prometheus.Registry {
	reg := telemetry.NewRegistry("fleetwire-agent")
	if inSnap {
		// A registry finds a collector by its descriptors, so a new
		// standard collector names the one NewRegistry added.
		if !reg.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})) {
			panic("telemetry.NewRegistry has no standard process collector to replace")
		}
		reg.MustRegister(newProcessCollector())
	}
	return reg
}

// processCollector exports the standard process metrics of this process,
// with the same names, help and values as client_golang's, except the
// two network counters. Errors leave a metric out, as there.
type processCollector struct {
	cpu, openFDs, maxFDs, vsize, maxVsize, rss, start *prometheus.Desc
}

func newProcessCollector() *processCollector {
	d := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	return &processCollector{
		cpu:      d("process_cpu_seconds_total", "Total user and system CPU time spent in seconds."),
		openFDs:  d("process_open_fds", "Number of open file descriptors."),
		maxFDs:   d("process_max_fds", "Maximum number of open file descriptors."),
		vsize:    d("process_virtual_memory_bytes", "Virtual memory size in bytes."),
		maxVsize: d("process_virtual_memory_max_bytes", "Maximum amount of virtual memory available in bytes."),
		rss:      d("process_resident_memory_bytes", "Resident memory size in bytes."),
		start:    d("process_start_time_seconds", "Start time of the process since unix epoch in seconds."),
	}
}

// Describe implements prometheus.Collector.
func (c *processCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.cpu, c.openFDs, c.maxFDs, c.vsize, c.maxVsize, c.rss, c.start} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *processCollector) Collect(ch chan<- prometheus.Metric) {
	p, err := procfs.Self()
	if err != nil {
		return
	}
	if st, err := p.Stat(); err == nil {
		ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.CounterValue, st.CPUTime())
		ch <- prometheus.MustNewConstMetric(c.vsize, prometheus.GaugeValue, float64(st.VirtualMemory()))
		ch <- prometheus.MustNewConstMetric(c.rss, prometheus.GaugeValue, float64(st.ResidentMemory()))
		if t, err := st.StartTime(); err == nil {
			ch <- prometheus.MustNewConstMetric(c.start, prometheus.GaugeValue, t)
		}
	}
	if n, err := p.FileDescriptorsLen(); err == nil {
		ch <- prometheus.MustNewConstMetric(c.openFDs, prometheus.GaugeValue, float64(n))
	}
	if l, err := p.Limits(); err == nil {
		ch <- prometheus.MustNewConstMetric(c.maxFDs, prometheus.GaugeValue, float64(l.OpenFiles))
		ch <- prometheus.MustNewConstMetric(c.maxVsize, prometheus.GaugeValue, float64(l.AddressSpace))
	}
}
