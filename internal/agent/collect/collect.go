// Package collect holds the agent's reading sources.
//
// The built-in collectors read kernel interfaces directly (no gopsutil), so
// each reading maps to one file and one snap interface:
//
//	cpu      /proc/stat                                system-observe
//	memory   /proc/meminfo                             system-observe
//	load     /proc/loadavg                             system-observe
//	uptime   /proc/uptime                              system-observe
//	disk     statfs($SNAP_DATA/state)                  none
//	thermal  /sys/class/thermal/thermal_zone*/{type,temp}  hardware-observe
//
// New sensors implement Collector; the core does not change.
package collect

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Emit records one value. labels may be nil.
type Emit func(metric string, value float64, labels map[string]string)

// Collector is one source of readings. Collect must respect ctx.
type Collector interface {
	Name() string
	Collect(ctx context.Context, emit Emit) error
}

// Builtin returns every built-in collector. root is the filesystem root
// (normally "/"; tests point it at captured samples), dataDir the
// partition whose usage the disk collector reports.
func Builtin(root, dataDir string) []Collector {
	return []Collector{
		&CPU{Root: root},
		&Memory{Root: root},
		&Load{Root: root},
		&Uptime{Root: root},
		&Disk{Path: dataDir},
		&Thermal{Root: root},
	}
}

func readFile(root, rel string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", rel, err)
	}
	return b, nil
}

// CPU reports the share of non-idle time between two collections.
type CPU struct {
	Root string
	mu   sync.Mutex
	prev *cpuTimes
}

type cpuTimes struct{ busy, total uint64 }

// Name implements Collector.
func (*CPU) Name() string { return "cpu" }

// Collect implements Collector. The first call only takes the baseline.
func (c *CPU) Collect(_ context.Context, emit Emit) error {
	b, err := readFile(c.Root, "proc/stat")
	if err != nil {
		return err
	}
	cur, err := parseStat(b)
	if err != nil {
		return err
	}
	c.mu.Lock()
	prev := c.prev
	c.prev = &cur
	c.mu.Unlock()
	if prev == nil || cur.total <= prev.total || cur.busy < prev.busy {
		return nil
	}
	emit("cpu.utilization", float64(cur.busy-prev.busy)/float64(cur.total-prev.total), nil)
	return nil
}

// parseStat reads the aggregate "cpu" line. idle and iowait count as idle;
// guest time is already included in user and nice, so it is not added again.
func parseStat(b []byte) (cpuTimes, error) {
	line, _, _ := bytes.Cut(b, []byte("\n"))
	f := strings.Fields(string(line))
	if len(f) < 5 || f[0] != "cpu" {
		return cpuTimes{}, errors.New("proc/stat: no aggregate cpu line")
	}
	var v [8]uint64
	for i := 0; i < 8 && i+1 < len(f); i++ {
		n, err := strconv.ParseUint(f[i+1], 10, 64)
		if err != nil {
			return cpuTimes{}, fmt.Errorf("proc/stat: field %d: %w", i+1, err)
		}
		v[i] = n
	}
	// user nice system idle iowait irq softirq steal
	idle := v[3] + v[4]
	var total uint64
	for _, x := range v {
		total += x
	}
	return cpuTimes{busy: total - idle, total: total}, nil
}

// Memory reports used memory as MemTotal - MemAvailable, which is what the
// kernel considers reclaimable-aware usage.
type Memory struct{ Root string }

// Name implements Collector.
func (*Memory) Name() string { return "memory" }

// Collect implements Collector.
func (m *Memory) Collect(_ context.Context, emit Emit) error {
	b, err := readFile(m.Root, "proc/meminfo")
	if err != nil {
		return err
	}
	total, avail, err := parseMeminfo(b)
	if err != nil {
		return err
	}
	emit("memory.total_bytes", float64(total), nil)
	emit("memory.used_bytes", float64(total-avail), nil)
	emit("memory.used_ratio", float64(total-avail)/float64(total), nil)
	return nil
}

func parseMeminfo(b []byte) (total, avail uint64, err error) {
	found := 0
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 2 {
			continue
		}
		var dst *uint64
		switch f[0] {
		case "MemTotal:":
			dst = &total
		case "MemAvailable:":
			dst = &avail
		default:
			continue
		}
		n, perr := strconv.ParseUint(f[1], 10, 64)
		if perr != nil {
			return 0, 0, fmt.Errorf("proc/meminfo %s: %w", f[0], perr)
		}
		if len(f) > 2 && f[2] == "kB" {
			n *= 1024
		}
		*dst = n
		found++
	}
	if found != 2 || total == 0 || avail > total {
		return 0, 0, errors.New("proc/meminfo: MemTotal or MemAvailable missing or inconsistent")
	}
	return total, avail, nil
}

// Load reports the 1, 5 and 15 minute load averages.
type Load struct{ Root string }

// Name implements Collector.
func (*Load) Name() string { return "load" }

// Collect implements Collector.
func (l *Load) Collect(_ context.Context, emit Emit) error {
	b, err := readFile(l.Root, "proc/loadavg")
	if err != nil {
		return err
	}
	f := strings.Fields(string(b))
	if len(f) < 3 {
		return errors.New("proc/loadavg: too few fields")
	}
	for i, name := range []string{"load.avg1", "load.avg5", "load.avg15"} {
		v, err := strconv.ParseFloat(f[i], 64)
		if err != nil {
			return fmt.Errorf("proc/loadavg: %w", err)
		}
		emit(name, v, nil)
	}
	return nil
}

// Uptime reports seconds since boot.
type Uptime struct{ Root string }

// Name implements Collector.
func (*Uptime) Name() string { return "uptime" }

// Collect implements Collector.
func (u *Uptime) Collect(_ context.Context, emit Emit) error {
	b, err := readFile(u.Root, "proc/uptime")
	if err != nil {
		return err
	}
	f := strings.Fields(string(b))
	if len(f) < 1 {
		return errors.New("proc/uptime: empty")
	}
	v, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return fmt.Errorf("proc/uptime: %w", err)
	}
	emit("system.uptime_seconds", v, nil)
	return nil
}

// Disk reports the usage of the file system that holds Path.
type Disk struct{ Path string }

// Name implements Collector.
func (*Disk) Name() string { return "disk" }

// Collect implements Collector.
func (d *Disk) Collect(_ context.Context, emit Emit) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(d.Path, &st); err != nil {
		return fmt.Errorf("statfs %s: %w", d.Path, err)
	}
	bs := uint64(st.Bsize)
	total, free := st.Blocks*bs, st.Bavail*bs
	if total == 0 {
		return fmt.Errorf("statfs %s: zero-size file system", d.Path)
	}
	emit("disk.total_bytes", float64(total), nil)
	emit("disk.free_bytes", float64(free), nil)
	emit("disk.used_ratio", 1-float64(free)/float64(total), nil)
	return nil
}

// Thermal reports every thermal zone the kernel exposes, in degrees
// Celsius, labeled with the zone type. Hardware without zones (most VMs)
// emits nothing, which is not an error.
type Thermal struct{ Root string }

// Name implements Collector.
func (*Thermal) Name() string { return "thermal" }

// Collect implements Collector.
func (t *Thermal) Collect(_ context.Context, emit Emit) error {
	zones, err := filepath.Glob(filepath.Join(t.Root, "sys", "class", "thermal", "thermal_zone*"))
	if err != nil {
		return fmt.Errorf("thermal glob: %w", err)
	}
	sort.Strings(zones)
	var errs []error
	for _, z := range zones {
		typ, err := os.ReadFile(filepath.Join(z, "type"))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		raw, err := os.ReadFile(filepath.Join(z, "temp"))
		if err != nil {
			// Some zones exist but cannot be read (sensor powered down).
			errs = append(errs, err)
			continue
		}
		milli, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s/temp: %w", filepath.Base(z), err))
			continue
		}
		zone := sanitizeLabel(strings.TrimSpace(string(typ)))
		emit("thermal.celsius", float64(milli)/1000, map[string]string{"zone": zone, "id": strings.TrimPrefix(filepath.Base(z), "thermal_zone")})
	}
	return errors.Join(errs...)
}

func sanitizeLabel(s string) string {
	if len(s) > 128 {
		s = s[:128]
	}
	return s
}

// Synthetic generates values for the load generator: a sine wave plus
// noise per metric. It is the only generated part of a load-generator
// device; everything else is the real agent code.
type Synthetic struct {
	// Readings is how many values each collection emits.
	Readings int
	// Seed makes every device's waves different.
	Seed  uint64
	mu    sync.Mutex
	count uint64
}

// Name implements Collector.
func (*Synthetic) Name() string { return "synthetic" }

// Collect implements Collector.
func (s *Synthetic) Collect(_ context.Context, emit Emit) error {
	s.mu.Lock()
	s.count++
	n := s.count
	s.mu.Unlock()
	for i := 0; i < s.Readings; i++ {
		phase := float64(s.Seed%1000)/1000*2*math.Pi + float64(i)
		noise := float64((s.Seed*31+n*17+uint64(i)*7)%100)/100 - 0.5
		emit("synthetic.value", 50+40*math.Sin(float64(n)/30+phase)+noise, map[string]string{"series": strconv.Itoa(i)})
	}
	return nil
}
