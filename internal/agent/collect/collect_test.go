package collect

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
)

type sample struct {
	metric string
	value  float64
	labels map[string]string
}

func collect(t *testing.T, c Collector) []sample {
	t.Helper()
	var out []sample
	if err := c.Collect(context.Background(), func(m string, v float64, l map[string]string) {
		out = append(out, sample{m, v, l})
	}); err != nil {
		t.Fatalf("%s: %v", c.Name(), err)
	}
	return out
}

// fakeRoot builds a root with /proc files from a captured sample dir.
func fakeRoot(t *testing.T, src string, stat string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "proc"), 0o755); err != nil {
		t.Fatal(err)
	}
	for dst, from := range map[string]string{"stat": stat, "meminfo": "meminfo", "loadavg": "loadavg", "uptime": "uptime"} {
		b, err := os.ReadFile(filepath.Join("testdata", src, from))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "proc", dst), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCapturedSamples(t *testing.T) {
	for _, src := range []string{"vm", "pc"} {
		t.Run(src, func(t *testing.T) {
			root := fakeRoot(t, src, "stat1")
			cpu := &CPU{Root: root}
			if got := collect(t, cpu); len(got) != 0 {
				t.Fatalf("first CPU sample emitted %v", got)
			}
			b, _ := os.ReadFile(filepath.Join("testdata", src, "stat2"))
			if err := os.WriteFile(filepath.Join(root, "proc", "stat"), b, 0o644); err != nil {
				t.Fatal(err)
			}
			got := collect(t, cpu)
			if len(got) != 1 || got[0].metric != "cpu.utilization" || got[0].value < 0 || got[0].value > 1 {
				t.Fatalf("cpu = %v", got)
			}
			mem := collect(t, &Memory{Root: root})
			if len(mem) != 3 || mem[2].metric != "memory.used_ratio" || mem[2].value <= 0 || mem[2].value >= 1 || mem[0].value < 1<<20 {
				t.Fatalf("memory = %v", mem)
			}
			if load := collect(t, &Load{Root: root}); len(load) != 3 || load[0].value < 0 {
				t.Fatalf("load = %v", load)
			}
			if up := collect(t, &Uptime{Root: root}); len(up) != 1 || up[0].value <= 0 {
				t.Fatalf("uptime = %v", up)
			}
		})
	}
}

func TestParseStatKnownValues(t *testing.T) {
	// user nice system idle iowait irq softirq steal
	got, err := parseStat([]byte("cpu  100 0 50 800 50 0 0 0 0 0\ncpu0 1 2 3 4\n"))
	if err != nil || got.total != 1000 || got.busy != 150 {
		t.Fatalf("parseStat = %+v, %v", got, err)
	}
	if _, err := parseStat([]byte("intr 1 2 3\n")); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestMeminfoInconsistent(t *testing.T) {
	if _, _, err := parseMeminfo([]byte("MemTotal: 100 kB\nMemAvailable: 200 kB\n")); err == nil {
		t.Fatal("available above total accepted")
	}
	if _, _, err := parseMeminfo([]byte("MemFree: 100 kB\n")); err == nil {
		t.Fatal("missing fields accepted")
	}
}

func TestThermalFromCapturedSysfs(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "sys", "class", "thermal")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	zones, _ := filepath.Glob("testdata/pc/thermal/thermal_zone*")
	if len(zones) == 0 {
		t.Fatal("no captured zones")
	}
	for _, z := range zones {
		if err := os.CopyFS(filepath.Join(dst, filepath.Base(z)), os.DirFS(z)); err != nil {
			t.Fatal(err)
		}
	}
	got := collect(t, &Thermal{Root: root})
	if len(got) != len(zones) {
		t.Fatalf("thermal = %v, want %d zones", got, len(zones))
	}
	for _, s := range got {
		if s.metric != "thermal.celsius" || s.labels["zone"] == "" || s.value < -40 || s.value > 150 {
			t.Fatalf("implausible thermal sample %+v", s)
		}
	}
	// No zones at all (a VM) is not an error.
	if got := collect(t, &Thermal{Root: t.TempDir()}); len(got) != 0 {
		t.Fatalf("empty sysfs emitted %v", got)
	}
}

func TestDisk(t *testing.T) {
	got := collect(t, &Disk{Path: t.TempDir()})
	if len(got) != 3 || got[2].value < 0 || got[2].value > 1 {
		t.Fatalf("disk = %v", got)
	}
	if err := (&Disk{Path: "/does/not/exist"}).Collect(context.Background(), func(string, float64, map[string]string) {}); err == nil {
		t.Fatal("statfs of a missing path succeeded")
	}
}

func TestSyntheticIsDeterministicAndFinite(t *testing.T) {
	a, b := &Synthetic{Readings: 3, Seed: 7}, &Synthetic{Readings: 3, Seed: 7}
	ga, gb := collect(t, a), collect(t, b)
	if len(ga) != 3 {
		t.Fatalf("synthetic = %v", ga)
	}
	for i := range ga {
		if ga[i].value != gb[i].value || math.IsNaN(ga[i].value) {
			t.Fatalf("synthetic not deterministic: %v vs %v", ga, gb)
		}
	}
}
