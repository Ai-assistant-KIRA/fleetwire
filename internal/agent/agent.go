// Package agent is the device runtime: it wires the collectors, the local
// log, the uplink, the MQTT connection, enrollment and certificate renewal.
// cmd/fleetwire-agent runs one Device; the load generator runs thousands,
// built from exactly the same code.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/uplink"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/identity"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/mqttconn"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// droppedDiskBudget is the reason label of readings the WAL dropped to stay
// within its disk budget.
const droppedDiskBudget = "disk_budget"

// Metrics are shared by every Device in a process.
type Metrics struct {
	Collected       *prometheus.CounterVec
	CollectorErrors *prometheus.CounterVec
	Dropped         *prometheus.CounterVec
	AppendErrors    prometheus.Counter
	WALBytes        prometheus.Gauge
	WALSegments     prometheus.Gauge
	Renewals        *prometheus.CounterVec
	Uplink          *uplink.Metrics
	MQTT            *mqttconn.Metrics
}

// NewMetrics registers the agent metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Collected: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_agent_readings_collected_total",
			Help: "Readings written to the local log."}, []string{"collector"}),
		CollectorErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_agent_collector_errors_total",
			Help: "Collector failures, timeouts and invalid readings."}, []string{"collector"}),
		Dropped: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_agent_readings_dropped_total",
			Help: "Readings dropped before they were sent (reported to the server as gaps)."}, []string{"reason"}),
		AppendErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_wal_append_errors_total",
			Help: "Readings that could not be written to the local log."}),
		WALBytes:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_agent_wal_bytes", Help: "Size of the local log."}),
		WALSegments: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_agent_wal_segments", Help: "Segment files in the local log."}),
		Renewals: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_agent_cert_renewals_total",
			Help: "Certificate renewal attempts."}, []string{"result"}),
		Uplink: uplink.NewMetrics(reg),
		MQTT:   mqttconn.NewMetrics(reg, "fleetwire_agent"),
	}
	reg.MustRegister(m.Collected, m.CollectorErrors, m.Dropped, m.AppendErrors, m.WALBytes, m.WALSegments, m.Renewals)
	// Known label values start at 0, so dashboards and rate() see the series
	// before the first event.
	m.Dropped.WithLabelValues(droppedDiskBudget)
	m.Renewals.WithLabelValues("ok")
	m.Renewals.WithLabelValues("error")
	return m
}

// Health reports the device's state to the platform (snapctl set-health).
type Health interface {
	Set(status, message string)
}

// NoHealth discards health reports (outside a snap).
type NoHealth struct{}

// Set implements Health.
func (NoHealth) Set(string, string) {}

// SnapHealth reports through `snapctl set-health`, only when it changes.
type SnapHealth struct {
	Log  *slog.Logger
	mu   sync.Mutex
	last string
}

// Set implements Health. Messages must be 7 to 70 characters for snapd.
func (h *SnapHealth) Set(status, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := status + "|" + message
	if key == h.last {
		return
	}
	h.last = key
	args := []string{"set-health", status}
	if status != "okay" {
		args = append(args, message)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "snapctl", args...).CombinedOutput(); err != nil { //nolint:gosec // G204: fixed program, status and message from this package
		h.Log.Warn("snapctl set-health failed", "err", err, "output", string(out))
	}
}

// Options configure a Device.
type Options struct {
	// Dir holds identity/ and wal/.
	Dir        string
	Config     config.Config
	Collectors []collect.Collector
	Metrics    *Metrics
	Logger     *slog.Logger
	Health     Health
	Version    string
	// EnrollRetry is the wait between enrollment attempts (default 30 s).
	EnrollRetry time.Duration
}

// Device is one running agent.
type Device struct {
	opts      Options
	id        *identity.Identity
	log       *wal.Log
	up        *uplink.Uplink
	lg        *slog.Logger
	stop      chan struct{}
	stopOnce  sync.Once
	connected atomic.Bool
	lastStats wal.Stats
	statsMu   sync.Mutex
}

// Open loads the device's identity and recovers its local log.
func Open(opts Options) (*Device, error) {
	if opts.Health == nil {
		opts.Health = NoHealth{}
	}
	if opts.EnrollRetry <= 0 {
		opts.EnrollRetry = 30 * time.Second
	}
	lg := opts.Logger
	id, err := identity.Open(filepath.Join(opts.Dir, "identity"))
	if err != nil {
		return nil, err
	}
	c := opts.Config
	l, rec, err := wal.Open(wal.Options{
		Dir: filepath.Join(opts.Dir, "wal"), SegmentSize: c.WALSegmentSize, MaxSize: c.WALMaxSize,
		Sync: c.WALSync, SyncInterval: c.WALSyncInterval, Logger: lg,
	})
	if err != nil {
		return nil, fmt.Errorf("open local log: %w", err)
	}
	if id.Enrolled() {
		devID := id.Info().DeviceID.String()
		if st := l.State(); st.DeviceID != "" && st.DeviceID != devID {
			_ = l.Close()
			return nil, fmt.Errorf("local log belongs to device %s, but the identity is %s; refusing to mix them", st.DeviceID, devID)
		}
		l.SetDeviceID(devID)
		lg = lg.With("device_id", devID)
	}
	lg.Info("local log recovered", "segments", rec.Segments, "records", rec.Records, "next_seq", rec.NextSeq,
		"truncated_bytes", rec.TruncatedBytes, "new_gaps", len(rec.NewGaps), "sync", c.WALSync.String())
	d := &Device{opts: opts, id: id, log: l, lg: lg, stop: make(chan struct{})}
	return d, nil
}

// StopCollecting stops new readings; sending continues (load-generator drain).
func (d *Device) StopCollecting() { d.stopOnce.Do(func() { close(d.stop) }) }

// Run collects and sends until ctx ends, then shuts down in order: stop
// collecting, make the log durable, stop the uplink, disconnect, persist.
func (d *Device) Run(ctx context.Context) error {
	c := d.opts.Config
	collectCtx, stopCollect := context.WithCancel(ctx)
	var wg sync.WaitGroup
	sampler := NewSampler(d.opts.Collectors, c.CollectInterval/2, d.opts.Metrics, d.lg)
	wg.Go(func() { d.collectLoop(collectCtx, sampler) })
	wg.Go(func() { d.statsLoop(collectCtx) })

	err := d.enroll(ctx)
	if err == nil {
		err = d.connectAndSend(ctx)
	}
	stopCollect()
	wg.Wait()
	if serr := d.log.Sync(); serr != nil && !errors.Is(serr, wal.ErrClosed) {
		d.lg.Error("final fsync failed", "err", serr)
	}
	if d.up != nil {
		d.up.Release()
	}
	if cerr := d.log.Close(); cerr != nil {
		return errors.Join(err, cerr)
	}
	d.updateStats()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (d *Device) enroll(ctx context.Context) error {
	c := d.opts.Config
	for !d.id.Enrolled() {
		if c.EnrollToken == "" || c.EnrollURL == "" {
			d.opts.Health.Set("waiting", "not enrolled: set enroll.url and enroll.token")
		} else {
			err := d.id.Enroll(ctx, c.EnrollURL, c.EnrollToken, c.EnrollName, []byte(c.EnrollCABundle))
			if err == nil {
				devID := d.id.Info().DeviceID.String()
				d.log.SetDeviceID(devID)
				d.lg = d.lg.With("device_id", devID)
				d.lg.Info("enrolled", "broker", d.id.Info().BrokerURL)
				break
			}
			if errors.Is(err, identity.ErrPermanent) {
				d.opts.Health.Set("blocked", "enrollment refused: set a new enroll.token")
				d.lg.Error("enrollment refused; waiting for a new token", "err", err)
			} else {
				d.opts.Health.Set("waiting", "enrollment failed; retrying")
				d.lg.Warn("enrollment failed; retrying", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.opts.EnrollRetry):
		}
	}
	return nil
}

type mqttPublisher struct {
	cm    *autopaho.ConnectionManager
	topic string
}

func (p *mqttPublisher) Publish(ctx context.Context, payload []byte) error {
	_, err := p.cm.Publish(ctx, &paho.Publish{QoS: 1, Topic: p.topic, Payload: payload})
	if errors.Is(err, autopaho.ConnectionDownError) {
		return uplink.ErrNotConnected
	}
	return err
}

func (d *Device) connectAndSend(ctx context.Context) error {
	c := d.opts.Config
	info := d.id.Info()
	broker := info.BrokerURL
	if c.BrokerURL != "" {
		broker = c.BrokerURL
	}
	u, err := url.Parse(broker)
	if err != nil {
		return fmt.Errorf("broker url %q: %w", broker, err)
	}
	upTopic := "fw/v1/d/" + info.DeviceID.String() + "/up"
	ackTopic := "fw/v1/d/" + info.DeviceID.String() + "/ack"

	upCtx, stopUplink := context.WithCancel(context.WithoutCancel(ctx))
	defer stopUplink()
	pub := &mqttPublisher{topic: upTopic}
	d.up = uplink.New(uplink.Config{
		BatchSize: c.BatchSize, BatchWait: c.BatchWait, MaxInflight: c.MaxInflight,
		AckTimeout: c.AckTimeout, AgentVersion: d.opts.Version,
	}, d.log, pub, d.opts.Metrics.Uplink, d.lg)

	connCtx, stopConn := context.WithCancel(context.WithoutCancel(ctx))
	defer stopConn()
	cm, err := mqttconn.Start(connCtx, mqttconn.Config{
		URL: broker, ClientID: info.DeviceID.String(), TLS: d.id.ClientTLS(u.Hostname()),
		SessionExpiry: time.Hour, KeepAlive: 30 * time.Second,
		Backoff: backoff.Backoff{Base: time.Second, Max: time.Minute},
		Logger:  d.lg, Metrics: d.opts.Metrics.MQTT,
		OnUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			// OnUp must not block, so the subscription runs on its own.
			go d.subscribe(connCtx, cm, ackTopic)
		},
		OnDown: func() {
			d.connected.Store(false)
			d.up.Disconnected()
			d.opts.Health.Set("waiting", "broker unreachable; readings are kept on disk")
		},
		OnPublish: func(pr paho.PublishReceived) (bool, error) {
			if pr.Packet.Topic != ackTopic {
				return false, nil
			}
			if err := d.up.HandleAck(pr.Packet.Payload); err != nil {
				d.lg.Error("malformed ack", "err", err)
			}
			return true, nil
		},
	})
	if err != nil {
		return err
	}
	pub.cm = cm

	var wg sync.WaitGroup
	upDone := make(chan error, 1)
	go func() { upDone <- d.up.Run(upCtx) }()
	wg.Go(func() { d.renewLoop(connCtx) })

	<-ctx.Done()
	// Shutdown order: collecting stops in Run; here the uplink stops and
	// saves its ack, then the connection closes.
	stopUplink()
	uerr := <-upDone
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := cm.Disconnect(dctx); err != nil && !errors.Is(err, autopaho.ConnectionDownError) {
		d.lg.Debug("mqtt disconnect", "err", err)
	}
	stopConn()
	wg.Wait()
	return errors.Join(ctx.Err(), uerr)
}

// subscribe subscribes to the ack topic. The retained ack arrives right
// after, which is how the device learns the server's watermark on connect.
func (d *Device) subscribe(ctx context.Context, cm *autopaho.ConnectionManager, topic string) {
	b := backoff.Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := cm.Subscribe(sctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}})
		cancel()
		if err == nil {
			d.connected.Store(true)
			d.up.Connected()
			d.opts.Health.Set("okay", "")
			return
		}
		if errors.Is(err, autopaho.ConnectionDownError) {
			return // OnUp runs again on the next connection
		}
		d.lg.Warn("subscribing to acks failed; retrying", "err", err)
		if backoff.Sleep(ctx, b.Delay(attempt)) != nil {
			return
		}
	}
}

func (d *Device) renewLoop(ctx context.Context) {
	b := backoff.Backoff{Base: time.Minute, Max: time.Hour}
	for attempt := 0; ctx.Err() == nil; {
		leaf := d.id.Leaf()
		if leaf == nil {
			return
		}
		now := time.Now()
		if now.After(leaf.NotAfter) {
			d.opts.Health.Set("error", "certificate expired: enroll the device again")
			d.lg.Error("device certificate expired; it cannot be renewed and the device must be enrolled again", "not_after", leaf.NotAfter)
			if backoff.Sleep(ctx, time.Hour) != nil {
				return
			}
			continue
		}
		if wait := identity.RenewAt(leaf).Sub(now); wait > 0 && attempt == 0 {
			if backoff.Sleep(ctx, wait) != nil {
				return
			}
		}
		err := d.id.Renew(ctx)
		if err == nil {
			d.opts.Metrics.Renewals.WithLabelValues("ok").Inc()
			d.lg.Info("certificate renewed", "not_after", d.id.Leaf().NotAfter)
			attempt = 0
			continue
		}
		d.opts.Metrics.Renewals.WithLabelValues("error").Inc()
		d.lg.Warn("certificate renewal failed; retrying", "err", err, "not_after", leaf.NotAfter)
		if backoff.Sleep(ctx, b.Delay(attempt)) != nil {
			return
		}
		attempt++
	}
}

func (d *Device) collectLoop(ctx context.Context, s *Sampler) {
	t := time.NewTicker(d.opts.Config.CollectInterval)
	defer t.Stop()
	var lastErrLog time.Time
	for {
		for _, r := range s.Collect(ctx) {
			payload, err := proto.Marshal(r)
			if err == nil {
				_, err = d.log.Append(payload)
			}
			if err != nil {
				d.opts.Metrics.AppendErrors.Inc()
				if time.Since(lastErrLog) > time.Minute {
					lastErrLog = time.Now()
					d.lg.Error("cannot write to the local log", "err", err)
					d.opts.Health.Set("error", "local log is not writable")
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case <-t.C:
		}
	}
}

func (d *Device) statsLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		d.updateStats()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// updateStats moves the shared gauges by this device's change since the
// last update, so the totals across devices add up.
func (d *Device) updateStats() {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	st := d.log.Stats()
	m := d.opts.Metrics
	m.WALBytes.Add(float64(st.Bytes - d.lastStats.Bytes))
	m.WALSegments.Add(float64(st.Segments - d.lastStats.Segments))
	if st.DroppedTotal > d.lastStats.DroppedTotal {
		m.Dropped.WithLabelValues(droppedDiskBudget).Add(float64(st.DroppedTotal - d.lastStats.DroppedTotal))
	}
	d.lastStats = st
}

// Status is a snapshot for the status command and the load generator.
type Status struct {
	DeviceID     string     `json:"device_id,omitempty"`
	Enrolled     bool       `json:"enrolled"`
	Connected    bool       `json:"connected"`
	LastSeq      uint64     `json:"last_seq"`
	Acked        uint64     `json:"acked"`
	PendingGaps  []wal.Gap  `json:"pending_gaps"`
	Gaps         []wal.Gap  `json:"gaps"`
	WALBytes     int64      `json:"wal_bytes"`
	WALSegments  int        `json:"wal_segments"`
	CertNotAfter *time.Time `json:"cert_not_after,omitempty"`
	Version      string     `json:"version"`
}

// Status returns the device's current state.
func (d *Device) Status() Status {
	st := d.log.Stats()
	s := Status{
		Enrolled: d.id.Enrolled(), Connected: d.connected.Load(), LastSeq: st.NextSeq - 1, Acked: st.Acked,
		PendingGaps: d.log.PendingGaps(), Gaps: d.log.GapHistory(), WALBytes: st.Bytes, WALSegments: st.Segments,
		Version: d.opts.Version,
	}
	if s.Enrolled {
		s.DeviceID = d.id.Info().DeviceID.String()
	}
	if leaf := d.id.Leaf(); leaf != nil {
		na := leaf.NotAfter
		s.CertNotAfter = &na
	}
	return s
}

// DeviceID returns the enrolled ID, or uuid.Nil.
func (d *Device) DeviceID() uuid.UUID { return d.id.Info().DeviceID }

// Drained reports whether every seq written so far is acked.
func (d *Device) Drained() bool {
	st := d.log.Stats()
	return st.Acked >= st.NextSeq-1 && st.PendingGaps == 0
}
