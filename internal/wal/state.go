package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	stateFile    = "state.json"
	stateTmpFile = "state.json.tmp"
	stateVersion = 1
)

// GapReason says why the device gave up on a seq range.
type GapReason string

// Gap reasons. The values match the protobuf enum names without prefix.
const (
	GapDiskBudget GapReason = "DISK_BUDGET"
	GapCorruption GapReason = "CORRUPTION"
	GapLost       GapReason = "LOST"
)

// Gap is an inclusive seq range that will never be sent.
type Gap struct {
	From   uint64    `json:"from"`
	To     uint64    `json:"to"`
	Reason GapReason `json:"reason"`
}

// State is the agent state kept next to the segments.
type State struct {
	Version  int    `json:"version"`
	DeviceID string `json:"device_id,omitempty"`
	// Acked is the highest seq covered by an end-to-end ack.
	Acked uint64 `json:"acked"`
	// MaxSeqEver is the highest seq that was ever durable. It keeps seqs from
	// being reused if the segment files are lost while this file survives.
	MaxSeqEver  uint64 `json:"max_seq_ever"`
	PendingGaps []Gap  `json:"pending_gaps"`
}

func (s State) clone() State {
	s.PendingGaps = append([]Gap(nil), s.PendingGaps...)
	return s
}

func (s State) equal(o State) bool {
	if s.Version != o.Version || s.DeviceID != o.DeviceID || s.Acked != o.Acked ||
		s.MaxSeqEver != o.MaxSeqEver || len(s.PendingGaps) != len(o.PendingGaps) {
		return false
	}
	for i := range s.PendingGaps {
		if s.PendingGaps[i] != o.PendingGaps[i] {
			return false
		}
	}
	return true
}

// ErrCorruptState means state.json exists but cannot be parsed. Starting
// without it could reuse seqs, so the agent stops and asks for an operator.
var ErrCorruptState = errors.New("wal: state file is corrupt")

// LoadState reads dir/state.json. A missing file yields the zero state.
func LoadState(dir string) (State, error) {
	data, err := os.ReadFile(filepath.Join(dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return State{Version: stateVersion}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("read state: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{}, fmt.Errorf("%w: %w", ErrCorruptState, err)
	}
	if s.Version != stateVersion {
		return State{}, fmt.Errorf("%w: unsupported version %d", ErrCorruptState, s.Version)
	}
	for _, g := range s.PendingGaps {
		if g.From == 0 || g.From > g.To {
			return State{}, fmt.Errorf("%w: invalid gap %d-%d", ErrCorruptState, g.From, g.To)
		}
	}
	return s, nil
}

// stateHook lets tests stop SaveState after any step to simulate a crash.
var stateHook func(step string) error

func hook(step string) error {
	if stateHook == nil {
		return nil
	}
	return stateHook(step)
}

// SaveState replaces dir/state.json atomically: write a temp file, fsync it,
// rename it over the old file, then fsync the directory so the rename itself
// survives a power cut. A crash at any point leaves either the old or the
// new state, never a mix.
func SaveState(dir string, s State) error {
	s.Version = stateVersion
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := filepath.Join(dir, stateTmpFile)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create state temp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := hook("write"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close state: %w", err)
	}
	if err := hook("fsync"); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, stateFile)); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	if err := hook("rename"); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return hook("syncdir")
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir: %w", err)
	}
	return nil
}
