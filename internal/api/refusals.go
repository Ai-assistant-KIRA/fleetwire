package api

import (
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// Refused enrollments are audited, but the audit log is append-only
// and enrollment needs no credentials, so made-up tokens must not be able
// to grow it without limit. Each known token, or each client network when
// the token matches nothing, gets one entry per refusalAuditEvery, like
// ingest's drop entries. Entries that match no tenant also share a
// fleet-wide budget, since an attacker can use many networks.
// fleetwire_api_enroll_refusals_total counts every refusal.
const (
	refusalAuditEvery = 10 * time.Minute
	fleetRefusalEvery = time.Second
	fleetRefusalBurst = 10
)

// auditRefusal reports whether a refused enrollment gets an audit entry.
func (s *Server) auditRefusal(tenant uuid.UUID, hash []byte, network string) bool {
	key := "net:" + network
	if tenant != uuid.Nil {
		key = "tok:" + hex.EncodeToString(hash)
	}
	now := s.now()
	if last, ok := s.refusals.Get(key); ok && now.Sub(last) < refusalAuditEvery {
		return false
	}
	if tenant == uuid.Nil && !s.fleetRefusals.AllowN(now, 1) {
		return false
	}
	s.refusals.Add(key, now)
	return true
}
