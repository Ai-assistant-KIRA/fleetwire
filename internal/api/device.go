package api

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

type enrollResponse struct {
	DeviceID       uuid.UUID `json:"device_id"`
	CertificatePEM string    `json:"certificate_pem"`
	CABundlePEM    string    `json:"ca_bundle_pem"`
	BrokerURL      string    `json:"broker_url"`
	APIURL         string    `json:"api_url"`
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// enroll turns a one-time token and a CSR into a device identity. The
// server picks the device ID; the CSR subject is ignored (ADR-0009). Failed
// attempts are audited, under the token's tenant when the token is known.
func (s *Server) enroll(w http.ResponseWriter, r *http.Request) {
	if !s.enrollLim.allow(w, r, "ip:"+clientIP(r)) {
		return
	}
	var req struct {
		Token  string `json:"token"`
		CSRPEM string `json:"csr_pem"`
		Name   string `json:"name"`
	}
	if e := decodeJSON(w, r, &req); e != nil {
		writeProblem(w, r, e)
		return
	}
	// Check everything that needs no database first, so a bad request
	// never uses up a token.
	csr, err := pki.ParseCSR([]byte(req.CSRPEM))
	if err != nil {
		writeProblem(w, r, errBadRequest(err.Error()))
		return
	}
	if req.Name != "" && !validName(req.Name) {
		writeProblem(w, r, errBadRequest("name must be 1 to 200 characters"))
		return
	}
	var hash []byte
	if tok, err := tokens.Parse(strings.TrimSpace(req.Token)); err == nil && tok.Prefix == tokens.PrefixEnrollment {
		hash = tok.Hash
	}
	deviceID, err := uuid.NewV7()
	if err != nil {
		writeProblem(w, r, s.internal(r, err))
		return
	}
	actor := store.Audit{ActorType: store.ActorDevice, ActorID: "remote:" + clientIP(r), Action: "device.enroll"}
	var outcome string
	var resp enrollResponse
	err = s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
		var tenant uuid.UUID
		if hash == nil {
			outcome = store.EnrollUnknown
		} else {
			var err error
			if tenant, outcome, err = tx.ConsumeEnrollmentToken(ctx, hash, deviceID); err != nil {
				return err
			}
		}
		if outcome != store.EnrollOK {
			if tenant != uuid.Nil {
				if err := tx.SetTenant(ctx, tenant); err != nil {
					return err
				}
			}
			a := actor
			a.TargetType, a.Result = "enrollment_token", store.ResultDenied
			a.TargetID = "-"
			if hash != nil {
				a.TargetID = hex.EncodeToString(hash[:4])
			}
			a.Details = map[string]any{"outcome": outcome}
			// Committed on purpose: the refusal is the audit record.
			return tx.InsertAudit(ctx, a)
		}
		if err := tx.SetTenant(ctx, tenant); err != nil {
			return err
		}
		if _, err := tx.InsertDevice(ctx, deviceID, strings.TrimSpace(req.Name)); err != nil {
			return err
		}
		cert, chain, err := s.ca.IssueDeviceCert(csr, deviceID, s.now())
		if err != nil {
			return err
		}
		if err := tx.SetDeviceCert(ctx, store.DeviceCert{Serial: pki.SerialString(cert.SerialNumber), DeviceID: deviceID, NotAfter: cert.NotAfter}); err != nil {
			return err
		}
		a := actor
		a.ActorID = deviceID.String()
		a.TargetType, a.TargetID, a.Result = "device", deviceID.String(), store.ResultSuccess
		a.Details = map[string]any{"serial": pki.SerialString(cert.SerialNumber), "remote": clientIP(r)}
		if err := tx.InsertAudit(ctx, a); err != nil {
			return err
		}
		resp = enrollResponse{DeviceID: deviceID, CertificatePEM: string(chain), CABundlePEM: string(s.cfg.CABundlePEM),
			BrokerURL: s.cfg.BrokerURL, APIURL: s.cfg.DeviceAPIURL}
		return nil
	})
	if err != nil {
		writeProblem(w, r, s.internal(r, err))
		return
	}
	switch outcome {
	case store.EnrollOK:
		setActor(r.Context(), "device:"+deviceID.String())
		writeJSON(w, http.StatusCreated, resp)
	case store.EnrollTenantInactive:
		writeProblem(w, r, &apiError{status: http.StatusForbidden, code: "tenant_inactive", detail: "the tenant is not active"})
	default:
		// Unknown, expired and used tokens look the same to the caller.
		writeProblem(w, r, &apiError{status: http.StatusUnauthorized, code: "invalid_token",
			detail: "the enrollment token is unknown, expired, or already used"})
	}
}

// renew issues a new certificate for the same key over mutual TLS. The
// device ID comes from the verified client certificate, never from the body.
func (s *Server) renew(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		writeProblem(w, r, &apiError{status: http.StatusUnauthorized, code: "client_certificate_required",
			detail: "renewal needs the current device certificate"})
		return
	}
	cert := r.TLS.VerifiedChains[0][0]
	deviceID, err := pki.DeviceID(cert)
	if err != nil {
		writeProblem(w, r, &apiError{status: http.StatusUnauthorized, code: "not_a_device", detail: err.Error()})
		return
	}
	setActor(r.Context(), "device:"+deviceID.String())
	if !s.enrollLim.allow(w, r, "dev:"+deviceID.String()) {
		return
	}
	var req struct {
		CSRPEM string `json:"csr_pem"`
	}
	if e := decodeJSON(w, r, &req); e != nil {
		writeProblem(w, r, e)
		return
	}
	csr, err := pki.ParseCSR([]byte(req.CSRPEM))
	if err != nil {
		writeProblem(w, r, errBadRequest(err.Error()))
		return
	}
	oldPub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !oldPub.Equal(csr.PublicKey) {
		writeProblem(w, r, errBadRequest("renewal must keep the device key"))
		return
	}
	var denied bool
	var chain []byte
	err = s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
		// Locked until commit: a revoke or tenant destroy racing this renewal
		// either is seen here or revokes the new certificate after it.
		ref, err := tx.LockDevice(ctx, deviceID)
		if err != nil {
			return err
		}
		if err := tx.SetTenant(ctx, ref.TenantID); err != nil {
			return err
		}
		a := store.Audit{ActorType: store.ActorDevice, ActorID: deviceID.String(), Action: "device.renew",
			TargetType: "device", TargetID: deviceID.String()}
		if ref.DeviceStatus != store.DeviceActive || ref.TenantStatus != store.TenantActive {
			denied = true
			a.Result = store.ResultDenied
			a.Details = map[string]any{"device_status": ref.DeviceStatus, "tenant_status": ref.TenantStatus}
			return tx.InsertAudit(ctx, a)
		}
		newCert, c, err := s.ca.IssueDeviceCert(csr, deviceID, s.now())
		if err != nil {
			return err
		}
		chain = c
		if err := tx.SetDeviceCert(ctx, store.DeviceCert{Serial: pki.SerialString(newCert.SerialNumber), DeviceID: deviceID, NotAfter: newCert.NotAfter}); err != nil {
			return err
		}
		a.Result = store.ResultSuccess
		a.Details = map[string]any{"serial": pki.SerialString(newCert.SerialNumber), "old_serial": pki.SerialString(cert.SerialNumber),
			"not_after": newCert.NotAfter.Format(time.RFC3339)}
		return tx.InsertAudit(ctx, a)
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeProblem(w, r, &apiError{status: http.StatusForbidden, code: "unknown_device", detail: "the device is not registered"})
			return
		}
		writeProblem(w, r, s.internal(r, err))
		return
	}
	if denied {
		writeProblem(w, r, &apiError{status: http.StatusForbidden, code: "device_inactive", detail: "the device or its tenant is not active"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"certificate_pem": string(chain), "ca_bundle_pem": string(s.cfg.CABundlePEM)})
}

// getCRL serves the device CRL, the way a CRL distribution point does. It
// is public: the list is signed by the Device Issuing CA and holds only
// serial numbers. Brokers that cannot read the API's CRL file fetch it here.
func (s *Server) getCRL(w http.ResponseWriter, r *http.Request) {
	pemBytes := s.crl.Latest()
	if pemBytes == nil {
		writeProblem(w, r, &apiError{status: http.StatusServiceUnavailable, code: "crl_not_ready",
			detail: "the CRL has not been published yet; retry shortly"})
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	// A failed write means the client is gone.
	_, _ = w.Write(pemBytes)
}
