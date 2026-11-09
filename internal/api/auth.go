package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

// principal is an authenticated caller.
type principal struct {
	operator bool
	tokenID  uuid.UUID
	tenant   uuid.UUID
	scopes   []string
	name     string
}

func (p *principal) actorType() string {
	if p.operator {
		return store.ActorOperator
	}
	return store.ActorTenant
}

func (p *principal) actorID() string { return p.tokenID.String() }

func (p *principal) has(scope string) bool { return slices.Contains(p.scopes, scope) }

// authenticate resolves the bearer token. Every failure looks the same to
// the client: which part was wrong is not revealed.
func (s *Server) authenticate(r *http.Request) (*principal, *apiError) {
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return nil, errUnauthorized
	}
	tok, err := tokens.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, errUnauthorized
	}
	var p *principal
	err = s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
		switch tok.Prefix {
		case tokens.PrefixTenant:
			a, err := tx.LookupAPIToken(ctx, tok.ID)
			if err != nil {
				return err
			}
			if !tokens.Equal(a.TokenHash, tok.Hash) || a.RevokedAt != nil || a.TenantStatus != store.TenantActive {
				return store.ErrNotFound
			}
			p = &principal{tokenID: tok.ID, tenant: a.TenantID, scopes: a.Scopes}
		case tokens.PrefixOperator:
			a, err := tx.LookupOperatorToken(ctx, tok.ID)
			if err != nil {
				return err
			}
			if !tokens.Equal(a.TokenHash, tok.Hash) || a.RevokedAt != nil {
				return store.ErrNotFound
			}
			p = &principal{operator: true, tokenID: tok.ID, name: a.Name}
		default:
			return store.ErrNotFound
		}
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, errUnauthorized
	}
	if err != nil {
		return nil, s.internal(r, err)
	}
	s.touch(p)
	setActor(r.Context(), p.actorType()+":"+p.actorID())
	return p, nil
}

// touch records last use at most once a minute per token, off the request path.
func (s *Server) touch(p *principal) {
	now := time.Now()
	if last, ok := s.lastTouch.Get(p.tokenID); ok && now.Sub(last) < time.Minute {
		return
	}
	s.lastTouch.Add(p.tokenID, now)
	select {
	case s.touches <- p:
	default: // a full queue only delays last_used_at
	}
}

func (s *Server) runTouches(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-s.touches:
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var err error
			if p.operator {
				err = s.db.InTx(tctx, func(ctx context.Context, tx *store.Tx) error { return tx.TouchOperatorToken(ctx, p.tokenID) })
			} else {
				err = s.db.InTenantTx(tctx, p.tenant, func(ctx context.Context, tx *store.Tx) error { return tx.TouchAPIToken(ctx, p.tokenID) })
			}
			cancel()
			if err != nil {
				s.log.Warn("could not record token use", "token_id", p.tokenID, "err", err)
			}
		}
	}
}

type handler func(w http.ResponseWriter, r *http.Request, p *principal) *apiError

// tenantRoute requires a tenant token with scope.
func (s *Server) tenantRoute(scope string, h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, e := s.authenticate(r)
		if e != nil {
			writeProblem(w, r, e)
			return
		}
		if p.operator || !p.has(scope) {
			writeProblem(w, r, errForbidden)
			return
		}
		if !s.limiter.allow(w, r, "t:"+p.tokenID.String()) {
			return
		}
		if e := h(w, r, p); e != nil {
			writeProblem(w, r, e)
		}
	})
}

// operatorRoute requires an operator token.
func (s *Server) operatorRoute(h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, e := s.authenticate(r)
		if e != nil {
			writeProblem(w, r, e)
			return
		}
		if !p.operator {
			writeProblem(w, r, errForbidden)
			return
		}
		if !s.limiter.allow(w, r, "o:"+p.tokenID.String()) {
			return
		}
		if e := h(w, r, p); e != nil {
			writeProblem(w, r, e)
		}
	})
}

// internal logs err with the request ID and returns a generic 500.
func (s *Server) internal(r *http.Request, err error) *apiError {
	if errors.Is(err, context.Canceled) {
		// The client went away; nothing useful to send or log loudly.
		s.log.Debug("request canceled", "request_id", requestID(r.Context()))
	} else {
		s.log.Error("request failed", "request_id", requestID(r.Context()), "err", err)
	}
	return internalError()
}

func pathID(r *http.Request) (uuid.UUID, *apiError) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, errBadRequest("id must be a UUID")
	}
	return id, nil
}
