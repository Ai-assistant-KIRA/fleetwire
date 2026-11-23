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

// bearer parses the Authorization header without touching the database.
// It accepts only tenant and operator tokens.
func bearer(r *http.Request) (tokens.Parsed, bool) {
	// The scheme name is case-insensitive (RFC 9110, section 11.1).
	scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return tokens.Parsed{}, false
	}
	tok, err := tokens.Parse(strings.TrimSpace(raw))
	if err != nil || (tok.Prefix != tokens.PrefixTenant && tok.Prefix != tokens.PrefixOperator) {
		return tokens.Parsed{}, false
	}
	return tok, true
}

// authorize runs the checks every authenticated route shares, in an order
// that keeps unauthenticated traffic away from the database:
//
//  1. A client network that keeps failing authentication is refused
//     before its token is looked up.
//  2. A token whose bucket is empty is refused before the lookup too.
//     Only authenticated requests take from that bucket, so a caller who
//     knows a token ID but not its secret cannot use up its budget.
//  3. The lookup. A failed lookup takes from the network's failure budget.
//  4. The token's rate limit. Routes check scopes after this, so a token
//     cannot make forbidden calls faster than allowed ones.
//
// It writes the error response itself and reports whether to go on.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (*principal, bool) {
	network := "n:" + clientNet(r)
	if !s.authFailLim.ready(w, r, network) {
		return nil, false
	}
	tok, ok := bearer(r)
	if !ok {
		writeProblem(w, r, errUnauthorized)
		return nil, false
	}
	key := "t:" + tok.ID.String()
	if tok.Prefix == tokens.PrefixOperator {
		key = "o:" + tok.ID.String()
	}
	if !s.limiter.ready(w, r, key) {
		return nil, false
	}
	p, e := s.authenticate(r, tok)
	if e != nil {
		if e == errUnauthorized {
			s.authFailLim.take(network)
		}
		writeProblem(w, r, e)
		return nil, false
	}
	if !s.limiter.allow(w, r, key) {
		return nil, false
	}
	return p, true
}

// authenticate looks the parsed token up. Every failure looks the same to
// the client: which part was wrong is not revealed.
func (s *Server) authenticate(r *http.Request, tok tokens.Parsed) (*principal, *apiError) {
	var p *principal
	err := s.db.InTx(r.Context(), func(ctx context.Context, tx *store.Tx) error {
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

// touchDrainLimit bounds how long runTouches goes on, once told to stop,
// recording the touches still queued.
const touchDrainLimit = 5 * time.Second

// runTouches records token use until ctx ends, then what is still queued.
// Serve ends ctx once both listeners have drained, and the last requests'
// touches may not have been picked up by then. A write under way when ctx
// ends is not cut off either.
func (s *Server) runTouches(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.drainTouches(context.WithoutCancel(ctx))
			return
		case p := <-s.touches:
			s.recordTouch(context.WithoutCancel(ctx), p)
		}
	}
}

func (s *Server) drainTouches(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, touchDrainLimit)
	defer cancel()
	for {
		select {
		case p := <-s.touches:
			if ctx.Err() != nil {
				s.log.Warn("token use not recorded at shutdown", "tokens", 1+len(s.touches))
				return
			}
			s.recordTouch(ctx, p)
		default:
			return
		}
	}
}

func (s *Server) recordTouch(ctx context.Context, p *principal) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var err error
	if p.operator {
		err = s.db.InTx(ctx, func(ctx context.Context, tx *store.Tx) error { return tx.TouchOperatorToken(ctx, p.tokenID) })
	} else {
		err = s.db.InTenantTx(ctx, p.tenant, func(ctx context.Context, tx *store.Tx) error { return tx.TouchAPIToken(ctx, p.tokenID) })
	}
	if err != nil {
		s.log.Warn("could not record token use", "token_id", p.tokenID, "err", err)
	}
}

type handler func(w http.ResponseWriter, r *http.Request, p *principal) *apiError

// scopeByJobKind is the scope of a route whose handler checks the scope
// itself, because it depends on the job the route loads.
const scopeByJobKind = ""

// tenantRoute requires a tenant token with scope.
func (s *Server) tenantRoute(scope string, h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := s.authorize(w, r)
		if !ok {
			return
		}
		if p.operator || (scope != scopeByJobKind && !p.has(scope)) {
			writeProblem(w, r, errForbidden)
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
		p, ok := s.authorize(w, r)
		if !ok {
			return
		}
		if !p.operator {
			writeProblem(w, r, errForbidden)
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
