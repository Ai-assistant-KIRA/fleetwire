package api

import (
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/lru"
)

// limiter keeps one token bucket per key (a token ID, or a client network
// for unauthenticated calls) in a bounded LRU. Limits are per API replica;
// the docs say so.
type limiter struct {
	r     rate.Limit
	burst int
	cache *lru.Cache[string, *rate.Limiter]
}

func newLimiter(perSecond float64, burst int) *limiter {
	return &limiter{r: rate.Limit(perSecond), burst: burst, cache: lru.New[string, *rate.Limiter](100_000, 0)}
}

// allow reports whether the request may proceed and takes a token if so.
// When it may not, it writes 429 with a Retry-After header.
func (l *limiter) allow(w http.ResponseWriter, r *http.Request, key string) bool {
	lim := l.cache.GetOrAdd(key, func() *rate.Limiter { return rate.NewLimiter(l.r, l.burst) })
	res := lim.Reserve()
	delay := res.Delay()
	if delay == 0 {
		return true
	}
	res.Cancel()
	tooManyRequests(w, r, delay)
	return false
}

// ready reports whether key has a token left, without taking it, and
// writes 429 when it has none. A key never seen has a full bucket.
func (l *limiter) ready(w http.ResponseWriter, r *http.Request, key string) bool {
	lim, ok := l.cache.Get(key)
	if !ok {
		return true
	}
	tokens := lim.Tokens()
	if tokens >= 1 {
		return true
	}
	tooManyRequests(w, r, time.Duration((1-tokens)/float64(l.r)*float64(time.Second)))
	return false
}

// take uses up a token for key, if one is left.
func (l *limiter) take(key string) {
	l.cache.GetOrAdd(key, func() *rate.Limiter { return rate.NewLimiter(l.r, l.burst) }).Allow()
}

func tooManyRequests(w http.ResponseWriter, r *http.Request, delay time.Duration) {
	secs := int(math.Ceil(delay.Seconds()))
	if secs < 1 || delay == rate.InfDuration {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeProblem(w, r, &apiError{status: http.StatusTooManyRequests, code: "rate_limited",
		detail: "too many requests; retry after " + (time.Duration(secs) * time.Second).String()})
}

// clientNet is the rate-limit key for an unauthenticated caller: its IPv4
// address, or the /64 prefix of its IPv6 address. One IPv6 host usually
// holds a whole /64, so keying by the full address would give it 2^64
// separate budgets. Like clientIP it uses the TCP peer address: the API
// terminates TLS itself and trusts no forwarding headers.
func clientNet(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	addr = addr.Unmap()
	if addr.Is4() {
		return addr.String()
	}
	// The zone is dropped: Prefix refuses addresses that have one.
	p, err := addr.WithZone("").Prefix(64)
	if err != nil {
		return host
	}
	return p.String()
}
