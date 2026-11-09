package api

import (
	"math"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/lru"
)

// limiter keeps one token bucket per key (a token ID, or a client address
// for unauthenticated device calls) in a bounded LRU. Limits are per API
// replica; the docs say so.
type limiter struct {
	r     rate.Limit
	burst int
	cache *lru.Cache[string, *rate.Limiter]
}

func newLimiter(perSecond float64, burst int) *limiter {
	return &limiter{r: rate.Limit(perSecond), burst: burst, cache: lru.New[string, *rate.Limiter](100_000, 0)}
}

// allow reports whether the request may proceed. When it may not, it
// writes 429 with a Retry-After header.
func (l *limiter) allow(w http.ResponseWriter, r *http.Request, key string) bool {
	lim := l.cache.GetOrAdd(key, func() *rate.Limiter { return rate.NewLimiter(l.r, l.burst) })
	res := lim.Reserve()
	delay := res.Delay()
	if delay == 0 {
		return true
	}
	res.Cancel()
	secs := int(math.Ceil(delay.Seconds()))
	if secs < 1 || delay == rate.InfDuration {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeProblem(w, r, &apiError{status: http.StatusTooManyRequests, code: "rate_limited",
		detail: "too many requests; retry after " + (time.Duration(secs) * time.Second).String()})
	return false
}
