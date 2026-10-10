package server

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// requestRateLimit tracks token buckets by client IP with a bounded number of client entries.
type requestRateLimit struct {
	perMinute int
	burst     int
	maxKeys   int
	trusted   []*net.IPNet
	now       func() time.Time
	mu        sync.Mutex
	keys      map[string]requestRateLimitBucket
}

// requestRateLimitBucket stores a client's available tokens and last refill time.
type requestRateLimitBucket struct {
	tokens float64
	at     time.Time
}

// newRequestRateLimit creates a client-IP limiter or returns nil when limiting is disabled.
func newRequestRateLimit(perMinute, burst, maxKeys int, trusted []*net.IPNet) *requestRateLimit {
	if perMinute <= 0 {
		return nil
	}
	return &requestRateLimit{perMinute: perMinute, burst: burst, maxKeys: maxKeys, trusted: trusted, now: time.Now, keys: make(map[string]requestRateLimitBucket)}
}

// allow consumes a client token when available or returns a suggested retry delay.
func (limit *requestRateLimit) allow(request *http.Request) (bool, time.Duration) {
	if limit == nil {
		return true, 0
	}
	key, ok := limit.key(request)
	if !ok {
		return false, time.Minute
	}
	now := limit.now()
	limit.mu.Lock()
	defer limit.mu.Unlock()
	bucket, exists := limit.keys[key]
	if !exists {
		if len(limit.keys) >= limit.maxKeys {
			return false, time.Minute
		}
		bucket = requestRateLimitBucket{tokens: float64(limit.burst), at: now}
	}
	elapsed := now.Sub(bucket.at)
	if elapsed > 0 {
		bucket.tokens = min(float64(limit.burst), bucket.tokens+elapsed.Minutes()*float64(limit.perMinute))
	}
	bucket.at = now
	if bucket.tokens >= 1 {
		bucket.tokens--
		limit.keys[key] = bucket
		return true, 0
	}
	limit.keys[key] = bucket
	seconds := time.Duration((1 - bucket.tokens) / float64(limit.perMinute) * float64(time.Minute))
	if seconds < time.Second {
		seconds = time.Second
	}
	return false, seconds
}

// key derives the client IP from the peer address or the first forwarded address supplied by a trusted proxy.
func (limit *requestRateLimit) key(request *http.Request) (string, bool) {
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		return "", false
	}
	peer := net.ParseIP(host)
	if peer == nil {
		return "", false
	}
	for _, network := range limit.trusted {
		if network.Contains(peer) {
			forwarded := strings.TrimSpace(strings.Split(request.Header.Get("X-Forwarded-For"), ",")[0])
			if client := net.ParseIP(forwarded); client != nil {
				return client.String(), true
			}
			break
		}
	}
	return peer.String(), true
}
