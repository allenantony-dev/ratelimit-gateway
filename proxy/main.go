package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const maxInFlight = 100

var sem = make(chan struct{}, maxInFlight)

const (
	rateLimit = 10
	window    = time.Minute
)

// Per-client fixed-window limit, keyed by source IP: at most rateLimit requests
// per window. Simplest shape; its flaw is the boundary -- a client can send
// rateLimit at the end of one window and rateLimit at the start of the next,
// up to 2*rateLimit in a short span. A token bucket or sliding window smooths
// that at the cost of more code.
type counter struct {
	windowStart time.Time
	count       int
}

var (
	mu       sync.Mutex
	counters = map[string]*counter{}
)

// limitState is a snapshot of one client's quota, taken under the lock so the
// handler can set rate-limit headers without racing on the map.
type limitState struct {
	allowed   bool
	remaining int
	resetIn   time.Duration
}

func snapshot(c *counter, now time.Time, allowed bool) limitState {
	remaining := rateLimit - c.count
	if remaining < 0 {
		remaining = 0
	}
	return limitState{allowed: allowed, remaining: remaining, resetIn: window - now.Sub(c.windowStart)}
}

// allow records one arrival from ip and returns the resulting quota snapshot.
func allow(ip string) limitState {
	now := time.Now()
	mu.Lock()
	defer mu.Unlock()

	c := counters[ip]
	if c == nil {
		log.Println("first request from", ip)
		c = &counter{windowStart: now, count: 1}
		counters[ip] = c
		return snapshot(c, now, true)
	}
	if now.Sub(c.windowStart) >= window {
		c.windowStart = now
		c.count = 1
		return snapshot(c, now, true)
	}
	if c.count >= rateLimit {
		return snapshot(c, now, false)
	}
	c.count++
	return snapshot(c, now, true)
}

// ceilSeconds rounds a duration up to whole seconds, for Retry-After / Reset.
func ceilSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}

// The limit never changes, so format it once instead of every response.
var rateLimitStr = strconv.Itoa(rateLimit)

func setRateHeaders(w http.ResponseWriter, st limitState) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", rateLimitStr)
	h.Set("X-RateLimit-Remaining", strconv.Itoa(st.remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(ceilSeconds(st.resetIn)))
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	// Charged on arrival: a request refused later (503) still spends quota --
	// we limit ask-rate, not serve-rate.
	st := allow(host)
	setRateHeaders(w, st)
	if !st.allowed {
		w.Header().Set("Retry-After", strconv.Itoa(ceilSeconds(st.resetIn)))
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Non-blocking acquire: reject when all slots are taken, don't queue.
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	default:
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
		return
	}

	resp, err := http.Get("http://127.0.0.1:9000" + r.URL.Path)
	if err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func main() {
	http.HandleFunc("/", proxyHandler)

	log.Println("Starting proxy on :8080...")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
