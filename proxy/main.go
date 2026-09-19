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
	rateLimit      = 10          // sustained rate: tokens per window
	window         = time.Minute
	burst          = rateLimit   // bucket capacity
	refillPerToken = window / rateLimit
)

// Per-client token bucket, keyed by source IP. A new bucket starts full, so an
// idle client may spend up to burst at once; refills continuously thereafter.
var (
	mu      sync.Mutex
	buckets = map[string]*bucket{}
)

type bucket struct {
	tokens float64
	last   time.Time
}

type limitState struct {
	allowed   bool
	remaining int
	resetIn   time.Duration
}

func allow(ip string) limitState {
	now := time.Now()
	mu.Lock()
	defer mu.Unlock()

	b, seen := buckets[ip]
	if !seen {
		log.Println("first request from", ip)
		b = &bucket{tokens: burst, last: now}
		buckets[ip] = b
	}

	b.tokens += float64(now.Sub(b.last)) / float64(refillPerToken)
	if b.tokens > burst {
		b.tokens = burst
	}
	b.last = now

	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	remaining := int(b.tokens)
	resetIn := time.Duration((float64(remaining+1) - b.tokens) * float64(refillPerToken))
	return limitState{allowed: allowed, remaining: remaining, resetIn: resetIn}
}

func ceilSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}

// Constant limit, formatted once.
var rateLimitStr = strconv.Itoa(rateLimit)

func setRateHeaders(w http.ResponseWriter, st limitState) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", rateLimitStr)
	h.Set("X-RateLimit-Remaining", strconv.Itoa(st.remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(ceilSeconds(st.resetIn)))
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	// Charged on arrival: a 503 later still spends a token (we limit ask-rate).
	st := allow(host)
	setRateHeaders(w, st)
	if !st.allowed {
		w.Header().Set("Retry-After", strconv.Itoa(ceilSeconds(st.resetIn)))
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

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
