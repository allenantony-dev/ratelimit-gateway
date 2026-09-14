package main

import (
	"io"
	"log"
	"net"
	"net/http"
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

// allow records one arrival from ip and reports whether it's within the limit.
func allow(ip string) bool {
	now := time.Now()
	mu.Lock()
	defer mu.Unlock()

	c := counters[ip]
	if c == nil {
		log.Println("first request from", ip)
		counters[ip] = &counter{windowStart: now, count: 1}
		return true
	}
	if now.Sub(c.windowStart) >= window {
		c.windowStart = now
		c.count = 1
		return true
	}
	if c.count >= rateLimit {
		return false
	}
	c.count++
	return true
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	// Charged on arrival: a request refused later (503) still spends quota --
	// we limit ask-rate, not serve-rate.
	if !allow(host) {
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
