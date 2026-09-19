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

// Per-client sliding window: timestamps of accepted requests within the last
// window. Rejects aren't recorded, so each slice stays bounded at rateLimit.
var (
	mu      sync.Mutex
	clients = map[string][]time.Time{}
)

type limitState struct {
	allowed   bool
	remaining int
	resetIn   time.Duration
}

func allow(ip string) limitState {
	now := time.Now()
	mu.Lock()
	defer mu.Unlock()

	times, seen := clients[ip]
	if !seen {
		log.Println("first request from", ip)
	}

	cutoff := now.Add(-window)
	drop := 0
	for drop < len(times) && times[drop].Before(cutoff) {
		drop++
	}
	times = times[drop:]

	if len(times) >= rateLimit {
		clients[ip] = times
		return limitState{allowed: false, remaining: 0, resetIn: window - now.Sub(times[0])}
	}

	times = append(times, now)
	clients[ip] = times
	return limitState{allowed: true, remaining: rateLimit - len(times), resetIn: window - now.Sub(times[0])}
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
	// Charged on arrival: a 503 later still spends quota (we limit ask-rate).
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
