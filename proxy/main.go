package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxInFlight = 100
const upstreamTimeout = 5 * time.Second

var sem = make(chan struct{}, maxInFlight)

const (
	window       = time.Minute
	defaultLimit = 10
)

// Requests allowed per window, by exact route path. A path matching nothing
// gets defaultLimit, which also covers routes added without a config entry.
// /login is strict (password guessing), /search loose.
var routeLimits = map[string]int{
	"/login":  5,
	"/search": 60,
}

var (
	rdb       *redis.Client
	redisDown atomic.Bool
)

// limitFor returns the matched route key (for the Redis bucket) and its limit.
// The path is cleaned first so /x/../login and //login resolve to /login, the
// same route the backend sees -- otherwise they'd slip past the strict limit.
func limitFor(p string) (string, int) {
	p = path.Clean(p)
	if n, ok := routeLimits[p]; ok {
		return p, n
	}
	return "default", defaultLimit
}

// One atomic step in Redis: refill from Redis's own clock, take a token if one
// is free, persist, and expire an idle bucket. Returns {allowed, remaining,
// secondsToNextToken}. Running it server-side is what stops two proxies from
// racing on the same key.
var bucketScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local b = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(b[1])
local last = tonumber(b[2])
if tokens == nil then
	tokens = burst
	last = now
end
tokens = math.min(burst, tokens + (now - last) * rate)
local allowed = 0
if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last', now)
redis.call('EXPIRE', KEYS[1], math.ceil(burst / rate))
local remaining = math.floor(tokens)
local reset = math.ceil((remaining + 1 - tokens) / rate)
return {allowed, remaining, reset}
`)

type limitState struct {
	allowed   bool
	remaining int
	resetSec  int
}

// allow runs the bucket script for ip. The bool is false when Redis didn't
// answer -- callers fail open (admit, no headers).
func allow(route, ip string, limit int) (limitState, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	rate := float64(limit) / window.Seconds()
	res, err := bucketScript.Run(ctx, rdb, []string{"rl:" + route + ":" + ip}, rate, limit).Result()
	if err != nil {
		if redisDown.CompareAndSwap(false, true) {
			log.Println("redis unavailable, failing open:", err)
		}
		// Fail open: only per-client fairness rides on Redis; the concurrency cap
		// (503) still protects the upstream without it, so a Redis outage costs
		// fairness for a few minutes, not the upstream. A fragile route might fail closed.
		return limitState{allowed: true}, false
	}
	if redisDown.CompareAndSwap(true, false) {
		log.Println("redis recovered")
	}
	arr, ok := res.([]any)
	if !ok || len(arr) < 3 {
		return limitState{allowed: true}, false
	}
	allowed, _ := arr[0].(int64)
	remaining, _ := arr[1].(int64)
	resetSec, _ := arr[2].(int64)
	return limitState{allowed: allowed == 1, remaining: int(remaining), resetSec: int(resetSec)}, true
}

func setRateHeaders(w http.ResponseWriter, st limitState, limit int) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.Itoa(limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(st.remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(st.resetSec))
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	route, limit := limitFor(r.URL.Path)
	// Charged on arrival: a 503 later still spends a token (we limit ask-rate).
	st, ok := allow(route, host, limit)
	if ok {
		setRateHeaders(w, st, limit)
		if !st.allowed {
			w.Header().Set("Retry-After", strconv.Itoa(st.resetSec))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	default:
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
		return
	}

	// Bound the upstream call and cancel it if the client hangs up (r.Context()
	// is cancelled on disconnect) -- either way the held slot is released.
	ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
	defer cancel()
	outReq, err := http.NewRequestWithContext(ctx, r.Method, "http://127.0.0.1:9000"+r.URL.RequestURI(), r.Body)
	if err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	outReq.Header = r.Header.Clone()
	outReq.ContentLength = r.ContentLength

	resp, err := http.DefaultClient.Do(outReq)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return // client hung up -- nothing to write a response to
		case errors.Is(err, context.DeadlineExceeded):
			http.Error(w, "upstream timeout", http.StatusGatewayTimeout)
		default:
			http.Error(w, "upstream error", http.StatusBadGateway)
		}
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// discardLogger silences go-redis's per-error internal logging; we report Redis
// state changes ourselves, once per transition.
type discardLogger struct{}

func (discardLogger) Printf(context.Context, string, ...any) {}

func main() {
	redis.SetLogger(discardLogger{})

	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	http.HandleFunc("/", proxyHandler)

	log.Printf("Starting proxy on %s (redis %s)...", *addr, redisAddr)

	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}
