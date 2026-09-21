package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxInFlight = 100

var sem = make(chan struct{}, maxInFlight)

const (
	rateLimit = 10
	window    = time.Minute
	burst     = rateLimit
)

var (
	ratePerSec   = float64(rateLimit) / window.Seconds()
	rateLimitStr = strconv.Itoa(rateLimit)
	rdb          *redis.Client
	redisDown    atomic.Bool
)

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
func allow(ip string) (limitState, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	res, err := bucketScript.Run(ctx, rdb, []string{"rl:" + ip}, ratePerSec, burst).Result()
	if err != nil {
		if redisDown.CompareAndSwap(false, true) {
			log.Println("redis unavailable, failing open:", err)
		}
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

func setRateHeaders(w http.ResponseWriter, st limitState) {
	h := w.Header()
	h.Set("X-RateLimit-Limit", rateLimitStr)
	h.Set("X-RateLimit-Remaining", strconv.Itoa(st.remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(st.resetSec))
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	// Charged on arrival: a 503 later still spends a token (we limit ask-rate).
	st, ok := allow(host)
	if ok {
		setRateHeaders(w, st)
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

	resp, err := http.Get("http://127.0.0.1:9000" + r.URL.Path)
	if err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

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
