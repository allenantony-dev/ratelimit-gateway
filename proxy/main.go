package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

const maxInFlight = 100

var sem = make(chan struct{}, maxInFlight)

// Log each source IP only the first time it's seen, so identifying clients
// costs one line per client instead of one per request.
var (
	seenMu sync.Mutex
	seen   = map[string]bool{}
)

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	seenMu.Lock()
	if !seen[host] {
		seen[host] = true
		log.Println("first request from", host)
	}
	seenMu.Unlock()

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
