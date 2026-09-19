package main

import (
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Requests to fire. With retries, a large number would hammer for a very long
// time (each client is capped at 10/min), so the default is small enough to
// watch. Override with a positional arg after any flags, e.g. `client 50`.
const defaultRequests = 25

// Give up on a request after this many rejections.
const maxAttempts = 20

func main() {
	from := flag.String("from", "", "source IP to dial from, e.g. 127.0.0.2 (default: OS chooses)")
	flag.Parse()

	requests := defaultRequests
	if a := flag.Arg(0); a != "" {
		n, err := strconv.Atoi(a)
		if err != nil || n < 1 {
			log.Fatalf("invalid request count %q: want a positive integer", a)
		}
		requests = n
	}
	proxies := os.Getenv("PROXIES")
	if proxies == "" {
		proxies = "http://127.0.0.1:8080"
	}
	targets := strings.Split(proxies, ",")

	// No client-side pool limit, so all requests dial out at once.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = requests
	if *from != "" {
		ip := net.ParseIP(*from)
		if ip == nil {
			log.Fatalf("invalid -from IP: %q", *from)
		}
		// Keep the default dialer's timeouts; only add the source address.
		d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, LocalAddr: &net.TCPAddr{IP: ip}}
		tr.DialContext = d.DialContext
	}
	http.DefaultClient.Transport = tr

	log.Printf("firing %d requests (from %q) across %v", requests, *from, targets)

	var served, gaveUp, failed, retries, firstPass atomic.Int64
	var wg sync.WaitGroup

	for i := range requests {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			url := targets[id%len(targets)] + "/hello"
			for attempt := 1; ; attempt++ {
				resp, err := http.Get(url)
				if err != nil {
					failed.Add(1)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					served.Add(1)
					if attempt == 1 {
						firstPass.Add(1)
					}
					return
				}
				if attempt >= maxAttempts {
					gaveUp.Add(1)
					return
				}
				wait := backoff(resp)
				retries.Add(1)
				log.Printf("req %d: %d, retry in %s", id, resp.StatusCode, wait.Round(time.Millisecond))
				time.Sleep(wait)
			}
		}(i)
	}

	wg.Wait()
	log.Printf("served=%d (first-pass=%d) gave-up=%d failed=%d (retries=%d)",
		served.Load(), firstPass.Load(), gaveUp.Load(), failed.Load(), retries.Load())
}

// backoff waits as the gateway asks: honor Retry-After (seconds) when present
// (429), else a short default (503). Jitter is added so retries don't all fire
// at the same instant and stampede the next window.
func backoff(resp *http.Response) time.Duration {
	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	if s := resp.Header.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(s); err == nil {
			return time.Duration(secs)*time.Second + jitter
		}
	}
	return time.Second + jitter
}
