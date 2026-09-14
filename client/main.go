package main

import (
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Concurrent requests to fire. Override with a positional arg after any flags,
// e.g. `go run client/main.go 1000` or `go run client/main.go -from 127.0.0.2 1000`.
const defaultRequests = 9500

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

	log.Printf("firing %d concurrent requests (from %q) at http://127.0.0.1:8080/hello", requests, *from)

	var ok, rejected, failed atomic.Int64
	var wg sync.WaitGroup

	for range requests {
		wg.Add(1)

		go func() {
			defer wg.Done()

			resp, err := http.Get("http://127.0.0.1:8080/hello")
			if err != nil {
				failed.Add(1)
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)

			switch resp.StatusCode {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusServiceUnavailable:
				rejected.Add(1)
			default:
				failed.Add(1)
			}
		}()
	}

	wg.Wait()
	log.Printf("ok=%d rejected(503)=%d failed=%d", ok.Load(), rejected.Load(), failed.Load())
}
