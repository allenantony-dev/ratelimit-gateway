package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

// Concurrent requests to fire; override as `go run client/main.go 1000`.
const defaultRequests = 9500

func main() {
	requests := defaultRequests
	if len(os.Args) > 1 {
		n, err := strconv.Atoi(os.Args[1])
		if err != nil || n < 1 {
			log.Fatalf("usage: %s [requests]  (positive integer, default %d)", os.Args[0], defaultRequests)
		}
		requests = n
	}
	log.Println("firing", requests, "concurrent requests at http://127.0.0.1:8080/hello")

	// No client-side pool limit, so all requests dial out at once.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = requests
	http.DefaultClient.Transport = tr

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
