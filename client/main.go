package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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

	var wg sync.WaitGroup

	for i := range requests {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			resp, err := http.Get("http://127.0.0.1:8080/hello")
			if err != nil {
				// Don't log.Fatal: one failure must not stop the load.
				log.Println("request", id, "failed:", err)
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}(i)
	}

	wg.Wait()
}
