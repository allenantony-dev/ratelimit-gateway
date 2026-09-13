package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func helloHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(10 * time.Second) // slow upstream so requests pile up in the proxy
	fmt.Fprintln(w, "Hello, World!")
}

func main() {
	http.HandleFunc("/hello", helloHandler)

	fmt.Println("Starting server on :9000...")

	if err := http.ListenAndServe(":9000", nil); err != nil {
		log.Fatal(err)
	}
}
