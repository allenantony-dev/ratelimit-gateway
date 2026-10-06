package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func helloHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	fmt.Fprintf(w, "%s %s\n", r.Method, r.URL)
	for k, v := range r.Header {
		fmt.Fprintf(w, "%s: %v\n", k, v)
	}
	fmt.Fprintf(w, "\nbody: %q\n", body)
}

func main() {
	http.HandleFunc("/hello", helloHandler)

	fmt.Println("Starting server on :9000...")

	if err := http.ListenAndServe(":9000", nil); err != nil {
		log.Fatal(err)
	}
}
