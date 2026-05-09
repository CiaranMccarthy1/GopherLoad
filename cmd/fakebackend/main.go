// Package main is a lightweight fake backend HTTP server used for local
// development and testing of a load balancer.
//
// Usage:
//
//	go run ./cmd/fakebackend -port 8081 -id cluster-a
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type response struct {
	Backend string `json:"backend"`
	Port    string `json:"port"`
	Path    string `json:"path"`
	Method  string `json:"method"`
}

func main() {
	port := flag.Int("port", 8081, "TCP port to listen on")
	id := flag.String("id", "backend", "Name/ID of this backend instance")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	portStr := fmt.Sprintf("%d", *port)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[%s] %s %s from %s", *id, r.Method, r.URL.Path, remoteHost(r))
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[%s] %s %s from %s", *id, r.Method, r.URL.Path, remoteHost(r))

		jitter := time.Duration(rand.Intn(51)) * time.Millisecond
		time.Sleep(jitter)

		if rand.Float64() < 0.05 {
			http.Error(w, `{"error":"simulated internal server error"}`, http.StatusInternalServerError)
			return
		}

		body := response{
			Backend: *id,
			Port:    portStr,
			Path:    r.URL.Path,
			Method:  r.Method,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			log.Printf("[%s] failed to write response: %v", *id, err)
		}
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("[%s] listening on %s", *id, addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("[%s] received signal %s — shutting down", *id, sig)
	case err := <-serverErr:
		log.Fatalf("[%s] server error: %v", *id, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[%s] forced shutdown: %v", *id, err)
	}

	log.Printf("[%s] server stopped cleanly", *id)
}

// remoteHost strips the port from r.RemoteAddr.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
