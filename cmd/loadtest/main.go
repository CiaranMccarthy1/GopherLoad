// Package main is a simple load tester for the GopherLoad balancer.
// It sends requests at a configurable rate and prints a live summary.
//
// Usage:
//
//	go run ./cmd/loadtest -rate 100 -url http://localhost:8080 -path /test
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

type result struct {
	status  int
	backend string
	latency time.Duration
	err     error
}

type stats struct {
	mu                sync.Mutex
	total             int
	success           int
	errors            int
	statusCodes       map[int]int
	backends          map[string]int
	totalLatency      time.Duration
	consecutiveErrors int
}

func (s *stats) record(r result) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	if r.err != nil || r.status >= 500 {
		s.errors++
		s.consecutiveErrors++
	} else {
		s.success++
		s.consecutiveErrors = 0
	}
	if r.status > 0 {
		s.statusCodes[r.status]++
	}
	if r.backend != "" {
		s.backends[r.backend]++
	}
	s.totalLatency += r.latency
	return s.consecutiveErrors
}

func (s *stats) print(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	avgLatency := time.Duration(0)
	if s.total > 0 {
		avgLatency = s.totalLatency / time.Duration(s.total)
	}
	rps := float64(s.total) / elapsed.Seconds()
	rpm := int64(rps * 60)

	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Elapsed:      %s\n", elapsed.Round(time.Second))
	fmt.Printf("  Total reqs:   %d  (%.1f req/s)\n", s.total, rps)
	fmt.Printf("  Requests per minute: %d\n", rpm)
	fmt.Printf("  Success:      %d\n", s.success)
	fmt.Printf("  Errors:       %d\n", s.errors)
	fmt.Printf("  Avg latency:  %s\n", avgLatency.Round(time.Millisecond))

	if len(s.statusCodes) > 0 {
		fmt.Println("  Status codes:")
		codes := make([]int, 0, len(s.statusCodes))
		for c := range s.statusCodes {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		for _, c := range codes {
			fmt.Printf("    %d: %d\n", c, s.statusCodes[c])
		}
	}

	if len(s.backends) > 0 {
		fmt.Println("  Backend distribution:")
		ids := make([]string, 0, len(s.backends))
		for id := range s.backends {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			pct := float64(s.backends[id]) / float64(s.total) * 100
			fmt.Printf("    %-20s %d requests (%.1f%%)\n", id, s.backends[id], pct)
		}
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func main() {
	rate := flag.Int("rate", 100, "Requests per minute to send")
	target := flag.String("url", "http://localhost:8080", "Base URL of the load balancer")
	path := flag.String("path", "/test", "Request path to hit")
	summary := flag.Duration("summary", 10*time.Second, "How often to print a summary")
	failAfter := flag.Int("fail-after", 0, "Stop after N consecutive failures (0 disables)")
	flag.Parse()

	interval := time.Minute / time.Duration(*rate)
	log.Printf("Sending %d req/min to %s%s  (1 request every %s)", *rate, *target, *path, interval)
	log.Printf("Press Ctrl+C to stop.\n")

	transport := &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 1000,
	}
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
	}
	st := &stats{
		statusCodes: make(map[int]int),
		backends:    make(map[string]int),
	}

	start := time.Now()
	ticker := time.NewTicker(interval)
	printer := time.NewTicker(*summary)
	quit := make(chan os.Signal, 1)
	stop := make(chan struct{})
	stopOnce := sync.Once{}
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-quit:
			ticker.Stop()
			printer.Stop()
			log.Println("Stopping — final summary:")
			st.print(time.Since(start))
			return

		case <-stop:
			ticker.Stop()
			printer.Stop()
			log.Printf("Stopping — %d consecutive failures reached:", *failAfter)
			st.print(time.Since(start))
			return

		case <-printer.C:
			st.print(time.Since(start))

		case <-ticker.C:
			go func() {
				r := sendRequest(client, *target+*path)
				consecutiveErrors := st.record(r)
				if *failAfter > 0 && consecutiveErrors >= *failAfter {
					stopOnce.Do(func() {
						close(stop)
					})
				}

				symbol := "✓"
				if r.err != nil {
					symbol = "✗"
					log.Printf("%s  error: %v", symbol, r.err)
				} else if r.status >= 500 {
					symbol = "✗"
					log.Printf("%s  HTTP %d from %s  (%s)", symbol, r.status, r.backend, r.latency.Round(time.Millisecond))
				} else {
					log.Printf("%s  HTTP %d from %-20s  (%s)", symbol, r.status, r.backend, r.latency.Round(time.Millisecond))
				}
			}()
		}
	}
}

func sendRequest(client *http.Client, url string) result {
	start := time.Now()
	resp, err := client.Get(url)
	latency := time.Since(start)
	if err != nil {
		return result{err: err, latency: latency}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var payload struct {
		Backend string `json:"backend"`
	}
	_ = json.Unmarshal(body, &payload)

	return result{
		status:  resp.StatusCode,
		backend: payload.Backend,
		latency: latency,
	}
}
