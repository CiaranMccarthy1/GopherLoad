package balancer_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ciara/gopherload/internal/balancer"
	"github.com/ciara/gopherload/internal/metrics"
	"github.com/ciara/gopherload/internal/strategy"
)

func TestLoadBalancer_EndToEnd(t *testing.T) {
	metrics.Register() // ensure metrics are available for the test

	// 1. Create a backend HTTP server
	backendRequests := 0
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendRequests++
		
		// Check that the proxy passes the custom header
		if r.Header.Get("X-GopherLoad-Cluster") == "" {
			t.Errorf("Expected X-GopherLoad-Cluster header to be set by the proxy")
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend-response"))
	}))
	defer backendServer.Close()

	// 2. Create the LoadBalancer with CurrentLoad strategy
	strat := strategy.CurrentLoadStrategy{}
	lb := balancer.NewLoadBalancer(strat)

	// 3. Register the backend cluster
	cluster, err := balancer.NewCluster("backend-1", backendServer.URL, "us-east", 100)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	if err := lb.AddCluster(cluster); err != nil {
		t.Fatalf("Failed to add cluster: %v", err)
	}

	// 4. Create an httptest server wrapping the LoadBalancer
	proxyServer := httptest.NewServer(lb)
	defer proxyServer.Close()

	// 5. Send HTTP requests to the LoadBalancer
	client := &http.Client{Timeout: 2 * time.Second}
	
	req, err := http.NewRequest(http.MethodGet, proxyServer.URL+"/testpath", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	// Simulate an active connection by keeping the request open briefly or just checking before it finishes?
	// The active connections counter is incremented/decremented synchronously in ServeHTTP, so checking it outside is hard 
	// because it will be 0 after the request finishes. We can just verify backend hits.

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "backend-response" {
		t.Errorf("Expected 'backend-response', got %q", string(body))
	}

	// Verify backend actually received the request
	if backendRequests != 1 {
		t.Errorf("Expected 1 backend request, got %d", backendRequests)
	}

	// 6. Test diagnostic endpoints
	healthReq, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/__health", nil)
	healthResp, err := client.Do(healthReq)
	if err != nil {
		t.Fatalf("Health check request failed: %v", err)
	}
	defer healthResp.Body.Close()

	if healthResp.StatusCode != http.StatusOK {
		t.Errorf("Expected health status 200, got %d", healthResp.StatusCode)
	}

	healthBody, _ := io.ReadAll(healthResp.Body)
	if string(healthBody) != "ok" {
		t.Errorf("Expected health body 'ok', got %q", string(healthBody))
	}

	metricsReq, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/metrics", nil)
	metricsResp, err := client.Do(metricsReq)
	if err != nil {
		t.Fatalf("Metrics request failed: %v", err)
	}
	defer metricsResp.Body.Close()

	if metricsResp.StatusCode != http.StatusOK {
		t.Errorf("Expected metrics status 200, got %d", metricsResp.StatusCode)
	}
}
