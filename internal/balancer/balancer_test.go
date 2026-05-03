package balancer

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Cluster tests
// ---------------------------------------------------------------------------

func TestNewCluster(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		rawURL         string
		region         string
		maxConnections int64
		wantErr        bool
	}{
		{
			name:           "valid_cluster",
			id:             "c-1",
			rawURL:         "http://localhost:8081",
			region:         "us-east",
			maxConnections: 500,
		},
		{
			name:    "empty_id",
			id:      "",
			rawURL:  "http://localhost:8081",
			wantErr: true,
		},
		{
			name:    "whitespace_only_id",
			id:      "   ",
			rawURL:  "http://localhost:8081",
			wantErr: true,
		},
		{
			name:    "missing_scheme",
			id:      "c-1",
			rawURL:  "localhost:8081",
			wantErr: true,
		},
		{
			name:    "missing_host",
			id:      "c-1",
			rawURL:  "http://",
			wantErr: true,
		},
		{
			name:           "negative_max_defaults_to_1000",
			id:             "c-1",
			rawURL:         "http://localhost:8081",
			maxConnections: -1,
		},
		{
			name:           "zero_max_defaults_to_1000",
			id:             "c-1",
			rawURL:         "http://localhost:8081",
			maxConnections: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, err := NewCluster(tt.id, tt.rawURL, tt.region, tt.maxConnections)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewCluster() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewCluster() unexpected error: %v", err)
			}
			if cluster.ID != tt.id {
				t.Errorf("got ID %q, want %q", cluster.ID, tt.id)
			}
			if tt.maxConnections <= 0 && cluster.MaxConnections != 1000 {
				t.Errorf("got MaxConnections %d, want 1000 (default)", cluster.MaxConnections)
			}
		})
	}
}

func TestCluster_ActiveConnections(t *testing.T) {
	c, _ := NewCluster("c-1", "http://localhost:8081", "us-east", 100)

	if got := c.ActiveConnections(); got != 0 {
		t.Fatalf("initial ActiveConnections: got %d, want 0", got)
	}

	c.IncActive()
	c.IncActive()
	c.IncActive()
	if got := c.ActiveConnections(); got != 3 {
		t.Fatalf("after 3 IncActive: got %d, want 3", got)
	}

	c.DecActive()
	if got := c.ActiveConnections(); got != 2 {
		t.Fatalf("after DecActive: got %d, want 2", got)
	}
}

func TestCluster_ReportedLoad(t *testing.T) {
	c, _ := NewCluster("c-1", "http://localhost:8081", "us-east", 100)

	if got := c.ReportedLoad(); got != 0 {
		t.Fatalf("initial ReportedLoad: got %d, want 0", got)
	}
	if got := c.LastReportTime(); !got.IsZero() {
		t.Fatalf("initial LastReportTime: got %v, want zero", got)
	}

	before := time.Now()
	c.UpdateReportedLoad(42)
	after := time.Now()

	if got := c.ReportedLoad(); got != 42 {
		t.Fatalf("after update: got %d, want 42", got)
	}
	lastReport := c.LastReportTime()
	if lastReport.Before(before) || lastReport.After(after) {
		t.Fatalf("LastReportTime %v not between %v and %v", lastReport, before, after)
	}
}

// ---------------------------------------------------------------------------
// RequestContext tests
// ---------------------------------------------------------------------------

func TestContextFromRequest(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		query      string
		remoteAddr string
		wantID     string
		wantRegion string
		wantAddr   string
	}{
		{
			name:       "headers_take_priority",
			headers:    map[string]string{"X-Client-ID": "abc", "X-Client-Region": "us-east"},
			query:      "client_id=fallback&region=eu",
			remoteAddr: "10.0.0.1:12345",
			wantID:     "abc",
			wantRegion: "us-east",
			wantAddr:   "10.0.0.1",
		},
		{
			name:       "falls_back_to_query",
			headers:    map[string]string{},
			query:      "client_id=qid&region=eu-west",
			remoteAddr: "10.0.0.1:12345",
			wantID:     "qid",
			wantRegion: "eu-west",
			wantAddr:   "10.0.0.1",
		},
		{
			name:       "empty_remote_addr",
			headers:    map[string]string{},
			query:      "",
			remoteAddr: "",
			wantID:     "",
			wantRegion: "",
			wantAddr:   "",
		},
		{
			name:       "remote_addr_without_port",
			headers:    map[string]string{},
			query:      "",
			remoteAddr: "10.0.0.1",
			wantID:     "",
			wantRegion: "",
			wantAddr:   "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqURL := "http://example.com/"
			if tt.query != "" {
				reqURL += "?" + tt.query
			}
			r := httptest.NewRequest(http.MethodGet, reqURL, nil)
			r.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}

			ctx := ContextFromRequest(r)
			if ctx.ClientID != tt.wantID {
				t.Errorf("ClientID: got %q, want %q", ctx.ClientID, tt.wantID)
			}
			if ctx.ClientRegion != tt.wantRegion {
				t.Errorf("ClientRegion: got %q, want %q", ctx.ClientRegion, tt.wantRegion)
			}
			if ctx.RemoteAddr != tt.wantAddr {
				t.Errorf("RemoteAddr: got %q, want %q", ctx.RemoteAddr, tt.wantAddr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// LoadBalancer tests
// ---------------------------------------------------------------------------

func mustCluster(t *testing.T, id, rawURL, region string) *Cluster {
	t.Helper()
	c, err := NewCluster(id, rawURL, region, 1000)
	if err != nil {
		t.Fatalf("mustCluster(%q): %v", id, err)
	}
	return c
}

func TestLoadBalancer_AddCluster(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		c := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
		if err := lb.AddCluster(c); err != nil {
			t.Fatalf("AddCluster() unexpected error: %v", err)
		}
	})

	t.Run("nil_cluster", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		if err := lb.AddCluster(nil); err == nil {
			t.Fatalf("AddCluster(nil) error = nil, want error")
		}
	})

	t.Run("missing_id", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		c := &Cluster{URL: &url.URL{Scheme: "http", Host: "localhost"}}
		if err := lb.AddCluster(c); err == nil {
			t.Fatalf("AddCluster(no id) error = nil, want error")
		}
	})

	t.Run("missing_url", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		c := &Cluster{ID: "c-1"}
		if err := lb.AddCluster(c); err == nil {
			t.Fatalf("AddCluster(no url) error = nil, want error")
		}
	})

	t.Run("duplicate_cluster", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		c := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
		_ = lb.AddCluster(c)
		if err := lb.AddCluster(c); err == nil {
			t.Fatalf("AddCluster(duplicate) error = nil, want error")
		}
	})
}

func TestLoadBalancer_RemoveCluster(t *testing.T) {
	t.Run("existing_cluster", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		c := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
		_ = lb.AddCluster(c)

		if got := lb.RemoveCluster("c-1"); !got {
			t.Fatalf("RemoveCluster(existing): got false, want true")
		}
		if _, ok := lb.GetCluster("c-1"); ok {
			t.Fatalf("GetCluster after remove: got ok=true, want false")
		}
	})

	t.Run("nonexistent_cluster", func(t *testing.T) {
		lb := NewLoadBalancer(nil)
		if got := lb.RemoveCluster("ghost"); got {
			t.Fatalf("RemoveCluster(nonexistent): got true, want false")
		}
	})
}

func TestLoadBalancer_GetCluster(t *testing.T) {
	lb := NewLoadBalancer(nil)
	c := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
	_ = lb.AddCluster(c)

	t.Run("found", func(t *testing.T) {
		got, ok := lb.GetCluster("c-1")
		if !ok {
			t.Fatalf("GetCluster(c-1): got ok=false, want true")
		}
		if got.ID != "c-1" {
			t.Errorf("got ID %q, want %q", got.ID, "c-1")
		}
	})

	t.Run("not_found", func(t *testing.T) {
		_, ok := lb.GetCluster("ghost")
		if ok {
			t.Fatalf("GetCluster(ghost): got ok=true, want false")
		}
	})
}

func TestLoadBalancer_ListClusters_deterministic_order(t *testing.T) {
	lb := NewLoadBalancer(nil)
	// Add in reverse alphabetical order to test sorting.
	_ = lb.AddCluster(mustCluster(t, "c-3", "http://localhost:8083", ""))
	_ = lb.AddCluster(mustCluster(t, "c-1", "http://localhost:8081", ""))
	_ = lb.AddCluster(mustCluster(t, "c-2", "http://localhost:8082", ""))

	list := lb.ListClusters()
	if len(list) != 3 {
		t.Fatalf("ListClusters length: got %d, want 3", len(list))
	}
	want := []string{"c-1", "c-2", "c-3"}
	for i, c := range list {
		if c.ID != want[i] {
			t.Errorf("ListClusters[%d]: got %q, want %q", i, c.ID, want[i])
		}
	}
}

func TestLoadBalancer_UpdateReportedLoad(t *testing.T) {
	lb := NewLoadBalancer(nil)
	c := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
	_ = lb.AddCluster(c)

	t.Run("happy_path", func(t *testing.T) {
		if err := lb.UpdateReportedLoad("c-1", 100); err != nil {
			t.Fatalf("UpdateReportedLoad() unexpected error: %v", err)
		}
		if got := lb.TotalReportedLoad(); got != 100 {
			t.Errorf("TotalReportedLoad: got %d, want 100", got)
		}
	})

	t.Run("unknown_cluster", func(t *testing.T) {
		err := lb.UpdateReportedLoad("ghost", 50)
		if err == nil {
			t.Fatalf("UpdateReportedLoad(unknown) error = nil, want error")
		}
		if err != ErrClusterNotFound {
			t.Errorf("got error %v, want ErrClusterNotFound", err)
		}
	})
}

func TestLoadBalancer_TotalReportedLoad_multiple(t *testing.T) {
	lb := NewLoadBalancer(nil)
	c1 := mustCluster(t, "c-1", "http://localhost:8081", "us-east")
	c2 := mustCluster(t, "c-2", "http://localhost:8082", "us-west")
	_ = lb.AddCluster(c1)
	_ = lb.AddCluster(c2)

	_ = lb.UpdateReportedLoad("c-1", 200)
	_ = lb.UpdateReportedLoad("c-2", 300)

	if got := lb.TotalReportedLoad(); got != 500 {
		t.Errorf("TotalReportedLoad: got %d, want 500", got)
	}
}

func TestLoadBalancer_TotalActiveConnections(t *testing.T) {
	lb := NewLoadBalancer(nil)
	c1 := mustCluster(t, "c-1", "http://localhost:8081", "")
	c2 := mustCluster(t, "c-2", "http://localhost:8082", "")
	_ = lb.AddCluster(c1)
	_ = lb.AddCluster(c2)

	c1.IncActive()
	c1.IncActive()
	c2.IncActive()

	if got := lb.TotalActiveConnections(); got != 3 {
		t.Errorf("TotalActiveConnections: got %d, want 3", got)
	}
}

func TestLoadBalancer_SelectCluster_no_clusters(t *testing.T) {
	lb := NewLoadBalancer(nil)
	_, err := lb.SelectCluster(RequestContext{})
	if err != ErrNoClusters {
		t.Fatalf("SelectCluster(empty): got %v, want ErrNoClusters", err)
	}
}

func TestLoadBalancer_SelectCluster_no_strategy(t *testing.T) {
	lb := NewLoadBalancer(nil)
	_ = lb.AddCluster(mustCluster(t, "c-1", "http://localhost:8081", ""))
	_, err := lb.SelectCluster(RequestContext{})
	if err == nil {
		t.Fatalf("SelectCluster(nil strategy): got nil, want error")
	}
}

// ---------------------------------------------------------------------------
// ServeHTTP tests
// ---------------------------------------------------------------------------

type stubStrategy struct{}

func (stubStrategy) Name() string { return "stub" }
func (stubStrategy) Select(_ RequestContext, clusters []*Cluster) (*Cluster, error) {
	if len(clusters) == 0 {
		return nil, ErrNoClusters
	}
	return clusters[0], nil
}

func TestLoadBalancer_ServeHTTP_health(t *testing.T) {
	lb := NewLoadBalancer(stubStrategy{})
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)
	rec := httptest.NewRecorder()

	lb.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("health check: got status %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("health check body: got %q, want %q", rec.Body.String(), "ok")
	}
}

func TestLoadBalancer_ServeHTTP_no_upstream(t *testing.T) {
	lb := NewLoadBalancer(stubStrategy{})
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	rec := httptest.NewRecorder()

	lb.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no upstream: got status %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
