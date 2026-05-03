package strategy

import (
	"testing"
	"time"

	"github.com/ciara/gopherload/internal/balancer"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func mustCluster(t *testing.T, id, rawURL, region string) *balancer.Cluster {
	t.Helper()
	c, err := balancer.NewCluster(id, rawURL, region, 1000)
	if err != nil {
		t.Fatalf("mustCluster(%q): %v", id, err)
	}
	return c
}

// ---------------------------------------------------------------------------
// ModuloStrategy tests
// ---------------------------------------------------------------------------

func TestModuloStrategy_Name(t *testing.T) {
	if got := (ModuloStrategy{}).Name(); got != "modulo" {
		t.Errorf("Name(): got %q, want %q", got, "modulo")
	}
}

func TestModuloStrategy_Select(t *testing.T) {
	cA := mustCluster(t, "a", "http://localhost:8081", "us-east")
	cB := mustCluster(t, "b", "http://localhost:8082", "us-west")
	cC := mustCluster(t, "c", "http://localhost:8083", "eu-central")

	tests := []struct {
		name     string
		ctx      balancer.RequestContext
		clusters []*balancer.Cluster
		wantErr  error
		wantNil  bool
	}{
		{
			name:     "empty_cluster_list",
			ctx:      balancer.RequestContext{ClientID: "user-1"},
			clusters: nil,
			wantErr:  balancer.ErrNoClusters,
		},
		{
			name:     "single_cluster",
			ctx:      balancer.RequestContext{ClientID: "user-1"},
			clusters: []*balancer.Cluster{cA},
		},
		{
			name:     "uses_client_id_for_hash",
			ctx:      balancer.RequestContext{ClientID: "user-1"},
			clusters: []*balancer.Cluster{cA, cB, cC},
		},
		{
			name:     "falls_back_to_remote_addr",
			ctx:      balancer.RequestContext{RemoteAddr: "10.0.0.1"},
			clusters: []*balancer.Cluster{cA, cB, cC},
		},
		{
			name:     "falls_back_to_cluster_id_when_all_empty",
			ctx:      balancer.RequestContext{},
			clusters: []*balancer.Cluster{cA, cB, cC},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := ModuloStrategy{}
			got, err := s.Select(tt.ctx, tt.clusters)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Fatalf("Select() error: got %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Select() unexpected error: %v", err)
			}
			if got == nil {
				t.Fatalf("Select() returned nil cluster")
			}
		})
	}
}

func TestModuloStrategy_Select_deterministic(t *testing.T) {
	cA := mustCluster(t, "a", "http://localhost:8081", "us-east")
	cB := mustCluster(t, "b", "http://localhost:8082", "us-west")
	cC := mustCluster(t, "c", "http://localhost:8083", "eu-central")
	clusters := []*balancer.Cluster{cA, cB, cC}

	s := ModuloStrategy{}
	ctx := balancer.RequestContext{ClientID: "user-42"}

	first, _ := s.Select(ctx, clusters)
	for i := 0; i < 100; i++ {
		got, _ := s.Select(ctx, clusters)
		if got.ID != first.ID {
			t.Fatalf("iteration %d: got %q, want %q (not deterministic)", i, got.ID, first.ID)
		}
	}
}

// ---------------------------------------------------------------------------
// CurrentLoadStrategy tests
// ---------------------------------------------------------------------------

func TestCurrentLoadStrategy_Name(t *testing.T) {
	if got := (CurrentLoadStrategy{}).Name(); got != "current_load" {
		t.Errorf("Name(): got %q, want %q", got, "current_load")
	}
}

func TestCurrentLoadStrategy_Select(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T) []*balancer.Cluster
		wantID   string
		wantErr  error
	}{
		{
			name:    "empty_cluster_list",
			setup:   func(t *testing.T) []*balancer.Cluster { return nil },
			wantErr: balancer.ErrNoClusters,
		},
		{
			name: "single_cluster",
			setup: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{mustCluster(t, "a", "http://localhost:8081", "")}
			},
			wantID: "a",
		},
		{
			name: "picks_least_loaded",
			setup: func(t *testing.T) []*balancer.Cluster {
				cA := mustCluster(t, "a", "http://localhost:8081", "")
				cB := mustCluster(t, "b", "http://localhost:8082", "")
				// Simulate: a has 5 connections, b has 2.
				for i := 0; i < 5; i++ {
					cA.IncActive()
				}
				for i := 0; i < 2; i++ {
					cB.IncActive()
				}
				return []*balancer.Cluster{cA, cB}
			},
			wantID: "b",
		},
		{
			name: "tie_breaks_by_id_ascending",
			setup: func(t *testing.T) []*balancer.Cluster {
				cB := mustCluster(t, "b", "http://localhost:8082", "")
				cA := mustCluster(t, "a", "http://localhost:8081", "")
				// Both have 0 connections — tie.
				return []*balancer.Cluster{cB, cA}
			},
			wantID: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := CurrentLoadStrategy{}
			clusters := tt.setup(t)
			got, err := s.Select(balancer.RequestContext{}, clusters)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Fatalf("Select() error: got %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Select() unexpected error: %v", err)
			}
			if got.ID != tt.wantID {
				t.Errorf("Select() cluster ID: got %q, want %q", got.ID, tt.wantID)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProximityStrategy tests
// ---------------------------------------------------------------------------

func TestProximityStrategy_Name(t *testing.T) {
	if got := (ProximityStrategy{}).Name(); got != "proximity" {
		t.Errorf("Name(): got %q, want %q", got, "proximity")
	}
}

func testLatencyMap() map[string]map[string]time.Duration {
	return map[string]map[string]time.Duration{
		"us-east": {
			"us-east":    10 * time.Millisecond,
			"us-west":    60 * time.Millisecond,
			"eu-central": 90 * time.Millisecond,
		},
		"us-west": {
			"us-west":    12 * time.Millisecond,
			"us-east":    65 * time.Millisecond,
			"eu-central": 110 * time.Millisecond,
		},
	}
}

func TestProximityStrategy_Select(t *testing.T) {
	tests := []struct {
		name     string
		ctx      balancer.RequestContext
		clusters func(t *testing.T) []*balancer.Cluster
		wantID   string
		wantErr  error
	}{
		{
			name:    "empty_cluster_list",
			ctx:     balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster { return nil },
			wantErr: balancer.ErrNoClusters,
		},
		{
			name: "single_cluster",
			ctx:  balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{mustCluster(t, "a", "http://localhost:8081", "us-east")}
			},
			wantID: "a",
		},
		{
			name: "picks_lowest_latency_region",
			ctx:  balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{
					mustCluster(t, "eu", "http://localhost:8083", "eu-central"),
					mustCluster(t, "east", "http://localhost:8081", "us-east"),
					mustCluster(t, "west", "http://localhost:8082", "us-west"),
				}
			},
			wantID: "east",
		},
		{
			name: "same_latency_picks_lower_load",
			ctx:  balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster {
				cA := mustCluster(t, "east-1", "http://localhost:8081", "us-east")
				cB := mustCluster(t, "east-2", "http://localhost:8082", "us-east")
				// east-1 has 10 connections, east-2 has 3.
				for i := 0; i < 10; i++ {
					cA.IncActive()
				}
				for i := 0; i < 3; i++ {
					cB.IncActive()
				}
				return []*balancer.Cluster{cA, cB}
			},
			wantID: "east-2",
		},
		{
			name: "same_latency_same_load_tie_breaks_by_id",
			ctx:  balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{
					mustCluster(t, "z", "http://localhost:8082", "us-east"),
					mustCluster(t, "a", "http://localhost:8081", "us-east"),
				}
			},
			wantID: "a",
		},
		{
			name: "missing_client_region_uses_default_latency",
			ctx:  balancer.RequestContext{},
			clusters: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{
					mustCluster(t, "b", "http://localhost:8082", "us-west"),
					mustCluster(t, "a", "http://localhost:8081", "us-east"),
				}
			},
			// All get default latency, tie break by load (both 0), then by ID.
			wantID: "a",
		},
		{
			name: "missing_cluster_region_uses_default_latency",
			ctx:  balancer.RequestContext{ClientRegion: "us-east"},
			clusters: func(t *testing.T) []*balancer.Cluster {
				return []*balancer.Cluster{
					mustCluster(t, "b", "http://localhost:8082", ""),
					mustCluster(t, "a", "http://localhost:8081", ""),
				}
			},
			wantID: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := ProximityStrategy{
				LatencyMap:     testLatencyMap(),
				DefaultLatency: 250 * time.Millisecond,
			}
			clusters := tt.clusters(t)
			got, err := s.Select(tt.ctx, clusters)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Fatalf("Select() error: got %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Select() unexpected error: %v", err)
			}
			if got.ID != tt.wantID {
				t.Errorf("Select() cluster ID: got %q, want %q", got.ID, tt.wantID)
			}
		})
	}
}

func TestProximityStrategy_Select_zero_default_latency(t *testing.T) {
	// When DefaultLatency is 0, it should internally default to 250ms.
	s := ProximityStrategy{
		LatencyMap: testLatencyMap(),
		// DefaultLatency intentionally left as zero.
	}
	cA := mustCluster(t, "a", "http://localhost:8081", "us-east")
	ctx := balancer.RequestContext{ClientRegion: "unknown-region"}

	got, err := s.Select(ctx, []*balancer.Cluster{cA})
	if err != nil {
		t.Fatalf("Select() unexpected error: %v", err)
	}
	if got.ID != "a" {
		t.Errorf("got %q, want %q", got.ID, "a")
	}
}
