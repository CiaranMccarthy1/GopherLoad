package rpc

import (
	"context"
	"testing"

	pb "github.com/ciara/gopherload/api/proto"
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

func setupLB(t *testing.T) *balancer.LoadBalancer {
	t.Helper()
	lb := balancer.NewLoadBalancer(nil)
	_ = lb.AddCluster(mustCluster(t, "c-1", "http://localhost:8081", "us-east"))
	_ = lb.AddCluster(mustCluster(t, "c-2", "http://localhost:8082", "us-west"))
	return lb
}

// ---------------------------------------------------------------------------
// ClusterStatusService.ReportLoad tests
// ---------------------------------------------------------------------------

func TestReportLoad(t *testing.T) {
	tests := []struct {
		name         string
		lb           *balancer.LoadBalancer
		req          *pb.LoadReport
		wantAccepted bool
		wantMessage  string
	}{
		{
			name:         "nil_load_balancer",
			lb:           nil,
			req:          &pb.LoadReport{ClusterId: "c-1", ActiveConnections: 10},
			wantAccepted: false,
			wantMessage:  "load balancer not ready",
		},
		{
			name:         "nil_request",
			req:          nil,
			wantAccepted: false,
			wantMessage:  "cluster_id is required",
		},
		{
			name:         "empty_cluster_id",
			req:          &pb.LoadReport{ClusterId: "", ActiveConnections: 10},
			wantAccepted: false,
			wantMessage:  "cluster_id is required",
		},
		{
			name:         "whitespace_cluster_id",
			req:          &pb.LoadReport{ClusterId: "   ", ActiveConnections: 10},
			wantAccepted: false,
			wantMessage:  "cluster_id is required",
		},
		{
			name:         "unknown_cluster",
			req:          &pb.LoadReport{ClusterId: "ghost", ActiveConnections: 10},
			wantAccepted: false,
			wantMessage:  "cluster not found",
		},
		{
			name:         "happy_path",
			req:          &pb.LoadReport{ClusterId: "c-1", ActiveConnections: 42},
			wantAccepted: true,
			wantMessage:  "load updated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lb *balancer.LoadBalancer
			if tt.lb == nil && tt.name != "nil_load_balancer" {
				lb = setupLB(t)
			} else {
				lb = tt.lb
			}

			svc := NewClusterStatusService(lb)
			ack, err := svc.ReportLoad(context.Background(), tt.req)
			if err != nil {
				t.Fatalf("ReportLoad() unexpected error: %v", err)
			}
			if ack.Accepted != tt.wantAccepted {
				t.Errorf("Accepted: got %v, want %v", ack.Accepted, tt.wantAccepted)
			}
			if ack.Message != tt.wantMessage {
				t.Errorf("Message: got %q, want %q", ack.Message, tt.wantMessage)
			}
		})
	}
}

func TestReportLoad_updates_total_load(t *testing.T) {
	lb := setupLB(t)
	svc := NewClusterStatusService(lb)

	_, _ = svc.ReportLoad(context.Background(), &pb.LoadReport{ClusterId: "c-1", ActiveConnections: 100})
	_, _ = svc.ReportLoad(context.Background(), &pb.LoadReport{ClusterId: "c-2", ActiveConnections: 200})

	ack, err := svc.ReportLoad(context.Background(), &pb.LoadReport{ClusterId: "c-1", ActiveConnections: 150})
	if err != nil {
		t.Fatalf("ReportLoad() unexpected error: %v", err)
	}
	// c-1: 150, c-2: 200 → total = 350
	if ack.TotalLoad != 350 {
		t.Errorf("TotalLoad: got %d, want 350", ack.TotalLoad)
	}
}

func TestReportLoad_scaler_nil_no_panic(t *testing.T) {
	lb := setupLB(t)
	// Passing nil scaler — must not panic.
	svc := NewClusterStatusService(lb)

	ack, err := svc.ReportLoad(context.Background(), &pb.LoadReport{ClusterId: "c-1", ActiveConnections: 50})
	if err != nil {
		t.Fatalf("ReportLoad() unexpected error: %v", err)
	}
	if !ack.Accepted {
		t.Errorf("Accepted: got false, want true")
	}
}
