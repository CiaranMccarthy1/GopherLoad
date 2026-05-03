package rpc

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/ciara/gopherload/api/proto"
	"github.com/ciara/gopherload/internal/balancer"
	"github.com/ciara/gopherload/internal/metrics"
	"github.com/ciara/gopherload/internal/scaler"
)

// ClusterStatusService handles gRPC load reports from clusters.
type ClusterStatusService struct {
	pb.UnimplementedClusterStatusServer
	lb     *balancer.LoadBalancer
	scaler *scaler.Controller
}

func NewClusterStatusService(lb *balancer.LoadBalancer, sc *scaler.Controller) *ClusterStatusService {
	return &ClusterStatusService{
		lb:     lb,
		scaler: sc,
	}
}

func (s *ClusterStatusService) ReportLoad(ctx context.Context, req *pb.LoadReport) (*pb.LoadAck, error) {
	if s.lb == nil {
		return &pb.LoadAck{Accepted: false, Message: "load balancer not ready"}, nil
	}
	if req == nil || strings.TrimSpace(req.ClusterId) == "" {
		return &pb.LoadAck{Accepted: false, Message: "cluster_id is required"}, nil
	}

	if err := s.lb.UpdateReportedLoad(req.ClusterId, req.ActiveConnections); err != nil {
		return &pb.LoadAck{Accepted: false, Message: err.Error()}, nil
	}

	if metrics.ReportedLoad != nil {
		metrics.ReportedLoad.WithLabelValues(req.ClusterId).Set(float64(req.ActiveConnections))
	}

	total := s.lb.TotalReportedLoad()
	if s.scaler != nil {
		if err := s.scaler.EvaluateAndScale(ctx, total); err != nil {
			return &pb.LoadAck{Accepted: true, Message: fmt.Sprintf("scaler error: %v", err), TotalLoad: total}, nil
		}
	}

	return &pb.LoadAck{Accepted: true, Message: "load updated", TotalLoad: total}, nil
}
