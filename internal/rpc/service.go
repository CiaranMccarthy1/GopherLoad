package rpc

import (
	"context"
	"strings"

	pb "github.com/ciara/gopherload/api/proto"
	"github.com/ciara/gopherload/internal/balancer"
	"github.com/ciara/gopherload/internal/metrics"
)

type ClusterStatusService struct {
	pb.UnimplementedClusterStatusServer
	lb *balancer.LoadBalancer
}

func NewClusterStatusService(lb *balancer.LoadBalancer) *ClusterStatusService {
	return &ClusterStatusService{
		lb: lb,
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

	metrics.SetReportedLoad(req.ClusterId, float64(req.ActiveConnections))

	total := s.lb.TotalReportedLoad()

	return &pb.LoadAck{Accepted: true, Message: "load updated", TotalLoad: total}, nil
}
