package rpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/ciara/gopherload/internal/balancer"
	"github.com/ciara/gopherload/internal/scaler"
	"google.golang.org/grpc"
)

// LoadReport captures cluster load metrics reported via gRPC.
type LoadReport struct {
	ClusterID         string `json:"cluster_id"`
	ActiveConnections int64  `json:"active_connections"`
	Region            string `json:"region,omitempty"`
	MaxConnections    int64  `json:"max_connections,omitempty"`
	ObservedAtUnix    int64  `json:"observed_at_unix,omitempty"`
}

// LoadAck is returned after the balancer processes a load report.
type LoadAck struct {
	Accepted  bool   `json:"accepted"`
	Message   string `json:"message,omitempty"`
	TotalLoad int64  `json:"total_load,omitempty"`
}

// ClusterStatusService handles gRPC load reports from clusters.
type ClusterStatusService struct {
	lb     *balancer.LoadBalancer
	scaler *scaler.Controller
}

func NewClusterStatusService(lb *balancer.LoadBalancer, sc *scaler.Controller) *ClusterStatusService {
	return &ClusterStatusService{
		lb:     lb,
		scaler: sc,
	}
}

func (s *ClusterStatusService) ReportLoad(ctx context.Context, req *LoadReport) (*LoadAck, error) {
	if s.lb == nil {
		return &LoadAck{Accepted: false, Message: "load balancer not ready"}, nil
	}
	if req == nil || strings.TrimSpace(req.ClusterID) == "" {
		return &LoadAck{Accepted: false, Message: "cluster_id is required"}, nil
	}

	if err := s.lb.UpdateReportedLoad(req.ClusterID, req.ActiveConnections); err != nil {
		return &LoadAck{Accepted: false, Message: err.Error()}, nil
	}

	total := s.lb.TotalReportedLoad()
	if s.scaler != nil {
		if err := s.scaler.EvaluateAndScale(ctx, total); err != nil {
			return &LoadAck{Accepted: true, Message: fmt.Sprintf("scaler error: %v", err), TotalLoad: total}, nil
		}
	}

	return &LoadAck{Accepted: true, Message: "load updated", TotalLoad: total}, nil
}

const clusterStatusServiceName = "gopherload.v1.ClusterStatus"

// ClusterStatusServer is the gRPC contract for load reporting.
type ClusterStatusServer interface {
	ReportLoad(context.Context, *LoadReport) (*LoadAck, error)
}

// RegisterClusterStatusServer wires the service into a gRPC server.
func RegisterClusterStatusServer(server *grpc.Server, svc ClusterStatusServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: clusterStatusServiceName,
		HandlerType: (*ClusterStatusServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "ReportLoad",
				Handler:    _ClusterStatus_ReportLoad_Handler,
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "api/proto/cluster_status.proto",
	}, svc)
}

const _ClusterStatus_ReportLoad_FullMethodName = "/gopherload.v1.ClusterStatus/ReportLoad"

func _ClusterStatus_ReportLoad_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LoadReport)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterStatusServer).ReportLoad(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: _ClusterStatus_ReportLoad_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterStatusServer).ReportLoad(ctx, req.(*LoadReport))
	}
	return interceptor(ctx, in, info, handler)
}
