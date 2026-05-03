package strategy

import (
	"github.com/ciara/gopherload/internal/balancer"
)

// Strategy selects a target cluster for each request.
type Strategy interface {
	Name() string
	Select(ctx balancer.RequestContext, clusters []*balancer.Cluster) (*balancer.Cluster, error)
}
