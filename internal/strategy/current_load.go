package strategy

import (
	"github.com/ciara/gopherload/internal/balancer"
)

var _ balancer.Strategy = CurrentLoadStrategy{}

type CurrentLoadStrategy struct{}

func (CurrentLoadStrategy) Name() string { return "current_load" }

func (CurrentLoadStrategy) Select(ctx balancer.RequestContext, clusters []*balancer.Cluster) (*balancer.Cluster, error) {
	if len(clusters) == 0 {
		return nil, balancer.ErrNoClusters
	}
	best := clusters[0]
	bestLoad := best.ActiveConnections()

	for _, cluster := range clusters[1:] {
		load := cluster.ActiveConnections()
		if load < bestLoad || (load == bestLoad && cluster.ID < best.ID) {
			best = cluster
			bestLoad = load
		}
	}

	return best, nil
}
