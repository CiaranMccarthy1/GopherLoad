package strategy

import (
	"time"

	"github.com/ciara/gopherload/internal/balancer"
)

// ProximityStrategy favors the lowest latency region, then lowest load.
type ProximityStrategy struct {
	LatencyMap     map[string]map[string]time.Duration
	DefaultLatency time.Duration
}

func (ProximityStrategy) Name() string { return "proximity" }

func (s ProximityStrategy) Select(ctx balancer.RequestContext, clusters []*balancer.Cluster) (*balancer.Cluster, error) {
	if len(clusters) == 0 {
		return nil, balancer.ErrNoClusters
	}
	best := clusters[0]
	bestLatency := s.latency(ctx.ClientRegion, best.Region)
	bestLoad := best.ActiveConnections()

	for _, cluster := range clusters[1:] {
		latency := s.latency(ctx.ClientRegion, cluster.Region)
		load := cluster.ActiveConnections()
		if latency < bestLatency ||
			(latency == bestLatency && load < bestLoad) ||
			(latency == bestLatency && load == bestLoad && cluster.ID < best.ID) {
			best = cluster
			bestLatency = latency
			bestLoad = load
		}
	}

	return best, nil
}

func (s ProximityStrategy) latency(clientRegion, clusterRegion string) time.Duration {
	defaultLatency := s.DefaultLatency
	if defaultLatency == 0 {
		defaultLatency = 250 * time.Millisecond
	}
	if clientRegion == "" || clusterRegion == "" {
		return defaultLatency
	}
	if byCluster, ok := s.LatencyMap[clientRegion]; ok {
		if latency, ok := byCluster[clusterRegion]; ok {
			return latency
		}
	}
	return defaultLatency
}
