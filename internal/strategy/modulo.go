package strategy

import (
	"hash/fnv"
	"io"

	"github.com/ciara/gopherload/internal/balancer"
)

var _ balancer.Strategy = ModuloStrategy{}

type ModuloStrategy struct{}

func (ModuloStrategy) Name() string { return "modulo" }

func (ModuloStrategy) Select(ctx balancer.RequestContext, clusters []*balancer.Cluster) (*balancer.Cluster, error) {
	if len(clusters) == 0 {
		return nil, balancer.ErrNoClusters
	}
	key := ctx.ClientID
	if key == "" {
		key = ctx.RemoteAddr
	}
	if key == "" {
		key = clusters[0].ID
	}
	hasher := fnv.New32a()
	_, _ = io.WriteString(hasher, key)
	idx := int(hasher.Sum32() % uint32(len(clusters)))
	return clusters[idx], nil
}
