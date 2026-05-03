package balancer

import (
	"errors"
	"net/http"
	"net/http/httputil"
	"sort"
	"sync"
)

var (
	ErrNoClusters      = errors.New("no clusters available")
	ErrClusterNotFound = errors.New("cluster not found")
)

// Strategy is the canonical routing interface for GopherLoad.
//
// All routing algorithm implementations (e.g. in internal/strategy) satisfy
// this interface implicitly via Go's structural typing. There must be exactly
// one definition of this contract — here in the balancer package — to maintain
// a single source of truth and avoid hidden coupling.
type Strategy interface {
	// Name returns a human-readable identifier for the strategy (e.g. "modulo").
	Name() string
	// Select chooses a target cluster from the provided list based on the
	// request context. Implementations must handle an empty cluster slice
	// by returning ErrNoClusters.
	Select(ctx RequestContext, clusters []*Cluster) (*Cluster, error)
}

// LoadBalancer manages routing decisions across clusters.
type LoadBalancer struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster
	strategy Strategy
}

// NewLoadBalancer creates a load balancer with the provided strategy.
func NewLoadBalancer(strategy Strategy) *LoadBalancer {
	return &LoadBalancer{
		clusters: make(map[string]*Cluster),
		strategy: strategy,
	}
}

// AddCluster registers a new backend target.
func (lb *LoadBalancer) AddCluster(cluster *Cluster) error {
	if cluster == nil {
		return errors.New("cluster is nil")
	}
	if cluster.ID == "" || cluster.URL == nil {
		return errors.New("cluster must have id and url")
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()
	if _, exists := lb.clusters[cluster.ID]; exists {
		return errors.New("cluster already exists")
	}
	lb.clusters[cluster.ID] = cluster
	return nil
}

// RemoveCluster removes a backend by id.
func (lb *LoadBalancer) RemoveCluster(id string) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if _, ok := lb.clusters[id]; !ok {
		return false
	}
	delete(lb.clusters, id)
	return true
}

// GetCluster returns a backend by id.
func (lb *LoadBalancer) GetCluster(id string) (*Cluster, bool) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	cluster, ok := lb.clusters[id]
	return cluster, ok
}

// ListClusters returns a deterministic snapshot of backends.
func (lb *LoadBalancer) ListClusters() []*Cluster {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	list := make([]*Cluster, 0, len(lb.clusters))
	for _, cluster := range lb.clusters {
		list = append(list, cluster)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	return list
}

// SetStrategy updates the routing strategy.
func (lb *LoadBalancer) SetStrategy(strategy Strategy) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.strategy = strategy
}

// SelectCluster chooses a backend using the configured strategy.
func (lb *LoadBalancer) SelectCluster(ctx RequestContext) (*Cluster, error) {
	clusters := lb.ListClusters()
	if len(clusters) == 0 {
		return nil, ErrNoClusters
	}

	lb.mu.RLock()
	strategy := lb.strategy
	lb.mu.RUnlock()
	if strategy == nil {
		return nil, errors.New("no strategy configured")
	}

	return strategy.Select(ctx, clusters)
}

// UpdateReportedLoad records cluster metrics from the gRPC service.
func (lb *LoadBalancer) UpdateReportedLoad(clusterID string, load int64) error {
	cluster, ok := lb.GetCluster(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	cluster.UpdateReportedLoad(load)
	return nil
}

// TotalReportedLoad sums the most recently reported cluster load values.
func (lb *LoadBalancer) TotalReportedLoad() int64 {
	var total int64
	clusters := lb.ListClusters()
	for _, cluster := range clusters {
		total += cluster.ReportedLoad()
	}
	return total
}

// TotalActiveConnections sums the current in-flight connections.
func (lb *LoadBalancer) TotalActiveConnections() int64 {
	var total int64
	clusters := lb.ListClusters()
	for _, cluster := range clusters {
		total += cluster.ActiveConnections()
	}
	return total
}

// ServeHTTP handles incoming L7 requests and proxies them to a cluster.
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/__health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}

	ctx := ContextFromRequest(r)
	cluster, err := lb.SelectCluster(ctx)
	if err != nil {
		http.Error(w, "no upstream available", http.StatusServiceUnavailable)
		return
	}

	cluster.IncActive()
	defer cluster.DecActive()

	proxy := httputil.NewSingleHostReverseProxy(cluster.URL)
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		http.Error(rw, "upstream error", http.StatusBadGateway)
	}

	r.Header.Set("X-GopherLoad-Cluster", cluster.ID)
	proxy.ServeHTTP(w, r)
}
