package balancer

import (
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ciara/gopherload/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ErrNoClusters      = errors.New("no clusters available")
	ErrClusterNotFound = errors.New("cluster not found")
)

type Strategy interface {
	Name() string
	Select(ctx RequestContext, clusters []*Cluster) (*Cluster, error)
}

type LoadBalancer struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster
	sorted   []*Cluster // Cached sorted list for O(1) read
	strategy Strategy
}

func NewLoadBalancer(strategy Strategy) *LoadBalancer {
	return &LoadBalancer{
		clusters: make(map[string]*Cluster),
		strategy: strategy,
	}
}

// Caller must hold lb.mu for writing.
func (lb *LoadBalancer) updateSorted() {
	list := make([]*Cluster, 0, len(lb.clusters))
	for _, cluster := range lb.clusters {
		list = append(list, cluster)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	lb.sorted = list
}

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
	lb.updateSorted()
	return nil
}

func (lb *LoadBalancer) RemoveCluster(id string) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if _, ok := lb.clusters[id]; !ok {
		return false
	}
	delete(lb.clusters, id)
	lb.updateSorted()
	return true
}

func (lb *LoadBalancer) GetCluster(id string) (*Cluster, bool) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	cluster, ok := lb.clusters[id]
	return cluster, ok
}

func (lb *LoadBalancer) ListClusters() []*Cluster {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	res := make([]*Cluster, 0, len(lb.sorted))
	for _, c := range lb.sorted {
		if c.IsHealthy() {
			res = append(res, c)
		}
	}
	return res
}

func (lb *LoadBalancer) SetStrategy(strategy Strategy) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.strategy = strategy
}

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

func (lb *LoadBalancer) UpdateReportedLoad(clusterID string, load int64) error {
	cluster, ok := lb.GetCluster(clusterID)
	if !ok {
		return ErrClusterNotFound
	}
	cluster.UpdateReportedLoad(load)
	return nil
}

func (lb *LoadBalancer) TotalReportedLoad() int64 {
	var total int64
	clusters := lb.ListClusters()
	for _, cluster := range clusters {
		total += cluster.ReportedLoad()
	}
	return total
}

func (lb *LoadBalancer) TotalActiveConnections() int64 {
	var total int64
	clusters := lb.ListClusters()
	for _, cluster := range clusters {
		total += cluster.ActiveConnections()
	}
	return total
}

type responseCapture struct {
	http.ResponseWriter
	statusCode int
}

func (rc *responseCapture) WriteHeader(code int) {
	rc.statusCode = code
	rc.ResponseWriter.WriteHeader(code)
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/__health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}

	if r.URL.Path == "/metrics" {
		promhttp.Handler().ServeHTTP(w, r)
		return
	}

	ctx := ContextFromRequest(r)
	cluster, err := lb.SelectCluster(ctx)
	if err != nil {
		http.Error(w, "no upstream available", http.StatusServiceUnavailable)
		return
	}

	log.Printf("[LB] Routing %s %s -> %s", r.Method, r.URL.Path, cluster.ID)

	cluster.IncActive()
	metrics.IncActiveConnections(cluster.ID)

	start := time.Now()

	r.Header.Set("X-GopherLoad-Cluster", cluster.ID)

	rc := &responseCapture{ResponseWriter: w, statusCode: http.StatusOK}
	cluster.Proxy.ServeHTTP(rc, r)

	if rc.statusCode >= 500 {
		cluster.RecordError()
	} else {
		cluster.RecordSuccess()
	}

	elapsed := time.Since(start).Seconds()

	cluster.DecActive()
	metrics.DecActiveConnections(cluster.ID)
	metrics.IncRequestsTotal(cluster.ID, strconv.Itoa(rc.statusCode))
	metrics.ObserveRequestDuration(cluster.ID, elapsed)
}
