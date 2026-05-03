package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ciara/gopherload/internal/balancer"
	"github.com/ciara/gopherload/internal/rpc"
	"github.com/ciara/gopherload/internal/scaler"
	"github.com/ciara/gopherload/internal/strategy"

	pb "github.com/ciara/gopherload/api/proto"
	"github.com/ciara/gopherload/internal/metrics"
	"google.golang.org/grpc"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ";")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Strategy names define the valid routing algorithms.
const (
	StrategyModulo      = "modulo"
	StrategyProximity   = "proximity"
	StrategyCurrentLoad = "current_load"
)

// Config holds the application configuration parsed from flags.
type Config struct {
	HTTPAddr      string
	GRPCAddr      string
	StrategyName  string
	Kubeconfig    string
	Namespace     string
	Deployment    string
	ScaleUp       int64
	ScaleDown     int64
	ScaleCooldown time.Duration
	Backends      stringList
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8080", "HTTP reverse proxy listen address")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", ":9090", "gRPC listen address")
	flag.StringVar(&cfg.StrategyName, "strategy", StrategyCurrentLoad, "Routing strategy: modulo|proximity|current_load")
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Path to kubeconfig (optional)")
	flag.StringVar(&cfg.Namespace, "namespace", "default", "Kubernetes namespace for scaling actions")
	flag.StringVar(&cfg.Deployment, "deployment", "gopherload", "Kubernetes deployment name to scale")
	flag.Int64Var(&cfg.ScaleUp, "scale-up", 800, "Scale up when total reported load exceeds this value")
	flag.Int64Var(&cfg.ScaleDown, "scale-down", 200, "Scale down when total reported load falls below this value")
	flag.DurationVar(&cfg.ScaleCooldown, "scale-cooldown", 2*time.Minute, "Minimum time between scaling actions")
	flag.Var(&cfg.Backends, "backend", "Backend cluster spec: id=<id>,url=<url>,region=<region>,max=<max>")
	flag.Parse()
	return cfg
}

func main() {
	cfg := parseFlags()

	metrics.Register()

	// 1. Build Strategy
	strategyImpl, err := buildStrategy(cfg.StrategyName)
	if err != nil {
		log.Fatalf("invalid strategy: %v", err)
	}

	// 2. Initialize Load Balancer
	lb := balancer.NewLoadBalancer(strategyImpl)
	specs := cfg.Backends
	if len(specs) == 0 {
		specs = defaultBackendSpecs()
	}

	for _, spec := range specs {
		cluster, err := parseClusterSpec(spec)
		if err != nil {
			log.Fatalf("invalid backend %q: %v", spec, err)
		}
		if err := lb.AddCluster(cluster); err != nil {
			log.Fatalf("failed to add backend %q: %v", cluster.ID, err)
		}
	}

	// 3. Initialize Scaler
	sc, err := scaler.NewController(cfg.Kubeconfig, cfg.Namespace, cfg.Deployment, cfg.ScaleUp, cfg.ScaleDown, cfg.ScaleCooldown)
	if err != nil {
		log.Printf("scaler disabled: %v", err)
		sc = nil
	}

	// 4. Setup gRPC
	grpcServer := grpc.NewServer()
	pb.RegisterClusterStatusServer(grpcServer, rpc.NewClusterStatusService(lb))

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("failed to listen on gRPC address %s: %v", cfg.GRPCAddr, err)
	}

	// 5. Setup HTTP
	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: lb,
	}

	// 6. Graceful Shutdown orchestration
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errChan := make(chan error, 2)

	go func() {
		log.Printf("gRPC server listening on %s (Protobuf codec)", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			errChan <- fmt.Errorf("gRPC server failed: %w", err)
		}
	}()

	go func() {
		log.Printf("HTTP proxy listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- fmt.Errorf("HTTP server failed: %w", err)
		}
	}()

	// 7. Background Scaling Loop
	if sc != nil {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					total := lb.TotalReportedLoad()
					if err := sc.EvaluateAndScale(ctx, total); err != nil {
						log.Printf("scaling error: %v", err)
					}
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		log.Printf("shutting down gracefully (signal received)")
	case err := <-errChan:
		log.Printf("shutting down due to error: %v", err)
		stop() // Cancel context to stop other goroutines
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
}

func buildStrategy(name string) (balancer.Strategy, error) {
	switch strings.ToLower(name) {
	case StrategyModulo:
		return strategy.ModuloStrategy{}, nil
	case StrategyProximity:
		return strategy.ProximityStrategy{
			LatencyMap:     defaultLatencyMap(),
			DefaultLatency: 250 * time.Millisecond,
		}, nil
	case StrategyCurrentLoad, "load", "current":
		return strategy.CurrentLoadStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown strategy %q", name)
	}
}

func parseClusterSpec(spec string) (*balancer.Cluster, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, errors.New("backend spec is empty")
	}

	var (
		id             string
		targetURL      string
		region         string
		maxConnections int64 = 1000
	)

	parts := strings.Split(spec, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid backend field %q", part)
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		value := strings.TrimSpace(kv[1])
		switch key {
		case "id":
			id = value
		case "url", "target":
			targetURL = value
		case "region":
			region = value
		case "max":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid max value %q: %w", value, err)
			}
			maxConnections = parsed
		default:
			return nil, fmt.Errorf("unknown backend field %q", key)
		}
	}

	if id == "" || targetURL == "" {
		return nil, errors.New("backend must include id and url")
	}

	return balancer.NewCluster(id, targetURL, region, maxConnections)
}

func defaultBackendSpecs() []string {
	return []string{
		"id=cluster-a,url=http://localhost:8081,region=us-east,max=1000",
		"id=cluster-b,url=http://localhost:8082,region=us-west,max=1000",
		"id=cluster-c,url=http://localhost:8083,region=eu-central,max=1000",
	}
}

func defaultLatencyMap() map[string]map[string]time.Duration {
	return map[string]map[string]time.Duration{
		"us-east": {
			"us-east":    10 * time.Millisecond,
			"us-west":    60 * time.Millisecond,
			"eu-central": 90 * time.Millisecond,
		},
		"us-west": {
			"us-west":    12 * time.Millisecond,
			"us-east":    65 * time.Millisecond,
			"eu-central": 110 * time.Millisecond,
		},
		"eu-central": {
			"eu-central": 15 * time.Millisecond,
			"us-east":    95 * time.Millisecond,
			"us-west":    120 * time.Millisecond,
		},
	}
}
