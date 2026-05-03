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

	"google.golang.org/grpc"
	pb "github.com/ciara/gopherload/api/proto"
	"github.com/ciara/gopherload/internal/metrics"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ";")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var (
		httpAddr      = flag.String("http-addr", ":8080", "HTTP reverse proxy listen address")
		grpcAddr      = flag.String("grpc-addr", ":9090", "gRPC listen address")
		strategyName  = flag.String("strategy", "current_load", "Routing strategy: modulo|proximity|current_load")
		kubeconfig    = flag.String("kubeconfig", "", "Path to kubeconfig (optional)")
		namespace     = flag.String("namespace", "default", "Kubernetes namespace for scaling actions")
		scaleUp       = flag.Int64("scale-up", 800, "Scale up when total reported load exceeds this value")
		scaleDown     = flag.Int64("scale-down", 200, "Scale down when total reported load falls below this value")
		scaleCooldown = flag.Duration("scale-cooldown", 2*time.Minute, "Minimum time between scaling actions")
		backends      stringList
	)
	flag.Var(&backends, "backend", "Backend cluster spec: id=<id>,url=<url>,region=<region>,max=<max>")
	flag.Parse()

	metrics.Register()

	// 1. Build Strategy
	strategyImpl, err := buildStrategy(*strategyName)
	if err != nil {
		log.Fatalf("invalid strategy: %v", err)
	}

	// 2. Initialize Load Balancer
	lb := balancer.NewLoadBalancer(strategyImpl)
	specs := backends
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
	sc, err := scaler.NewController(*kubeconfig, *namespace, *scaleUp, *scaleDown, *scaleCooldown)
	if err != nil {
		log.Printf("scaler disabled: %v", err)
		sc = nil
	}

	// 4. Setup gRPC
	grpcServer := grpc.NewServer()
	pb.RegisterClusterStatusServer(grpcServer, rpc.NewClusterStatusService(lb, sc))

	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on gRPC address %s: %v", *grpcAddr, err)
	}

	// 5. Setup HTTP
	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: lb,
	}

	// 6. Graceful Shutdown orchestration
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("gRPC server listening on %s (Protobuf codec)", *grpcAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	go func() {
		log.Printf("HTTP proxy listening on %s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
}

func buildStrategy(name string) (balancer.Strategy, error) {
	switch strings.ToLower(name) {
	case "modulo":
		return strategy.ModuloStrategy{}, nil
	case "proximity":
		return strategy.ProximityStrategy{
			LatencyMap:     defaultLatencyMap(),
			DefaultLatency: 250 * time.Millisecond,
		}, nil
	case "current_load", "load", "current":
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
