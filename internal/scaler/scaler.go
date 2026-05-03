package scaler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var ErrNotConfigured = errors.New("kubernetes client not configured")

// Controller drives scale decisions based on total system load.
type Controller struct {
	clientset          *kubernetes.Clientset
	namespace          string
	scaleUpThreshold   int64
	scaleDownThreshold int64
	cooldown           time.Duration
	lastActionUnix     int64
	mu                 sync.Mutex
}

// NewController creates a Kubernetes client and configures thresholds.
func NewController(kubeconfigPath, namespace string, scaleUp, scaleDown int64, cooldown time.Duration) (*Controller, error) {
	config, err := buildKubeConfig(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}
	if namespace == "" {
		namespace = "default"
	}
	if cooldown <= 0 {
		cooldown = 2 * time.Minute
	}

	return &Controller{
		clientset:          clientset,
		namespace:          namespace,
		scaleUpThreshold:   scaleUp,
		scaleDownThreshold: scaleDown,
		cooldown:           cooldown,
	}, nil
}

func buildKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	return rest.InClusterConfig()
}

// EvaluateAndScale compares total load to thresholds and triggers scaling.
func (s *Controller) EvaluateAndScale(ctx context.Context, totalLoad int64) error {
	if s == nil || s.clientset == nil {
		return ErrNotConfigured
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.readyForAction() {
		return nil
	}

	if totalLoad > s.scaleUpThreshold {
		if err := s.CreateCluster(ctx); err != nil {
			return err
		}
		s.markAction()
		return nil
	}

	if totalLoad < s.scaleDownThreshold {
		if err := s.DeleteCluster(ctx); err != nil {
			return err
		}
		s.markAction()
		return nil
	}

	return nil
}

func (s *Controller) readyForAction() bool {
	if s.cooldown <= 0 {
		return true
	}
	last := atomic.LoadInt64(&s.lastActionUnix)
	if last == 0 {
		return true
	}
	return time.Since(time.Unix(0, last)) >= s.cooldown
}

func (s *Controller) markAction() {
	atomic.StoreInt64(&s.lastActionUnix, time.Now().UnixNano())
}

// CreateCluster is a stub for provisioning a new cluster or node pool.
func (s *Controller) CreateCluster(ctx context.Context) error {
	if s.clientset == nil {
		return ErrNotConfigured
	}

	// TODO: Replace with actual infrastructure provisioning logic.
	_, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("kubernetes api check failed: %w", err)
	}

	return nil
}

// DeleteCluster is a stub for decommissioning a cluster or node pool.
func (s *Controller) DeleteCluster(ctx context.Context) error {
	if s.clientset == nil {
		return ErrNotConfigured
	}

	// TODO: Replace with actual infrastructure deprovisioning logic.
	_, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("kubernetes api check failed: %w", err)
	}

	return nil
}
