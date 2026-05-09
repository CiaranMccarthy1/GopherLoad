package scaler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ciara/gopherload/internal/metrics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

var ErrNotConfigured = errors.New("kubernetes client not configured")

type Controller struct {
	clientset          kubernetes.Interface
	namespace          string
	deploymentName     string
	scaleUpThreshold   int64
	scaleDownThreshold int64
	cooldown           time.Duration
	lastActionUnix     int64
}

func NewController(kubeconfigPath, namespace, deploymentName string, scaleUp, scaleDown int64, cooldown time.Duration) (*Controller, error) {
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
	if deploymentName == "" {
		deploymentName = "gopherload"
	}
	if cooldown <= 0 {
		cooldown = 2 * time.Minute
	}

	return &Controller{
		clientset:          clientset,
		namespace:          namespace,
		deploymentName:     deploymentName,
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

func (s *Controller) EvaluateAndScale(ctx context.Context, totalLoad int64) error {
	if s == nil || s.clientset == nil {
		return ErrNotConfigured
	}

	if !s.readyForAction() {
		return nil
	}

	if totalLoad > s.scaleUpThreshold {
		if err := s.ScaleUp(ctx); err != nil {
			return err
		}
		s.markAction()
		return nil
	}

	if totalLoad < s.scaleDownThreshold {
		if err := s.ScaleDown(ctx); err != nil {
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

func (s *Controller) ScaleUp(ctx context.Context) error {
	if s.clientset == nil {
		return ErrNotConfigured
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := s.clientset.AppsV1().Deployments(s.namespace).Get(ctx, s.deploymentName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("deployment %q not found in namespace %q", s.deploymentName, s.namespace)
			}
			return fmt.Errorf("failed to get deployment: %w", err)
		}

		oldReplicas := int32(1)
		if deployment.Spec.Replicas != nil {
			oldReplicas = *deployment.Spec.Replicas
		}
		newReplicas := oldReplicas + 1
		deployment.Spec.Replicas = &newReplicas

		updated, err := s.clientset.AppsV1().Deployments(s.namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		log.Printf("Scaled up deployment %s/%s from %d to %d replicas", s.namespace, s.deploymentName, oldReplicas, *updated.Spec.Replicas)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scale up: %w", err)
	}

	metrics.IncScaleEventsTotal("up")

	return nil
}

func (s *Controller) ScaleDown(ctx context.Context) error {
	if s.clientset == nil {
		return ErrNotConfigured
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := s.clientset.AppsV1().Deployments(s.namespace).Get(ctx, s.deploymentName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("deployment %q not found in namespace %q", s.deploymentName, s.namespace)
			}
			return fmt.Errorf("failed to get deployment: %w", err)
		}

		oldReplicas := int32(1)
		if deployment.Spec.Replicas != nil {
			oldReplicas = *deployment.Spec.Replicas
		}
		
		if oldReplicas > 1 {
			newReplicas := oldReplicas - 1
			deployment.Spec.Replicas = &newReplicas
		} else {
			log.Printf("Deployment %s/%s is already at minimum replicas (1), not scaling down", s.namespace, s.deploymentName)
			return nil
		}

		updated, err := s.clientset.AppsV1().Deployments(s.namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		log.Printf("Scaled down deployment %s/%s from %d to %d replicas", s.namespace, s.deploymentName, oldReplicas, *updated.Spec.Replicas)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scale down: %w", err)
	}

	metrics.IncScaleEventsTotal("down")

	return nil
}
