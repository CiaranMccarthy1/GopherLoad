package scaler

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestController_ScaleUp(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gopherload",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
		},
	})

	sc := &Controller{
		clientset:          clientset,
		namespace:          "default",
		deploymentName:     "gopherload",
		scaleUpThreshold:   800,
		scaleDownThreshold: 200,
		cooldown:           time.Second,
	}

	err := sc.ScaleUp(context.Background())
	if err != nil {
		t.Fatalf("ScaleUp failed: %v", err)
	}

	scale, err := clientset.AppsV1().Deployments("default").Get(context.Background(), "gopherload", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get scale: %v", err)
	}

	if *scale.Spec.Replicas != 2 {
		t.Errorf("Expected 2 replicas, got %d", *scale.Spec.Replicas)
	}
}

func TestController_ScaleDown(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gopherload",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
		},
	})

	sc := &Controller{
		clientset:          clientset,
		namespace:          "default",
		deploymentName:     "gopherload",
		scaleUpThreshold:   800,
		scaleDownThreshold: 200,
		cooldown:           time.Second,
	}

	err := sc.ScaleDown(context.Background())
	if err != nil {
		t.Fatalf("ScaleDown failed: %v", err)
	}

	scale, err := clientset.AppsV1().Deployments("default").GetScale(context.Background(), "gopherload", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get scale: %v", err)
	}

	if scale.Spec.Replicas != 1 {
		t.Errorf("Expected 1 replica, got %d", scale.Spec.Replicas)
	}
}

func TestController_ScaleDown_MinimumBound(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gopherload",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1), // already at minimum
		},
	})

	sc := &Controller{
		clientset:          clientset,
		namespace:          "default",
		deploymentName:     "gopherload",
		scaleUpThreshold:   800,
		scaleDownThreshold: 200,
		cooldown:           time.Second,
	}

	err := sc.ScaleDown(context.Background())
	if err != nil {
		t.Fatalf("ScaleDown failed: %v", err)
	}

	scale, err := clientset.AppsV1().Deployments("default").GetScale(context.Background(), "gopherload", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get scale: %v", err)
	}

	if scale.Spec.Replicas != 1 {
		t.Errorf("Expected 1 replica, got %d", scale.Spec.Replicas)
	}
}

func TestController_EvaluateAndScale_Cooldown(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gopherload",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
		},
	})

	sc := &Controller{
		clientset:          clientset,
		namespace:          "default",
		deploymentName:     "gopherload",
		scaleUpThreshold:   800,
		scaleDownThreshold: 200,
		cooldown:           time.Hour, // very long cooldown
	}

	// Trigger scale up
	err := sc.EvaluateAndScale(context.Background(), 1000)
	if err != nil {
		t.Fatalf("EvaluateAndScale failed: %v", err)
	}

	scale, _ := clientset.AppsV1().Deployments("default").Get(context.Background(), "gopherload", metav1.GetOptions{})
	if *scale.Spec.Replicas != 2 {
		t.Errorf("Expected 2 replicas after first scale, got %d", *scale.Spec.Replicas)
	}

	// Trigger another scale up immediately
	err = sc.EvaluateAndScale(context.Background(), 1000)
	if err != nil {
		t.Fatalf("EvaluateAndScale failed: %v", err)
	}

	scale2, _ := clientset.AppsV1().Deployments("default").Get(context.Background(), "gopherload", metav1.GetOptions{})
	if *scale2.Spec.Replicas != 2 {
		t.Errorf("Expected 2 replicas after second scale (cooldown should prevent it), got %d", *scale2.Spec.Replicas)
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}
