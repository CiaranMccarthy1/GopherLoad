// Package metrics provides production-grade Prometheus observability for GopherLoad.
//
// All collectors are registered exactly once via sync.Once to prevent
// duplicate registration panics when tests or multiple init paths run.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	once sync.Once

	// RequestsTotal counts proxied HTTP requests per cluster and status code.
	RequestsTotal *prometheus.CounterVec

	// RequestDuration observes request latency per cluster.
	RequestDuration *prometheus.HistogramVec

	// ActiveConnections tracks in-flight connections per cluster.
	ActiveConnections *prometheus.GaugeVec

	// ReportedLoad tracks the most recent load report per cluster.
	ReportedLoad *prometheus.GaugeVec

	// ScaleEventsTotal counts scaling actions by direction (up or down).
	ScaleEventsTotal *prometheus.CounterVec
)

// Register initialises and registers all Prometheus collectors.
// It is safe to call multiple times; registration happens exactly once.
func Register() {
	once.Do(func() {
		RequestsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gopherload",
				Name:      "requests_total",
				Help:      "Total number of proxied HTTP requests.",
			},
			[]string{"cluster_id", "status_code"},
		)

		RequestDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "gopherload",
				Name:      "request_duration_seconds",
				Help:      "Histogram of request latency in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"cluster_id"},
		)

		ActiveConnections = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "gopherload",
				Name:      "active_connections",
				Help:      "Current number of in-flight connections per cluster.",
			},
			[]string{"cluster_id"},
		)

		ReportedLoad = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "gopherload",
				Name:      "reported_load",
				Help:      "Most recently reported load per cluster.",
			},
			[]string{"cluster_id"},
		)

		ScaleEventsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gopherload",
				Name:      "scale_events_total",
				Help:      "Total number of scaling events by direction.",
			},
			[]string{"direction"},
		)

		prometheus.MustRegister(
			RequestsTotal,
			RequestDuration,
			ActiveConnections,
			ReportedLoad,
			ScaleEventsTotal,
		)
	})
}

// IncActiveConnections increments the active connections gauge for a cluster.
func IncActiveConnections(clusterID string) {
	if ActiveConnections != nil {
		ActiveConnections.WithLabelValues(clusterID).Inc()
	}
}

// DecActiveConnections decrements the active connections gauge for a cluster.
func DecActiveConnections(clusterID string) {
	if ActiveConnections != nil {
		ActiveConnections.WithLabelValues(clusterID).Dec()
	}
}

// ObserveRequestDuration records the duration of a request for a cluster.
func ObserveRequestDuration(clusterID string, duration float64) {
	if RequestDuration != nil {
		RequestDuration.WithLabelValues(clusterID).Observe(duration)
	}
}

// IncRequestsTotal increments the requests counter for a cluster and status code.
func IncRequestsTotal(clusterID string, statusCode string) {
	if RequestsTotal != nil {
		RequestsTotal.WithLabelValues(clusterID, statusCode).Inc()
	}
}

// SetReportedLoad sets the reported load gauge for a cluster.
func SetReportedLoad(clusterID string, load float64) {
	if ReportedLoad != nil {
		ReportedLoad.WithLabelValues(clusterID).Set(load)
	}
}

// IncScaleEventsTotal increments the scale events counter for a direction.
func IncScaleEventsTotal(direction string) {
	if ScaleEventsTotal != nil {
		ScaleEventsTotal.WithLabelValues(direction).Inc()
	}
}
