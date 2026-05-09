package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	once sync.Once

	RequestsTotal *prometheus.CounterVec

	RequestDuration *prometheus.HistogramVec

	ActiveConnections *prometheus.GaugeVec

	ReportedLoad *prometheus.GaugeVec

	ScaleEventsTotal *prometheus.CounterVec
)

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

func IncActiveConnections(clusterID string) {
	if ActiveConnections != nil {
		ActiveConnections.WithLabelValues(clusterID).Inc()
	}
}

func DecActiveConnections(clusterID string) {
	if ActiveConnections != nil {
		ActiveConnections.WithLabelValues(clusterID).Dec()
	}
}

func ObserveRequestDuration(clusterID string, duration float64) {
	if RequestDuration != nil {
		RequestDuration.WithLabelValues(clusterID).Observe(duration)
	}
}

func IncRequestsTotal(clusterID string, statusCode string) {
	if RequestsTotal != nil {
		RequestsTotal.WithLabelValues(clusterID, statusCode).Inc()
	}
}

func SetReportedLoad(clusterID string, load float64) {
	if ReportedLoad != nil {
		ReportedLoad.WithLabelValues(clusterID).Set(load)
	}
}

func IncScaleEventsTotal(direction string) {
	if ScaleEventsTotal != nil {
		ScaleEventsTotal.WithLabelValues(direction).Inc()
	}
}
