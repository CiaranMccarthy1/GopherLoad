package balancer

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// Cluster represents a backend target and its runtime metrics.
type Cluster struct {
	ID             string
	URL            *url.URL
	Region         string
	MaxConnections int64

	activeConnections int64
	reportedLoad      int64
	lastReportAt      int64
}

// NewCluster parses and validates a backend target.
func NewCluster(id, rawURL, region string, maxConnections int64) (*Cluster, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("cluster id is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("cluster url must include scheme and host")
	}
	if maxConnections <= 0 {
		maxConnections = 1000
	}
	return &Cluster{
		ID:             id,
		URL:            parsed,
		Region:         region,
		MaxConnections: maxConnections,
	}, nil
}

func (c *Cluster) ActiveConnections() int64 {
	return atomic.LoadInt64(&c.activeConnections)
}

func (c *Cluster) IncActive() {
	atomic.AddInt64(&c.activeConnections, 1)
}

func (c *Cluster) DecActive() {
	atomic.AddInt64(&c.activeConnections, -1)
}

func (c *Cluster) ReportedLoad() int64 {
	return atomic.LoadInt64(&c.reportedLoad)
}

func (c *Cluster) UpdateReportedLoad(load int64) {
	atomic.StoreInt64(&c.reportedLoad, load)
	atomic.StoreInt64(&c.lastReportAt, time.Now().UnixNano())
}

func (c *Cluster) LastReportTime() time.Time {
	last := atomic.LoadInt64(&c.lastReportAt)
	if last == 0 {
		return time.Time{}
	}
	return time.Unix(0, last)
}

// RequestContext carries request metadata used by strategies.
type RequestContext struct {
	ClientID     string
	ClientRegion string
	RemoteAddr   string
}

// ContextFromRequest extracts routing hints from an HTTP request.
func ContextFromRequest(r *http.Request) RequestContext {
	return RequestContext{
		ClientID:     firstNonEmpty(r.Header.Get("X-Client-ID"), r.URL.Query().Get("client_id")),
		ClientRegion: firstNonEmpty(r.Header.Get("X-Client-Region"), r.URL.Query().Get("region")),
		RemoteAddr:   clientIP(r.RemoteAddr),
	}
}

func clientIP(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
