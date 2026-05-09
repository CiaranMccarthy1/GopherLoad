package balancer

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type Cluster struct {
	ID             string
	URL            *url.URL
	Region         string
	MaxConnections int64
	Proxy          *httputil.ReverseProxy

	activeConnections int64
	reportedLoad      int64
	lastReportAt      int64
	consecutiveErrors int64
	lastErrorAt       int64
}

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

	proxy := httputil.NewSingleHostReverseProxy(parsed)
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		http.Error(rw, "upstream error", http.StatusBadGateway)
	}

	return &Cluster{
		ID:             id,
		URL:            parsed,
		Region:         region,
		MaxConnections: maxConnections,
		Proxy:          proxy,
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

func (c *Cluster) RecordError() {
	atomic.AddInt64(&c.consecutiveErrors, 1)
	atomic.StoreInt64(&c.lastErrorAt, time.Now().UnixNano())
}

func (c *Cluster) RecordSuccess() {
	atomic.StoreInt64(&c.consecutiveErrors, 0)
}

func (c *Cluster) IsHealthy() bool {
	errs := atomic.LoadInt64(&c.consecutiveErrors)
	if errs < 5 {
		return true
	}
	lastErr := atomic.LoadInt64(&c.lastErrorAt)
	return time.Since(time.Unix(0, lastErr)) > 10*time.Second
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

type RequestContext struct {
	ClientID     string
	ClientRegion string
	RemoteAddr   string
}

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
