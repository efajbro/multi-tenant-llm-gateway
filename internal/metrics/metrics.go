// Package metrics registers all Prometheus metrics for the AI gateway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all registered counters, histograms, and gauges.
type Metrics struct {
	Registry              *prometheus.Registry
	RequestsTotal         *prometheus.CounterVec
	RequestDuration       *prometheus.HistogramVec
	RateLimitTotal        *prometheus.CounterVec
	QueueDepth            *prometheus.GaugeVec
	ActiveJobs            *prometheus.GaugeVec
	WorkerPoolUtilization  prometheus.Gauge
	TokensStreamed         *prometheus.CounterVec
	GPUVRAMUsedBytes       *prometheus.GaugeVec
	GPUUtilizationPercent  *prometheus.GaugeVec
}

// New creates and registers all metrics.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total", Help: "Total gateway API requests.",
		}, []string{"tenant", "model", "status"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gateway_request_duration_seconds", Help: "End-to-end request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"tenant", "model"}),
		RateLimitTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_rate_limit_total", Help: "Rate limit denials by tenant and layer.",
		}, []string{"tenant", "limit_type"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_queue_depth", Help: "Pending jobs per Kueue queue.",
		}, []string{"queue"}),
		ActiveJobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_active_jobs", Help: "In-flight K8s Jobs.",
		}, []string{"tenant", "model"}),
		WorkerPoolUtilization: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_worker_pool_utilization", Help: "K8s worker pool slot occupancy (0.0-1.0).",
		}),
		TokensStreamed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_tokens_streamed_total", Help: "LLM tokens delivered to clients.",
		}, []string{"tenant", "model"}),
		GPUVRAMUsedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gpu_vram_used_bytes", Help: "VRAM consumed per GPU device.",
		}, []string{"node", "device"}),
		GPUUtilizationPercent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gpu_utilization_percent", Help: "GPU compute utilization %.",
		}, []string{"node", "device"}),
	}
	reg.MustRegister(
		m.RequestsTotal, m.RequestDuration, m.RateLimitTotal,
		m.QueueDepth, m.ActiveJobs, m.WorkerPoolUtilization,
		m.TokensStreamed, m.GPUVRAMUsedBytes, m.GPUUtilizationPercent,
	)
	return m
}

// Handler returns the Prometheus metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
