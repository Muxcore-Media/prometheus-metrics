// Package prometheusmetrics implements a Prometheus metrics exposition module.
// It provides counter, gauge, and histogram metrics tracking for HTTP requests,
// events, module health, cluster membership, and task completion.
package prometheusmetrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// defaultBuckets are the default Prometheus histogram bucket boundaries in seconds.
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// histogram stores per-label-combination bucket counts for a single histogram metric.
type histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

func newHistogram(buckets []float64) *histogram {
	return &histogram{
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1), // +1 for +Inf bucket
	}
}

// Observe records a single observation.
func (h *histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.count++
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.counts[len(h.buckets)]++ // +Inf bucket always increments
}

// metricsStore holds all metric data in thread-safe structures.
type metricsStore struct {
	httpRequests   sync.Map // key: "METHOD path" -> *atomic.Int64
	histograms     sync.Map // key: "METHOD path" -> *histogram
	eventCounts    sync.Map // key: eventType -> *atomic.Int64
	moduleHealth   sync.Map // key: moduleID -> *atomic.Int64
	clusterMembers atomic.Int64
	isLeader       atomic.Int64
	tasksCompleted atomic.Int64
	tasksFailed    atomic.Int64
}

// incCounter atomically increments a counter stored in a sync.Map.
func (s *metricsStore) incCounter(m *sync.Map, key string) {
	v, _ := m.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// setGauge atomically sets a gauge value stored in a sync.Map.
func (s *metricsStore) setGauge(m *sync.Map, key string, val int64) {
	v, _ := m.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Store(val)
}

// incHTTPRequest increments the counter for the given method+path pair.
func (s *metricsStore) incHTTPRequest(method, path string) {
	s.incCounter(&s.httpRequests, method+" "+path)
}

// observeDuration records a single duration observation into the histogram
// for the given method+path pair.
func (s *metricsStore) observeDuration(method, path string, seconds float64) {
	key := method + " " + path
	h, _ := s.histograms.LoadOrStore(key, newHistogram(defaultBuckets))
	h.(*histogram).Observe(seconds)
}

// incEvent increments the counter for the given event type.
func (s *metricsStore) incEvent(eventType string) {
	s.incCounter(&s.eventCounts, eventType)
}

// setModuleHealth sets the health gauge for a module (1=healthy, 0=degraded).
func (s *metricsStore) setModuleHealth(moduleID string, healthy bool) {
	val := int64(1)
	if !healthy {
		val = 0
	}
	s.setGauge(&s.moduleHealth, moduleID, val)
}

// collect builds the complete Prometheus text-format exposition string.
// Each metric family gets one HELP and one TYPE line followed by its data lines.
func (s *metricsStore) collect() string {
	var b strings.Builder

	// -- HTTP request counts --
	s.writeCounters(&b, "muxcore_http_requests_total",
		"Total HTTP requests",
		[]string{"method", "path"},
		&s.httpRequests)

	// -- HTTP request duration histogram --
	s.writeHistograms(&b)

	// -- Event counts --
	s.writeCounters(&b, "muxcore_events_total",
		"Total events by type",
		[]string{"type"},
		&s.eventCounts)

	// -- Module health --
	b.WriteString("# HELP muxcore_module_health Module health (1=healthy, 0=degraded)\n")
	b.WriteString("# TYPE muxcore_module_health gauge\n")
	s.moduleHealth.Range(func(key, val any) bool {
		moduleID := key.(string)
		health := val.(*atomic.Int64).Load()
		fmt.Fprintf(&b, `muxcore_module_health{module_id=%q} %d`, moduleID, health)
		b.WriteString("\n")
		return true
	})
	b.WriteString("\n")

	// -- Cluster members --
	fmt.Fprintf(&b, "# HELP muxcore_cluster_members Number of cluster members\n")
	fmt.Fprintf(&b, "# TYPE muxcore_cluster_members gauge\n")
	fmt.Fprintf(&b, `muxcore_cluster_members %d`, s.clusterMembers.Load())
	b.WriteString("\n\n")

	// -- Cluster is leader --
	fmt.Fprintf(&b, "# HELP muxcore_cluster_is_leader 1 if this node is the cluster leader\n")
	fmt.Fprintf(&b, "# TYPE muxcore_cluster_is_leader gauge\n")
	fmt.Fprintf(&b, `muxcore_cluster_is_leader %d`, s.isLeader.Load())
	b.WriteString("\n\n")

	// -- Task completion --
	fmt.Fprintf(&b, "# HELP muxcore_tasks_completed_total Total completed tasks\n")
	fmt.Fprintf(&b, "# TYPE muxcore_tasks_completed_total counter\n")
	fmt.Fprintf(&b, `muxcore_tasks_completed_total %d`, s.tasksCompleted.Load())
	b.WriteString("\n\n")

	// -- Task failures --
	fmt.Fprintf(&b, "# HELP muxcore_tasks_failed_total Total failed tasks\n")
	fmt.Fprintf(&b, "# TYPE muxcore_tasks_failed_total counter\n")
	fmt.Fprintf(&b, `muxcore_tasks_failed_total %d`, s.tasksFailed.Load())
	b.WriteString("\n")

	return b.String()
}

// writeCounters writes a counter metric family (HELP, TYPE, and per-label data lines).
func (s *metricsStore) writeCounters(b *strings.Builder, name, help string, labelNames []string, m *sync.Map) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	m.Range(func(key, val any) bool {
		count := val.(*atomic.Int64).Load()
		labelValues := strings.SplitN(key.(string), " ", len(labelNames))
		b.WriteString(name)
		b.WriteString("{")
		for i, ln := range labelNames {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(b, "%s=%q", ln, labelValues[i])
		}
		b.WriteString("} ")
		fmt.Fprintf(b, "%d", count)
		b.WriteString("\n")
		return true
	})
	b.WriteString("\n")
}

// writeHistograms writes the HTTP request duration histogram family.
func (s *metricsStore) writeHistograms(b *strings.Builder) {
	b.WriteString("# HELP muxcore_http_request_duration_seconds HTTP request duration in seconds\n")
	b.WriteString("# TYPE muxcore_http_request_duration_seconds histogram\n")
	s.histograms.Range(func(key, val any) bool {
		methodPath := key.(string)
		h := val.(*histogram)
		h.mu.Lock()

		parts := strings.SplitN(methodPath, " ", 2)
		method, path := parts[0], parts[1]

		for i, bucket := range h.buckets {
			le := fmt.Sprintf("%g", bucket)
			fmt.Fprintf(b, `muxcore_http_request_duration_seconds_bucket{method=%q,path=%q,le=%q} %d`,
				method, path, le, h.counts[i])
			b.WriteString("\n")
		}
		fmt.Fprintf(b, `muxcore_http_request_duration_seconds_bucket{method=%q,path=%q,le=%q} %d`,
			method, path, "+Inf", h.counts[len(h.buckets)])
		b.WriteString("\n")
		fmt.Fprintf(b, `muxcore_http_request_duration_seconds_sum{method=%q,path=%q} %g`,
			method, path, h.sum)
		b.WriteString("\n")
		fmt.Fprintf(b, `muxcore_http_request_duration_seconds_count{method=%q,path=%q} %d`,
			method, path, h.count)
		b.WriteString("\n")

		h.mu.Unlock()
		return true
	})
	b.WriteString("\n")
}
