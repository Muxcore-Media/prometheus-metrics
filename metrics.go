package prometheusmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func init() {
	contracts.Register(func(deps contracts.ModuleDeps) contracts.Module {
		return &Module{
			deps:    deps,
			metrics: &metricsStore{},
		}
	})
}

// Module implements the Prometheus metrics exposition module.
// It exposes a /metrics endpoint, subscribes to event bus events for
// event-level metrics, and tracks cluster membership.
type Module struct {
	mu      sync.Mutex
	deps    contracts.ModuleDeps
	metrics *metricsStore

	// cleanup holds unsubscribe functions for all event bus subscriptions.
	cleanup []func()
	started bool
}

// Info returns the module's metadata.
func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:          "prometheus-metrics",
		Name:        "Prometheus Metrics",
		Version:     "1.0.0",
		Kinds:       []contracts.ModuleKind{contracts.ModuleKindProvider},
		Description: "Prometheus metrics exposition with request tracking, event throughput, module health, and cluster monitoring",
		Author:      "Muxcore-Media",
		Capabilities: []string{
			"metrics.prometheus",
			"metrics.http",
			"metrics.events",
			"metrics.cluster",
		},
	}
}

// Init prepares the module for startup. No-op for this module.
func (m *Module) Init(ctx context.Context) error { return nil }

// Start registers the /metrics route and subscribes to event bus events.
func (m *Module) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	// Register /metrics HTTP endpoint.
	m.deps.Routes.HandleFunc("/metrics", m.handleMetrics)

	// Subscribe to all events to count event throughput.
	if err := m.deps.EventBus.Subscribe(ctx, "*", m.onAnyEvent); err != nil {
		return fmt.Errorf("subscribe to all events: %w", err)
	}
	m.cleanup = append(m.cleanup, func() {
		m.deps.EventBus.Unsubscribe(ctx, "*", m.onAnyEvent)
	})

	// Subscribe to module.degraded events to track module health.
	if err := m.deps.EventBus.Subscribe(ctx, contracts.EventModuleDegraded, m.onModuleDegraded); err != nil {
		return fmt.Errorf("subscribe to module.degraded: %w", err)
	}
	m.cleanup = append(m.cleanup, func() {
		m.deps.EventBus.Unsubscribe(ctx, contracts.EventModuleDegraded, m.onModuleDegraded)
	})

	// Subscribe to task events.
	if err := m.deps.EventBus.Subscribe(ctx, "worker.task.completed", m.onTaskCompleted); err != nil {
		return fmt.Errorf("subscribe to worker.task.completed: %w", err)
	}
	m.cleanup = append(m.cleanup, func() {
		m.deps.EventBus.Unsubscribe(ctx, "worker.task.completed", m.onTaskCompleted)
	})

	if err := m.deps.EventBus.Subscribe(ctx, "worker.task.failed", m.onTaskFailed); err != nil {
		return fmt.Errorf("subscribe to worker.task.failed: %w", err)
	}
	m.cleanup = append(m.cleanup, func() {
		m.deps.EventBus.Unsubscribe(ctx, "worker.task.failed", m.onTaskFailed)
	})

	// If cluster is available, subscribe to cluster events and initialize gauges.
	if m.deps.Cluster != nil {
		if err := m.deps.EventBus.Subscribe(ctx, contracts.EventClusterNodeJoined, m.onClusterNodeJoined); err != nil {
			return fmt.Errorf("subscribe to cluster.node.joined: %w", err)
		}
		m.cleanup = append(m.cleanup, func() {
			m.deps.EventBus.Unsubscribe(ctx, contracts.EventClusterNodeJoined, m.onClusterNodeJoined)
		})

		if err := m.deps.EventBus.Subscribe(ctx, contracts.EventClusterNodeLeft, m.onClusterNodeLeft); err != nil {
			return fmt.Errorf("subscribe to cluster.node.left: %w", err)
		}
		m.cleanup = append(m.cleanup, func() {
			m.deps.EventBus.Unsubscribe(ctx, contracts.EventClusterNodeLeft, m.onClusterNodeLeft)
		})

		if err := m.deps.EventBus.Subscribe(ctx, contracts.EventClusterLeaderChanged, m.onClusterLeaderChanged); err != nil {
			return fmt.Errorf("subscribe to cluster.leader.changed: %w", err)
		}
		m.cleanup = append(m.cleanup, func() {
			m.deps.EventBus.Unsubscribe(ctx, contracts.EventClusterLeaderChanged, m.onClusterLeaderChanged)
		})

		// Initialize cluster metrics from current state.
		m.metrics.clusterMembers.Store(int64(len(m.deps.Cluster.Members())))
		leader := m.deps.Cluster.Leader()
		if leader != nil && leader.ID == m.deps.Cluster.LocalNode().ID {
			m.metrics.isLeader.Store(1)
		}
	}

	m.started = true
	slog.Info("prometheus-metrics module started")
	return nil
}

// Stop unsubscribes all event handlers and stops the module.
func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	for _, unsub := range m.cleanup {
		unsub()
	}
	m.cleanup = nil
	m.started = false

	slog.Info("prometheus-metrics module stopped")
	return nil
}

// Health returns nil to indicate the module is healthy.
func (m *Module) Health(ctx context.Context) error { return nil }

// handleMetrics serves the /metrics endpoint with Prometheus text-format output.
func (m *Module) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m.metrics.incHTTPRequest(r.Method, r.URL.Path)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	data := m.metrics.collect()
	w.Write([]byte(data))
}

// -- Event bus handlers --

func (m *Module) onAnyEvent(ctx context.Context, event contracts.Event) error {
	m.metrics.incEvent(event.Type)
	return nil
}

func (m *Module) onModuleDegraded(ctx context.Context, event contracts.Event) error {
	moduleID := extractModuleID(event)
	if moduleID != "" {
		m.metrics.setModuleHealth(moduleID, false)
	}
	return nil
}

func (m *Module) onTaskCompleted(ctx context.Context, event contracts.Event) error {
	m.metrics.tasksCompleted.Add(1)
	return nil
}

func (m *Module) onTaskFailed(ctx context.Context, event contracts.Event) error {
	m.metrics.tasksFailed.Add(1)
	return nil
}

func (m *Module) onClusterNodeJoined(ctx context.Context, event contracts.Event) error {
	m.metrics.clusterMembers.Add(1)
	return nil
}

func (m *Module) onClusterNodeLeft(ctx context.Context, event contracts.Event) error {
	m.metrics.clusterMembers.Add(-1)
	return nil
}

func (m *Module) onClusterLeaderChanged(ctx context.Context, event contracts.Event) error {
	// Parse payload to determine if this node became leader.
	var payload contracts.LeaderChangedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		// If parsing fails, set to 1 (leader changed means we got a leader).
		m.metrics.isLeader.Store(1)
		return nil
	}
	// We know our own node ID from the cluster.
	if m.deps.Cluster != nil {
		if payload.NewLeader == m.deps.Cluster.LocalNode().ID {
			m.metrics.isLeader.Store(1)
		} else {
			m.metrics.isLeader.Store(0)
		}
	}
	return nil
}

// extractModuleID attempts to extract a module ID from a module.degraded event.
func extractModuleID(event contracts.Event) string {
	// Try metadata first.
	if id, ok := event.Metadata["module_id"]; ok {
		return id
	}
	// Fall back to parsing the payload.
	var payload contracts.ModuleDegradedPayload
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		return payload.ModuleID
	}
	return ""
}
