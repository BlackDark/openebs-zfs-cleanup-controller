package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// MetricsCollector holds all Prometheus metrics for the controller
type MetricsCollector struct {
	OrphanedVolumesTotal   *prometheus.CounterVec
	DeletionAttemptsTotal  *prometheus.CounterVec
	ReconciliationDuration *prometheus.HistogramVec
	LastReconcileTime      *prometheus.GaugeVec
}

// NewMetricsCollector creates a new metrics collector with all required metrics
func NewMetricsCollector() *MetricsCollector {
	mc := &MetricsCollector{
		OrphanedVolumesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfsvolume_cleanup_orphaned_volumes_total",
				Help: "Total number of orphaned ZFSVolumes found",
			},
			[]string{"namespace"},
		),
		DeletionAttemptsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfsvolume_cleanup_deletion_attempts_total",
				Help: "Total number of ZFSVolume deletion attempts",
			},
			[]string{"namespace", "result"},
		),
		ReconciliationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "zfsvolume_cleanup_reconciliation_duration_seconds",
				Help: "Time taken to complete a reconciliation cycle",
			},
			[]string{"namespace"},
		),
		LastReconcileTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "zfsvolume_cleanup_last_reconcile_time",
				Help: "Timestamp of the last successful reconciliation",
			},
			[]string{"namespace"},
		),
	}

	// Register metrics with controller-runtime
	metrics.Registry.MustRegister(
		mc.OrphanedVolumesTotal,
		mc.DeletionAttemptsTotal,
		mc.ReconciliationDuration,
		mc.LastReconcileTime,
	)

	return mc
}
