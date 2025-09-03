package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// MetricsCollector holds all Prometheus metrics for the controller
type MetricsCollector struct {
	// Counters for tracking volume operations
	OrphanedVolumesTotal  *prometheus.CounterVec
	DeletionAttemptsTotal *prometheus.CounterVec
	ProcessingErrorsTotal *prometheus.CounterVec
	ReconciliationsTotal  *prometheus.CounterVec

	// Histograms for tracking durations
	ReconciliationDuration *prometheus.HistogramVec
	DeletionDuration       *prometheus.HistogramVec

	// Gauges for current state
	LastReconcileTime     *prometheus.GaugeVec
	ActiveReconciliations *prometheus.GaugeVec
	VolumesProcessedTotal *prometheus.CounterVec
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
		ProcessingErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfsvolume_cleanup_processing_errors_total",
				Help: "Total number of processing errors encountered",
			},
			[]string{"namespace", "error_type"},
		),
		ReconciliationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfsvolume_cleanup_reconciliations_total",
				Help: "Total number of reconciliation cycles completed",
			},
			[]string{"namespace", "result"},
		),
		ReconciliationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "zfsvolume_cleanup_reconciliation_duration_seconds",
				Help:    "Time taken to complete a reconciliation cycle",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"namespace"},
		),
		DeletionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "zfsvolume_cleanup_deletion_duration_seconds",
				Help:    "Time taken to delete a ZFSVolume",
				Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
			},
			[]string{"namespace", "result"},
		),
		LastReconcileTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "zfsvolume_cleanup_last_reconcile_time",
				Help: "Timestamp of the last successful reconciliation",
			},
			[]string{"namespace"},
		),
		ActiveReconciliations: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "zfsvolume_cleanup_active_reconciliations",
				Help: "Number of currently active reconciliations",
			},
			[]string{"namespace"},
		),
		VolumesProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfsvolume_cleanup_volumes_processed_total",
				Help: "Total number of ZFSVolumes processed",
			},
			[]string{"namespace"},
		),
	}

	// Register metrics with controller-runtime (ignore duplicate registration errors for tests)
	_ = metrics.Registry.Register(mc.OrphanedVolumesTotal)
	_ = metrics.Registry.Register(mc.DeletionAttemptsTotal)
	_ = metrics.Registry.Register(mc.ProcessingErrorsTotal)
	_ = metrics.Registry.Register(mc.ReconciliationsTotal)
	_ = metrics.Registry.Register(mc.ReconciliationDuration)
	_ = metrics.Registry.Register(mc.DeletionDuration)
	_ = metrics.Registry.Register(mc.LastReconcileTime)
	_ = metrics.Registry.Register(mc.ActiveReconciliations)
	_ = metrics.Registry.Register(mc.VolumesProcessedTotal)

	return mc
}

// RecordOrphanedVolume increments the orphaned volumes counter
func (mc *MetricsCollector) RecordOrphanedVolume(namespace string) {
	mc.OrphanedVolumesTotal.WithLabelValues(namespace).Inc()
}

// RecordDeletionAttempt records a deletion attempt with the result
func (mc *MetricsCollector) RecordDeletionAttempt(namespace, result string, duration time.Duration) {
	mc.DeletionAttemptsTotal.WithLabelValues(namespace, result).Inc()
	mc.DeletionDuration.WithLabelValues(namespace, result).Observe(duration.Seconds())
}

// RecordProcessingError increments the processing errors counter
func (mc *MetricsCollector) RecordProcessingError(namespace, errorType string) {
	mc.ProcessingErrorsTotal.WithLabelValues(namespace, errorType).Inc()
}

// RecordReconciliation records a completed reconciliation
func (mc *MetricsCollector) RecordReconciliation(namespace, result string, duration time.Duration) {
	mc.ReconciliationsTotal.WithLabelValues(namespace, result).Inc()
	mc.ReconciliationDuration.WithLabelValues(namespace).Observe(duration.Seconds())
	mc.LastReconcileTime.WithLabelValues(namespace).SetToCurrentTime()
}

// RecordVolumeProcessed increments the volumes processed counter
func (mc *MetricsCollector) RecordVolumeProcessed(namespace string) {
	mc.VolumesProcessedTotal.WithLabelValues(namespace).Inc()
}

// SetActiveReconciliations sets the current number of active reconciliations
func (mc *MetricsCollector) SetActiveReconciliations(namespace string, count float64) {
	mc.ActiveReconciliations.WithLabelValues(namespace).Set(count)
}

// IncActiveReconciliations increments the active reconciliations gauge
func (mc *MetricsCollector) IncActiveReconciliations(namespace string) {
	mc.ActiveReconciliations.WithLabelValues(namespace).Inc()
}

// DecActiveReconciliations decrements the active reconciliations gauge
func (mc *MetricsCollector) DecActiveReconciliations(namespace string) {
	mc.ActiveReconciliations.WithLabelValues(namespace).Dec()
}
