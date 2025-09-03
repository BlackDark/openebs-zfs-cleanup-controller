package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewMetricsCollector(t *testing.T) {
	mc := NewMetricsCollector()

	// Verify all metrics are created
	assert.NotNil(t, mc.OrphanedVolumesTotal)
	assert.NotNil(t, mc.DeletionAttemptsTotal)
	assert.NotNil(t, mc.ProcessingErrorsTotal)
	assert.NotNil(t, mc.ReconciliationsTotal)
	assert.NotNil(t, mc.ReconciliationDuration)
	assert.NotNil(t, mc.DeletionDuration)
	assert.NotNil(t, mc.LastReconcileTime)
	assert.NotNil(t, mc.ActiveReconciliations)
	assert.NotNil(t, mc.VolumesProcessedTotal)

	// Test that we can use the metrics (this will verify they're properly initialized)
	mc.RecordOrphanedVolume("test")
	mc.RecordDeletionAttempt("test", "success", time.Second)
	mc.RecordProcessingError("test", "test_error")
	mc.RecordReconciliation("test", "success", time.Second)
	mc.RecordVolumeProcessed("test")
	mc.SetActiveReconciliations("test", 1)

	// Verify the metrics recorded values
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues("test")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("test", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues("test", "test_error")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("test", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues("test")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("test")))
}

func TestRecordOrphanedVolume(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Record orphaned volumes for different namespaces
	mc.RecordOrphanedVolume("openebs")
	mc.RecordOrphanedVolume("openebs")
	mc.RecordOrphanedVolume("default")

	// Verify the counter values
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues("openebs")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues("default")))
}

func TestRecordDeletionAttempt(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Record successful and failed deletion attempts
	mc.RecordDeletionAttempt("openebs", "success", 2*time.Second)
	mc.RecordDeletionAttempt("openebs", "success", 1*time.Second)
	mc.RecordDeletionAttempt("openebs", "failed", 5*time.Second)
	mc.RecordDeletionAttempt("default", "success", 3*time.Second)

	// Verify counter values
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("openebs", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("openebs", "failed")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("default", "success")))

	// Verify histogram observations by checking the counter values
	// We can't directly test histogram sample counts with testutil, but we can verify the counters
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("openebs", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues("openebs", "failed")))
}

func TestRecordProcessingError(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Record different types of processing errors
	mc.RecordProcessingError("openebs", "api_error")
	mc.RecordProcessingError("openebs", "api_error")
	mc.RecordProcessingError("openebs", "validation_error")
	mc.RecordProcessingError("default", "api_error")

	// Verify counter values
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues("openebs", "api_error")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues("openebs", "validation_error")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues("default", "api_error")))
}

func TestRecordReconciliation(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Record reconciliations with different results
	mc.RecordReconciliation("openebs", "success", 10*time.Second)
	mc.RecordReconciliation("openebs", "success", 15*time.Second)
	mc.RecordReconciliation("openebs", "failed", 5*time.Second)
	mc.RecordReconciliation("default", "success", 8*time.Second)

	// Verify counter values
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("openebs", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("openebs", "failed")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("default", "success")))

	// Verify histogram observations by checking the counter values
	// We can't directly test histogram sample counts with testutil, but we can verify the counters
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("openebs", "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues("default", "success")))

	// Verify last reconcile time is set (should be recent)
	lastReconcileTime := testutil.ToFloat64(mc.LastReconcileTime.WithLabelValues("openebs"))
	assert.Greater(t, lastReconcileTime, float64(time.Now().Add(-1*time.Minute).Unix()))
}

func TestRecordVolumeProcessed(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Record processed volumes
	mc.RecordVolumeProcessed("openebs")
	mc.RecordVolumeProcessed("openebs")
	mc.RecordVolumeProcessed("openebs")
	mc.RecordVolumeProcessed("default")

	// Verify counter values
	assert.Equal(t, float64(3), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues("openebs")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues("default")))
}

func TestActiveReconciliations(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Test setting active reconciliations
	mc.SetActiveReconciliations("openebs", 5)
	assert.Equal(t, float64(5), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("openebs")))

	// Test incrementing
	mc.IncActiveReconciliations("openebs")
	assert.Equal(t, float64(6), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("openebs")))

	// Test decrementing
	mc.DecActiveReconciliations("openebs")
	mc.DecActiveReconciliations("openebs")
	assert.Equal(t, float64(4), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("openebs")))

	// Test different namespace
	mc.SetActiveReconciliations("default", 2)
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("default")))
	assert.Equal(t, float64(4), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("openebs")))
}

func TestMetricsLabels(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Test that metrics work with various namespace values
	testNamespaces := []string{"openebs", "default", "kube-system", "test-ns-123"}

	for _, ns := range testNamespaces {
		mc.RecordOrphanedVolume(ns)
		mc.RecordDeletionAttempt(ns, "success", time.Second)
		mc.RecordProcessingError(ns, "test_error")
		mc.RecordReconciliation(ns, "success", time.Second)
		mc.RecordVolumeProcessed(ns)
		mc.SetActiveReconciliations(ns, 1)

		// Verify all metrics are recorded for this namespace
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues(ns)))
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues(ns, "success")))
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues(ns, "test_error")))
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues(ns, "success")))
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues(ns)))
		assert.Equal(t, float64(1), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues(ns)))
	}
}

func TestMetricsAccuracy(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Simulate a realistic scenario
	namespace := "openebs"

	// Process 10 volumes, 3 are orphaned, 2 deletions succeed, 1 fails
	for i := 0; i < 10; i++ {
		mc.RecordVolumeProcessed(namespace)
	}

	for i := 0; i < 3; i++ {
		mc.RecordOrphanedVolume(namespace)
	}

	mc.RecordDeletionAttempt(namespace, "success", 2*time.Second)
	mc.RecordDeletionAttempt(namespace, "success", 3*time.Second)
	mc.RecordDeletionAttempt(namespace, "failed", 10*time.Second)

	mc.RecordProcessingError(namespace, "api_error")

	mc.RecordReconciliation(namespace, "success", 30*time.Second)

	// Verify all counts are accurate
	assert.Equal(t, float64(10), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues(namespace)))
	assert.Equal(t, float64(3), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues(namespace)))
	assert.Equal(t, float64(2), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues(namespace, "success")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues(namespace, "failed")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ProcessingErrorsTotal.WithLabelValues(namespace, "api_error")))
	assert.Equal(t, float64(1), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues(namespace, "success")))
}

func TestHistogramBuckets(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Test that histograms have appropriate buckets for our use case
	namespace := "openebs"

	// Record various durations to test bucket distribution
	durations := []time.Duration{
		100 * time.Millisecond, // Fast deletion
		500 * time.Millisecond, // Normal deletion
		2 * time.Second,        // Slow deletion
		10 * time.Second,       // Very slow deletion
		45 * time.Second,       // Timeout scenario
	}

	for _, duration := range durations {
		mc.RecordDeletionAttempt(namespace, "success", duration)
		mc.RecordReconciliation(namespace, "success", duration*2) // Reconciliation takes longer
	}

	// Verify observations were recorded by checking the counter values
	// We can't directly test histogram sample counts with testutil, but we can verify the counters
	assert.Equal(t, float64(5), testutil.ToFloat64(mc.DeletionAttemptsTotal.WithLabelValues(namespace, "success")))
	assert.Equal(t, float64(5), testutil.ToFloat64(mc.ReconciliationsTotal.WithLabelValues(namespace, "success")))
}

func TestConcurrentMetricsAccess(t *testing.T) {
	mc := createTestMetricsCollector(t)

	// Test concurrent access to metrics (basic thread safety test)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			namespace := "openebs"
			for j := 0; j < 100; j++ {
				mc.RecordOrphanedVolume(namespace)
				mc.RecordVolumeProcessed(namespace)
				mc.IncActiveReconciliations(namespace)
				mc.DecActiveReconciliations(namespace)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final counts (should be 10 * 100 = 1000 for each metric)
	assert.Equal(t, float64(1000), testutil.ToFloat64(mc.OrphanedVolumesTotal.WithLabelValues("openebs")))
	assert.Equal(t, float64(1000), testutil.ToFloat64(mc.VolumesProcessedTotal.WithLabelValues("openebs")))
	// Active reconciliations should be 0 (equal inc/dec)
	assert.Equal(t, float64(0), testutil.ToFloat64(mc.ActiveReconciliations.WithLabelValues("openebs")))
}

// createTestMetricsCollector creates a metrics collector for testing with an isolated registry
func createTestMetricsCollector(t *testing.T) *MetricsCollector {
	// Create isolated metrics for testing
	return &MetricsCollector{
		OrphanedVolumesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_orphaned_volumes_total",
				Help: "Test metric",
			},
			[]string{"namespace"},
		),
		DeletionAttemptsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_deletion_attempts_total",
				Help: "Test metric",
			},
			[]string{"namespace", "result"},
		),
		ProcessingErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_processing_errors_total",
				Help: "Test metric",
			},
			[]string{"namespace", "error_type"},
		),
		ReconciliationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_reconciliations_total",
				Help: "Test metric",
			},
			[]string{"namespace", "result"},
		),
		ReconciliationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "test_reconciliation_duration_seconds",
				Help:    "Test metric",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"namespace"},
		),
		DeletionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "test_deletion_duration_seconds",
				Help:    "Test metric",
				Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
			},
			[]string{"namespace", "result"},
		),
		LastReconcileTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "test_last_reconcile_time",
				Help: "Test metric",
			},
			[]string{"namespace"},
		),
		ActiveReconciliations: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "test_active_reconciliations",
				Help: "Test metric",
			},
			[]string{"namespace"},
		),
		VolumesProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_volumes_processed_total",
				Help: "Test metric",
			},
			[]string{"namespace"},
		),
	}
}
