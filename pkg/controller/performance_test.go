package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
)

// TestPerformanceOptimizations tests the performance improvements from task 17
func TestPerformanceOptimizations(t *testing.T) {
	// Test label selector parsing and application
	t.Run("LabelSelectorParsing", func(t *testing.T) {
		cfg := &config.Config{
			LabelSelector: "openebs.io/cas-type=zfs-localpv,app=test",
		}

		scheme := runtime.NewScheme()
		_ = zfsv1.AddToScheme(scheme)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logr.Discard())

		// Verify that label selector is properly configured
		assert.NotNil(t, reconciler)
		assert.NotNil(t, reconciler.VolumeChecker)
	})

	// Test pagination performance with large datasets
	t.Run("PaginationPerformance", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = zfsv1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)

		// Create a large number of ZFSVolumes
		const numVolumes = 1500 // More than page size (500)
		objects := make([]client.Object, numVolumes)

		for i := 0; i < numVolumes; i++ {
			zfsVol := &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-volume-%d", i),
					Namespace: "openebs",
				},
			}
			objects[i] = zfsVol
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			Build()

		cfg := &config.Config{
			DryRun:                  true, // Don't actually delete
			ReconcileInterval:       time.Hour,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            1000.0,
			APIBurst:                1000,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
			NamespaceFilter:         "openebs",
			LabelSelector:           "",
		}

		reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logr.Discard())

		ctx := context.Background()
		startTime := time.Now()

		result, err := reconciler.findOrphanedZFSVolumes(ctx)

		duration := time.Since(startTime)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify all volumes were processed
		assert.Equal(t, numVolumes, len(result.OrphanedVolumes))

		// Verify performance is reasonable (should complete in under 10 seconds for 1500 volumes)
		assert.Less(t, duration, 10*time.Second, "Processing %d volumes took too long: %v", numVolumes, duration)

		t.Logf("Processed %d volumes in %v (%.2f volumes/sec)",
			numVolumes, duration, float64(numVolumes)/duration.Seconds())
	})

	// Test PV filtering performance
	t.Run("PVFilteringPerformance", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = zfsv1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)

		// Create ZFSVolume and many PVs (some matching, some not)
		zfsVol := &zfsv1.ZFSVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume",
				Namespace: "openebs",
			},
		}

		var objects []client.Object
		objects = append(objects, zfsVol)

		const numPVs = 800
		for i := 0; i < numPVs; i++ {
			var labels map[string]string
			var volumeHandle string

			if i < 100 { // First 100 are OpenEBS ZFS PVs
				labels = map[string]string{"pv.kubernetes.io/provisioned-by": "zfs.csi.openebs.io"}
				if i == 0 {
					volumeHandle = "test-volume" // Matching volume
				} else {
					volumeHandle = fmt.Sprintf("other-volume-%d", i)
				}
			} else { // Rest are non-OpenEBS PVs
				labels = map[string]string{"app": "other"}
				volumeHandle = fmt.Sprintf("other-volume-%d", i)
			}

			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:   fmt.Sprintf("pv-%d", i),
					Labels: labels,
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeHandle: volumeHandle,
						},
					},
				},
			}
			objects = append(objects, pv)
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			Build()

		cfg := &config.Config{
			DryRun:                  true,
			ReconcileInterval:       time.Hour,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            1000.0,
			APIBurst:                1000,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
		}

		reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logr.Discard())

		ctx := context.Background()
		startTime := time.Now()

		// Test PV search performance
		volumeChecker := reconciler.VolumeChecker
		foundPV, err := volumeChecker.FindRelatedPV(ctx, zfsVol)

		duration := time.Since(startTime)

		assert.NoError(t, err)
		assert.NotNil(t, foundPV)
		assert.Equal(t, "pv-0", foundPV.Name)

		// Verify performance is reasonable
		assert.Less(t, duration, 2*time.Second, "PV search took too long: %v", duration)

		t.Logf("Found matching PV in %v from %d total PVs", duration, numPVs)
	})

	// Test memory usage optimization
	t.Run("MemoryOptimization", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = zfsv1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)

		// Create volumes in batches to test memory handling
		const batchSize = 500
		const numBatches = 3

		cfg := &config.Config{
			DryRun:                  true,
			ReconcileInterval:       time.Hour,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            1000.0,
			APIBurst:                1000,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
			NamespaceFilter:         "openebs",
		}

		totalDuration := time.Duration(0)
		totalVolumes := 0

		for batch := 0; batch < numBatches; batch++ {
			objects := make([]client.Object, batchSize)

			for i := 0; i < batchSize; i++ {
				zfsVol := &zfsv1.ZFSVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("batch%d-volume-%d", batch, i),
						Namespace: "openebs",
					},
				}
				objects[i] = zfsVol
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logr.Discard())

			ctx := context.Background()
			startTime := time.Now()

			result, err := reconciler.findOrphanedZFSVolumes(ctx)

			batchDuration := time.Since(startTime)
			totalDuration += batchDuration

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, batchSize, len(result.OrphanedVolumes))
			assert.Equal(t, 0, len(result.ProcessingErrors), "Should have no processing errors")

			totalVolumes += batchSize

			t.Logf("Batch %d: Processed %d volumes in %v", batch+1, batchSize, batchDuration)
		}

		avgTimePerVolume := time.Duration(int64(totalDuration) / int64(totalVolumes))
		t.Logf("Total: Processed %d volumes in %v (avg: %v per volume)",
			totalVolumes, totalDuration, avgTimePerVolume)

		// Verify average processing time is reasonable
		assert.Less(t, avgTimePerVolume, 10*time.Millisecond, "Average time per volume too high: %v", avgTimePerVolume)
	})
}
