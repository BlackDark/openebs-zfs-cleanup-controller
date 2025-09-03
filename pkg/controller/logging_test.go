package controller

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/internal/checker"
	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
)

// LogCapture captures log output for testing
type LogCapture struct {
	buffer *bytes.Buffer
	logger logr.Logger
}

// NewLogCapture creates a new log capture instance
func NewLogCapture() *LogCapture {
	buffer := &bytes.Buffer{}
	opts := zap.Options{
		Development: true,
		DestWriter:  buffer,
	}
	logger := zap.New(zap.UseFlagOptions(&opts))

	return &LogCapture{
		buffer: buffer,
		logger: logger,
	}
}

// GetLogs returns the captured log output
func (lc *LogCapture) GetLogs() string {
	return lc.buffer.String()
}

// GetLogger returns the logger instance
func (lc *LogCapture) GetLogger() logr.Logger {
	return lc.logger
}

// TestStartupLogging tests requirement 4.1 - startup configuration and version information
func TestStartupLogging(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  true,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		LogLevel:                "info",
		LogFormat:               "json",
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	// Test that the reconciler was created with proper logging setup
	assert.NotNil(t, reconciler)
	assert.NotNil(t, reconciler.Logger)

	// Simulate startup logging by calling a method that logs startup info
	ctx := context.Background()
	result, err := reconciler.findOrphanedZFSVolumes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	logs := logCapture.GetLogs()

	// Verify startup logging contains configuration information
	assert.Contains(t, logs, "Starting comprehensive scan for orphaned ZFSVolumes")
	assert.Contains(t, logs, "dryRun")
	assert.Contains(t, logs, "maxRetryAttempts")
	assert.Contains(t, logs, "retryBackoffBase")
}

// TestVolumeProcessingLogging tests requirement 4.2 - logging number of volumes being processed
func TestVolumeProcessingLogging(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create test ZFSVolumes
	zfsVolume1 := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-volume-1",
			Namespace: "openebs",
		},
	}

	zfsVolume2 := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-volume-2",
			Namespace: "openebs",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume1, zfsVolume2).
		Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	ctx := context.Background()
	result, err := reconciler.findOrphanedZFSVolumes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	logs := logCapture.GetLogs()

	// Verify volume processing logging
	assert.Contains(t, logs, "Found ZFSVolumes to process")
	assert.Contains(t, logs, "totalCount")
	assert.Contains(t, logs, "Processing ZFSVolume")
	assert.Contains(t, logs, "Completed scan for orphaned ZFSVolumes")
	assert.Contains(t, logs, "totalProcessed")
}

// TestOrphanedVolumeLogging tests requirement 4.3 - detailed logging for orphaned volumes
func TestOrphanedVolumeLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create orphaned ZFSVolume (no corresponding PV)
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "orphaned-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	volumeChecker := checker.NewVolumeChecker(fakeClient, logCapture.GetLogger(), false)

	ctx := context.Background()
	isOrphaned, err := volumeChecker.IsOrphaned(ctx, zfsVolume)

	assert.NoError(t, err)
	assert.True(t, isOrphaned)

	logs := logCapture.GetLogs()

	// Verify orphaned volume logging contains detailed information
	assert.Contains(t, logs, "No related PV found, ZFSVolume is orphaned")
	assert.Contains(t, logs, "status")
	assert.Contains(t, logs, "ORPHANED")
	assert.Contains(t, logs, "volumeName")
	assert.Contains(t, logs, "namespace")
	assert.Contains(t, logs, "checkDuration")
}

// TestDeletionLogging tests requirement 4.4 - logging successful and failed deletion attempts
func TestDeletionLogging(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create ZFSVolume for deletion test
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	ctx := context.Background()

	// Test successful deletion logging
	err := reconciler.deleteZFSVolume(ctx, zfsVolume)
	assert.NoError(t, err)

	logs := logCapture.GetLogs()

	// Verify deletion logging
	assert.Contains(t, logs, "Starting ZFSVolume deletion process")
	assert.Contains(t, logs, "volumeName")
	assert.Contains(t, logs, "namespace")
	assert.Contains(t, logs, "maxRetryAttempts")
	assert.Contains(t, logs, "Successfully deleted ZFSVolume")
	assert.Contains(t, logs, "action")
	assert.Contains(t, logs, "status")
}

// TestErrorLogging tests requirement 4.5 - detailed error information with context
func TestErrorLogging(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        0, // Force immediate failure
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create a client that will fail operations
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	ctx := context.Background()

	// Test reconcile with non-existent volume to trigger error
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent-volume",
			Namespace: "openebs",
		},
	}

	_, err := reconciler.Reconcile(ctx, req)
	assert.NoError(t, err) // Should not error for not found

	logs := logCapture.GetLogs()

	// Verify error logging contains context
	assert.Contains(t, logs, "ZFSVolume not found, likely deleted")
}

// TestDryRunLogging tests requirement 4.6 - clear dry-run action indicators
func TestDryRunLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create orphaned ZFSVolume
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "orphaned-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	volumeChecker := checker.NewVolumeChecker(fakeClient, logCapture.GetLogger(), true)

	// Test dry-run logging
	validation := &checker.ValidationResult{
		IsSafe: true,
		Reason: "All safety checks passed",
	}

	volumeChecker.LogDeletionAction(zfsVolume, validation)

	logs := logCapture.GetLogs()

	// Verify dry-run logging contains clear indicators
	assert.Contains(t, logs, "DRY-RUN")
	assert.Contains(t, logs, "mode")
	assert.Contains(t, logs, "WOULD_DELETE")
	assert.Contains(t, logs, "action")
	assert.Contains(t, logs, "DELETE")
}

// TestLogLevels tests that different log levels work correctly
func TestLogLevels(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	// Test different log levels
	reconciler.Logger.Info("Info level message")
	reconciler.Logger.V(1).Info("Debug level message")
	reconciler.Logger.Error(fmt.Errorf("test error"), "Error level message")

	logs := logCapture.GetLogs()

	// Verify different log levels are captured
	assert.Contains(t, logs, "Info level message")
	assert.Contains(t, logs, "Error level message")
}

// TestStructuredLogging tests that logs are properly structured with key-value pairs
func TestStructuredLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	volumeChecker := checker.NewVolumeChecker(fakeClient, logCapture.GetLogger(), false)

	ctx := context.Background()
	_, err := volumeChecker.IsOrphaned(ctx, zfsVolume)

	assert.NoError(t, err)

	logs := logCapture.GetLogs()

	// Verify that logs contain structured key-value pairs
	assert.Contains(t, logs, "volumeName")
	assert.Contains(t, logs, "namespace")
	assert.Contains(t, logs, "checkDuration")
	assert.Contains(t, logs, "Starting orphan status check")

	// Verify logs are not empty
	assert.NotEmpty(t, logs, "Should have log output")
}

// TestLogContextPropagation tests that log context is properly propagated
func TestLogContextPropagation(t *testing.T) {
	logCapture := NewLogCapture()

	cfg := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
	}

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler := NewZFSVolumeReconciler(fakeClient, scheme, cfg, logCapture.GetLogger())

	// Test that reconciler has proper logger setup
	assert.NotNil(t, reconciler.Logger)
	assert.NotNil(t, reconciler.VolumeChecker)

	// Test logging with context
	reconciler.Logger.Info("Test message with context", "testKey", "testValue")

	logs := logCapture.GetLogs()
	assert.Contains(t, logs, "Test message with context")
	assert.Contains(t, logs, "testKey")
	assert.Contains(t, logs, "testValue")
}
