package checker

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
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

// TestOrphanCheckLogging tests detailed logging during orphan status checks
func TestOrphanCheckLogging(t *testing.T) {
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

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "", true)

	ctx := context.Background()
	isOrphaned, err := volumeChecker.IsOrphaned(ctx, zfsVolume)

	assert.NoError(t, err)
	assert.True(t, isOrphaned)

	logs := logCapture.GetLogs()

	// Verify comprehensive orphan check logging
	assert.Contains(t, logs, "Starting orphan status check for ZFSVolume")
	assert.Contains(t, logs, "Searching for related PersistentVolume")
	assert.Contains(t, logs, "PV not found in cache")
	assert.Contains(t, logs, "No related PV found, ZFSVolume is orphaned")
	assert.Contains(t, logs, "ORPHANED")
	assert.Contains(t, logs, "checkDuration")
}

// TestPVSearchLogging tests detailed logging during PV search
func TestPVSearchLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create ZFSVolume and matching PV
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume",
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume, pv).
		Build()

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "pv.kubernetes.io/provisioned-by=zfs.csi.openebs.io", true)

	ctx := context.Background()
	foundPV, err := volumeChecker.FindRelatedPV(ctx, zfsVolume)

	assert.NoError(t, err)
	assert.NotNil(t, foundPV)
	assert.Equal(t, "test-pv", foundPV.Name)

	logs := logCapture.GetLogs()

	// Verify PV search logging
	assert.Contains(t, logs, "Starting search for related PersistentVolume")
	assert.Contains(t, logs, "Retrieved PersistentVolume list")
	assert.Contains(t, logs, "Found matching PV via fallback CSI volumeHandle matching")
	assert.Contains(t, logs, "searchDuration")
	assert.Contains(t, logs, "pvsExamined")
}

// TestPVCSearchLogging tests detailed logging during PVC search
func TestPVCSearchLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create PV with claimRef and matching PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Name:      "test-pvc",
				Namespace: "default",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pv, pvc).
		Build()

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "openebs.io/cas-type=zfs-localpv", true)

	ctx := context.Background()
	foundPVC, err := volumeChecker.FindRelatedPVC(ctx, pv)

	assert.NoError(t, err)
	assert.NotNil(t, foundPVC)
	assert.Equal(t, "test-pvc", foundPVC.Name)

	logs := logCapture.GetLogs()

	// Verify PVC search logging
	assert.Contains(t, logs, "Starting search for related PersistentVolumeClaim")
	assert.Contains(t, logs, "PV has claimRef, searching for PVC")
	assert.Contains(t, logs, "Retrieving PVC from Kubernetes API")
	assert.Contains(t, logs, "Found related PVC")
	assert.Contains(t, logs, "searchDuration")
}

// TestValidationLogging tests detailed logging during safety validation
func TestValidationLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create orphaned ZFSVolume that should pass validation
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

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "openebs.io/cas-type=zfs-localpv", true)

	ctx := context.Background()
	validation, err := volumeChecker.ValidateForDeletion(ctx, zfsVolume)

	assert.NoError(t, err)
	assert.NotNil(t, validation)
	assert.True(t, validation.IsSafe)

	logs := logCapture.GetLogs()

	// Verify validation logging
	assert.Contains(t, logs, "Starting safety validation for ZFSVolume deletion")
	assert.Contains(t, logs, "Verifying orphan status for validation")
	assert.Contains(t, logs, "ZFSVolume passed all safety validation checks")
	assert.Contains(t, logs, "validationResult")
	assert.Contains(t, logs, "PASSED")
	assert.Contains(t, logs, "validationDuration")
	assert.Contains(t, logs, "checksPerformed")
}

// TestDryRunActionLogging tests dry-run specific logging
func TestDryRunActionLogging(t *testing.T) {
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

	// Test dry-run mode
	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), true, "openebs.io/cas-type=zfs-localpv", true)

	validation := &ValidationResult{
		IsSafe: true,
		Reason: "All safety checks passed",
	}

	volumeChecker.LogDeletionAction(zfsVolume, validation)

	logs := logCapture.GetLogs()

	// Verify dry-run specific logging
	assert.Contains(t, logs, "DRY-RUN: Would delete orphaned ZFSVolume")
	assert.Contains(t, logs, "mode")
	assert.Contains(t, logs, "DRY-RUN")
	assert.Contains(t, logs, "action")
	assert.Contains(t, logs, "DELETE")
	assert.Contains(t, logs, "status")
	assert.Contains(t, logs, "WOULD_DELETE")
	assert.Contains(t, logs, "volumeInfo")
}

// TestLiveActionLogging tests live mode specific logging
func TestLiveActionLogging(t *testing.T) {
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

	// Test live mode
	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "openebs.io/cas-type=zfs-localpv", true)

	validation := &ValidationResult{
		IsSafe: true,
		Reason: "All safety checks passed",
	}

	volumeChecker.LogDeletionAction(zfsVolume, validation)

	logs := logCapture.GetLogs()

	// Verify live mode specific logging
	assert.Contains(t, logs, "Deleting orphaned ZFSVolume")
	assert.Contains(t, logs, "mode")
	assert.Contains(t, logs, "LIVE")
	assert.Contains(t, logs, "action")
	assert.Contains(t, logs, "DELETE")
	assert.Contains(t, logs, "status")
	assert.Contains(t, logs, "PROCEEDING")
	assert.Contains(t, logs, "volumeInfo")
}

// TestValidationFailureLogging tests logging when validation fails
func TestValidationFailureLogging(t *testing.T) {
	logCapture := NewLogCapture()

	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create ZFSVolume that's too new (should fail validation)
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "new-volume",
			Namespace:         "openebs",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Minute)), // Too new
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "openebs.io/cas-type=zfs-localpv", true)

	ctx := context.Background()
	validation, err := volumeChecker.ValidateForDeletion(ctx, zfsVolume)

	assert.NoError(t, err)
	assert.NotNil(t, validation)
	assert.False(t, validation.IsSafe)

	logs := logCapture.GetLogs()

	// Verify validation failure logging
	assert.Contains(t, logs, "ZFSVolume failed safety validation checks")
	assert.Contains(t, logs, "validationResult")
	assert.Contains(t, logs, "FAILED")
	assert.Contains(t, logs, "failureReason")
	assert.Contains(t, logs, "validationErrors")
}

// TestStructuredLogging tests that all logs are properly structured
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

	volumeChecker := NewVolumeChecker(fakeClient, logCapture.GetLogger(), false, "openebs.io/cas-type=zfs-localpv", true)

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
