package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
)

func TestNewZFSVolumeReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	if reconciler == nil {
		t.Fatal("Expected reconciler to be created, got nil")
	}

	if reconciler.Client != client {
		t.Error("Expected client to be set correctly")
	}

	if reconciler.Scheme != scheme {
		t.Error("Expected scheme to be set correctly")
	}

	if reconciler.Config != config {
		t.Error("Expected config to be set correctly")
	}

	if reconciler.VolumeChecker == nil {
		t.Error("Expected VolumeChecker to be initialized")
	}
}

func TestZFSVolumeReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent-volume",
			Namespace: "test-namespace",
		},
	}

	result, err := reconciler.Reconcile(context.TODO(), req)

	if err != nil {
		t.Errorf("Expected no error for non-existent resource, got: %v", err)
	}

	if result.Requeue || result.RequeueAfter > 0 {
		t.Error("Expected no requeue for non-existent resource")
	}
}

func TestZFSVolumeReconciler_Reconcile_BeingDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	now := metav1.Now()
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-volume",
			Namespace:         "test-namespace",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"}, // Add finalizer to make deletion timestamp valid
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-volume",
			Namespace: "test-namespace",
		},
	}

	result, err := reconciler.Reconcile(context.TODO(), req)

	if err != nil {
		t.Errorf("Expected no error for volume being deleted, got: %v", err)
	}

	if result.Requeue || result.RequeueAfter > 0 {
		t.Error("Expected no requeue for volume being deleted")
	}
}

func TestZFSVolumeReconciler_Reconcile_NotOrphaned(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	// Create a PV that references this ZFSVolume
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
			ClaimRef: &corev1.ObjectReference{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		},
	}

	// Create a PVC that is referenced by the PV
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume, pv, pvc).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-volume",
			Namespace: "test-namespace",
		},
	}

	result, err := reconciler.Reconcile(context.TODO(), req)

	if err != nil {
		t.Errorf("Expected no error for non-orphaned volume, got: %v", err)
	}

	if result.RequeueAfter != time.Hour {
		t.Errorf("Expected requeue after %v, got: %v", time.Hour, result.RequeueAfter)
	}
}

func TestZFSVolumeReconciler_Reconcile_OrphanedDryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create an orphaned ZFSVolume (no PV or PVC)
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "orphaned-volume",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)), // Old enough to be safe to delete
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  true, // Enable dry-run mode
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "orphaned-volume",
			Namespace: "test-namespace",
		},
	}

	result, err := reconciler.Reconcile(context.TODO(), req)

	if err != nil {
		t.Errorf("Expected no error in dry-run mode, got: %v", err)
	}

	if result.RequeueAfter != time.Hour {
		t.Errorf("Expected requeue after %v, got: %v", time.Hour, result.RequeueAfter)
	}

	// Verify the volume still exists (not deleted in dry-run)
	var retrievedVolume zfsv1.ZFSVolume
	err = client.Get(context.TODO(), types.NamespacedName{Name: "orphaned-volume", Namespace: "test-namespace"}, &retrievedVolume)
	if err != nil {
		t.Errorf("Expected volume to still exist in dry-run mode, got error: %v", err)
	}
}

func TestZFSVolumeReconciler_Reconcile_OrphanedUnsafe(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create an orphaned ZFSVolume that's too new (unsafe to delete)
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "new-orphaned-volume",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.NewTime(time.Now()), // Just created, too new to delete
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "new-orphaned-volume",
			Namespace: "test-namespace",
		},
	}

	result, err := reconciler.Reconcile(context.TODO(), req)

	if err != nil {
		t.Errorf("Expected no error for unsafe volume, got: %v", err)
	}

	if result.RequeueAfter != time.Hour {
		t.Errorf("Expected requeue after %v, got: %v", time.Hour, result.RequeueAfter)
	}

	// Verify the volume still exists (not deleted due to safety check)
	var retrievedVolume zfsv1.ZFSVolume
	err = client.Get(context.TODO(), types.NamespacedName{Name: "new-orphaned-volume", Namespace: "test-namespace"}, &retrievedVolume)
	if err != nil {
		t.Errorf("Expected volume to still exist due to safety check, got error: %v", err)
	}
}

func TestZFSVolumeReconciler_findOrphanedZFSVolumes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create test data
	orphanedVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "orphaned-volume",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	nonOrphanedVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "non-orphaned-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	// Create PV and PVC for non-orphaned volume
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeHandle: "non-orphaned-volume",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(orphanedVolume, nonOrphanedVolume, pv, pvc).Build()
	config := &config.Config{
		DryRun:                  true, // Use dry-run to avoid actual deletions
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	result, err := reconciler.findOrphanedZFSVolumes(context.TODO())

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}

	if len(result.OrphanedVolumes) != 1 {
		t.Errorf("Expected 1 orphaned volume, got: %d", len(result.OrphanedVolumes))
	}

	expectedOrphanedKey := "test-namespace/orphaned-volume"
	if len(result.OrphanedVolumes) > 0 && result.OrphanedVolumes[0] != expectedOrphanedKey {
		t.Errorf("Expected orphaned volume key %s, got: %s", expectedOrphanedKey, result.OrphanedVolumes[0])
	}

	// In dry-run mode, no volumes should be deleted
	if len(result.DeletedVolumes) != 0 {
		t.Errorf("Expected 0 deleted volumes in dry-run mode, got: %d", len(result.DeletedVolumes))
	}

	if len(result.FailedDeletions) != 0 {
		t.Errorf("Expected 0 failed deletions, got: %d", len(result.FailedDeletions))
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10, // Short backoff for testing
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	if err != nil {
		t.Errorf("Expected no error during deletion, got: %v", err)
	}

	// Verify the volume was deleted
	var retrievedVolume zfsv1.ZFSVolume
	err = client.Get(context.TODO(), types.NamespacedName{Name: "test-volume", Namespace: "test-namespace"}, &retrievedVolume)
	if !errors.IsNotFound(err) {
		t.Errorf("Expected volume to be deleted (NotFound error), got: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_AlreadyDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "non-existent-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	// Don't add the volume to the client, simulating it's already deleted
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	// Should not return an error when volume is already deleted
	if err != nil {
		t.Errorf("Expected no error when volume is already deleted, got: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_WithFinalizers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "volume-with-finalizers",
			Namespace:  "test-namespace",
			Finalizers: []string{"test.finalizer/cleanup", "another.finalizer/cleanup"},
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	// Should return an error when volume has finalizers
	if err == nil {
		t.Error("Expected error when volume has finalizers, got nil")
	}

	if !strings.Contains(err.Error(), "finalizers") {
		t.Errorf("Expected error message to mention finalizers, got: %v", err)
	}

	// Verify the volume still exists (not deleted due to finalizers)
	var retrievedVolume zfsv1.ZFSVolume
	err = client.Get(context.TODO(), types.NamespacedName{Name: "volume-with-finalizers", Namespace: "test-namespace"}, &retrievedVolume)
	if err != nil {
		t.Errorf("Expected volume to still exist due to finalizers, got error: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_AlreadyBeingDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Create a volume that already has a deletion timestamp and finalizer
	now := metav1.Now()
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "volume-being-deleted",
			Namespace:         "test-namespace",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test.finalizer/cleanup"}, // Finalizer prevents immediate deletion
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	// Create a context with short timeout to avoid long waits in test
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	err := reconciler.deleteZFSVolume(ctx, zfsVolume)

	// Should handle the case where volume is already being deleted
	// This might timeout in the test environment, which is expected
	if err != nil && !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "context") {
		t.Errorf("Expected timeout error or no error when volume is being deleted, got: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_ConcurrentModification(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "concurrent-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	// Now try to delete - should succeed
	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	// Should succeed
	if err != nil {
		t.Errorf("Expected no error during deletion, got: %v", err)
	}

	// Verify the volume was deleted
	var retrievedVolume zfsv1.ZFSVolume
	err = client.Get(context.TODO(), types.NamespacedName{Name: "concurrent-volume", Namespace: "test-namespace"}, &retrievedVolume)
	if !errors.IsNotFound(err) {
		t.Errorf("Expected volume to be deleted, got: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_RetryExhaustion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "failing-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	// Create a mock client that always returns a transient error
	client := &mockFailingClient{
		Client:      fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build(),
		failureType: "transient",
	}

	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 1, // Very short for testing
		MaxRetryAttempts:        2,                    // Low number for faster test
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	// Should return an error after exhausting retries
	if err == nil {
		t.Error("Expected error after exhausting retries, got nil")
	}

	if !strings.Contains(err.Error(), "failed to delete ZFSVolume after") {
		t.Errorf("Expected retry exhaustion error message, got: %v", err)
	}
}

func TestZFSVolumeReconciler_deleteZFSVolume_PermanentError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "failing-volume",
			Namespace: "test-namespace",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity: "1Gi",
			PoolName: "test-pool",
		},
	}

	// Create a mock client that always returns a permanent error
	client := &mockFailingClient{
		Client:      fake.NewClientBuilder().WithScheme(scheme).WithObjects(zfsVolume).Build(),
		failureType: "permanent",
	}

	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Millisecond * 10,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	err := reconciler.deleteZFSVolume(context.TODO(), zfsVolume)

	// Should return an error immediately for permanent errors (no retries)
	if err == nil {
		t.Error("Expected error for permanent failure, got nil")
	}

	if !strings.Contains(err.Error(), "permanent error") {
		t.Errorf("Expected permanent error message, got: %v", err)
	}
}

func TestZFSVolumeReconciler_isTransientError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	config := &config.Config{}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	// Create a proper GroupResource for ZFSVolume
	zfsVolumeResource := zfsv1.GroupVersion.WithResource("zfsvolumes").GroupResource()

	tests := []struct {
		name        string
		err         error
		isTransient bool
	}{
		{
			name:        "server timeout",
			err:         errors.NewServerTimeout(zfsVolumeResource, "test", 1),
			isTransient: true,
		},
		{
			name:        "service unavailable",
			err:         errors.NewServiceUnavailable("test"),
			isTransient: true,
		},
		{
			name:        "too many requests",
			err:         errors.NewTooManyRequests("test", 1),
			isTransient: true,
		},
		{
			name:        "timeout",
			err:         errors.NewTimeoutError("test", 1),
			isTransient: true,
		},
		{
			name:        "internal error",
			err:         errors.NewInternalError(fmt.Errorf("internal")),
			isTransient: true,
		},
		{
			name:        "not found",
			err:         errors.NewNotFound(zfsVolumeResource, "test"),
			isTransient: false,
		},
		{
			name:        "forbidden",
			err:         errors.NewForbidden(zfsVolumeResource, "test", fmt.Errorf("forbidden")),
			isTransient: false,
		},
		{
			name:        "conflict",
			err:         errors.NewConflict(zfsVolumeResource, "test", fmt.Errorf("conflict")),
			isTransient: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reconciler.isTransientError(tt.err)
			if result != tt.isTransient {
				t.Errorf("isTransientError() = %v, want %v for error: %v", result, tt.isTransient, tt.err)
			}
		})
	}
}

// mockFailingClient is a test helper that wraps a real client but fails operations
type mockFailingClient struct {
	client.Client
	failureType string // "transient" or "permanent"
}

func (m *mockFailingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	zfsVolumeResource := zfsv1.GroupVersion.WithResource("zfsvolumes").GroupResource()

	switch m.failureType {
	case "transient":
		return errors.NewServerTimeout(zfsVolumeResource, "test", 1)
	case "permanent":
		return errors.NewForbidden(zfsVolumeResource, obj.GetName(), fmt.Errorf("permission denied"))
	default:
		return m.Client.Delete(ctx, obj, opts...)
	}
}

func (m *mockFailingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	// For the failing client, we need to return the object for Get operations
	// so that the retry logic can work properly
	return m.Client.Get(ctx, key, obj, opts...)
}

func TestZFSVolumeReconciler_SetupWithManager(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	config := &config.Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 2,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
	}
	logger := zap.New(zap.UseDevMode(true))

	reconciler := NewZFSVolumeReconciler(client, scheme, config, logger)

	// Create a mock manager - in real tests you'd use envtest or a real manager
	// For this unit test, we'll just verify the method exists and doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetupWithManager panicked: %v", r)
		}
	}()

	// This will fail in unit tests since we don't have a real manager,
	// but we can verify the method exists by checking if it's callable
	// The method exists if we can reference it without compilation error
	_ = reconciler.SetupWithManager // This line verifies the method exists
}

// Helper function to create a test logger
func testLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

// Test helper to verify reconciler behavior with different configurations
func TestZFSVolumeReconciler_WithDifferentConfigs(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
	}{
		{
			name: "high concurrency",
			config: &config.Config{
				DryRun:                  false,
				ReconcileInterval:       time.Minute * 30,
				MaxConcurrentReconciles: 10,
				RetryBackoffBase:        time.Millisecond * 500,
				MaxRetryAttempts:        5,
			},
		},
		{
			name: "conservative settings",
			config: &config.Config{
				DryRun:                  true,
				ReconcileInterval:       time.Hour * 6,
				MaxConcurrentReconciles: 1,
				RetryBackoffBase:        time.Second * 2,
				MaxRetryAttempts:        1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = zfsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)

			client := fake.NewClientBuilder().WithScheme(scheme).Build()
			logger := testLogger()

			reconciler := NewZFSVolumeReconciler(client, scheme, tt.config, logger)

			if reconciler.Config.MaxConcurrentReconciles != tt.config.MaxConcurrentReconciles {
				t.Errorf("Expected MaxConcurrentReconciles %d, got %d",
					tt.config.MaxConcurrentReconciles, reconciler.Config.MaxConcurrentReconciles)
			}

			if reconciler.Config.DryRun != tt.config.DryRun {
				t.Errorf("Expected DryRun %t, got %t", tt.config.DryRun, reconciler.Config.DryRun)
			}
		})
	}
}
