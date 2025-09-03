package checker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
)

// VolumeChecker handles the logic for determining if a ZFSVolume is orphaned
type VolumeChecker struct {
	client.Client
	logger logr.Logger
	dryRun bool
}

// ValidationResult contains the result of safety validation checks
type ValidationResult struct {
	IsSafe           bool
	Reason           string
	ValidationErrors []string
}

// NewVolumeChecker creates a new VolumeChecker instance
func NewVolumeChecker(client client.Client, logger logr.Logger, dryRun bool) *VolumeChecker {
	return &VolumeChecker{
		Client: client,
		logger: logger,
		dryRun: dryRun,
	}
}

// IsOrphaned determines if a ZFSVolume is orphaned (no associated PV or PVC)
// Enhanced logging for Requirements 4.2 (processing details) and 4.3 (orphaned volume details)
func (vc *VolumeChecker) IsOrphaned(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (bool, error) {
	startTime := time.Now()
	logger := vc.logger.WithValues(
		"zfsvolume", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"orphanCheckID", time.Now().UnixNano())

	// Enhanced startup logging - Requirement 4.2
	logger.V(1).Info("Starting orphan status check for ZFSVolume",
		"volumeName", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"createdAt", zfsVol.CreationTimestamp,
		"age", time.Since(zfsVol.CreationTimestamp.Time).String())

	// First, try to find a related PV
	logger.V(1).Info("Searching for related PersistentVolume")
	pv, err := vc.FindRelatedPV(ctx, zfsVol)
	if err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to check for related PV",
			"error", err.Error(),
			"context", "PV relationship check",
			"checkDuration", time.Since(startTime))
		return false, fmt.Errorf("failed to check for related PV: %w", err)
	}

	// If no PV found, the ZFSVolume is orphaned
	if pv == nil {
		// Enhanced orphaned logging - Requirement 4.3
		logger.Info("No related PV found, ZFSVolume is orphaned",
			"status", "ORPHANED",
			"reason", "no related PersistentVolume found",
			"volumeName", zfsVol.Name,
			"namespace", zfsVol.Namespace,
			"checkDuration", time.Since(startTime))
		return true, nil
	}

	// Enhanced PV found logging - Requirement 4.2
	logger.Info("Found related PV, checking for PVC",
		"pv", pv.Name,
		"pvPhase", pv.Status.Phase,
		"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
		"pvClaimRef", pv.Spec.ClaimRef,
		"checkDuration", time.Since(startTime))

	// If PV exists, check if it has a related PVC
	logger.V(1).Info("Searching for related PersistentVolumeClaim")
	pvc, err := vc.FindRelatedPVC(ctx, pv)
	if err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to check for related PVC",
			"pv", pv.Name,
			"error", err.Error(),
			"context", "PVC relationship check",
			"checkDuration", time.Since(startTime))
		return false, fmt.Errorf("failed to check for related PVC: %w", err)
	}

	// If no PVC found, the ZFSVolume is orphaned
	if pvc == nil {
		// Enhanced orphaned logging - Requirement 4.3
		logger.Info("No related PVC found for PV, ZFSVolume is orphaned",
			"status", "ORPHANED",
			"reason", "PV exists but no related PersistentVolumeClaim found",
			"pv", pv.Name,
			"pvPhase", pv.Status.Phase,
			"pvClaimRef", pv.Spec.ClaimRef,
			"volumeName", zfsVol.Name,
			"namespace", zfsVol.Namespace,
			"checkDuration", time.Since(startTime))
		return true, nil
	}

	// Enhanced not orphaned logging - Requirement 4.2
	logger.Info("Found related PVC, ZFSVolume is not orphaned",
		"status", "NOT_ORPHANED",
		"reason", "active PV and PVC references found",
		"pv", pv.Name,
		"pvPhase", pv.Status.Phase,
		"pvc", pvc.Name,
		"pvcNamespace", pvc.Namespace,
		"pvcPhase", pvc.Status.Phase,
		"volumeName", zfsVol.Name,
		"checkDuration", time.Since(startTime))
	return false, nil
}

// ValidateForDeletion performs safety checks before allowing a ZFSVolume to be deleted
// Enhanced logging for Requirements 4.2 (processing details) and 4.5 (error details)
func (vc *VolumeChecker) ValidateForDeletion(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*ValidationResult, error) {
	startTime := time.Now()
	logger := vc.logger.WithValues(
		"zfsvolume", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"validationID", time.Now().UnixNano())

	// Enhanced validation startup logging - Requirement 4.2
	logger.Info("Starting safety validation for ZFSVolume deletion",
		"volumeName", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"createdAt", zfsVol.CreationTimestamp,
		"age", time.Since(zfsVol.CreationTimestamp.Time).String(),
		"finalizers", zfsVol.Finalizers,
		"annotations", zfsVol.Annotations,
		"deletionTimestamp", zfsVol.DeletionTimestamp)

	result := &ValidationResult{
		IsSafe:           true,
		ValidationErrors: []string{},
	}

	validationChecks := []string{}

	// First check if the ZFSVolume is orphaned to determine finalizer handling
	logger.V(1).Info("Verifying orphan status for validation")
	isOrphaned, err := vc.IsOrphaned(ctx, zfsVol)
	if err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to verify orphan status during validation",
			"error", err.Error(),
			"context", "orphan status verification",
			"validationDuration", time.Since(startTime))
		return nil, fmt.Errorf("failed to verify orphan status: %w", err)
	}

	validationChecks = append(validationChecks, fmt.Sprintf("orphan_status_check:passed(orphaned=%t)", isOrphaned))

	// Check if the ZFSVolume has finalizers that would prevent deletion
	if len(zfsVol.Finalizers) > 0 {
		logger.V(1).Info("Checking finalizers for blocking conditions",
			"totalFinalizers", len(zfsVol.Finalizers),
			"finalizers", zfsVol.Finalizers,
			"isOrphaned", isOrphaned)

		// Filter out zfs.openebs.io/finalizer if the volume is orphaned
		blockingFinalizers := []string{}
		ignoredFinalizers := []string{}

		for _, finalizer := range zfsVol.Finalizers {
			if finalizer == "zfs.openebs.io/finalizer" && isOrphaned {
				// Skip this finalizer for orphaned volumes
				ignoredFinalizers = append(ignoredFinalizers, finalizer)
				logger.Info("Ignoring zfs.openebs.io/finalizer for orphaned volume",
					"finalizer", finalizer,
					"reason", "volume is orphaned")
				continue
			}
			blockingFinalizers = append(blockingFinalizers, finalizer)
		}

		if len(blockingFinalizers) > 0 {
			result.IsSafe = false
			result.Reason = "ZFSVolume has finalizers that must be handled first"
			result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("blocking finalizers present: %v", blockingFinalizers))
			logger.Info("ZFSVolume has blocking finalizers, not safe to delete",
				"blockingFinalizers", blockingFinalizers,
				"ignoredFinalizers", ignoredFinalizers,
				"validationResult", "FAILED")
		}
		validationChecks = append(validationChecks, fmt.Sprintf("finalizer_check:blocking=%d,ignored=%d", len(blockingFinalizers), len(ignoredFinalizers)))
	} else {
		validationChecks = append(validationChecks, "finalizer_check:none")
	}

	// Check if the ZFSVolume is being deleted already
	if zfsVol.DeletionTimestamp != nil {
		result.IsSafe = false
		result.Reason = "ZFSVolume is already being deleted"
		result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("deletion timestamp: %v", zfsVol.DeletionTimestamp))
		logger.Info("ZFSVolume is already being deleted",
			"deletionTimestamp", zfsVol.DeletionTimestamp,
			"validationResult", "FAILED")
		validationChecks = append(validationChecks, "deletion_timestamp_check:FAILED")
	} else {
		validationChecks = append(validationChecks, "deletion_timestamp_check:passed")
	}

	// Check if the ZFSVolume is too new (created within the last 5 minutes)
	// This prevents accidental deletion of recently created volumes
	volumeAge := time.Since(zfsVol.CreationTimestamp.Time)
	minAge := 5 * time.Minute
	if volumeAge < minAge {
		result.IsSafe = false
		result.Reason = "ZFSVolume is too new (created within last 5 minutes)"
		result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("created at: %v", zfsVol.CreationTimestamp))
		logger.Info("ZFSVolume is too new, not safe to delete",
			"creationTimestamp", zfsVol.CreationTimestamp,
			"age", volumeAge.String(),
			"minimumAge", minAge.String(),
			"validationResult", "FAILED")
		validationChecks = append(validationChecks, fmt.Sprintf("age_check:FAILED(age=%s,min=%s)", volumeAge.String(), minAge.String()))
	} else {
		validationChecks = append(validationChecks, fmt.Sprintf("age_check:passed(age=%s)", volumeAge.String()))
	}

	// Check if the ZFSVolume has any annotations that indicate it should be preserved
	if zfsVol.Annotations != nil {
		if preserveAnnotation, exists := zfsVol.Annotations["zfs.openebs.io/preserve"]; exists && preserveAnnotation == "true" {
			result.IsSafe = false
			result.Reason = "ZFSVolume has preserve annotation set to true"
			result.ValidationErrors = append(result.ValidationErrors, "preserve annotation is set")
			logger.Info("ZFSVolume has preserve annotation, not safe to delete",
				"preserveAnnotation", preserveAnnotation,
				"validationResult", "FAILED")
			validationChecks = append(validationChecks, "preserve_annotation_check:FAILED")
		} else {
			validationChecks = append(validationChecks, "preserve_annotation_check:passed")
		}
	} else {
		validationChecks = append(validationChecks, "preserve_annotation_check:passed(no_annotations)")
	}

	// Check if the volume is not orphaned (we already checked this above)
	if !isOrphaned {
		result.IsSafe = false
		result.Reason = "ZFSVolume is not orphaned"
		result.ValidationErrors = append(result.ValidationErrors, "volume has active PV or PVC references")
		logger.Info("ZFSVolume is not orphaned, not safe to delete",
			"validationResult", "FAILED")
		validationChecks = append(validationChecks, "orphan_check:FAILED")
	} else {
		validationChecks = append(validationChecks, "orphan_check:passed")
	}

	validationDuration := time.Since(startTime)

	if result.IsSafe {
		result.Reason = "All safety checks passed"
		// Enhanced success logging - Requirement 4.2
		logger.Info("ZFSVolume passed all safety validation checks",
			"validationResult", "PASSED",
			"validationDuration", validationDuration,
			"checksPerformed", validationChecks,
			"volumeName", zfsVol.Name,
			"namespace", zfsVol.Namespace)
	} else {
		// Enhanced failure logging - Requirement 4.2, 4.5
		logger.Info("ZFSVolume failed safety validation checks",
			"validationResult", "FAILED",
			"failureReason", result.Reason,
			"validationErrors", result.ValidationErrors,
			"validationDuration", validationDuration,
			"checksPerformed", validationChecks,
			"volumeName", zfsVol.Name,
			"namespace", zfsVol.Namespace)
	}

	return result, nil
}

// LogDeletionAction logs what would happen in dry-run mode or what is happening in real mode
// Enhanced logging for Requirements 4.6 (dry-run indicators) and 4.3 (orphaned volume details)
func (vc *VolumeChecker) LogDeletionAction(zfsVol *zfsv1.ZFSVolume, validation *ValidationResult) {
	logger := vc.logger.WithValues(
		"zfsvolume", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"actionID", time.Now().UnixNano())

	// Common volume information for all log entries
	volumeInfo := map[string]interface{}{
		"volumeName":      zfsVol.Name,
		"namespace":       zfsVol.Namespace,
		"createdAt":       zfsVol.CreationTimestamp,
		"age":             time.Since(zfsVol.CreationTimestamp.Time).String(),
		"finalizers":      zfsVol.Finalizers,
		"annotations":     zfsVol.Annotations,
		"labels":          zfsVol.Labels,
		"resourceVersion": zfsVol.ResourceVersion,
	}

	if vc.dryRun {
		if validation.IsSafe {
			// Enhanced dry-run success logging - Requirement 4.6
			logger.Info("DRY-RUN: Would delete orphaned ZFSVolume",
				"mode", "DRY-RUN",
				"action", "DELETE",
				"status", "WOULD_DELETE",
				"reason", "orphaned volume with no PV/PVC references",
				"safetyValidation", "PASSED",
				"volumeInfo", volumeInfo)
		} else {
			// Enhanced dry-run skip logging - Requirement 4.6
			logger.Info("DRY-RUN: Would skip ZFSVolume deletion",
				"mode", "DRY-RUN",
				"action", "SKIP",
				"status", "WOULD_SKIP",
				"reason", validation.Reason,
				"safetyValidation", "FAILED",
				"validationErrors", validation.ValidationErrors,
				"volumeInfo", volumeInfo)
		}
	} else {
		if validation.IsSafe {
			// Enhanced live deletion logging - Requirement 4.3, 4.4
			logger.Info("Deleting orphaned ZFSVolume",
				"mode", "LIVE",
				"action", "DELETE",
				"status", "PROCEEDING",
				"reason", "orphaned volume with no PV/PVC references",
				"safetyValidation", "PASSED",
				"volumeInfo", volumeInfo)
		} else {
			// Enhanced live skip logging - Requirement 4.3
			logger.Info("Skipping ZFSVolume deletion due to safety validation",
				"mode", "LIVE",
				"action", "SKIP",
				"status", "SKIPPED",
				"reason", validation.Reason,
				"safetyValidation", "FAILED",
				"validationErrors", validation.ValidationErrors,
				"volumeInfo", volumeInfo)
		}
	}
}

// IsDryRun returns whether the checker is in dry-run mode
func (vc *VolumeChecker) IsDryRun() bool {
	return vc.dryRun
}

// FindRelatedPV finds the PersistentVolume related to a ZFSVolume via CSI volumeHandle
// Enhanced logging for Requirements 4.2 (processing details) and 4.5 (error details)
func (vc *VolumeChecker) FindRelatedPV(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*corev1.PersistentVolume, error) {
	startTime := time.Now()
	logger := vc.logger.WithValues(
		"zfsvolume", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"pvSearchID", time.Now().UnixNano())

	// Enhanced search startup logging - Requirement 4.2
	logger.V(1).Info("Starting search for related PersistentVolume",
		"searchTarget", zfsVol.Name,
		"searchMethod", "CSI volumeHandle matching")

	// List all PVs in the cluster
	pvList := &corev1.PersistentVolumeList{}
	if err := vc.List(ctx, pvList); err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to list PersistentVolumes from Kubernetes API",
			"error", err.Error(),
			"context", "PV listing",
			"searchDuration", time.Since(startTime))
		return nil, fmt.Errorf("failed to list PersistentVolumes: %w", err)
	}

	// Enhanced PV list logging - Requirement 4.2
	logger.V(1).Info("Retrieved PersistentVolume list",
		"totalPVs", len(pvList.Items),
		"searchTarget", zfsVol.Name,
		"listDuration", time.Since(startTime))

	// Look for a PV with CSI volumeHandle matching the ZFSVolume name
	for i, pv := range pvList.Items {
		logger.V(2).Info("Examining PersistentVolume",
			"pvIndex", i+1,
			"pvName", pv.Name,
			"pvPhase", pv.Status.Phase,
			"hasCSI", pv.Spec.PersistentVolumeSource.CSI != nil)

		if pv.Spec.PersistentVolumeSource.CSI != nil {
			volumeHandle := pv.Spec.PersistentVolumeSource.CSI.VolumeHandle
			logger.V(2).Info("Checking CSI volumeHandle",
				"pvName", pv.Name,
				"volumeHandle", volumeHandle,
				"targetVolume", zfsVol.Name,
				"matches", volumeHandle == zfsVol.Name)

			if volumeHandle == zfsVol.Name {
				// Enhanced match found logging - Requirement 4.2
				logger.Info("Found matching PV via CSI volumeHandle",
					"pv", pv.Name,
					"volumeHandle", volumeHandle,
					"pvPhase", pv.Status.Phase,
					"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
					"pvClaimRef", pv.Spec.ClaimRef,
					"searchDuration", time.Since(startTime),
					"pvsExamined", i+1)
				return &pv, nil
			}
		}
	}

	// Enhanced no match logging - Requirement 4.2
	logger.V(1).Info("No matching PV found",
		"searchTarget", zfsVol.Name,
		"totalPVsExamined", len(pvList.Items),
		"searchDuration", time.Since(startTime),
		"result", "no match")
	return nil, nil
}

// FindRelatedPVC finds the PersistentVolumeClaim related to a PersistentVolume via claimRef
// Enhanced logging for Requirements 4.2 (processing details) and 4.5 (error details)
func (vc *VolumeChecker) FindRelatedPVC(ctx context.Context, pv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, error) {
	startTime := time.Now()
	logger := vc.logger.WithValues(
		"pv", pv.Name,
		"pvcSearchID", time.Now().UnixNano())

	// Enhanced search startup logging - Requirement 4.2
	logger.V(1).Info("Starting search for related PersistentVolumeClaim",
		"pvName", pv.Name,
		"pvPhase", pv.Status.Phase,
		"searchMethod", "claimRef reference")

	// Check if PV has a claimRef
	if pv.Spec.ClaimRef == nil {
		// Enhanced no claimRef logging - Requirement 4.2
		logger.Info("PV has no claimRef, no related PVC",
			"pvName", pv.Name,
			"pvPhase", pv.Status.Phase,
			"result", "no claimRef",
			"searchDuration", time.Since(startTime))
		return nil, nil
	}

	claimRef := pv.Spec.ClaimRef
	// Enhanced claimRef found logging - Requirement 4.2
	logger.Info("PV has claimRef, searching for PVC",
		"pvName", pv.Name,
		"pvcName", claimRef.Name,
		"pvcNamespace", claimRef.Namespace,
		"claimRefUID", claimRef.UID,
		"claimRefResourceVersion", claimRef.ResourceVersion)

	// Try to get the PVC
	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := types.NamespacedName{
		Name:      claimRef.Name,
		Namespace: claimRef.Namespace,
	}

	logger.V(1).Info("Retrieving PVC from Kubernetes API",
		"pvcKey", pvcKey.String())

	if err := vc.Get(ctx, pvcKey, pvc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Enhanced PVC not found logging - Requirement 4.2
			logger.Info("PVC referenced by PV not found",
				"pvName", pv.Name,
				"pvcName", claimRef.Name,
				"pvcNamespace", claimRef.Namespace,
				"result", "PVC not found",
				"searchDuration", time.Since(startTime))
			return nil, nil
		}
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to get PVC from Kubernetes API",
			"pvName", pv.Name,
			"pvcName", claimRef.Name,
			"pvcNamespace", claimRef.Namespace,
			"error", err.Error(),
			"context", "PVC retrieval",
			"searchDuration", time.Since(startTime))
		return nil, fmt.Errorf("failed to get PVC %s/%s: %w", claimRef.Namespace, claimRef.Name, err)
	}

	// Enhanced PVC found logging - Requirement 4.2
	logger.Info("Found related PVC",
		"pvName", pv.Name,
		"pvcName", pvc.Name,
		"pvcNamespace", pvc.Namespace,
		"pvcPhase", pvc.Status.Phase,
		"pvcUID", pvc.UID,
		"pvcResourceVersion", pvc.ResourceVersion,
		"searchDuration", time.Since(startTime),
		"result", "PVC found")
	return pvc, nil
}
