package checker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
)

// VolumeChecker handles the logic for determining if a ZFSVolume is orphaned
type VolumeChecker struct {
	client.Client
	logger          logr.Logger
	dryRun          bool
	pvLabelSelector string

	// Caching fields for performance optimization
	pvCache        map[string]*corev1.PersistentVolume      // keyed by CSI volumeHandle
	pvcCache       map[string]*corev1.PersistentVolumeClaim // keyed by "namespace/name"
	cacheTimestamp time.Time
	cacheTTL       time.Duration
}

// ValidationResult contains the result of safety validation checks
type ValidationResult struct {
	IsSafe           bool
	Reason           string
	ValidationErrors []string
}

// NewVolumeChecker creates a new VolumeChecker instance
func NewVolumeChecker(client client.Client, logger logr.Logger, dryRun bool, pvLabelSelector string, enableCaching bool) *VolumeChecker {
	vc := &VolumeChecker{
		Client:          client,
		logger:          logger,
		dryRun:          dryRun,
		pvLabelSelector: pvLabelSelector,
		pvCache:         make(map[string]*corev1.PersistentVolume),
		pvcCache:        make(map[string]*corev1.PersistentVolumeClaim),
		cacheTTL:        5 * time.Minute, // Cache for 5 minutes
	}

	// Disable caching if not enabled
	if !enableCaching {
		vc.cacheTTL = 0
	}

	return vc
}

// IsCacheValid checks if the current cache is still valid (not expired)
func (vc *VolumeChecker) IsCacheValid() bool {
	if vc.cacheTimestamp.IsZero() {
		return false
	}
	return time.Since(vc.cacheTimestamp) < vc.cacheTTL
}

// ClearCache invalidates the current cache
func (vc *VolumeChecker) ClearCache() {
	vc.pvCache = make(map[string]*corev1.PersistentVolume)
	vc.pvcCache = make(map[string]*corev1.PersistentVolumeClaim)
	vc.cacheTimestamp = time.Time{}
	vc.logger.V(1).Info("Cache cleared")
}

// PopulateCache loads all PVs and PVCs into memory for fast lookups
func (vc *VolumeChecker) PopulateCache(ctx context.Context) error {
	startTime := time.Now()
	logger := vc.logger.WithValues("cachePopulationID", time.Now().UnixNano())

	logger.Info("Starting cache population for PVs and PVCs")

	// Clear existing cache
	vc.ClearCache()

	// Populate PV cache
	if err := vc.populatePVCache(ctx, logger); err != nil {
		logger.Error(err, "Failed to populate PV cache")
		return fmt.Errorf("failed to populate PV cache: %w", err)
	}

	// Populate PVC cache
	if err := vc.populatePVCCache(ctx, logger); err != nil {
		logger.Error(err, "Failed to populate PVC cache")
		return fmt.Errorf("failed to populate PVC cache: %w", err)
	}

	vc.cacheTimestamp = time.Now()
	duration := time.Since(startTime)

	logger.Info("Cache population completed",
		"pvCount", len(vc.pvCache),
		"pvcCount", len(vc.pvcCache),
		"duration", duration,
		"cacheTTL", vc.cacheTTL)

	return nil
}

// populatePVCache loads all PVs into the cache
func (vc *VolumeChecker) populatePVCache(ctx context.Context, logger logr.Logger) error {
	const pageSize = 200
	allPVs := make([]corev1.PersistentVolume, 0)
	continueToken := ""

	for {
		pvList := &corev1.PersistentVolumeList{}
		listOpts := []client.ListOption{client.Limit(pageSize)}
		if continueToken != "" {
			listOpts = append(listOpts, client.Continue(continueToken))
		}

		// Apply label selector if configured
		if vc.pvLabelSelector != "" {
			selector, err := labels.Parse(vc.pvLabelSelector)
			if err != nil {
				logger.Error(err, "Failed to parse PV label selector for cache, using all PVs",
					"selector", vc.pvLabelSelector)
			} else {
				listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
			}
		}

		if err := vc.List(ctx, pvList, listOpts...); err != nil {
			return fmt.Errorf("failed to list PVs for cache: %w", err)
		}

		allPVs = append(allPVs, pvList.Items...)
		continueToken = pvList.GetContinue()
		if continueToken == "" {
			break
		}
	}

	// If no PVs found with label selector, try fallback: search all PVs without label filter
	if len(allPVs) == 0 && vc.pvLabelSelector != "" {
		logger.V(1).Info("No PVs found with label selector, trying fallback search without label filter")

		fallbackPVs := make([]corev1.PersistentVolume, 0)
		fallbackContinueToken := ""

		for {
			fallbackPvList := &corev1.PersistentVolumeList{}
			fallbackListOpts := []client.ListOption{client.Limit(pageSize)}
			if fallbackContinueToken != "" {
				fallbackListOpts = append(fallbackListOpts, client.Continue(fallbackContinueToken))
			}

			if err := vc.List(ctx, fallbackPvList, fallbackListOpts...); err != nil {
				logger.Error(err, "Failed to list all PVs for fallback cache population")
				break // Don't return error, just skip fallback
			}

			fallbackPVs = append(fallbackPVs, fallbackPvList.Items...)
			fallbackContinueToken = fallbackPvList.GetContinue()
			if fallbackContinueToken == "" {
				break
			}
		}

		logger.V(1).Info("Retrieved all PVs for fallback cache population",
			"totalPVs", len(fallbackPVs))
		allPVs = fallbackPVs
	}

	// Build PV cache indexed by CSI volumeHandle
	for i, pv := range allPVs {
		if pv.Spec.CSI != nil {
			volumeHandle := pv.Spec.CSI.VolumeHandle
			if volumeHandle != "" {
				vc.pvCache[volumeHandle] = &allPVs[i] // Store reference to the PV
			}
		}
	}

	logger.V(1).Info("PV cache populated",
		"totalPVs", len(allPVs),
		"cachedPVs", len(vc.pvCache))

	return nil
}

// populatePVCCache loads all PVCs into the cache
func (vc *VolumeChecker) populatePVCCache(ctx context.Context, logger logr.Logger) error {
	const pageSize = 500
	allPVCs := make([]corev1.PersistentVolumeClaim, 0)
	continueToken := ""

	for {
		pvcList := &corev1.PersistentVolumeClaimList{}
		listOpts := []client.ListOption{client.Limit(pageSize)}
		if continueToken != "" {
			listOpts = append(listOpts, client.Continue(continueToken))
		}

		if err := vc.List(ctx, pvcList, listOpts...); err != nil {
			return fmt.Errorf("failed to list PVCs for cache: %w", err)
		}

		allPVCs = append(allPVCs, pvcList.Items...)
		continueToken = pvcList.GetContinue()
		if continueToken == "" {
			break
		}
	}

	// Build PVC cache indexed by "namespace/name"
	for i, pvc := range allPVCs {
		key := fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
		vc.pvcCache[key] = &allPVCs[i] // Store reference to the PVC
	}

	logger.V(1).Info("PVC cache populated",
		"totalPVCs", len(allPVCs),
		"cachedPVCs", len(vc.pvcCache))

	return nil
}

// CachedFindRelatedPV finds PV using cache (populates cache if needed)
func (vc *VolumeChecker) CachedFindRelatedPV(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*corev1.PersistentVolume, error) {
	logger := vc.logger.WithValues("zfsvolume", zfsVol.Name, "namespace", zfsVol.Namespace)

	// Check if cache needs to be populated or refreshed
	if !vc.IsCacheValid() {
		logger.V(1).Info("Cache invalid or empty, populating cache")
		if err := vc.PopulateCache(ctx); err != nil {
			logger.Error(err, "Failed to populate cache, falling back to direct API calls")
			return vc.FindRelatedPV(ctx, zfsVol)
		}
	}

	// First, try CSI volumeHandle matching (primary method)
	if pv, exists := vc.pvCache[zfsVol.Name]; exists {
		logger.V(1).Info("Found PV in cache via CSI volumeHandle",
			"pv", pv.Name,
			"cacheHit", true,
			"matchType", "csi_volumeHandle")
		return pv, nil
	}

	// Second, try direct name matching as fallback
	for _, pv := range vc.pvCache {
		if pv.Name == zfsVol.Name {
			logger.V(1).Info("Found PV in cache via direct name matching",
				"pv", pv.Name,
				"cacheHit", true,
				"matchType", "direct_name")
			return pv, nil
		}
	}

	logger.V(1).Info("PV not found in cache",
		"cacheSize", len(vc.pvCache),
		"cacheHit", false)
	return nil, nil
}

// CachedFindRelatedPVC finds PVC using cache
func (vc *VolumeChecker) CachedFindRelatedPVC(ctx context.Context, pv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, error) {
	logger := vc.logger.WithValues("pv", pv.Name)

	// Check if PV has a claimRef
	if pv.Spec.ClaimRef == nil {
		logger.V(1).Info("PV has no claimRef")
		return nil, nil
	}

	claimRef := pv.Spec.ClaimRef
	key := fmt.Sprintf("%s/%s", claimRef.Namespace, claimRef.Name)

	// Lookup in cache
	if pvc, exists := vc.pvcCache[key]; exists {
		logger.V(1).Info("Found PVC in cache",
			"pvc", pvc.Name,
			"namespace", pvc.Namespace,
			"cacheHit", true)
		return pvc, nil
	}

	logger.V(1).Info("PVC not found in cache",
		"pvcKey", key,
		"cacheSize", len(vc.pvcCache),
		"cacheHit", false)
	return nil, nil
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
	pv, err := vc.CachedFindRelatedPV(ctx, zfsVol)
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
	pvc, err := vc.CachedFindRelatedPVC(ctx, pv)
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
		"searchMethod", "CSI volumeHandle matching",
		"pvLabelSelector", vc.pvLabelSelector)

	// Use pagination for efficient PV listing
	const pageSize = 200 // Smaller page size for PVs as they're less numerous than ZFSVolumes
	allPVs := make([]corev1.PersistentVolume, 0)
	continueToken := ""

	for {
		// Create a fresh list for each page
		pvList := &corev1.PersistentVolumeList{}
		listOpts := []client.ListOption{client.Limit(pageSize)}
		if continueToken != "" {
			listOpts = append(listOpts, client.Continue(continueToken))
		}

		// Apply label selector if configured
		if vc.pvLabelSelector != "" {
			// Parse the label selector
			selector, err := labels.Parse(vc.pvLabelSelector)
			if err != nil {
				logger.Error(err, "Failed to parse PV label selector, skipping filter",
					"selector", vc.pvLabelSelector,
					"error", err.Error())
			} else {
				listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
			}
		}

		logger.V(2).Info("Listing PersistentVolumes from Kubernetes API",
			"pageSize", pageSize,
			"continueToken", continueToken,
			"labelSelector", vc.pvLabelSelector)

		if err := vc.List(ctx, pvList, listOpts...); err != nil {
			// Enhanced error logging - Requirement 4.5
			logger.Error(err, "Failed to list PersistentVolumes from Kubernetes API",
				"error", err.Error(),
				"context", "PV listing",
				"searchDuration", time.Since(startTime))
			return nil, fmt.Errorf("failed to list PersistentVolumes: %w", err)
		}

		// Add this page's items to our collection
		allPVs = append(allPVs, pvList.Items...)

		// Check if there are more pages
		continueToken = pvList.GetContinue()
		if continueToken == "" {
			break // No more pages
		}

		logger.V(2).Info("Fetched page of PersistentVolumes",
			"pageSize", len(pvList.Items),
			"totalSoFar", len(allPVs),
			"hasMorePages", continueToken != "")
	}

	// Enhanced PV list logging - Requirement 4.2
	logger.V(1).Info("Retrieved PersistentVolume list",
		"totalPVs", len(allPVs),
		"searchTarget", zfsVol.Name,
		"listDuration", time.Since(startTime),
		"labelSelector", vc.pvLabelSelector)

	// Look for a PV with CSI volumeHandle matching the ZFSVolume name
	for i, pv := range allPVs {
		logger.V(2).Info("Examining PersistentVolume",
			"pvIndex", i+1,
			"pvName", pv.Name,
			"pvPhase", pv.Status.Phase,
			"hasCSI", pv.Spec.CSI != nil)

		// First, try CSI volumeHandle matching
		if pv.Spec.CSI != nil {
			volumeHandle := pv.Spec.CSI.VolumeHandle
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

		// Second, try direct name matching
		logger.V(2).Info("Checking direct name matching",
			"pvName", pv.Name,
			"targetVolume", zfsVol.Name,
			"matches", pv.Name == zfsVol.Name)

		if pv.Name == zfsVol.Name {
			// Enhanced match found logging - Requirement 4.2
			logger.Info("Found matching PV via direct name matching",
				"pv", pv.Name,
				"pvPhase", pv.Status.Phase,
				"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
				"pvClaimRef", pv.Spec.ClaimRef,
				"searchMethod", "direct name matching",
				"searchDuration", time.Since(startTime),
				"pvsExamined", i+1)
			return &pv, nil
		}
	}

	// If no PV found with label selector, try fallback: search all PVs without label filter
	if vc.pvLabelSelector != "" {
		logger.V(1).Info("No matching PV found with label selector, trying fallback search without label filter",
			"originalSelector", vc.pvLabelSelector,
			"searchTarget", zfsVol.Name)

		// Search all PVs without label filter
		fallbackPVs := make([]corev1.PersistentVolume, 0)
		fallbackContinueToken := ""

		for {
			fallbackPvList := &corev1.PersistentVolumeList{}
			fallbackListOpts := []client.ListOption{client.Limit(pageSize)}
			if fallbackContinueToken != "" {
				fallbackListOpts = append(fallbackListOpts, client.Continue(fallbackContinueToken))
			}

			logger.V(2).Info("Listing all PersistentVolumes for fallback search",
				"pageSize", pageSize,
				"continueToken", fallbackContinueToken)

			if err := vc.List(ctx, fallbackPvList, fallbackListOpts...); err != nil {
				logger.Error(err, "Failed to list all PersistentVolumes for fallback search",
					"error", err.Error(),
					"context", "PV fallback listing")
				break // Don't return error, just skip fallback
			}

			fallbackPVs = append(fallbackPVs, fallbackPvList.Items...)

			fallbackContinueToken = fallbackPvList.GetContinue()
			if fallbackContinueToken == "" {
				break
			}
		}

		logger.V(1).Info("Retrieved all PersistentVolumes for fallback search",
			"totalPVs", len(fallbackPVs),
			"searchTarget", zfsVol.Name)

		// Look for a PV with CSI volumeHandle matching the ZFSVolume name (fallback search)
		for i, pv := range fallbackPVs {
			logger.V(2).Info("Fallback: Examining PersistentVolume",
				"pvIndex", i+1,
				"pvName", pv.Name,
				"pvPhase", pv.Status.Phase,
				"hasCSI", pv.Spec.CSI != nil)

			if pv.Spec.CSI != nil {
				volumeHandle := pv.Spec.CSI.VolumeHandle
				logger.V(2).Info("Fallback: Checking CSI volumeHandle",
					"pvName", pv.Name,
					"volumeHandle", volumeHandle,
					"targetVolume", zfsVol.Name,
					"matches", volumeHandle == zfsVol.Name)

				if volumeHandle == zfsVol.Name {
					// Enhanced match found logging - Requirement 4.2
					logger.Info("Found matching PV via fallback CSI volumeHandle matching",
						"pv", pv.Name,
						"volumeHandle", volumeHandle,
						"pvPhase", pv.Status.Phase,
						"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
						"pvClaimRef", pv.Spec.ClaimRef,
						"searchMethod", "fallback CSI volumeHandle matching",
						"searchDuration", time.Since(startTime),
						"pvsExamined", i+1)
					return &pv, nil
				}
			}
		}

		logger.V(1).Info("No matching PV found in fallback search",
			"searchTarget", zfsVol.Name,
			"totalPVsExamined", len(fallbackPVs),
			"searchMethod", "fallback CSI volumeHandle matching",
			"result", "no match")
	}

	// Enhanced no match logging - Requirement 4.2
	logger.V(1).Info("No matching PV found",
		"searchTarget", zfsVol.Name,
		"totalPVsExamined", len(allPVs),
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
