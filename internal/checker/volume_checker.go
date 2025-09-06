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

const (
	defaultCacheTTL = 5 * time.Minute
	minVolumeAge    = 5 * time.Minute
	zfsFinalizer    = "zfs.openebs.io/finalizer"
	preserveAnn     = "zfs.openebs.io/preserve"
)

type volumeCache struct {
	pv  map[string]*corev1.PersistentVolume      // CSI volumeHandle
	pvc map[string]*corev1.PersistentVolumeClaim // namespace/name
	ts  time.Time
	ttl time.Duration
}

type VolumeChecker struct {
	client.Client
	logger          logr.Logger
	dryRun          bool
	pvLabelSelector string
	cache           *volumeCache
	enableCaching   bool
}

// ValidationResult is the result of safety validation checks
type ValidationResult struct {
	IsSafe           bool
	IsOrphaned       bool
	Reason           string
	ValidationErrors []string
}

// NewVolumeChecker creates a new VolumeChecker instance
func NewVolumeChecker(client client.Client, logger logr.Logger, dryRun bool, pvLabelSelector string, enableCaching bool) *VolumeChecker {
	ttl := defaultCacheTTL
	if !enableCaching {
		ttl = 0
	}
	return &VolumeChecker{
		Client:          client,
		logger:          logger,
		dryRun:          dryRun,
		pvLabelSelector: pvLabelSelector,
		enableCaching:   enableCaching,
		cache: &volumeCache{
			pv:  make(map[string]*corev1.PersistentVolume),
			pvc: make(map[string]*corev1.PersistentVolumeClaim),
			ttl: ttl,
		},
	}
}

// IsCachingEnabled returns true if caching is enabled
func (vc *VolumeChecker) IsCachingEnabled() bool { return vc.enableCaching }

// IsCacheValid returns true if the cache is still valid
func (vc *VolumeChecker) IsCacheValid() bool {
	if vc.cache == nil || vc.cache.ts.IsZero() {
		return false
	}
	return time.Since(vc.cache.ts) < vc.cache.ttl
}

// ClearCache invalidates the cache
func (vc *VolumeChecker) ClearCache() {
	if vc.cache != nil {
		vc.cache.pv = make(map[string]*corev1.PersistentVolume)
		vc.cache.pvc = make(map[string]*corev1.PersistentVolumeClaim)
		vc.cache.ts = time.Time{}
	}
}

// PopulateCache loads all PVs and PVCs into memory
func (vc *VolumeChecker) PopulateCache(ctx context.Context) error {
	start := time.Now()
	logger := vc.logger.WithValues("cachePopulationID", time.Now().UnixNano())
	vc.ClearCache()
	if err := vc.populatePVCache(ctx, logger); err != nil {
		return fmt.Errorf("populate PV cache: %w", err)
	}
	if err := vc.populatePVCCache(ctx, logger); err != nil {
		return fmt.Errorf("populate PVC cache: %w", err)
	}
	vc.cache.ts = time.Now()
	logger.V(1).Info("Cache populated", "pvCount", len(vc.cache.pv), "pvcCount", len(vc.cache.pvc), "duration", time.Since(start))
	return nil
}

func (vc *VolumeChecker) populatePVCache(ctx context.Context, logger logr.Logger) error {
	pvList := &corev1.PersistentVolumeList{}
	opts := []client.ListOption{}
	if vc.pvLabelSelector != "" {
		if selector, err := labels.Parse(vc.pvLabelSelector); err == nil {
			opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
		}
	}
	if err := vc.List(ctx, pvList, opts...); err != nil {
		return err
	}
	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
			vc.cache.pv[pv.Spec.CSI.VolumeHandle] = pv
		}
	}
	return nil
}

func (vc *VolumeChecker) populatePVCCache(ctx context.Context, logger logr.Logger) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := vc.List(ctx, pvcList); err != nil {
		return err
	}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		key := pvc.Namespace + "/" + pvc.Name
		vc.cache.pvc[key] = pvc
	}
	return nil
}

// Unified PV lookup: tries cache if enabled, else direct
func (vc *VolumeChecker) findRelatedPV(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*corev1.PersistentVolume, string, error) {
	if vc.IsCachingEnabled() {
		if !vc.IsCacheValid() {
			if err := vc.PopulateCache(ctx); err != nil {
				return nil, "cache_error", err
			}
		}
		if pv, ok := vc.cache.pv[zfsVol.Name]; ok {
			return pv, "csi_volumeHandle", nil
		}
		for _, pv := range vc.cache.pv {
			if pv.Name == zfsVol.Name {
				return pv, "direct_name", nil
			}
		}
		return nil, "not_found", nil
	}
	// fallback to direct
	pv, err := vc.FindRelatedPV(ctx, zfsVol)
	if pv != nil {
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle == zfsVol.Name {
			return pv, "csi_volumeHandle", nil
		}
		if pv.Name == zfsVol.Name {
			return pv, "direct_name", nil
		}
	}
	return nil, "not_found", err
}

func (vc *VolumeChecker) findRelatedPVC(ctx context.Context, pv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, error) {
	if pv.Spec.ClaimRef == nil {
		return nil, nil
	}
	key := pv.Spec.ClaimRef.Namespace + "/" + pv.Spec.ClaimRef.Name
	if vc.IsCachingEnabled() {
		if !vc.IsCacheValid() {
			if err := vc.PopulateCache(ctx); err != nil {
				return nil, err
			}
		}
		if pvc, ok := vc.cache.pvc[key]; ok {
			return pvc, nil
		}
		return nil, nil
	}
	return vc.FindRelatedPVC(ctx, pv)
}

// ValidateForAction performs all safety checks and returns a ValidationResult including orphan status
func (vc *VolumeChecker) ValidateForAction(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*ValidationResult, error) {
	logger := vc.logger.WithValues("zfsvolume", zfsVol.Name, "namespace", zfsVol.Namespace, "validationID", time.Now().UnixNano())
	logger.V(1).Info("Starting safety validation for ZFSVolume action", "volumeName", zfsVol.Name, "namespace", zfsVol.Namespace, "createdAt", zfsVol.CreationTimestamp, "age", time.Since(zfsVol.CreationTimestamp.Time).String(), "finalizers", zfsVol.Finalizers, "annotations", zfsVol.Annotations, "deletionTimestamp", zfsVol.DeletionTimestamp)

	// Orphan check (internal, not exposed)
	start := time.Now()
	pv, _, err := vc.findRelatedPV(ctx, zfsVol)
	if err != nil {
		logger.Error(err, "Failed to check for related PV", "error", err.Error(), "context", "PV relationship check", "checkDuration", time.Since(start))
		return nil, fmt.Errorf("failed to check for related PV: %w", err)
	}
	isOrphaned := false
	if pv == nil {
		logger.V(0).Info("No related PV found, ZFSVolume is orphaned", "status", "ORPHANED", "reason", "no related PersistentVolume found", "volumeName", zfsVol.Name, "namespace", zfsVol.Namespace, "checkDuration", time.Since(start))
		isOrphaned = true
	} else {
		logger.V(1).Info("Found related PV, checking for PVC", "pv", pv.Name, "pvPhase", pv.Status.Phase, "pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy, "pvClaimRef", pv.Spec.ClaimRef, "checkDuration", time.Since(start))
		pvc, err := vc.findRelatedPVC(ctx, pv)
		if err != nil {
			logger.Error(err, "Failed to check for related PVC", "pv", pv.Name, "error", err.Error(), "context", "PVC relationship check", "checkDuration", time.Since(start))
			return nil, fmt.Errorf("failed to check for related PVC: %w", err)
		}
		if pvc == nil {
			logger.V(0).Info("No related PVC found for PV, ZFSVolume is orphaned", "status", "ORPHANED", "reason", "PV exists but no related PersistentVolumeClaim found", "pv", pv.Name, "pvPhase", pv.Status.Phase, "pvClaimRef", pv.Spec.ClaimRef, "volumeName", zfsVol.Name, "namespace", zfsVol.Namespace, "checkDuration", time.Since(start))
			isOrphaned = true
		} else {
			logger.V(1).Info("Found related PVC, ZFSVolume is not orphaned", "status", "NOT_ORPHANED", "reason", "active PV and PVC references found", "pv", pv.Name, "pvPhase", pv.Status.Phase, "pvc", pvc.Name, "pvcNamespace", pvc.Namespace, "pvcPhase", pvc.Status.Phase, "volumeName", zfsVol.Name, "checkDuration", time.Since(start))
		}
	}

	result := &ValidationResult{IsSafe: true, IsOrphaned: isOrphaned, ValidationErrors: []string{}}
	validations := []validationFunc{
		orphanedValidation(isOrphaned),
		finalizerValidation,
		deletionTimestampValidation,
		ageValidation,
		preserveAnnotationValidation,
	}
	for _, v := range validations {
		if !v(zfsVol, result, logger) {
			break
		}
	}
	if result.IsSafe {
		result.Reason = "All safety checks passed"
		logger.V(0).Info("ZFSVolume passed all safety validation checks", "validationResult", "PASSED", "volumeName", zfsVol.Name, "namespace", zfsVol.Namespace)
	} else {
		logger.V(0).Info("ZFSVolume failed safety validation checks", "validationResult", "FAILED", "failureReason", result.Reason, "validationErrors", result.ValidationErrors, "volumeName", zfsVol.Name, "namespace", zfsVol.Namespace)
	}
	return result, nil
}

// Composable validation
type validationFunc func(*zfsv1.ZFSVolume, *ValidationResult, logr.Logger) bool

func orphanedValidation(isOrphaned bool) validationFunc {
	return func(_ *zfsv1.ZFSVolume, result *ValidationResult, logger logr.Logger) bool {
		if !isOrphaned {
			result.IsSafe = false
			result.Reason = "ZFSVolume is not orphaned"
			result.ValidationErrors = append(result.ValidationErrors, "volume has active PV or PVC references")
			logger.V(0).Info("ZFSVolume is not orphaned, not safe to delete", "validationResult", "FAILED")
			return false
		}
		return true
	}
}

func finalizerValidation(zfsVol *zfsv1.ZFSVolume, result *ValidationResult, logger logr.Logger) bool {
	if len(zfsVol.Finalizers) == 0 {
		return true
	}
	blocking := []string{}
	for _, f := range zfsVol.Finalizers {
		if f != zfsFinalizer {
			blocking = append(blocking, f)
		}
	}
	if len(blocking) > 0 {
		result.IsSafe = false
		result.Reason = "ZFSVolume has finalizers that must be handled first"
		result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("blocking finalizers present: %v", blocking))
		logger.V(0).Info("ZFSVolume has blocking finalizers, not safe to delete", "blockingFinalizers", blocking, "validationResult", "FAILED")
		return false
	}
	return true
}

func deletionTimestampValidation(zfsVol *zfsv1.ZFSVolume, result *ValidationResult, logger logr.Logger) bool {
	if zfsVol.DeletionTimestamp != nil {
		result.IsSafe = false
		result.Reason = "ZFSVolume is already being deleted"
		result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("deletion timestamp: %v", zfsVol.DeletionTimestamp))
		logger.V(0).Info("ZFSVolume is already being deleted", "deletionTimestamp", zfsVol.DeletionTimestamp, "validationResult", "FAILED")
		return false
	}
	return true
}

func ageValidation(zfsVol *zfsv1.ZFSVolume, result *ValidationResult, logger logr.Logger) bool {
	age := time.Since(zfsVol.CreationTimestamp.Time)
	if age < minVolumeAge {
		result.IsSafe = false
		result.Reason = "ZFSVolume is too new (created within last 5 minutes)"
		result.ValidationErrors = append(result.ValidationErrors, fmt.Sprintf("created at: %v", zfsVol.CreationTimestamp))
		logger.V(0).Info("ZFSVolume is too new, not safe to delete", "creationTimestamp", zfsVol.CreationTimestamp, "age", age.String(), "minimumAge", minVolumeAge.String(), "validationResult", "FAILED")
		return false
	}
	return true
}

func preserveAnnotationValidation(zfsVol *zfsv1.ZFSVolume, result *ValidationResult, logger logr.Logger) bool {
	if zfsVol.Annotations != nil {
		if v, ok := zfsVol.Annotations[preserveAnn]; ok && v == "true" {
			result.IsSafe = false
			result.Reason = "ZFSVolume has preserve annotation set to true"
			result.ValidationErrors = append(result.ValidationErrors, "preserve annotation is set")
			logger.V(0).Info("ZFSVolume has preserve annotation, not safe to delete", "preserveAnnotation", v, "validationResult", "FAILED")
			return false
		}
	}
	return true
}

// LogDeletionAction logs what would happen in dry-run mode or what is happening in real mode
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
			logger.V(0).Info("DRY-RUN: Would delete orphaned ZFSVolume",
				"mode", "DRY-RUN",
				"action", "DELETE",
				"status", "WOULD_DELETE",
				"reason", "orphaned volume with no PV/PVC references",
				"safetyValidation", "PASSED",
				"volumeInfo", volumeInfo)
		} else {
			logger.V(0).Info("DRY-RUN: Would skip ZFSVolume deletion",
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
			logger.V(0).Info("Deleting orphaned ZFSVolume",
				"mode", "LIVE",
				"action", "DELETE",
				"status", "PROCEEDING",
				"reason", "orphaned volume with no PV/PVC references",
				"safetyValidation", "PASSED",
				"volumeInfo", volumeInfo)
		} else {
			logger.V(0).Info("Skipping ZFSVolume deletion due to safety validation",
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

// IsDryRun returns true if dry-run mode is enabled
func (vc *VolumeChecker) IsDryRun() bool {
	return vc.dryRun
}

// FindRelatedPV finds the PersistentVolume related to a ZFSVolume via CSI volumeHandle
func (vc *VolumeChecker) FindRelatedPV(ctx context.Context, zfsVol *zfsv1.ZFSVolume) (*corev1.PersistentVolume, error) {
	startTime := time.Now()
	logger := vc.logger.WithValues(
		"zfsvolume", zfsVol.Name,
		"namespace", zfsVol.Namespace,
		"pvSearchID", time.Now().UnixNano())

	logger.V(1).Info("Starting search for related PersistentVolume",
		"searchTarget", zfsVol.Name,
		"searchMethod", "CSI volumeHandle matching",
		"pvLabelSelector", vc.pvLabelSelector)

	pvList := &corev1.PersistentVolumeList{}
	listOpts := []client.ListOption{}
	if vc.pvLabelSelector != "" {
		selector, err := labels.Parse(vc.pvLabelSelector)
		if err != nil {
			logger.Error(err, "Failed to parse PV label selector, skipping filter",
				"selector", vc.pvLabelSelector,
				"error", err.Error())
		} else {
			listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
		}
	}
	if err := vc.List(ctx, pvList, listOpts...); err != nil {
		logger.Error(err, "Failed to list PersistentVolumes from Kubernetes API",
			"error", err.Error(),
			"context", "PV listing",
			"searchDuration", time.Since(startTime))
		return nil, fmt.Errorf("failed to list PersistentVolumes: %w", err)
	}

	logger.V(2).Info("Retrieved PersistentVolume list",
		"totalPVs", len(pvList.Items),
		"searchTarget", zfsVol.Name,
		"listDuration", time.Since(startTime),
		"labelSelector", vc.pvLabelSelector)

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		logger.V(2).Info("Examining PersistentVolume",
			"pvIndex", i+1,
			"pvName", pv.Name,
			"pvPhase", pv.Status.Phase,
			"hasCSI", pv.Spec.CSI != nil)

		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle == zfsVol.Name {
			logger.V(0).Info("Found matching PV via CSI volumeHandle",
				"pv", pv.Name,
				"volumeHandle", pv.Spec.CSI.VolumeHandle,
				"pvPhase", pv.Status.Phase,
				"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
				"pvClaimRef", pv.Spec.ClaimRef,
				"searchDuration", time.Since(startTime),
				"pvsExamined", i+1)
			return pv, nil
		}
		if pv.Name == zfsVol.Name {
			logger.V(0).Info("Found matching PV via direct name matching",
				"pv", pv.Name,
				"pvPhase", pv.Status.Phase,
				"pvReclaimPolicy", pv.Spec.PersistentVolumeReclaimPolicy,
				"pvClaimRef", pv.Spec.ClaimRef,
				"searchMethod", "direct name matching",
				"searchDuration", time.Since(startTime),
				"pvsExamined", i+1)
			return pv, nil
		}
	}

	logger.V(1).Info("No matching PV found",
		"searchTarget", zfsVol.Name,
		"totalPVsExamined", len(pvList.Items),
		"searchDuration", time.Since(startTime),
		"result", "no match")
	return nil, nil
}

// FindRelatedPVC finds the PersistentVolumeClaim related to a PersistentVolume via claimRef
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
		logger.V(0).Info("PV has no claimRef, no related PVC",
			"pvName", pv.Name,
			"pvPhase", pv.Status.Phase,
			"result", "no claimRef",
			"searchDuration", time.Since(startTime))
		return nil, nil
	}

	claimRef := pv.Spec.ClaimRef
	// Enhanced claimRef found logging - Requirement 4.2
	logger.V(1).Info("PV has claimRef, searching for PVC",
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

	logger.V(2).Info("Retrieving PVC from Kubernetes API",
		"pvcKey", pvcKey.String())

	if err := vc.Get(ctx, pvcKey, pvc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			logger.V(0).Info("PVC referenced by PV not found",
				"pvName", pv.Name,
				"pvcName", claimRef.Name,
				"pvcNamespace", claimRef.Namespace,
				"result", "PVC not found",
				"searchDuration", time.Since(startTime))
			return nil, nil
		}
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
	logger.V(1).Info("Found related PVC",
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
