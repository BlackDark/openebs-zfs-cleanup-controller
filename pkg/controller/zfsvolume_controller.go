package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/internal/checker"
	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/metrics"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/ratelimiter"
)

// ReconcileResult contains the results of a reconciliation operation
type ReconcileResult struct {
	OrphanedVolumes  []string
	DeletedVolumes   []string
	FailedDeletions  []string
	ProcessingErrors []error
}

// ZFSVolumeReconciler reconciles ZFSVolume objects
type ZFSVolumeReconciler struct {
	client.Client
	RateLimitedClient *ratelimiter.RateLimitedClient
	Scheme            *runtime.Scheme
	Config            *config.Config
	VolumeChecker     *checker.VolumeChecker
	Metrics           *metrics.MetricsCollector
	Logger            logr.Logger
}

// NewZFSVolumeReconciler creates a new ZFSVolumeReconciler instance
func NewZFSVolumeReconciler(client client.Client, scheme *runtime.Scheme, config *config.Config, logger logr.Logger) *ZFSVolumeReconciler {
	// Create rate-limited client wrapper
	rateLimitedClient := ratelimiter.NewRateLimitedClient(client, config.APIRateLimit, config.APIBurst)

	// Use rate-limited client for volume checker with configurable PV label selector
	pvLabelSelector := config.PVLabelSelector
	if pvLabelSelector == "" {
		pvLabelSelector = "pv.kubernetes.io/provisioned-by=zfs.csi.openebs.io" // Default fallback
	}

	// Enable caching for cronjob mode, disable for controller mode
	enableCaching := config.CronJobMode

	volumeChecker := checker.NewVolumeChecker(rateLimitedClient, logger.WithName("volume-checker"), config.DryRun, pvLabelSelector, enableCaching)
	metricsCollector := metrics.NewMetricsCollector()

	return &ZFSVolumeReconciler{
		Client:            client,
		RateLimitedClient: rateLimitedClient,
		Scheme:            scheme,
		Config:            config,
		VolumeChecker:     volumeChecker,
		Metrics:           metricsCollector,
		Logger:            logger.WithName("zfsvolume-reconciler"),
	}
}

// Reconcile handles the reconciliation logic for ZFSVolume resources
func (r *ZFSVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()
	logger := r.Logger.WithValues("zfsvolume", req.NamespacedName, "reconcileID", time.Now().UnixNano())
	namespace := req.Namespace
	if namespace == "" {
		namespace = "cluster-wide"
	}

	// Create a timeout context for this reconciliation
	reconcileCtx, cancel := context.WithTimeout(ctx, r.Config.ReconcileTimeout)
	defer cancel()

	// Track active reconciliations
	r.Metrics.IncActiveReconciliations(namespace)
	defer r.Metrics.DecActiveReconciliations(namespace)

	// Enhanced startup logging - Requirement 4.1, 4.2
	logger.Info("Starting ZFSVolume reconciliation",
		"reconcileInterval", r.Config.ReconcileInterval,
		"dryRun", r.Config.DryRun)

	var reconcileResult string
	defer func() {
		duration := time.Since(startTime)
		logger.V(1).Info("Reconciliation completed", "duration", duration)
		// Record reconciliation metrics
		r.Metrics.RecordReconciliation(namespace, reconcileResult, duration)
	}()

	// Get the ZFSVolume resource using rate-limited client
	zfsVolume := &zfsv1.ZFSVolume{}
	if err := r.RateLimitedClient.Get(reconcileCtx, req.NamespacedName, zfsVolume); err != nil {
		if errors.IsNotFound(err) {
			// ZFSVolume was deleted, nothing to do
			logger.V(1).Info("ZFSVolume not found, likely deleted - reconciliation complete")
			reconcileResult = "success"
			return ctrl.Result{}, nil
		}
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to get ZFSVolume from API server",
			"namespace", req.Namespace,
			"name", req.Name,
			"error", err.Error())
		r.Metrics.RecordProcessingError(namespace, "api_error")
		reconcileResult = "failed"
		return ctrl.Result{}, fmt.Errorf("failed to get ZFSVolume: %w", err)
	}

	// Record that we processed a volume
	r.Metrics.RecordVolumeProcessed(namespace)

	// Log volume details - Requirement 4.2, 4.3
	logger.Info("Processing ZFSVolume",
		"volumeName", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"createdAt", zfsVolume.CreationTimestamp,
		"age", time.Since(zfsVolume.CreationTimestamp.Time).String(),
		"finalizers", zfsVolume.Finalizers,
		"deletionTimestamp", zfsVolume.DeletionTimestamp)

	// Check if the ZFSVolume is being deleted
	if zfsVolume.DeletionTimestamp != nil {
		logger.Info("ZFSVolume is being deleted, skipping reconciliation",
			"deletionTimestamp", zfsVolume.DeletionTimestamp)
		reconcileResult = "success"
		return ctrl.Result{}, nil
	}

	// Check if the ZFSVolume is orphaned
	logger.V(1).Info("Checking if ZFSVolume is orphaned")
	isOrphaned, err := r.VolumeChecker.IsOrphaned(reconcileCtx, zfsVolume)
	if err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to check if ZFSVolume is orphaned",
			"volumeName", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"error", err.Error(),
			"context", "orphan status check")
		r.Metrics.RecordProcessingError(namespace, "orphan_check_error")
		reconcileResult = "failed"
		return ctrl.Result{RequeueAfter: r.Config.RetryBackoffBase}, fmt.Errorf("failed to check orphan status: %w", err)
	}

	if !isOrphaned {
		logger.Info("ZFSVolume is not orphaned, no action needed",
			"action", "skip",
			"reason", "volume has active PV or PVC references",
			"nextReconcile", r.Config.ReconcileInterval)
		reconcileResult = "success"
		return ctrl.Result{RequeueAfter: r.Config.ReconcileInterval}, nil
	}

	// Record orphaned volume found
	r.Metrics.RecordOrphanedVolume(namespace)

	// Enhanced orphaned volume logging - Requirement 4.3
	logger.Info("Found orphaned ZFSVolume",
		"volumeName", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"createdAt", zfsVolume.CreationTimestamp,
		"age", time.Since(zfsVolume.CreationTimestamp.Time).String(),
		"status", "orphaned")

	// Validate if it's safe to delete
	logger.V(1).Info("Validating ZFSVolume for safe deletion")
	validation, err := r.VolumeChecker.ValidateForDeletion(reconcileCtx, zfsVolume)
	if err != nil {
		// Enhanced error logging - Requirement 4.5
		logger.Error(err, "Failed to validate ZFSVolume for deletion",
			"volumeName", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"error", err.Error(),
			"context", "deletion validation")
		r.Metrics.RecordProcessingError(namespace, "validation_error")
		reconcileResult = "failed"
		return ctrl.Result{RequeueAfter: r.Config.RetryBackoffBase}, fmt.Errorf("failed to validate for deletion: %w", err)
	}

	// Log the action (dry-run or actual) - Requirement 4.6
	r.VolumeChecker.LogDeletionAction(zfsVolume, validation)

	if !validation.IsSafe {
		logger.Info("ZFSVolume failed safety validation, skipping deletion",
			"action", "skip",
			"reason", validation.Reason,
			"validationErrors", validation.ValidationErrors,
			"nextReconcile", r.Config.ReconcileInterval)
		reconcileResult = "success"
		return ctrl.Result{RequeueAfter: r.Config.ReconcileInterval}, nil
	}

	// If dry-run mode, don't actually delete - Requirement 4.6
	if r.Config.DryRun {
		logger.Info("DRY-RUN: Would delete ZFSVolume",
			"action", "delete",
			"mode", "dry-run",
			"volumeName", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"reason", "orphaned volume passed safety validation")
		r.Metrics.RecordDeletionAttempt(namespace, "dry_run", 0)
		reconcileResult = "success"
		return ctrl.Result{RequeueAfter: r.Config.ReconcileInterval}, nil
	}

	// Perform the actual deletion
	logger.Info("Attempting to delete orphaned ZFSVolume",
		"action", "delete",
		"mode", "live",
		"volumeName", zfsVolume.Name,
		"namespace", zfsVolume.Namespace)

	deletionStartTime := time.Now()
	if err := r.deleteZFSVolume(reconcileCtx, zfsVolume); err != nil {
		// Enhanced error logging - Requirement 4.4, 4.5
		logger.Error(err, "Failed to delete ZFSVolume",
			"volumeName", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"error", err.Error(),
			"context", "volume deletion",
			"retryAfter", r.Config.RetryBackoffBase)
		r.Metrics.RecordDeletionAttempt(namespace, "failed", time.Since(deletionStartTime))
		reconcileResult = "failed"
		return ctrl.Result{RequeueAfter: r.Config.RetryBackoffBase}, fmt.Errorf("failed to delete ZFSVolume: %w", err)
	}

	// Enhanced success logging - Requirement 4.4
	logger.Info("Successfully deleted orphaned ZFSVolume",
		"action", "delete",
		"status", "success",
		"volumeName", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"duration", time.Since(startTime))
	r.Metrics.RecordDeletionAttempt(namespace, "success", time.Since(deletionStartTime))
	reconcileResult = "success"
	return ctrl.Result{}, nil
}

// findOrphanedZFSVolumes scans all ZFSVolumes and identifies orphaned candidates
func (r *ZFSVolumeReconciler) findOrphanedZFSVolumes(ctx context.Context) (*ReconcileResult, error) {
	startTime := time.Now()
	logger := r.Logger.WithName("find-orphaned").WithValues("scanID", time.Now().UnixNano())

	// Determine namespace for metrics (use filter or cluster-wide)
	metricsNamespace := r.Config.NamespaceFilter
	if metricsNamespace == "" {
		metricsNamespace = "cluster-wide"
	}

	// Track active reconciliation
	r.Metrics.IncActiveReconciliations(metricsNamespace)
	defer r.Metrics.DecActiveReconciliations(metricsNamespace)

	// Enhanced startup logging - Requirement 4.1, 4.2
	logger.Info("Starting comprehensive scan for orphaned ZFSVolumes",
		"dryRun", r.Config.DryRun,
		"namespaceFilter", r.Config.NamespaceFilter,
		"labelSelector", r.Config.LabelSelector,
		"maxRetryAttempts", r.Config.MaxRetryAttempts,
		"retryBackoffBase", r.Config.RetryBackoffBase)

	result := &ReconcileResult{
		OrphanedVolumes:  []string{},
		DeletedVolumes:   []string{},
		FailedDeletions:  []string{},
		ProcessingErrors: []error{},
	}

	// Build list options with filters and pagination
	listOpts := []client.ListOption{}

	// Apply namespace filter if configured
	if r.Config.NamespaceFilter != "" {
		listOpts = append(listOpts, client.InNamespace(r.Config.NamespaceFilter))
		logger.Info("Applying namespace filter", "namespace", r.Config.NamespaceFilter)
	}

	// Apply label selector if configured
	if r.Config.LabelSelector != "" {
		selector, err := labels.Parse(r.Config.LabelSelector)
		if err != nil {
			logger.Error(err, "Failed to parse label selector, skipping filter",
				"selector", r.Config.LabelSelector,
				"error", err.Error())
			r.Metrics.RecordProcessingError(metricsNamespace, "label_selector_parse_error")
		} else {
			listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
			logger.Info("Applying label selector", "selector", r.Config.LabelSelector)
		}
	}

	// Create a timeout context for list operations
	listCtx, listCancel := context.WithTimeout(ctx, r.Config.ListOperationTimeout)
	defer listCancel()

	// Use pagination to handle large result sets efficiently
	const pageSize = 500 // Process in batches of 500 to optimize memory usage
	allZFSVolumes := make([]zfsv1.ZFSVolume, 0)
	continueToken := ""

	for {
		// Create a fresh list for each page
		zfsVolumeList := &zfsv1.ZFSVolumeList{}
		pageOpts := append(listOpts, client.Limit(pageSize))
		if continueToken != "" {
			pageOpts = append(pageOpts, client.Continue(continueToken))
		}

		logger.V(1).Info("Listing ZFSVolumes from Kubernetes API",
			"pageSize", pageSize,
			"continueToken", continueToken)

		if err := r.RateLimitedClient.List(listCtx, zfsVolumeList, pageOpts...); err != nil {
			// Enhanced error logging - Requirement 4.5
			logger.Error(err, "Failed to list ZFSVolumes from Kubernetes API",
				"error", err.Error(),
				"context", "volume listing",
				"namespaceFilter", r.Config.NamespaceFilter,
				"labelSelector", r.Config.LabelSelector)
			r.Metrics.RecordProcessingError(metricsNamespace, "api_error")
			r.Metrics.RecordReconciliation(metricsNamespace, "failed", time.Since(startTime))
			return nil, fmt.Errorf("failed to list ZFSVolumes: %w", err)
		}

		// Add this page's items to our collection
		allZFSVolumes = append(allZFSVolumes, zfsVolumeList.Items...)

		// Check if there are more pages
		continueToken = zfsVolumeList.GetContinue()
		if continueToken == "" {
			break // No more pages
		}

		logger.V(1).Info("Fetched page of ZFSVolumes",
			"pageSize", len(zfsVolumeList.Items),
			"totalSoFar", len(allZFSVolumes),
			"hasMorePages", continueToken != "")
	}

	// Enhanced volume processing logging - Requirement 4.2
	logger.Info("Found ZFSVolumes to process",
		"totalCount", len(allZFSVolumes),
		"namespaceFilter", r.Config.NamespaceFilter,
		"labelSelector", r.Config.LabelSelector,
		"processingStartTime", startTime)

	if len(allZFSVolumes) == 0 {
		logger.Info("No ZFSVolumes found in cluster", "action", "complete")
		r.Metrics.RecordReconciliation(metricsNamespace, "success", time.Since(startTime))
		return result, nil
	}

	// Populate cache for efficient PV/PVC lookups
	logger.Info("Populating PV/PVC cache for efficient lookups")
	if err := r.VolumeChecker.PopulateCache(ctx); err != nil {
		logger.Error(err, "Failed to populate cache, performance may be degraded")
		// Continue without cache - fallback to direct API calls
	}

	// Process each ZFSVolume
	for i, zfsVolume := range allZFSVolumes {
		volumeStartTime := time.Now()
		volumeNamespace := zfsVolume.Namespace
		if volumeNamespace == "" {
			volumeNamespace = "cluster-wide"
		}

		volumeLogger := logger.WithValues(
			"zfsvolume", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"volumeIndex", i+1,
			"totalVolumes", len(allZFSVolumes))

		volumeLogger.V(1).Info("Processing ZFSVolume",
			"createdAt", zfsVolume.CreationTimestamp,
			"age", time.Since(zfsVolume.CreationTimestamp.Time).String(),
			"finalizers", zfsVolume.Finalizers)

		// Record that we processed a volume
		r.Metrics.RecordVolumeProcessed(volumeNamespace)

		// Skip volumes that are being deleted
		if zfsVolume.DeletionTimestamp != nil {
			volumeLogger.Info("Skipping ZFSVolume that is being deleted",
				"action", "skip",
				"reason", "deletion in progress",
				"deletionTimestamp", zfsVolume.DeletionTimestamp)
			continue
		}

		// Check if orphaned
		volumeLogger.V(1).Info("Checking orphan status for ZFSVolume")
		isOrphaned, err := r.VolumeChecker.IsOrphaned(ctx, &zfsVolume)
		if err != nil {
			// Enhanced error logging - Requirement 4.5
			volumeLogger.Error(err, "Failed to check if ZFSVolume is orphaned",
				"error", err.Error(),
				"context", "orphan status check",
				"volumeAge", time.Since(zfsVolume.CreationTimestamp.Time).String())
			r.Metrics.RecordProcessingError(volumeNamespace, "orphan_check_error")
			result.ProcessingErrors = append(result.ProcessingErrors, fmt.Errorf("failed to check %s/%s: %w", zfsVolume.Namespace, zfsVolume.Name, err))
			continue
		}

		if !isOrphaned {
			volumeLogger.V(1).Info("ZFSVolume is not orphaned",
				"action", "skip",
				"reason", "has active PV or PVC references",
				"processingTime", time.Since(volumeStartTime))
			continue
		}

		volumeKey := fmt.Sprintf("%s/%s", zfsVolume.Namespace, zfsVolume.Name)
		result.OrphanedVolumes = append(result.OrphanedVolumes, volumeKey)

		// Record orphaned volume found
		r.Metrics.RecordOrphanedVolume(volumeNamespace)

		// Enhanced orphaned volume logging - Requirement 4.3
		volumeLogger.Info("Found orphaned ZFSVolume",
			"status", "orphaned",
			"volumeName", zfsVolume.Name,
			"namespace", zfsVolume.Namespace,
			"createdAt", zfsVolume.CreationTimestamp,
			"age", time.Since(zfsVolume.CreationTimestamp.Time).String(),
			"finalizers", zfsVolume.Finalizers,
			"orphanedCount", len(result.OrphanedVolumes))

		// Validate for deletion
		volumeLogger.V(1).Info("Validating orphaned ZFSVolume for safe deletion")
		validation, err := r.VolumeChecker.ValidateForDeletion(ctx, &zfsVolume)
		if err != nil {
			// Enhanced error logging - Requirement 4.5
			volumeLogger.Error(err, "Failed to validate ZFSVolume for deletion",
				"error", err.Error(),
				"context", "deletion validation",
				"volumeAge", time.Since(zfsVolume.CreationTimestamp.Time).String())
			r.Metrics.RecordProcessingError(volumeNamespace, "validation_error")
			result.ProcessingErrors = append(result.ProcessingErrors, fmt.Errorf("failed to validate %s: %w", volumeKey, err))
			continue
		}

		// Log the action (dry-run or actual) - Requirement 4.6
		r.VolumeChecker.LogDeletionAction(&zfsVolume, validation)

		if !validation.IsSafe {
			volumeLogger.Info("ZFSVolume failed safety validation, skipping deletion",
				"action", "skip",
				"reason", validation.Reason,
				"validationErrors", validation.ValidationErrors,
				"processingTime", time.Since(volumeStartTime))
			continue
		}

		// If dry-run mode, don't actually delete - Requirement 4.6
		if r.Config.DryRun {
			volumeLogger.Info("DRY-RUN: Would delete ZFSVolume",
				"action", "delete",
				"mode", "dry-run",
				"reason", "orphaned volume passed safety validation",
				"processingTime", time.Since(volumeStartTime))
			r.Metrics.RecordDeletionAttempt(volumeNamespace, "dry_run", time.Since(volumeStartTime))
			continue
		}

		// Perform the actual deletion
		volumeLogger.Info("Attempting to delete orphaned ZFSVolume",
			"action", "delete",
			"mode", "live",
			"reason", "orphaned volume passed safety validation")

		deletionStartTime := time.Now()
		if err := r.deleteZFSVolume(ctx, &zfsVolume); err != nil {
			// Enhanced error logging - Requirement 4.4, 4.5
			volumeLogger.Error(err, "Failed to delete ZFSVolume",
				"action", "delete",
				"status", "failed",
				"error", err.Error(),
				"context", "volume deletion",
				"processingTime", time.Since(volumeStartTime))
			r.Metrics.RecordDeletionAttempt(volumeNamespace, "failed", time.Since(deletionStartTime))
			result.FailedDeletions = append(result.FailedDeletions, volumeKey)
			result.ProcessingErrors = append(result.ProcessingErrors, fmt.Errorf("failed to delete %s: %w", volumeKey, err))
			continue
		}

		result.DeletedVolumes = append(result.DeletedVolumes, volumeKey)
		r.Metrics.RecordDeletionAttempt(volumeNamespace, "success", time.Since(deletionStartTime))
		// Enhanced success logging - Requirement 4.4
		volumeLogger.Info("Successfully deleted orphaned ZFSVolume",
			"action", "delete",
			"status", "success",
			"processingTime", time.Since(volumeStartTime),
			"deletedCount", len(result.DeletedVolumes))
	}

	totalDuration := time.Since(startTime)
	// Enhanced completion logging - Requirement 4.2, 4.3, 4.4
	logger.Info("Completed scan for orphaned ZFSVolumes",
		"totalProcessed", len(allZFSVolumes),
		"orphanedFound", len(result.OrphanedVolumes),
		"volumesDeleted", len(result.DeletedVolumes),
		"deletionsFailed", len(result.FailedDeletions),
		"processingErrors", len(result.ProcessingErrors),
		"totalDuration", totalDuration,
		"averageTimePerVolume", time.Duration(int64(totalDuration)/int64(max(len(allZFSVolumes), 1))),
		"orphanedVolumes", result.OrphanedVolumes,
		"deletedVolumes", result.DeletedVolumes,
		"failedDeletions", result.FailedDeletions)

	// Log processing errors if any - Requirement 4.5
	if len(result.ProcessingErrors) > 0 {
		logger.Info("Processing errors encountered during scan",
			"errorCount", len(result.ProcessingErrors))
		for i, procErr := range result.ProcessingErrors {
			logger.Error(procErr, "Processing error details", "errorIndex", i+1)
		}
	}

	// Record final reconciliation metrics
	reconcileResult := "success"
	if len(result.ProcessingErrors) > 0 {
		reconcileResult = "partial_failure"
	}
	r.Metrics.RecordReconciliation(metricsNamespace, reconcileResult, time.Since(startTime))

	return result, nil
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// deleteZFSVolume handles the actual deletion of a ZFSVolume with proper error handling,
// exponential backoff retry logic, concurrent deletion handling, and finalizer respect
func (r *ZFSVolumeReconciler) deleteZFSVolume(ctx context.Context, zfsVolume *zfsv1.ZFSVolume) error {
	startTime := time.Now()
	logger := r.Logger.WithValues(
		"zfsvolume", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"deletionID", time.Now().UnixNano())

	// Enhanced deletion startup logging - Requirement 4.4
	logger.Info("Starting ZFSVolume deletion process",
		"volumeName", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"createdAt", zfsVolume.CreationTimestamp,
		"age", time.Since(zfsVolume.CreationTimestamp.Time).String(),
		"finalizers", zfsVolume.Finalizers,
		"maxRetryAttempts", r.Config.MaxRetryAttempts,
		"retryBackoffBase", r.Config.RetryBackoffBase)

	// Check if the volume is already being deleted
	if zfsVolume.DeletionTimestamp != nil {
		logger.Info("ZFSVolume is already being deleted, waiting for completion",
			"deletionTimestamp", zfsVolume.DeletionTimestamp,
			"action", "wait")
		return r.waitForDeletion(ctx, zfsVolume)
	}

	// Check if the volume has finalizers - respect them and don't force deletion
	if len(zfsVolume.Finalizers) > 0 {
		// Filter out zfs.openebs.io/finalizer for orphaned volumes (same logic as validation)
		blockingFinalizers := []string{}
		ignoredFinalizers := []string{}

		for _, finalizer := range zfsVolume.Finalizers {
			if finalizer == "zfs.openebs.io/finalizer" {
				// Skip this finalizer for orphaned volumes
				ignoredFinalizers = append(ignoredFinalizers, finalizer)
				logger.Info("Ignoring zfs.openebs.io/finalizer for orphaned volume during deletion",
					"finalizer", finalizer,
					"reason", "volume is orphaned")
				continue
			}
			blockingFinalizers = append(blockingFinalizers, finalizer)
		}

		if len(blockingFinalizers) > 0 {
			logger.Info("ZFSVolume has blocking finalizers, respecting them and not forcing deletion",
				"blockingFinalizers", blockingFinalizers,
				"ignoredFinalizers", ignoredFinalizers,
				"action", "abort",
				"reason", "finalizers must be handled first")
			return fmt.Errorf("ZFSVolume has finalizers that must be handled first: %v", blockingFinalizers)
		}
	}

	// Implement retry logic with exponential backoff
	var lastErr error
	backoff := r.Config.RetryBackoffBase
	maxBackoff := time.Minute * 5 // Cap backoff at 5 minutes

	for attempt := 0; attempt <= r.Config.MaxRetryAttempts; attempt++ {
		attemptStartTime := time.Now()
		attemptLogger := logger.WithValues("attempt", attempt+1, "maxAttempts", r.Config.MaxRetryAttempts+1)

		if attempt > 0 {
			// Enhanced retry logging - Requirement 4.4, 4.5
			attemptLogger.Info("Retrying ZFSVolume deletion",
				"backoffDuration", backoff,
				"previousError", lastErr.Error(),
				"totalAttempts", attempt,
				"timeElapsed", time.Since(startTime))

			select {
			case <-ctx.Done():
				logger.Error(ctx.Err(), "Context cancelled during retry backoff",
					"attempt", attempt,
					"timeElapsed", time.Since(startTime))
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(backoff):
				// Continue with retry
			}

			// Exponential backoff with jitter and cap
			backoff = time.Duration(float64(backoff) * 2.0)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		attemptLogger.V(1).Info("Attempting ZFSVolume deletion", "backoffUsed", backoff)

		// Get the latest version of the resource to handle concurrent modifications
		latestVolume := &zfsv1.ZFSVolume{}
		if err := r.RateLimitedClient.Get(ctx, client.ObjectKeyFromObject(zfsVolume), latestVolume); err != nil {
			if errors.IsNotFound(err) {
				// Volume was already deleted by someone else - success
				logger.Info("ZFSVolume was already deleted by another process",
					"action", "delete",
					"status", "success",
					"reason", "concurrent deletion",
					"totalDuration", time.Since(startTime))
				return nil
			}
			// Enhanced error logging - Requirement 4.5
			attemptLogger.Error(err, "Failed to get latest ZFSVolume version",
				"error", err.Error(),
				"context", "resource refresh",
				"attemptDuration", time.Since(attemptStartTime))
			lastErr = fmt.Errorf("failed to get latest ZFSVolume version: %w", err)
			continue
		}

		// Check if the volume is now being deleted by another process
		if latestVolume.DeletionTimestamp != nil {
			logger.Info("ZFSVolume is now being deleted by another process, waiting for completion",
				"deletionTimestamp", latestVolume.DeletionTimestamp,
				"action", "wait",
				"reason", "concurrent deletion detected")
			return r.waitForDeletion(ctx, latestVolume)
		}

		// Check if finalizers were added since we last checked
		if len(latestVolume.Finalizers) > 0 {
			// Filter out zfs.openebs.io/finalizer for orphaned volumes (same logic as validation)
			blockingFinalizers := []string{}
			ignoredFinalizers := []string{}

			for _, finalizer := range latestVolume.Finalizers {
				if finalizer == "zfs.openebs.io/finalizer" {
					// Skip this finalizer for orphaned volumes
					ignoredFinalizers = append(ignoredFinalizers, finalizer)
					attemptLogger.Info("Ignoring zfs.openebs.io/finalizer for orphaned volume during deletion",
						"finalizer", finalizer,
						"reason", "volume is orphaned")
					continue
				}
				blockingFinalizers = append(blockingFinalizers, finalizer)
			}

			if len(blockingFinalizers) > 0 {
				attemptLogger.Info("ZFSVolume now has blocking finalizers, cannot proceed with deletion",
					"blockingFinalizers", blockingFinalizers,
					"ignoredFinalizers", ignoredFinalizers,
					"action", "abort",
					"reason", "finalizers added during deletion process")
				return fmt.Errorf("ZFSVolume now has finalizers that must be handled first: %v", blockingFinalizers)
			}
		}

		// Attempt to delete the ZFSVolume using the latest version
		attemptLogger.V(1).Info("Submitting deletion request to Kubernetes API")
		if err := r.RateLimitedClient.Delete(ctx, latestVolume); err != nil {
			if errors.IsNotFound(err) {
				// Volume was deleted between our Get and Delete calls - success
				logger.Info("ZFSVolume was deleted between get and delete operations",
					"action", "delete",
					"status", "success",
					"reason", "concurrent deletion",
					"totalDuration", time.Since(startTime))
				return nil
			}

			if errors.IsConflict(err) {
				// Resource was modified again, retry with fresh version
				attemptLogger.Info("Conflict during deletion, will retry with fresh resource version",
					"error", err.Error(),
					"reason", "resource version conflict",
					"attemptDuration", time.Since(attemptStartTime))
				lastErr = fmt.Errorf("conflict during deletion (resource modified): %w", err)
				continue
			}

			// Check for transient errors that are worth retrying
			if r.isTransientError(err) {
				// Enhanced transient error logging - Requirement 4.5
				attemptLogger.Info("Transient error during deletion, will retry",
					"error", err.Error(),
					"errorType", "transient",
					"willRetry", attempt < r.Config.MaxRetryAttempts,
					"attemptDuration", time.Since(attemptStartTime))
				lastErr = fmt.Errorf("transient error during deletion: %w", err)
				continue
			}

			// For permanent errors, don't retry - Enhanced error logging - Requirement 4.5
			attemptLogger.Error(err, "Permanent error during deletion, not retrying",
				"error", err.Error(),
				"errorType", "permanent",
				"totalDuration", time.Since(startTime),
				"attemptDuration", time.Since(attemptStartTime))
			return fmt.Errorf("permanent error during deletion: %w", err)
		}

		// Deletion request submitted successfully
		attemptLogger.Info("ZFSVolume deletion request submitted successfully",
			"action", "delete",
			"status", "submitted",
			"attemptDuration", time.Since(attemptStartTime))

		// Optionally wait for the deletion to complete to ensure it's actually deleted
		if err := r.waitForDeletion(ctx, latestVolume); err != nil {
			attemptLogger.Info("Deletion request submitted but waiting for completion failed",
				"error", err.Error(),
				"note", "deletion request was submitted successfully")
			// Don't treat this as a failure - the deletion request was submitted
		}

		// Enhanced success logging - Requirement 4.4
		logger.Info("Successfully deleted ZFSVolume",
			"action", "delete",
			"status", "success",
			"totalDuration", time.Since(startTime),
			"attemptsUsed", attempt+1,
			"finalAttemptDuration", time.Since(attemptStartTime))
		return nil
	}

	// Enhanced failure logging - Requirement 4.4, 4.5
	logger.Error(lastErr, "Failed to delete ZFSVolume after all retry attempts",
		"action", "delete",
		"status", "failed",
		"totalAttempts", r.Config.MaxRetryAttempts+1,
		"totalDuration", time.Since(startTime),
		"finalError", lastErr.Error())
	return fmt.Errorf("failed to delete ZFSVolume after %d attempts, last error: %w",
		r.Config.MaxRetryAttempts+1, lastErr)
}

// waitForDeletion waits for a ZFSVolume to be completely deleted from the cluster
func (r *ZFSVolumeReconciler) waitForDeletion(ctx context.Context, zfsVolume *zfsv1.ZFSVolume) error {
	startTime := time.Now()
	logger := r.Logger.WithValues(
		"zfsvolume", zfsVolume.Name,
		"namespace", zfsVolume.Namespace,
		"waitID", time.Now().UnixNano())

	waitTimeout := time.Minute * 2
	checkInterval := time.Second * 2

	// Enhanced wait startup logging - Requirement 4.4
	logger.Info("Waiting for ZFSVolume deletion to complete",
		"action", "wait",
		"deletionTimestamp", zfsVolume.DeletionTimestamp,
		"waitTimeout", waitTimeout,
		"checkInterval", checkInterval)

	// Create a timeout context for waiting
	waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	checkCount := 0
	for {
		select {
		case <-waitCtx.Done():
			// Enhanced timeout logging - Requirement 4.5
			logger.Error(waitCtx.Err(), "Timeout waiting for ZFSVolume deletion to complete",
				"action", "wait",
				"status", "timeout",
				"waitDuration", time.Since(startTime),
				"checksPerformed", checkCount,
				"timeoutDuration", waitTimeout)
			return fmt.Errorf("timeout waiting for deletion to complete: %w", waitCtx.Err())
		case <-ticker.C:
			checkCount++
			checkStartTime := time.Now()

			logger.V(1).Info("Checking ZFSVolume deletion status",
				"checkNumber", checkCount,
				"timeElapsed", time.Since(startTime))

			// Check if the volume still exists
			currentVolume := &zfsv1.ZFSVolume{}
			if err := r.RateLimitedClient.Get(waitCtx, client.ObjectKeyFromObject(zfsVolume), currentVolume); err != nil {
				if errors.IsNotFound(err) {
					// Enhanced success logging - Requirement 4.4
					logger.Info("ZFSVolume deletion completed successfully",
						"action", "wait",
						"status", "success",
						"totalWaitDuration", time.Since(startTime),
						"checksPerformed", checkCount,
						"finalCheckDuration", time.Since(checkStartTime))
					return nil
				}
				// Enhanced error logging - Requirement 4.5
				logger.V(1).Info("Error checking ZFSVolume deletion status, continuing to wait",
					"error", err.Error(),
					"checkNumber", checkCount,
					"checkDuration", time.Since(checkStartTime),
					"timeElapsed", time.Since(startTime))
				// Continue waiting despite the error
			} else {
				// Volume still exists, log current state
				logger.V(1).Info("ZFSVolume still exists, continuing to wait",
					"checkNumber", checkCount,
					"currentDeletionTimestamp", currentVolume.DeletionTimestamp,
					"currentFinalizers", currentVolume.Finalizers,
					"timeElapsed", time.Since(startTime))
			}
		}
	}
}

// isTransientError determines if an error is transient and worth retrying
func (r *ZFSVolumeReconciler) isTransientError(err error) bool {
	return errors.IsServerTimeout(err) ||
		errors.IsServiceUnavailable(err) ||
		errors.IsTooManyRequests(err) ||
		errors.IsTimeout(err) ||
		errors.IsInternalError(err)
}

// ExecuteCleanup performs a one-time cleanup operation for CronJob mode
func (r *ZFSVolumeReconciler) ExecuteCleanup(ctx context.Context) (*ReconcileResult, error) {
	logger := r.Logger.WithName("cleanup-execution")
	logger.Info("Starting one-time cleanup execution")

	// Populate cache for efficient lookups in cronjob mode
	logger.Info("Populating PV/PVC cache for cronjob execution")
	if err := r.VolumeChecker.PopulateCache(ctx); err != nil {
		logger.Error(err, "Failed to populate cache, falling back to direct API calls")
	}

	startTime := time.Now()
	result, err := r.findOrphanedZFSVolumes(ctx)
	duration := time.Since(startTime)

	if err != nil {
		logger.Error(err, "cleanup execution failed", "duration", duration)
		return result, fmt.Errorf("cleanup execution failed: %w", err)
	}

	logger.Info("Cleanup execution completed",
		"duration", duration,
		"orphanedFound", len(result.OrphanedVolumes),
		"deleted", len(result.DeletedVolumes),
		"failed", len(result.FailedDeletions),
		"errors", len(result.ProcessingErrors))

	return result, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *ZFSVolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSVolume{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.Config.MaxConcurrentReconciles,
			RateLimiter:             ratelimiter.ControllerRateLimiter(r.Config.RetryBackoffBase, time.Minute*5),
		}).
		Complete(r)
}
