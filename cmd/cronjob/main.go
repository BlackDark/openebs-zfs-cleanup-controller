package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/controller"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(zfsv1.AddToScheme(scheme))
}

func main() {
	var timeout string
	var version string
	flag.StringVar(&timeout, "timeout", "5m", "Maximum execution time for the cleanup job")
	flag.StringVar(&version, "version", "dev", "Version of the controller")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		setupLog.Error(err, "invalid timeout duration")
		os.Exit(1)
	}

	// Load configuration from environment variables
	cfg, err := config.LoadConfig()
	if err != nil {
		setupLog.Error(err, "failed to load configuration")
		os.Exit(1)
	}

	// Log startup information - Requirement 4.1
	setupLog.Info("ZFSVolume Cleanup Controller starting",
		"version", version,
		"mode", "cronjob",
		"configuration", cfg.String(),
		"timeout", timeoutDuration)

	if cfg.DryRun {
		setupLog.Info("DRY-RUN MODE ENABLED - No actual deletions will be performed")
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	// Create a channel to receive OS signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Create a context that gets cancelled on signal
	signalCtx, signalCancel := context.WithCancel(ctx)
	defer signalCancel()

	// Start a goroutine to handle signals
	go func() {
		sig := <-signalChan
		setupLog.Info("Received shutdown signal, initiating graceful shutdown", "signal", sig.String())
		signalCancel()
	}()

	k8sConfig := ctrl.GetConfigOrDie()
	k8sClient, err := client.New(k8sConfig, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create client")
		os.Exit(1)
	}

	setupLog.Info("starting one-time cleanup job", "timeout", timeoutDuration)

	// Create reconciler for one-time execution
	reconciler := controller.NewZFSVolumeReconciler(
		k8sClient,
		scheme,
		cfg,
		ctrl.Log.WithName("cronjob"),
	)

	// Execute the cleanup operation with signal-aware context
	startTime := time.Now()
	result, err := reconciler.ExecuteCleanup(signalCtx)
	duration := time.Since(startTime)

	if err != nil {
		// Check if the error was due to context cancellation (graceful shutdown)
		if signalCtx.Err() == context.Canceled {
			setupLog.Info("cleanup job interrupted by shutdown signal",
				"duration", duration,
				"orphanedFound", len(result.OrphanedVolumes),
				"deleted", len(result.DeletedVolumes),
				"failed", len(result.FailedDeletions),
				"errors", len(result.ProcessingErrors),
				"exitCode", 130) // Standard exit code for SIGINT
			os.Exit(130)
		}

		setupLog.Error(err, "cleanup job failed",
			"duration", duration,
			"orphanedFound", len(result.OrphanedVolumes),
			"deleted", len(result.DeletedVolumes),
			"failed", len(result.FailedDeletions),
			"errors", len(result.ProcessingErrors),
			"exitCode", 1)
		os.Exit(1)
	}

	// Log completion with comprehensive results - Requirement 4.2, 4.3, 4.4
	setupLog.Info("cleanup job completed successfully",
		"duration", duration,
		"orphanedVolumesFound", len(result.OrphanedVolumes),
		"volumesDeleted", len(result.DeletedVolumes),
		"deletionsFailed", len(result.FailedDeletions),
		"processingErrors", len(result.ProcessingErrors),
		"orphanedVolumes", result.OrphanedVolumes,
		"deletedVolumes", result.DeletedVolumes,
		"failedDeletions", result.FailedDeletions)

	if len(result.ProcessingErrors) > 0 {
		setupLog.Info("processing errors encountered during cleanup",
			"errorCount", len(result.ProcessingErrors))
		for i, procErr := range result.ProcessingErrors {
			setupLog.Error(procErr, "processing error", "errorIndex", i)
		}
	}

	// Exit with appropriate code
	if len(result.FailedDeletions) > 0 || len(result.ProcessingErrors) > 0 {
		setupLog.Info("cleanup completed with some errors", "exitCode", 1)
		os.Exit(1)
	}

	setupLog.Info("cleanup completed successfully", "exitCode", 0)
}
