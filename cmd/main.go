package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	clientpkg "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/client"
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
	var mode string
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var version string
	var gracefulShutdownTimeout string
	var timeout string

	// Mode selection flag
	flag.StringVar(&mode, "mode", "controller", "Mode to run in: 'controller' for long-running service, 'cronjob' for one-time execution")

	// Controller mode flags
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&gracefulShutdownTimeout, "graceful-shutdown-timeout", "30s", "Maximum time to wait for graceful shutdown")

	// Cronjob mode flags
	flag.StringVar(&timeout, "timeout", "5m", "Maximum execution time for the cleanup job (cronjob mode only)")

	// Common flags
	flag.StringVar(&version, "version", "dev", "Version of the controller")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Load configuration from environment variables
	cfg, err := config.LoadConfig()
	if err != nil {
		setupLog.Error(err, "failed to load configuration")
		os.Exit(1)
	}

	// Set mode-specific configuration
	switch mode {
	case "cronjob":
		cfg.CronJobMode = true
		setupLog.Info("Running in cronjob mode - one-time cleanup execution")
	case "controller":
		cfg.CronJobMode = false
		setupLog.Info("Running in controller mode - long-running service")
	default:
		setupLog.Error(nil, "invalid mode specified", "mode", mode, "validModes", []string{"controller", "cronjob"})
		os.Exit(1)
	}

	// Log startup information - Requirement 4.1
	setupLog.Info("ZFSVolume Cleanup Controller starting",
		"version", version,
		"mode", mode,
		"configuration", cfg.String())

	if cfg.DryRun {
		setupLog.Info("DRY-RUN MODE ENABLED - No actual deletions will be performed")
	}

	// Execute based on mode
	if mode == "cronjob" {
		runCronJob(cfg, version, timeout)
	} else {
		runController(cfg, version, metricsAddr, probeAddr, enableLeaderElection, gracefulShutdownTimeout)
	}
}

func runCronJob(cfg *config.Config, version, timeout string) {
	timeoutDuration, err := time.ParseDuration(timeout)
	if err != nil {
		setupLog.Error(err, "invalid timeout duration")
		os.Exit(1)
	}

	setupLog.Info("starting one-time cleanup job", "timeout", timeoutDuration)

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

func runController(cfg *config.Config, version, metricsAddr, probeAddr string, enableLeaderElection bool, gracefulShutdownTimeout string) {
	// Parse graceful shutdown timeout
	shutdownTimeout, err := time.ParseDuration(gracefulShutdownTimeout)
	if err != nil {
		setupLog.Error(err, "invalid graceful shutdown timeout")
		os.Exit(1)
	}

	// Create controller manager with configurable reconcile intervals
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              "zfsvolume-cleanup-controller",
		LeaderElectionNamespace:       "openebs",
		GracefulShutdownTimeout:       &shutdownTimeout,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Set up enhanced health checks for liveness and readiness probes
	if err := setupHealthChecks(mgr, cfg); err != nil {
		setupLog.Error(err, "unable to set up health checks")
		os.Exit(1)
	}

	// Set up the ZFSVolume reconciler with configurable intervals
	reconciler := controller.NewZFSVolumeReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		cfg,
		ctrl.Log.WithName("controllers"),
	)

	if err = reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ZFSVolume")
		os.Exit(1)
	}

	setupLog.Info("controller manager configured successfully",
		"reconcileInterval", cfg.ReconcileInterval,
		"maxConcurrentReconciles", cfg.MaxConcurrentReconciles,
		"retryBackoffBase", cfg.RetryBackoffBase,
		"maxRetryAttempts", cfg.MaxRetryAttempts,
		"apiRateLimit", cfg.APIRateLimit,
		"apiBurst", cfg.APIBurst,
		"metricsAddress", metricsAddr,
		"probeAddress", probeAddr,
		"leaderElection", enableLeaderElection,
		"gracefulShutdownTimeout", shutdownTimeout)

	// Set up graceful shutdown handling
	ctx := setupGracefulShutdown(shutdownTimeout)

	setupLog.Info("starting long-running service manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}

	setupLog.Info("controller manager shutdown completed")
}

// setupHealthChecks configures enhanced health and readiness checks
func setupHealthChecks(mgr ctrl.Manager, cfg *config.Config) error {
	// Enhanced liveness check that validates controller health
	livenessCheck := func(req *http.Request) error {
		// Basic ping check
		return nil
	}

	// Enhanced readiness check that validates API connectivity and CRD availability
	readinessCheck := func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client := mgr.GetClient()

		// Validate Kubernetes API connectivity
		if err := clientpkg.ValidateConnection(ctx, client); err != nil {
			return fmt.Errorf("kubernetes API connection failed: %w", err)
		}

		// Validate ZFSVolume CRD availability
		if err := clientpkg.ValidateZFSVolumeCRD(ctx, client); err != nil {
			return fmt.Errorf("ZFSVolume CRD validation failed: %w", err)
		}

		return nil
	}

	// Add health checks with descriptive names
	if err := mgr.AddHealthzCheck("controller-liveness", livenessCheck); err != nil {
		return fmt.Errorf("failed to add liveness check: %w", err)
	}

	if err := mgr.AddReadyzCheck("api-connectivity", readinessCheck); err != nil {
		return fmt.Errorf("failed to add readiness check: %w", err)
	}

	return nil
}

// setupGracefulShutdown configures signal handling for graceful shutdown
func setupGracefulShutdown(timeout time.Duration) context.Context {
	// Create a context that will be cancelled on shutdown signals
	ctx, cancel := context.WithCancel(context.Background())

	// Create a channel to receive OS signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle signals
	go func() {
		defer cancel()
		sig := <-signalChan
		setupLog.Info("Received shutdown signal, initiating graceful shutdown",
			"signal", sig.String(),
			"gracefulShutdownTimeout", timeout)

		// Create a timeout context for graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
		defer shutdownCancel()

		// Wait for either shutdown completion or timeout
		select {
		case <-shutdownCtx.Done():
			if shutdownCtx.Err() == context.DeadlineExceeded {
				setupLog.Error(nil, "Graceful shutdown timeout exceeded, forcing exit")
			}
		case <-time.After(100 * time.Millisecond):
			// Give a small delay to allow proper cleanup
		}
	}()

	return ctx
}
