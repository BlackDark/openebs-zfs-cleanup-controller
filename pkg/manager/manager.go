package manager

import (
	"context"
	"fmt"
	"net/http"
	"time"

	clientpkg "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/client"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ControllerManager manages the lifecycle of the ZFS cleanup controller
type ControllerManager struct {
	manager ctrl.Manager
	config  *config.Config
	logger  logr.Logger
}

// NewControllerManager creates a new controller manager instance
func NewControllerManager(cfg *config.Config, restConfig *rest.Config) (*ControllerManager, error) {
	logger := log.Log.WithName("controller-manager")

	clientConfig := clientpkg.ClientConfig{
		Config:               restConfig,
		MetricsAddr:          fmt.Sprintf(":%d", cfg.MetricsPort),
		ProbeAddr:            fmt.Sprintf(":%d", cfg.ProbePort),
		EnableLeaderElection: cfg.EnableLeaderElection,
		LeaderElectionID:     "zfsvolume-cleanup-controller",
	}

	mgr, err := clientpkg.NewManager(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create controller manager: %w", err)
	}

	cm := &ControllerManager{
		manager: mgr,
		config:  cfg,
		logger:  logger,
	}

	// Add custom health checks
	if err := cm.setupHealthChecks(); err != nil {
		return nil, fmt.Errorf("failed to setup health checks: %w", err)
	}

	return cm, nil
}

// setupHealthChecks configures health and readiness checks
func (cm *ControllerManager) setupHealthChecks() error {
	// Add a custom readiness check that validates ZFS CRD availability
	readinessCheck := func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		c := cm.manager.GetClient()
		if err := clientpkg.ValidateConnection(ctx, c); err != nil {
			cm.logger.Error(err, "Kubernetes API connection check failed")
			return err
		}

		if err := clientpkg.ValidateZFSVolumeCRD(ctx, c); err != nil {
			cm.logger.Error(err, "ZFSVolume CRD validation failed")
			return err
		}

		return nil
	}

	// Replace the default readyz check with our custom one
	if err := cm.manager.AddReadyzCheck("zfs-crd-check", readinessCheck); err != nil {
		return fmt.Errorf("failed to add ZFS CRD readiness check: %w", err)
	}

	// Add a liveness check that ensures the manager is responsive
	livenessCheck := healthz.Ping
	if err := cm.manager.AddHealthzCheck("ping", livenessCheck); err != nil {
		return fmt.Errorf("failed to add liveness check: %w", err)
	}

	return nil
}

// Start starts the controller manager
func (cm *ControllerManager) Start(ctx context.Context) error {
	cm.logger.Info("Starting controller manager",
		"metricsAddr", cm.config.MetricsPort,
		"probeAddr", cm.config.ProbePort,
		"leaderElection", cm.config.EnableLeaderElection,
	)

	// Validate initial connection before starting
	c := cm.manager.GetClient()
	if err := clientpkg.ValidateConnection(ctx, c); err != nil {
		return fmt.Errorf("initial connection validation failed: %w", err)
	}

	if err := clientpkg.ValidateZFSVolumeCRD(ctx, c); err != nil {
		cm.logger.Error(err, "ZFSVolume CRD not available - controller may not function properly")
		// Don't fail startup, but log the warning
	}

	cm.logger.Info("Controller manager validation successful, starting...")

	if err := cm.manager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start controller manager: %w", err)
	}

	return nil
}

// GetManager returns the underlying controller-runtime manager
func (cm *ControllerManager) GetManager() ctrl.Manager {
	return cm.manager
}

// GetClient returns the Kubernetes client
func (cm *ControllerManager) GetClient() client.Client {
	return cm.manager.GetClient()
}
