package client

import (
	"context"
	"fmt"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(zfsv1.AddToScheme(scheme))
}

// ClientConfig holds configuration for the Kubernetes client
type ClientConfig struct {
	// Config is the rest config for connecting to Kubernetes
	Config *rest.Config

	// MetricsAddr is the address the metric endpoint binds to
	MetricsAddr string

	// ProbeAddr is the address the probe endpoint binds to
	ProbeAddr string

	// EnableLeaderElection enables leader election for controller manager
	EnableLeaderElection bool

	// LeaderElectionID is the name of the configmap used for leader election
	LeaderElectionID string
}

// NewClient creates a new Kubernetes client with the ZFS CRD scheme registered
func NewClient(config *rest.Config) (client.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("rest config cannot be nil")
	}

	c, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return c, nil
}

// NewManager creates a new controller manager with proper scheme registration and health checks
func NewManager(config ClientConfig) (ctrl.Manager, error) {
	if config.Config == nil {
		var err error
		config.Config, err = ctrl.GetConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get rest config: %w", err)
		}
	}

	// Set default values
	if config.MetricsAddr == "" {
		config.MetricsAddr = ":8080"
	}
	if config.ProbeAddr == "" {
		config.ProbeAddr = ":8081"
	}
	if config.LeaderElectionID == "" {
		config.LeaderElectionID = "zfsvolume-cleanup-controller"
	}

	mgr, err := ctrl.NewManager(config.Config, ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: config.ProbeAddr,
		LeaderElection:         config.EnableLeaderElection,
		LeaderElectionID:       config.LeaderElectionID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	// Add health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("failed to add healthz check: %w", err)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("failed to add readyz check: %w", err)
	}

	return mgr, nil
}

// ValidateConnection validates that the client can connect to the Kubernetes API
func ValidateConnection(ctx context.Context, c client.Client) error {
	// Try to list namespaces as a basic connectivity test
	namespaceList := &corev1.NamespaceList{}
	if err := c.List(ctx, namespaceList, client.Limit(1)); err != nil {
		return fmt.Errorf("failed to validate connection to Kubernetes API: %w", err)
	}
	return nil
}

// ValidateZFSVolumeCRD validates that the ZFSVolume CRD is available in the cluster
func ValidateZFSVolumeCRD(ctx context.Context, c client.Client) error {
	// Try to list ZFSVolumes to validate CRD availability
	zfsVolumeList := &zfsv1.ZFSVolumeList{}
	if err := c.List(ctx, zfsVolumeList, client.Limit(1)); err != nil {
		return fmt.Errorf("ZFSVolume CRD not available or accessible: %w", err)
	}

	log.FromContext(ctx).Info("ZFSVolume CRD validation successful")
	return nil
}

// GetScheme returns the runtime scheme with all registered types
func GetScheme() *runtime.Scheme {
	return scheme
}
