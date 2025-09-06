package manager

import (
	"context"
	"testing"
	"time"

	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"k8s.io/client-go/rest"
)

func TestNewControllerManager(t *testing.T) {
	tests := []struct {
		name       string
		config     *config.Config
		restConfig *rest.Config
		wantErr    bool
	}{
		{
			name: "valid config should succeed",
			config: &config.Config{
				MetricsPort:          8080,
				ProbePort:            8081,
				EnableLeaderElection: false,
			},
			restConfig: &rest.Config{
				Host: "https://localhost:8443",
			},
			wantErr: false,
		},
		{
			name: "custom ports should succeed",
			config: &config.Config{
				MetricsPort:          0,     // Use random port
				ProbePort:            0,     // Use random port
				EnableLeaderElection: false, // Disable for test
			},
			restConfig: &rest.Config{
				Host: "https://localhost:8443",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm, err := NewControllerManager(tt.config, tt.restConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewControllerManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if cm == nil {
					t.Error("NewControllerManager() returned nil without error")
				} else {
					if cm.manager == nil {
						t.Error("NewControllerManager() returned manager with nil internal manager")
					}
					if cm.config != tt.config {
						t.Error("NewControllerManager() config not properly set")
					}
				}
			}
		})
	}
}

func TestControllerManager_GetManager(t *testing.T) {
	cfg := &config.Config{
		MetricsPort:          0, // Use random port
		ProbePort:            0, // Use random port
		EnableLeaderElection: false,
	}
	restConfig := &rest.Config{
		Host: "https://localhost:8443",
	}

	cm, err := NewControllerManager(cfg, restConfig)
	if err != nil {
		t.Fatalf("Failed to create controller manager: %v", err)
	}

	mgr := cm.GetManager()
	if mgr == nil {
		t.Error("GetManager() should not return nil")
	}
}

func TestControllerManager_GetClient(t *testing.T) {
	cfg := &config.Config{
		MetricsPort:          0, // Use random port
		ProbePort:            0, // Use random port
		EnableLeaderElection: false,
	}
	restConfig := &rest.Config{
		Host: "https://localhost:8443",
	}

	cm, err := NewControllerManager(cfg, restConfig)
	if err != nil {
		t.Fatalf("Failed to create controller manager: %v", err)
	}

	client := cm.GetClient()
	if client == nil {
		t.Error("GetClient() should not return nil")
	}
}

// TestControllerManager_Start tests the start functionality with a timeout
// This test will timeout quickly since we don't have a real cluster
func TestControllerManager_Start(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping start test in short mode")
	}

	cfg := &config.Config{
		MetricsPort:          0, // Use random port
		ProbePort:            0, // Use random port
		EnableLeaderElection: false,
	}
	restConfig := &rest.Config{
		Host: "https://localhost:8443",
	}

	cm, err := NewControllerManager(cfg, restConfig)
	if err != nil {
		t.Fatalf("Failed to create controller manager: %v", err)
	}

	// Create a context with a short timeout since this will fail to connect
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// This should fail because we don't have a real cluster
	err = cm.Start(ctx)
	if err == nil {
		t.Error("Start() should fail without a real cluster connection")
	}

	// Verify the error is related to connection issues
	if err != nil && err.Error() == "" {
		t.Error("Start() should return a meaningful error message")
	}
}
