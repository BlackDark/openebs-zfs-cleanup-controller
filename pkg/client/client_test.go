package client

import (
	"context"
	"testing"
	"time"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		config  *rest.Config
		wantErr bool
	}{
		{
			name:    "nil config should return error",
			config:  nil,
			wantErr: true,
		},
		{
			name: "valid config should succeed",
			config: &rest.Config{
				Host: "https://localhost:8443",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	tests := []struct {
		name    string
		config  ClientConfig
		wantErr bool
	}{
		{
			name: "default config should succeed",
			config: ClientConfig{
				Config: &rest.Config{
					Host: "https://localhost:8443",
				},
			},
			wantErr: false,
		},
		{
			name: "custom config should succeed",
			config: ClientConfig{
				Config:               &rest.Config{Host: "https://localhost:8443"},
				MetricsAddr:          ":9090",
				ProbeAddr:            ":9091",
				EnableLeaderElection: false, // Disable leader election for test
				LeaderElectionID:     "test-controller",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := NewManager(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && mgr == nil {
				t.Error("NewManager() returned nil manager without error")
			}
		})
	}
}

func TestValidateConnection(t *testing.T) {
	// Create a fake client with some test data
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-namespace",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(namespace).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ValidateConnection(ctx, fakeClient)
	if err != nil {
		t.Errorf("ValidateConnection() with valid client should not return error, got: %v", err)
	}
}

func TestValidateZFSVolumeCRD(t *testing.T) {
	// Create a fake client with ZFS CRD support
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-volume",
			Namespace: "openebs",
		},
		Spec: zfsv1.ZFSVolumeSpec{
			Capacity:   "10Gi",
			PoolName:   "test-pool",
			VolumeType: "DATASET",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(zfsVolume).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ValidateZFSVolumeCRD(ctx, fakeClient)
	if err != nil {
		t.Errorf("ValidateZFSVolumeCRD() with valid ZFS CRD should not return error, got: %v", err)
	}
}

func TestValidateZFSVolumeCRD_NotAvailable(t *testing.T) {
	// Create a fake client without ZFS CRD support
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme) // Only add core types, not ZFS

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ValidateZFSVolumeCRD(ctx, fakeClient)
	if err == nil {
		t.Error("ValidateZFSVolumeCRD() without ZFS CRD should return error")
	}
}

func TestGetScheme(t *testing.T) {
	scheme := GetScheme()
	if scheme == nil {
		t.Error("GetScheme() should not return nil")
	}

	// Verify that ZFS types are registered
	gvk := zfsv1.GroupVersion.WithKind("ZFSVolume")
	obj, err := scheme.New(gvk)
	if err != nil {
		t.Errorf("Scheme should have ZFSVolume registered, got error: %v", err)
	}

	if _, ok := obj.(*zfsv1.ZFSVolume); !ok {
		t.Error("Scheme should return ZFSVolume instance")
	}
}

// Integration test using envtest (requires kubebuilder test environment)
func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up envtest environment
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{"../../config/crd/bases"}, // Adjust path as needed
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Skip("Could not start test environment, skipping integration test")
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Logf("Failed to stop test environment: %v", err)
		}
	}()

	// Test client creation
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection validation
	if err := ValidateConnection(ctx, c); err != nil {
		t.Errorf("Connection validation failed: %v", err)
	}

	// Test manager creation
	clientConfig := ClientConfig{
		Config:      cfg,
		MetricsAddr: ":0", // Use random port
		ProbeAddr:   ":0", // Use random port
	}

	mgr, err := NewManager(clientConfig)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	if mgr == nil {
		t.Error("Manager should not be nil")
	}
}
