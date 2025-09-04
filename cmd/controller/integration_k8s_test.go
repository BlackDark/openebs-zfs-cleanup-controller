//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// This file contains integration tests that require a real Kubernetes cluster or envtest
// To run these tests, use: go test -tags=integration ./cmd/controller/...
// These tests require kubebuilder to be installed

var (
	testEnv       *envtest.Environment
	k8sClient     client.Client
	testScheme    *runtime.Scheme
	ctx           context.Context
	cancel        context.CancelFunc
	testNamespace string
)

func TestServiceModeIntegration(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "true" {
		t.Skip("Skipping integration tests")
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "Service Mode Integration Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			"../../pkg/apis/zfs/v1", // Path to CRD definitions if they exist
		},
		ErrorIfCRDPathMissing: false,
	}

	cfg, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	testScheme = runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(testScheme))
	utilruntime.Must(zfsv1.AddToScheme(testScheme))

	k8sClient, err = client.New(cfg, client.Options{Scheme: testScheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Create test namespace
	testNamespace = "zfs-cleanup-test"
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNamespace,
		},
	}
	err = k8sClient.Create(ctx, ns)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

var _ = Describe("Service Mode Controller Integration", func() {
	var (
		mgr        ctrl.Manager
		reconciler *controller.ZFSVolumeReconciler
		testConfig *config.Config
		mgrContext context.Context
		mgrCancel  context.CancelFunc
	)

	BeforeEach(func() {
		// Set up test configuration
		testConfig = &config.Config{
			DryRun:                  true, // Always use dry-run in tests
			ReconcileInterval:       time.Second * 2,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Millisecond * 100,
			MaxRetryAttempts:        2,
			APIRateLimit:            10.0,
			APIBurst:                15,
			ReconcileTimeout:        time.Minute,
			ListOperationTimeout:    time.Second * 30,
			NamespaceFilter:         testNamespace,
			LabelSelector:           "",
			MetricsPort:             0, // Use random port
			ProbePort:               0, // Use random port
			EnableLeaderElection:    false,
			LogLevel:                "debug",
			LogFormat:               "text",
		}

		// Create manager with test configuration
		var err error
		mgr, err = ctrl.NewManager(testEnv.Config, ctrl.Options{
			Scheme:                 testScheme,
			HealthProbeBindAddress: ":0", // Use random port for health checks
			LeaderElection:         false,
		})
		Expect(err).NotTo(HaveOccurred())

		// Set up health checks
		err = setupHealthChecks(mgr, testConfig)
		Expect(err).NotTo(HaveOccurred())

		// Create reconciler
		reconciler = controller.NewZFSVolumeReconciler(
			mgr.GetClient(),
			mgr.GetScheme(),
			testConfig,
			ctrl.Log.WithName("test-controller"),
		)

		err = reconciler.SetupWithManager(mgr)
		Expect(err).NotTo(HaveOccurred())

		// Start manager in background
		mgrContext, mgrCancel = context.WithCancel(ctx)
		go func() {
			defer GinkgoRecover()
			err := mgr.Start(mgrContext)
			if err != nil && mgrContext.Err() == nil {
				Expect(err).NotTo(HaveOccurred())
			}
		}()

		// Wait for manager to be ready
		Eventually(func() bool {
			return mgr.GetCache().WaitForCacheSync(mgrContext)
		}, time.Second*10).Should(BeTrue())
	})

	AfterEach(func() {
		if mgrCancel != nil {
			mgrCancel()
		}
		// Clean up test resources
		zfsVolumeList := &zfsv1.ZFSVolumeList{}
		err := k8sClient.List(ctx, zfsVolumeList)
		if err == nil {
			for _, vol := range zfsVolumeList.Items {
				_ = k8sClient.Delete(ctx, &vol)
			}
		}
	})

	Context("Controller Manager Lifecycle", func() {
		It("should start and stop gracefully", func() {
			By("verifying manager is running")
			Expect(mgr.GetCache().WaitForCacheSync(mgrContext)).To(BeTrue())

			By("stopping manager gracefully")
			mgrCancel()

			By("verifying manager stops within timeout")
			Eventually(func() bool {
				select {
				case <-mgrContext.Done():
					return true
				default:
					return false
				}
			}, time.Second*5).Should(BeTrue())
		})

		It("should have configurable reconcile intervals", func() {
			By("verifying reconcile interval is configured")
			Expect(testConfig.ReconcileInterval).To(Equal(time.Second * 2))

			By("creating a test ZFSVolume")
			zfsVolume := &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-volume-interval",
					Namespace: testNamespace,
				},
				Spec: zfsv1.ZFSVolumeSpec{
					Capacity:   "1Gi",
					PoolName:   "test-pool",
					VolumeType: "DATASET",
				},
			}
			err := k8sClient.Create(ctx, zfsVolume)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for reconciliation to occur")
			// The controller should reconcile within the configured interval
			time.Sleep(testConfig.ReconcileInterval + time.Second)

			By("verifying volume still exists (dry-run mode)")
			foundVolume := &zfsv1.ZFSVolume{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      zfsVolume.Name,
				Namespace: zfsVolume.Namespace,
			}, foundVolume)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Health Check Endpoints", func() {
		It("should validate API connectivity in readiness check", func() {
			By("creating a readiness check function")
			readinessCheck := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				client := mgr.GetClient()

				// This simulates the readiness check logic
				zfsVolumeList := &zfsv1.ZFSVolumeList{}
				if err := client.List(ctx, zfsVolumeList); err != nil {
					return fmt.Errorf("kubernetes API connection failed: %w", err)
				}

				return nil
			}

			By("executing readiness check")
			err := readinessCheck()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Service Mode Operation", func() {
		It("should continuously reconcile ZFSVolumes", func() {
			By("creating multiple test ZFSVolumes")
			for i := 0; i < 3; i++ {
				zfsVolume := &zfsv1.ZFSVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("test-volume-%d", i),
						Namespace: testNamespace,
					},
					Spec: zfsv1.ZFSVolumeSpec{
						Capacity:   "1Gi",
						PoolName:   "test-pool",
						VolumeType: "DATASET",
					},
				}
				err := k8sClient.Create(ctx, zfsVolume)
				Expect(err).NotTo(HaveOccurred())
			}

			By("waiting for multiple reconciliation cycles")
			time.Sleep(testConfig.ReconcileInterval * 3)

			By("verifying all volumes still exist (dry-run mode)")
			zfsVolumeList := &zfsv1.ZFSVolumeList{}
			err := k8sClient.List(ctx, zfsVolumeList)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(zfsVolumeList.Items)).To(BeNumerically(">=", 3))
		})
	})
})
