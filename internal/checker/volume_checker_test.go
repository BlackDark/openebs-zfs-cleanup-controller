package checker

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
)

func TestVolumeChecker_IsOrphaned(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		zfsVolume      *zfsv1.ZFSVolume
		existingPVs    []corev1.PersistentVolume
		existingPVCs   []corev1.PersistentVolumeClaim
		expectedResult bool
		expectError    bool
	}{
		{
			name: "orphaned - no related PV",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "not orphaned - PV and PVC both exist",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						ClaimRef: &corev1.ObjectReference{
							Name:      "test-pvc",
							Namespace: "default",
						},
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			expectedResult: false,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVs {
				objects = append(objects, &tt.existingPVs[i])
			}
			for i := range tt.existingPVCs {
				objects = append(objects, &tt.existingPVCs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.IsOrphaned(context.TODO(), tt.zfsVolume)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expectedResult {
				t.Errorf("Expected result %v, got %v", tt.expectedResult, result)
			}
		})
	}
}
func TestVolumeChecker_IsOrphaned_EdgeCases(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		zfsVolume      *zfsv1.ZFSVolume
		existingPVs    []corev1.PersistentVolume
		existingPVCs   []corev1.PersistentVolumeClaim
		expectedResult bool
		expectError    bool
	}{
		{
			name: "orphaned - PV exists but no PVC",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						ClaimRef: &corev1.ObjectReference{
							Name:      "test-pvc",
							Namespace: "default",
						},
					},
				},
			},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "orphaned - PV exists but no claimRef",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						// No ClaimRef
					},
				},
			},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "orphaned - multiple PVs but none match",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-other1",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "other-volume-1",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-other2",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "other-volume-2",
							},
						},
					},
				},
			},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedResult: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVs {
				objects = append(objects, &tt.existingPVs[i])
			}
			for i := range tt.existingPVCs {
				objects = append(objects, &tt.existingPVCs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.IsOrphaned(context.TODO(), tt.zfsVolume)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expectedResult {
				t.Errorf("Expected result %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

func TestVolumeChecker_FindRelatedPV(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	tests := []struct {
		name        string
		zfsVolume   *zfsv1.ZFSVolume
		existingPVs []corev1.PersistentVolume
		expectedPV  *corev1.PersistentVolume
		expectError bool
	}{
		{
			name: "find matching PV by volumeHandle",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-other",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "other-volume",
							},
						},
					},
				},
			},
			expectedPV: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
			},
			expectError: false,
		},
		{
			name: "no matching PV found",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-other",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "other-volume",
							},
						},
					},
				},
			},
			expectedPV:  nil,
			expectError: false,
		},
		{
			name: "PV without CSI spec",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-no-csi",
					},
					Spec: corev1.PersistentVolumeSpec{
						// No CSI spec
					},
				},
			},
			expectedPV:  nil,
			expectError: false,
		},
		{
			name: "empty PV list",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-12345678-1234-1234-1234-123456789012",
					Namespace: "openebs",
				},
			},
			existingPVs: []corev1.PersistentVolume{},
			expectedPV:  nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVs {
				objects = append(objects, &tt.existingPVs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.FindRelatedPV(context.TODO(), tt.zfsVolume)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if tt.expectedPV == nil && result != nil {
				t.Errorf("Expected nil PV but got %v", result)
			}
			if tt.expectedPV != nil && result == nil {
				t.Errorf("Expected PV %v but got nil", tt.expectedPV.Name)
			}
			if tt.expectedPV != nil && result != nil {
				if result.Name != tt.expectedPV.Name {
					t.Errorf("Expected PV name %v, got %v", tt.expectedPV.Name, result.Name)
				}
			}
		})
	}
}

func TestVolumeChecker_FindRelatedPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name         string
		pv           *corev1.PersistentVolume
		existingPVCs []corev1.PersistentVolumeClaim
		expectedPVC  *corev1.PersistentVolumeClaim
		expectError  bool
	}{
		{
			name: "find matching PVC by claimRef",
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "other-pvc",
						Namespace: "default",
					},
				},
			},
			expectedPVC: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
				},
			},
			expectError: false,
		},
		{
			name: "PV with no claimRef",
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
				Spec: corev1.PersistentVolumeSpec{
					// No ClaimRef
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{},
			expectedPVC:  nil,
			expectError:  false,
		},
		{
			name: "PVC referenced by PV not found",
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Name:      "missing-pvc",
						Namespace: "default",
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "other-pvc",
						Namespace: "default",
					},
				},
			},
			expectedPVC: nil,
			expectError: false,
		},
		{
			name: "PVC in different namespace",
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "kube-system",
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "kube-system",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			expectedPVC: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "kube-system",
				},
			},
			expectError: false,
		},
		{
			name: "empty PVC list",
			pv: &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-test",
				},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{},
			expectedPVC:  nil,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVCs {
				objects = append(objects, &tt.existingPVCs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.FindRelatedPVC(context.TODO(), tt.pv)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if tt.expectedPVC == nil && result != nil {
				t.Errorf("Expected nil PVC but got %v", result)
			}
			if tt.expectedPVC != nil && result == nil {
				t.Errorf("Expected PVC %v/%v but got nil", tt.expectedPVC.Namespace, tt.expectedPVC.Name)
			}
			if tt.expectedPVC != nil && result != nil {
				if result.Name != tt.expectedPVC.Name || result.Namespace != tt.expectedPVC.Namespace {
					t.Errorf("Expected PVC %v/%v, got %v/%v", tt.expectedPVC.Namespace, tt.expectedPVC.Name, result.Namespace, result.Name)
				}
			}
		})
	}
}

func TestVolumeChecker_ValidateForDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	now := metav1.Now()
	oldTime := metav1.NewTime(now.Add(-10 * time.Minute))
	recentTime := metav1.NewTime(now.Add(-2 * time.Minute))

	tests := []struct {
		name           string
		zfsVolume      *zfsv1.ZFSVolume
		existingPVs    []corev1.PersistentVolume
		existingPVCs   []corev1.PersistentVolumeClaim
		expectedSafe   bool
		expectedReason string
		expectError    bool
	}{
		{
			name: "safe to delete - orphaned and old volume",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   true,
			expectedReason: "All safety checks passed",
			expectError:    false,
		},
		{
			name: "safe to delete - orphaned volume with zfs.openebs.io/finalizer",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"zfs.openebs.io/finalizer"},
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   true,
			expectedReason: "All safety checks passed",
			expectError:    false,
		},
		{
			name: "unsafe - has other finalizers",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"other.finalizer/test"},
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   false,
			expectedReason: "ZFSVolume has finalizers that must be handled first",
			expectError:    false,
		},
		{
			name: "unsafe - has zfs finalizer but not orphaned",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"zfs.openebs.io/finalizer"},
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						ClaimRef: &corev1.ObjectReference{
							Name:      "test-pvc",
							Namespace: "default",
						},
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			expectedSafe:   false,
			expectedReason: "ZFSVolume is not orphaned",
			expectError:    false,
		},
		{
			name: "unsafe - being deleted",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					DeletionTimestamp: &now,
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   false,
			expectedReason: "ZFSVolume is already being deleted",
			expectError:    false,
		},
		{
			name: "unsafe - too new",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: recentTime,
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   false,
			expectedReason: "ZFSVolume is too new (created within last 5 minutes)",
			expectError:    false,
		},
		{
			name: "unsafe - preserve annotation",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Annotations: map[string]string{
						"zfs.openebs.io/preserve": "true",
					},
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   false,
			expectedReason: "ZFSVolume has preserve annotation set to true",
			expectError:    false,
		},
		{
			name: "unsafe - not orphaned",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						ClaimRef: &corev1.ObjectReference{
							Name:      "test-pvc",
							Namespace: "default",
						},
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			expectedSafe:   false,
			expectedReason: "ZFSVolume is not orphaned",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVs {
				objects = append(objects, &tt.existingPVs[i])
			}
			for i := range tt.existingPVCs {
				objects = append(objects, &tt.existingPVCs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.ValidateForDeletion(context.TODO(), tt.zfsVolume)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result.IsSafe != tt.expectedSafe {
				t.Errorf("Expected IsSafe %v, got %v", tt.expectedSafe, result.IsSafe)
			}
			if result.Reason != tt.expectedReason {
				t.Errorf("Expected reason %q, got %q", tt.expectedReason, result.Reason)
			}
		})
	}
}

func TestVolumeChecker_DryRunMode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	// Test dry-run mode
	dryRunChecker := NewVolumeChecker(fakeClient, logr.Discard(), true)
	if !dryRunChecker.IsDryRun() {
		t.Errorf("Expected dry-run mode to be true")
	}

	// Test normal mode
	normalChecker := NewVolumeChecker(fakeClient, logr.Discard(), false)
	if normalChecker.IsDryRun() {
		t.Errorf("Expected dry-run mode to be false")
	}
}

func TestVolumeChecker_LogDeletionAction(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-12345678-1234-1234-1234-123456789012",
			Namespace:         "openebs",
			CreationTimestamp: oldTime,
		},
	}

	safeValidation := &ValidationResult{
		IsSafe: true,
		Reason: "All safety checks passed",
	}

	unsafeValidation := &ValidationResult{
		IsSafe:           false,
		Reason:           "ZFSVolume has finalizers",
		ValidationErrors: []string{"finalizers present"},
	}

	// Test dry-run mode logging (should not cause errors)
	dryRunChecker := NewVolumeChecker(fakeClient, logr.Discard(), true)
	dryRunChecker.LogDeletionAction(zfsVolume, safeValidation)
	dryRunChecker.LogDeletionAction(zfsVolume, unsafeValidation)

	// Test normal mode logging (should not cause errors)
	normalChecker := NewVolumeChecker(fakeClient, logr.Discard(), false)
	normalChecker.LogDeletionAction(zfsVolume, safeValidation)
	normalChecker.LogDeletionAction(zfsVolume, unsafeValidation)
}

func TestVolumeChecker_ValidationResult_MultipleErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	now := metav1.Now()
	recentTime := metav1.NewTime(now.Add(-2 * time.Minute))

	// ZFSVolume with multiple validation issues
	zfsVolume := &zfsv1.ZFSVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-12345678-1234-1234-1234-123456789012",
			Namespace:         "openebs",
			CreationTimestamp: recentTime,
			DeletionTimestamp: &now,
			Finalizers:        []string{"zfs.openebs.io/finalizer"},
			Annotations: map[string]string{
				"zfs.openebs.io/preserve": "true",
			},
		},
	}

	checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
	result, err := checker.ValidateForDeletion(context.TODO(), zfsVolume)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.IsSafe {
		t.Errorf("Expected validation to fail with multiple issues")
	}
	if len(result.ValidationErrors) == 0 {
		t.Errorf("Expected validation errors to be populated")
	}

	// Should have multiple validation errors
	// Note: zfs.openebs.io/finalizer is ignored for orphaned volumes, so we expect 3 errors:
	// deletion timestamp, too new, preserve annotation
	expectedErrors := 3
	if len(result.ValidationErrors) != expectedErrors {
		t.Errorf("Expected %d validation errors, got %d: %v", expectedErrors, len(result.ValidationErrors), result.ValidationErrors)
	}
}
func TestVolumeChecker_ValidateForDeletion_MixedFinalizers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = zfsv1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))

	tests := []struct {
		name           string
		zfsVolume      *zfsv1.ZFSVolume
		existingPVs    []corev1.PersistentVolume
		existingPVCs   []corev1.PersistentVolumeClaim
		expectedSafe   bool
		expectedReason string
		expectError    bool
	}{
		{
			name: "safe - only zfs finalizer on orphaned volume",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"zfs.openebs.io/finalizer"},
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   true,
			expectedReason: "All safety checks passed",
			expectError:    false,
		},
		{
			name: "unsafe - mixed finalizers on orphaned volume",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"zfs.openebs.io/finalizer", "other.finalizer/test"},
				},
			},
			existingPVs:    []corev1.PersistentVolume{},
			existingPVCs:   []corev1.PersistentVolumeClaim{},
			expectedSafe:   false,
			expectedReason: "ZFSVolume has finalizers that must be handled first",
			expectError:    false,
		},
		{
			name: "unsafe - zfs finalizer on non-orphaned volume",
			zfsVolume: &zfsv1.ZFSVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pvc-12345678-1234-1234-1234-123456789012",
					Namespace:         "openebs",
					CreationTimestamp: oldTime,
					Finalizers:        []string{"zfs.openebs.io/finalizer"},
				},
			},
			existingPVs: []corev1.PersistentVolume{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-test",
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeHandle: "pvc-12345678-1234-1234-1234-123456789012",
							},
						},
						ClaimRef: &corev1.ObjectReference{
							Name:      "test-pvc",
							Namespace: "default",
						},
					},
				},
			},
			existingPVCs: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
			},
			expectedSafe:   false,
			expectedReason: "ZFSVolume is not orphaned",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{}
			for i := range tt.existingPVs {
				objects = append(objects, &tt.existingPVs[i])
			}
			for i := range tt.existingPVCs {
				objects = append(objects, &tt.existingPVCs[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			checker := NewVolumeChecker(fakeClient, logr.Discard(), false)
			result, err := checker.ValidateForDeletion(context.TODO(), tt.zfsVolume)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result.IsSafe != tt.expectedSafe {
				t.Errorf("Expected IsSafe %v, got %v", tt.expectedSafe, result.IsSafe)
			}
			if result.Reason != tt.expectedReason {
				t.Errorf("Expected reason %q, got %q", tt.expectedReason, result.Reason)
			}
		})
	}
}
