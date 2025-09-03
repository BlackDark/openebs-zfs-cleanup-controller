package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZFSVolumeSpec defines the desired state of ZFSVolume
type ZFSVolumeSpec struct {
	// Capacity represents the desired size of the underlying volume.
	Capacity string `json:"capacity"`

	// FSType represents the filesystem type
	FSType string `json:"fsType,omitempty"`

	// PoolName represents the pool name where the volume is created
	PoolName string `json:"poolName"`

	// VolumeType represents the type of volume (DATASET or ZVOL)
	VolumeType string `json:"volumeType"`

	// Compression represents the compression algorithm used for this volume
	Compression string `json:"compression,omitempty"`

	// Dedup represents the deduplication setting for this volume
	Dedup string `json:"dedup,omitempty"`

	// Encryption represents the encryption setting for this volume
	Encryption string `json:"encryption,omitempty"`

	// KeyFormat represents the key format for encryption
	KeyFormat string `json:"keyformat,omitempty"`

	// KeyLocation represents the key location for encryption
	KeyLocation string `json:"keylocation,omitempty"`

	// Recordsize represents the recordsize property for the volume
	Recordsize string `json:"recordsize,omitempty"`

	// Volblocksize represents the volblocksize for ZVOL
	Volblocksize string `json:"volblocksize,omitempty"`

	// Shared represents whether the volume is shared
	Shared string `json:"shared,omitempty"`

	// ThinProvision represents whether the volume is thin provisioned
	ThinProvision string `json:"thinProvision,omitempty"`

	// OwnerNodeID represents the node where the volume is provisioned
	OwnerNodeID string `json:"ownerNodeID,omitempty"`
}

// ZFSVolumeStatus defines the observed state of ZFSVolume
type ZFSVolumeStatus struct {
	// State represents the current state of the volume
	State string `json:"state,omitempty"`

	// Error represents any error encountered during volume operations
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=zfsvol

// ZFSVolume is the Schema for the zfsvolumes API
type ZFSVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSVolumeSpec   `json:"spec,omitempty"`
	Status ZFSVolumeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ZFSVolumeList contains a list of ZFSVolume
type ZFSVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZFSVolume `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ZFSVolume{}, &ZFSVolumeList{})
}
