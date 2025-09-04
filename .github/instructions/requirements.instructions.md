# Requirements Document

## Introduction

This feature implements an automated cleanup controller for orphaned ZFSVolume Custom Resource Definitions (CRDs) in Kubernetes clusters using OpenEBS with ZFS storage. The controller identifies ZFSVolumes that are no longer associated with any PersistentVolume (PV) or PersistentVolumeClaim (PVC) and safely removes them to prevent resource accumulation and maintain cluster hygiene.

The controller addresses the gap in OpenEBS ZFS CSI driver behavior where ZFSVolume CRDs remain after manual PV deletion when using the "Retain" reclaim policy, leading to orphaned resources that consume cluster resources and create management overhead.

## Requirements

### Requirement 1

**User Story:** As a Kubernetes cluster administrator, I want an automated system to identify and clean up orphaned ZFSVolume CRDs, so that my cluster remains clean and doesn't accumulate unused storage resources.

#### Acceptance Criteria

1. WHEN the controller runs THEN it SHALL enumerate all ZFSVolume CRDs in the cluster
2. WHEN a ZFSVolume exists THEN the controller SHALL check if a corresponding PVC exists
3. IF no PVC exists for a ZFSVolume THEN the controller SHALL check if a corresponding PV exists
4. IF neither PVC nor PV exists for a ZFSVolume THEN the controller SHALL mark it as eligible for deletion
5. WHEN deletion is enabled THEN the controller SHALL delete orphaned ZFSVolumes
6. WHEN dry-run mode is enabled THEN the controller SHALL log what would be deleted without performing actual deletion

### Requirement 2

**User Story:** As a cluster administrator, I want the cleanup controller to run on a configurable schedule, so that I can control how frequently orphaned resources are cleaned up based on my cluster's needs.

#### Acceptance Criteria

1. WHEN deployed as a CronJob THEN the controller SHALL execute at user-defined intervals
2. WHEN deployed as a long-running service THEN the controller SHALL have configurable check intervals
3. WHEN configuration changes THEN the controller SHALL apply new settings without requiring pod restart
4. IF the controller fails THEN it SHALL log errors and continue with the next scheduled execution

### Requirement 3

**User Story:** As a security-conscious administrator, I want the controller to have minimal required permissions, so that it follows the principle of least privilege and reduces security risks.

#### Acceptance Criteria

1. WHEN accessing ZFSVolume CRDs THEN the controller SHALL have only list, get, and delete permissions
2. WHEN accessing PVs THEN the controller SHALL have only list and get permissions  
3. WHEN accessing PVCs THEN the controller SHALL have only list and get permissions
4. WHEN running in the cluster THEN the controller SHALL use a dedicated ServiceAccount with minimal RBAC permissions
5. IF additional permissions are needed THEN they SHALL be explicitly documented and justified

### Requirement 4

**User Story:** As an operations engineer, I want comprehensive logging and observability from the cleanup controller, so that I can monitor its behavior and troubleshoot issues effectively.

#### Acceptance Criteria

1. WHEN the controller starts THEN it SHALL log startup configuration and version information
2. WHEN checking ZFSVolumes THEN it SHALL log the number of volumes being processed
3. WHEN orphaned volumes are found THEN it SHALL log detailed information about each orphaned volume
4. WHEN deletions occur THEN it SHALL log successful and failed deletion attempts
5. WHEN errors occur THEN it SHALL log detailed error information with context
6. WHEN dry-run mode is active THEN it SHALL clearly indicate simulated actions in logs

### Requirement 5

**User Story:** As a platform engineer, I want the controller to be resilient and handle edge cases gracefully, so that it doesn't cause cluster instability or data loss.

#### Acceptance Criteria

1. WHEN API calls fail THEN the controller SHALL retry with exponential backoff
2. WHEN a ZFSVolume is being deleted by another process THEN the controller SHALL handle concurrent deletion gracefully
3. WHEN cluster resources are temporarily unavailable THEN the controller SHALL wait and retry rather than failing permanently
4. IF a ZFSVolume has finalizers THEN the controller SHALL respect them and not force deletion
5. WHEN processing large numbers of volumes THEN the controller SHALL implement rate limiting to avoid overwhelming the API server

### Requirement 6

**User Story:** As a DevOps engineer, I want the controller to be easily deployable and configurable in different Kubernetes environments, so that I can integrate it into various cluster setups with minimal effort.

#### Acceptance Criteria

1. WHEN deploying THEN the controller SHALL be available as both CronJob and Deployment manifests
2. WHEN configuring THEN all settings SHALL be available via environment variables or ConfigMap
3. WHEN installing THEN the controller SHALL include all necessary RBAC resources in the deployment manifests
4. WHEN running THEN the controller SHALL work with standard Kubernetes RBAC and security policies
5. WHEN updating THEN the controller SHALL support rolling updates without service interruption
