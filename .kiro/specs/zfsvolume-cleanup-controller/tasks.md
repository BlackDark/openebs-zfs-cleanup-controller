# Implementation Plan

- [x] 1. Initialize Go project structure and dependencies
  - Create Go module with controller-runtime, client-go, and testing dependencies
  - Set up project directory structure following Go conventions
  - Create Dockerfile and .gitignore for containerized deployment
  - _Requirements: 6.1, 6.4_

- [x] 2. Implement core configuration management
  - Create Config struct with all required configuration fields
  - Implement environment variable parsing with validation and defaults
  - Write unit tests for configuration parsing and validation logic
  - _Requirements: 2.3, 6.2_

- [x] 3. Set up Kubernetes client and CRD definitions
  - Define ZFSVolume CRD Go structs matching OpenEBS schema
  - Initialize Kubernetes client with proper scheme registration
  - Create basic controller manager setup with health checks
  - Write tests for client initialization and CRD handling
  - _Requirements: 1.1, 3.4, 6.4_

- [x] 4. Implement volume relationship detection logic
  - Create VolumeChecker component to identify ZFSVolume-PV-PVC relationships
  - Implement methods to find related PV from ZFSVolume via CSI volumeHandle
  - Implement methods to find related PVC from PV via claimRef
  - Write comprehensive unit tests for relationship detection edge cases
  - _Requirements: 1.2, 1.3, 5.4_

- [x] 5. Build orphan detection and validation system
  - Implement IsOrphaned method combining PV and PVC checks
  - Add safety validation logic to prevent accidental deletions
  - Create dry-run mode functionality for logging without deletion
  - Write unit tests covering all orphan detection scenarios
  - _Requirements: 1.4, 1.6, 5.4_

- [x] 6. Implement ZFSVolume reconciler core logic
  - Create ZFSVolumeReconciler struct with controller-runtime integration
  - Implement main Reconcile method with error handling and logging
  - Add findOrphanedZFSVolumes method to scan and identify candidates
  - Write unit tests for reconciler initialization and basic operations
  - _Requirements: 1.1, 4.2, 4.3_

- [x] 7. Add deletion logic with retry mechanisms
  - Implement deleteZFSVolume method with proper error handling
  - Add exponential backoff retry logic for transient failures
  - Handle concurrent deletion scenarios and finalizer respect
  - Write unit tests for deletion logic and retry behavior
  - _Requirements: 1.5, 5.1, 5.2, 5.3_

- [x] 8. Implement comprehensive logging system
  - Add structured logging throughout all components using logr
  - Implement detailed logging for startup, processing, and deletion events
  - Add dry-run specific logging with clear action indicators
  - Write tests to verify logging output and levels
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 9. Create Prometheus metrics collection
  - Implement MetricsCollector with counters, histograms, and gauges
  - Add metrics for orphaned volumes, deletion attempts, and reconciliation duration
  - Integrate metrics collection into reconciler operations
  - Write unit tests for metrics accuracy and collection
  - _Requirements: 4.1, 4.4_

- [x] 10. Build rate limiting and concurrency controls
  - Implement rate limiting to prevent API server overload
  - Add configurable max concurrent reconciles setting
  - Create proper context handling for cancellation and timeouts
  - Write tests for rate limiting behavior under load
  - _Requirements: 5.5, 2.3_

- [x] 11. Create CronJob deployment mode
  - Implement main function variant for one-time execution
  - Add proper exit codes and completion logging for CronJob pattern
  - Create signal handling for graceful shutdown
  - Write integration tests for CronJob execution flow
  - _Requirements: 2.1, 6.1_

- [x] 12. Create long-running service deployment mode
  - Implement controller manager with configurable reconcile intervals
  - Add health check endpoints for liveness and readiness probes
  - Implement graceful shutdown handling with proper cleanup
  - Write integration tests for service mode operation
  - _Requirements: 2.2, 6.4_

- [x] 13. Generate Kubernetes deployment manifests
  - Create RBAC manifests with minimal required permissions
  - Generate CronJob and Deployment YAML templates
  - Create ConfigMap template for configuration management
  - Add ServiceAccount and ClusterRoleBinding definitions
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 6.1, 6.3_

- [ ] 14. Implement comprehensive error handling
  - Add proper error categorization (transient vs permanent)
  - Implement graceful degradation for missing CRDs or permissions
  - Add error aggregation and reporting in reconciliation results
  - Write unit tests for all error handling scenarios
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 15. Create integration test suite
  - Set up envtest environment for realistic Kubernetes API testing
  - Create test scenarios with actual ZFSVolume, PV, and PVC resources
  - Test complete reconciliation flows including deletion operations
  - Verify RBAC permissions and ServiceAccount functionality
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 3.1, 3.2, 3.3_

- [ ] 16. Add configuration validation and documentation
  - Implement startup configuration validation with helpful error messages
  - Create comprehensive README with deployment and configuration instructions
  - Add example configuration files and deployment scenarios
  - Document troubleshooting steps and common issues
  - _Requirements: 6.2, 6.3_

- [ ] 17. Implement performance optimizations
  - Add efficient list operations with label selectors and field selectors
  - Implement pagination for large volume sets
  - Add memory usage optimization for processing large numbers of resources
  - Write performance tests to validate scalability improvements
  - _Requirements: 5.5, 6.4_

- [ ] 18. Create end-to-end testing framework
  - Build test harness that creates realistic OpenEBS ZFS scenarios
  - Test complete workflows from volume creation to cleanup
  - Verify metrics accuracy and logging completeness
  - Test both CronJob and Deployment modes in realistic conditions
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2_
