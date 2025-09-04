# Integration Tests

This directory contains both unit tests and integration tests for the ZFS Volume Cleanup Controller service mode.

## Unit Tests

Unit tests can be run without any external dependencies:

```bash
go test ./cmd/controller/... -v
```

These tests cover:
- Configuration validation
- Health check setup
- Graceful shutdown logic
- Service mode features

## Integration Tests

Integration tests require a Kubernetes cluster or kubebuilder's envtest environment. They are tagged with `integration` and are skipped by default.

### Prerequisites

To run integration tests, you need kubebuilder installed:

```bash
# Install kubebuilder
curl -L -o kubebuilder https://go.kubebuilder.io/dl/latest/$(go env GOOS)/$(go env GOARCH)
chmod +x kubebuilder && sudo mv kubebuilder /usr/local/bin/
```

### Running Integration Tests

```bash
# Run integration tests (requires kubebuilder)
go test -tags=integration ./cmd/controller/... -v

# Skip integration tests explicitly
SKIP_INTEGRATION_TESTS=true go test -tags=integration ./cmd/controller/... -v
```

### What Integration Tests Cover

The integration tests (`integration_k8s_test.go`) cover:
- Real Kubernetes API interactions
- Controller manager lifecycle with actual etcd
- Health check endpoints with real HTTP servers
- ZFSVolume CRD operations
- Reconciliation loops with real timing

### CI/CD Considerations

In CI/CD environments where kubebuilder is not available, integration tests will be automatically skipped. Only unit tests will run, ensuring the build pipeline remains stable.

## Test Structure

- `main_test.go` - Unit tests that don't require Kubernetes
- `service_test.go` - Service mode configuration and feature tests
- `integration_k8s_test.go` - Integration tests requiring Kubernetes (tagged with `integration`)

This separation ensures that:
1. Unit tests always pass in any environment
2. Integration tests can be run when the proper infrastructure is available
3. The build pipeline remains stable regardless of external dependencies
