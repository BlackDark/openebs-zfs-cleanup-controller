# CronJob Mode Tests

This directory contains tests for the CronJob deployment mode of the ZFSVolume Cleanup Controller.

## Test Types

### Unit Tests (`main_test.go`)
These tests run automatically with `go test` and don't require a Kubernetes cluster:
- Timeout parsing validation
- Exit code constants verification

### Integration Tests (`integration_test.go`)
These tests require a real Kubernetes cluster and must be run manually with the `integration` build tag.

## Running Tests

### Unit Tests (Safe - No Cluster Required)
```bash
go test -v ./cmd/cronjob/
```

### Integration Tests (Requires Kubernetes Cluster)
```bash
# Run integration tests manually when you have a cluster available
go test -tags=integration -v ./cmd/cronjob/

# Or run specific integration tests
go test -tags=integration -v ./cmd/cronjob/ -run TestCronJobIntegrationExecution
go test -tags=integration -v ./cmd/cronjob/ -run TestCronJobIntegrationSignalHandling
go test -tags=integration -v ./cmd/cronjob/ -run TestCronJobIntegrationConfigurationLoading
```

## Integration Test Coverage

The integration tests verify:
1. **Execution Flow**: Tests successful dry-run execution, custom timeout handling, and invalid configuration handling
2. **Signal Handling**: Tests SIGINT and SIGTERM signal handling with proper exit codes
3. **Configuration Loading**: Tests valid and invalid configuration scenarios

## Safety Notes

- Unit tests are safe to run automatically as part of CI/CD
- Integration tests will connect to your current Kubernetes context and should only be run manually
- Integration tests use `DRY_RUN=true` by default to avoid making actual changes to your cluster
- All integration tests have timeouts to prevent hanging
