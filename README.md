# OpenEBS ZFSVolume Cleanup Controller

An automated cleanup controller for orphaned ZFSVolume Custom Resource Definitions (CRDs) in Kubernetes clusters using OpenEBS with ZFS storage.

## Overview

This controller identifies ZFSVolumes that are no longer associated with any PersistentVolume (PV) or PersistentVolumeClaim (PVC) and safely removes them to prevent resource accumulation and maintain cluster hygiene.

## Features

- Automated detection of orphaned ZFSVolume CRDs
- Safe deletion with comprehensive validation
- Support for both CronJob and long-running deployment modes
- Comprehensive logging and Prometheus metrics
- Configurable dry-run mode
- Minimal RBAC permissions following security best practices

## Quick Start

### Prerequisites

- Kubernetes cluster with OpenEBS ZFS CSI driver installed
- Go 1.21+ for development
- Docker for containerized deployment

### Building

```bash
# Build the controller binary
make build

# Build the cronjob binary
make build-cronjob

# Build Docker image
make docker-build
```

### Running

```bash
# Run as controller (long-running service)
make run

# Run as one-time job
make run-cronjob
```

## Configuration

The controller is configured via environment variables:

| Variable                    | Default | Description                                           |
| --------------------------- | ------- | ----------------------------------------------------- |
| `DRY_RUN`                   | `false` | Enable dry-run mode (log only, no deletions)          |
| `RECONCILE_INTERVAL`        | `1h`    | Reconciliation interval for controller mode           |
| `MAX_CONCURRENT_RECONCILES` | `1`     | Maximum concurrent reconciliation operations          |
| `NAMESPACE_FILTER`          | `""`    | Filter to specific namespace (empty = all namespaces) |
| `LOG_LEVEL`                 | `info`  | Log level (debug, info, warn, error)                  |

## Deployment

See the `config/` directory for Kubernetes deployment manifests.

## Development

```bash
# Run tests
make test

# Format code
make fmt

# Run linter
make vet
```

## License

This project is licensed under the Apache License 2.0.
