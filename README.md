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

### Example Environment File

Create a `.env` file with the following content:

```
DRY_RUN=true
RECONCILE_INTERVAL=30m
MAX_CONCURRENT_RECONCILES=2
NAMESPACE_FILTER="default"
LOG_LEVEL=debug
```

### Example ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
	name: zfsvolume-cleanup-config
	namespace: kube-system
data:
	DRY_RUN: "false"
	RECONCILE_INTERVAL: "1h"
	MAX_CONCURRENT_RECONCILES: "1"
	LOG_LEVEL: "info"
```

## Deployment Scenarios

### CronJob Example

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
	name: zfsvolume-cleanup-cronjob
spec:
	schedule: "0 2 * * *"
	jobTemplate:
		spec:
			template:
				spec:
					containers:
					- name: cleanup
						image: your-repo/zfsvolume-cleanup:latest
						envFrom:
						- configMapRef:
								name: zfsvolume-cleanup-config
					restartPolicy: OnFailure
```

### Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
	name: zfsvolume-cleanup-controller
spec:
	replicas: 1
	selector:
		matchLabels:
			app: zfsvolume-cleanup
	template:
		metadata:
			labels:
				app: zfsvolume-cleanup
		spec:
			containers:
			- name: controller
				image: your-repo/zfsvolume-cleanup:latest
				envFrom:
				- configMapRef:
						name: zfsvolume-cleanup-config
			restartPolicy: Always
```

## Configuration Validation & Error Messages

On startup, the controller validates all configuration fields. If a value is invalid, a clear error message is printed and the process exits. Example:

```
invalid configuration for RECONCILE_INTERVAL=0s: must be greater than 0
```

Refer to the [Configuration section](#configuration) for valid ranges and options.

## Troubleshooting & Common Issues

- **Controller fails to start:**
	- Check logs for configuration validation errors.
	- Ensure all required environment variables are set and valid.
- **No orphaned volumes detected:**
	- Verify that ZFSVolume, PV, and PVC resources exist and are not associated.
	- Check label and namespace filters.
- **Permission errors:**
	- Ensure RBAC roles and ServiceAccount are correctly configured.
	- See `deploy/rbac.yaml` for minimal required permissions.
- **API rate limiting:**
	- Adjust `API_RATE_LIMIT` and `API_BURST` settings if you see throttling errors.
- **Metrics not exposed:**
	- Confirm `METRICS_PORT` is set and port is open in your deployment.


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
