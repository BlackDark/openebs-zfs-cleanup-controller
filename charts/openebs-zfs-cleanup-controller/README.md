# OpenEBS ZFS Cleanup Controller

A Helm chart for deploying the OpenEBS ZFS Volume Cleanup Controller.

## Description

This chart deploys the OpenEBS ZFS Cleanup Controller, which automatically identifies and cleans up orphaned ZFS volumes in Kubernetes clusters using OpenEBS ZFS LocalPV.

## Features

- **Mode Selection**: Choose between continuous reconciliation (Deployment) or scheduled cleanup (CronJob)
- **Continuous Reconciliation**: Monitors and cleans up orphaned ZFS volumes in real-time
- **Scheduled Cleanup**: Periodic cleanup tasks via CronJob
- **Configurable**: Extensive configuration options via ConfigMap
- **Metrics**: Prometheus-compatible metrics endpoint (Deployment mode only)
- **Health Checks**: Liveness and readiness probes (Deployment mode only)
- **RBAC**: Proper Kubernetes RBAC configuration
- **Dry Run**: Safe testing mode that doesn't delete resources

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- OpenEBS ZFS LocalPV installed in the cluster

## Installing the Chart

```bash
# Add the repository (if applicable)
# helm repo add openebs https://openebs.github.io/charts
# helm repo update

# Install the chart
helm install zfs-cleanup ./charts/openebs-zfs-cleanup-controller

# Install with custom values
helm install zfs-cleanup ./charts/openebs-zfs-cleanup-controller -f my-values.yaml
```

## Configuration

The following table lists the configurable parameters of the chart and their default values.

| Parameter                        | Description                                       | Default                                            |
| -------------------------------- | ------------------------------------------------- | -------------------------------------------------- |
| `mode`                           | Deployment mode: `deployment` or `cronjob`        | `deployment`                                       |
| `replicaCount`                   | Number of controller replicas                     | `1`                                                |
| `image.repository`               | Controller image repository                       | `ghcr.io/blackdark/openebs-zfs-cleanup-controller` |
| `image.tag`                      | Controller image tag (overrides Chart.AppVersion) | `Chart.AppVersion` (1.0.0)                         |
| `namespace`                      | Namespace to deploy into                          | `openebs`                                          |
| `config.dryRun`                  | Enable dry-run mode (no actual deletions)         | `true`                                             |
| `config.reconcileInterval`       | How often to reconcile                            | `60s`                                              |
| `config.maxConcurrentReconciles` | Max concurrent reconciles                         | `2`                                                |
| `cronJob.schedule`               | Cron schedule for cleanup                         | `"0 2 * * *"`                                      |
| `rbac.create`                    | Create RBAC resources                             | `true`                                             |

## Example Values

```yaml
# Deployment mode (default) - continuous reconciliation
mode: deployment

# Disable dry-run mode for production
config:
  dryRun: false
  reconcileInterval: 30s
  maxConcurrentReconciles: 5

# Resource limits
resources:
  limits:
    cpu: 100m
    memory: 128Mi
  requests:
    cpu: 50m
    memory: 64Mi
```

```yaml
# CronJob mode - scheduled cleanup
mode: cronjob

# Customize CronJob schedule
cronJob:
  schedule: "0 */6 * * *"  # Every 6 hours

# Disable dry-run mode for production
config:
  dryRun: false
```

```yaml
# Override image tag (applies to both deployment and cronjob modes)
image:
  tag: "v2.1.0"
```

## Monitoring

The controller exposes metrics at port 8080 and health endpoints at port 8081 (Deployment mode only).

### Metrics Endpoint (Deployment Mode)
```bash
kubectl port-forward -n openebs svc/zfs-cleanup-controller 8080:8080
curl http://localhost:8080/metrics
```

### Health Checks (Deployment Mode)
```bash
# Liveness probe
curl http://localhost:8081/healthz

# Readiness probe
curl http://localhost:8081/readyz
```

## RBAC Permissions

The controller requires the following permissions:
- Read/write access to ZFS volumes (`zfs.openebs.io`)
- Read access to PersistentVolumes and PersistentVolumeClaims
- Read access to Namespaces
- Create/patch Events

## Troubleshooting

### Check Controller Logs (Deployment Mode)
```bash
kubectl logs -n openebs deployment/zfs-cleanup-controller
```

### Check CronJob Logs (CronJob Mode)
```bash
kubectl logs -n openebs -l app=zfs-cleanup-controller-cronjob
```

### View Configuration
```bash
kubectl get configmap -n openebs zfs-cleanup-controller-config -o yaml
```

## Uninstalling the Chart

```bash
helm uninstall zfs-cleanup
```

## Development

To modify the chart:
1. Edit templates in `charts/openebs-zfs-cleanup-controller/templates/`
2. Update values in `charts/openebs-zfs-cleanup-controller/values.yaml`
3. Test with `helm template test-release ./charts/openebs-zfs-cleanup-controller`
4. Test both modes: `helm template test-release ./charts/openebs-zfs-cleanup-controller --set mode=deployment` and `helm template test-release ./charts/openebs-zfs-cleanup-controller --set mode=cronjob`
