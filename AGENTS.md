# openebs-zfs-cleanup-controller

Kubernetes controller that removes orphaned ZFSVolume CRDs after their PersistentVolumes disappear.

## Verify

`mise run check`: gofmt, vet, golangci-lint, all tests, and Helm lint.

CI uses one required `check` gate plus actionlint, zizmor, and strict Renovate validation. Keep the separate image, chart, and manual release workflows.

## Stack

Follows https://github.com/BlackDark/agent-presets/blob/main/stack/core.md.

## Map

- `cmd/`: controller entrypoint.
- `internal/checker/`: orphan detection.
- `pkg/controller/`, `pkg/manager/`: reconciliation and lifecycle.
- `pkg/apis/`: API types; generated deepcopy code is not hand-edited.
- `charts/openebs-zfs-cleanup-controller/`: Helm deployment.

## Git

- Conventional Commits, one topic per PR.
- Squash unless commits need separate review.
- Force-push only with `--force-with-lease` on own branches.

## Boundaries

- Tools come from `mise.toml`; no global installs.
- Do not create tags, publish charts/images, or run the manual release workflow from a local session.
