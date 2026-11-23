# Build the manager binary
FROM gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
ARG TARGETARCH
WORKDIR /
COPY bin/manager-linux-${TARGETARCH} /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
