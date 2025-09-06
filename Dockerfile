# Build the manager binary
FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH
WORKDIR /
COPY bin/manager-linux-${TARGETARCH} /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
