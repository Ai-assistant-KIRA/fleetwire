# Release images: GoReleaser has already built the static binary for the
# target platform; this only places it on distroless. The base is pinned to
# its multi-arch index digest, as in the root Dockerfile.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG BINARY
ARG TARGETPLATFORM
COPY ${TARGETPLATFORM}/${BINARY} /usr/local/bin/service
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/service"]
