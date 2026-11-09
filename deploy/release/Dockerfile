# Release images: GoReleaser has already built the static binary for the
# target platform; this only places it on distroless.
FROM gcr.io/distroless/static-debian12:nonroot
ARG BINARY
ARG TARGETPLATFORM
COPY ${TARGETPLATFORM}/${BINARY} /usr/local/bin/service
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/service"]
