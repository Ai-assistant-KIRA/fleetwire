# syntax=docker/dockerfile:1
# One Dockerfile for every service image: docker build --build-arg BINARY=fleetwire-api .
# Base images are pinned to their multi-arch index digests so a rebuilt tag
# cannot change what is built; Dependabot proposes digest updates.
FROM golang:1.27.4@sha256:5f1b98fa02fe396ebce2a0227a0e08309e11792915b9dd2e1f71053d7e7e6666 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG BINARY
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    test -n "${BINARY}" && \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false \
      -ldflags "-s -w -X github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo.Version=${VERSION} -X github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo.Commit=${COMMIT}" \
      -o /out/service ./cmd/${BINARY}

# Static binary on distroless: no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/service /usr/local/bin/service
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/service"]
