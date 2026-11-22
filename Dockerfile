# syntax=docker/dockerfile:1
# One Dockerfile for every service image: docker build --build-arg BINARY=fleetwire-api .
# Base images are pinned to their multi-arch index digests so a rebuilt tag
# cannot change what is built; Dependabot proposes digest updates.
FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
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
