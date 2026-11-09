SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO      ?= go
PKG     := github.com/Ai-assistant-KIRA/fleetwire
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X $(PKG)/internal/buildinfo.Version=$(VERSION) \
	-X $(PKG)/internal/buildinfo.Commit=$(COMMIT) \
	-X $(PKG)/internal/buildinfo.Date=$(DATE)
BINARIES := $(notdir $(wildcard cmd/*))
FUZZTIME ?= 30s

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## Format Go code
	gofmt -s -w $$(git ls-files '*.go' 2>/dev/null || find . -name '*.go' -not -path './internal/gen/*')

.PHONY: lint
lint: ## Run gofmt check, go vet and golangci-lint
	@unformatted=$$(gofmt -s -l $$(find . -name '*.go' -not -path './internal/gen/*')); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...
	golangci-lint run ./...

.PHONY: test
test: ## Run unit tests (integration tests skip when Docker is missing)
	$(GO) test -count=1 ./...

.PHONY: test-race
test-race: ## Run all tests with the race detector
	$(GO) test -count=1 -race ./...

.PHONY: fuzz-smoke
fuzz-smoke: ## Run every fuzz target for FUZZTIME (default 30s)
	./scripts/fuzz-smoke.sh $(FUZZTIME)

.PHONY: generate
generate: ## Regenerate protobuf code
	buf generate
	gofmt -s -w internal/gen

.PHONY: check-generated
check-generated: generate ## Fail if generated code is out of date
	@git diff --exit-code -- internal/gen || (echo "generated code is stale: run make generate" && exit 1)

.PHONY: build
build: ## Build every binary into bin/
	@mkdir -p bin
	@for b in $(BINARIES); do \
		echo "go build $$b"; \
		CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b; \
	done
	@[ -n "$(BINARIES)" ] || $(GO) build ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: clean
clean: ## Remove build output
	rm -rf bin dist
