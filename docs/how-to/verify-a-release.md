# Verify a release

Every release archive, the checksum file and every container image are
signed with [cosign](https://docs.sigstore.dev/) keyless signing from the
GitHub Actions release workflow. A software bill of materials (SPDX, from
Syft) is attached for every archive.

## Binaries

```console
$ VERSION=v0.1.0
$ BASE=https://github.com/Ai-assistant-KIRA/fleetwire/releases/download/$VERSION
$ curl -fLO "$BASE/checksums.txt" -fLO "$BASE/checksums.txt.sig" -fLO "$BASE/checksums.txt.pem"
$ cosign verify-blob \
    --certificate checksums.txt.pem --signature checksums.txt.sig \
    --certificate-identity-regexp '^https://github.com/Ai-assistant-KIRA/fleetwire/\.github/workflows/release\.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    checksums.txt
$ curl -fLO "$BASE/fleetwire_${VERSION#v}_linux_amd64.tar.gz"
$ sha256sum --ignore-missing -c checksums.txt
```

## Container images

```console
$ cosign verify ghcr.io/ai-assistant-kira/fleetwire-api:$VERSION \
    --certificate-identity-regexp '^https://github.com/Ai-assistant-KIRA/fleetwire/\.github/workflows/release\.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The same command works for `fleetwire-bridge` and `fleetwire-ingest`.
