# Verify a release

The release workflow signs the checksum file and every container image with
[cosign](https://docs.sigstore.dev/) keyless signing. The archives are
covered by the signed checksum file. A software bill of materials (SPDX,
from Syft) is attached for every archive.

You need cosign 3 or later.

## Binaries

```console
$ VERSION=v0.1.0
$ BASE=https://github.com/Ai-assistant-KIRA/fleetwire/releases/download/$VERSION
$ curl -fLO "$BASE/checksums.txt" -fLO "$BASE/checksums.txt.sigstore.json"
$ cosign verify-blob \
    --bundle checksums.txt.sigstore.json \
    --certificate-identity "https://github.com/Ai-assistant-KIRA/fleetwire/.github/workflows/release.yml@refs/tags/$VERSION" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    checksums.txt
$ curl -fLO "$BASE/fleetwire_${VERSION#v}_linux_amd64.tar.gz"
$ sha256sum --ignore-missing -c checksums.txt
```

Check the archives only after the signature check succeeds.

## Container images

Image tags have no leading `v`:

```console
$ cosign verify "ghcr.io/ai-assistant-kira/fleetwire-api:${VERSION#v}" \
    --certificate-identity "https://github.com/Ai-assistant-KIRA/fleetwire/.github/workflows/release.yml@refs/tags/$VERSION" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The same command works for `fleetwire-bridge` and `fleetwire-ingest`.
