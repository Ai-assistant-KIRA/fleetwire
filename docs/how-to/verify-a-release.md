# Verify a release

The release workflow signs the checksum file and every container image with
[cosign](https://docs.sigstore.dev/) keyless signing. The archives and the
agent snaps are covered by the signed checksum file. A software bill of
materials (SPDX, from Syft) is attached for every archive and snap.

You need cosign 3 or later.

## Binaries

```console
$ VERSION=v0.2.0
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

Check the archives only after the signature check succeeds. In releases
after v0.1.1 the checksum file also lists the agent snaps (v0.1.1
attached its snaps by hand, without checksums), so they are checked the
same way:

```console
$ curl -fLO "$BASE/fleetwire-agent_${VERSION}_amd64.snap"
$ sha256sum --ignore-missing -c checksums.txt
$ sudo snap install --dangerous "./fleetwire-agent_${VERSION}_amd64.snap"
```

`--dangerous` is still needed: the snap is not in the Snap Store, so
snapd has no store signature for it; the checksum file's signature takes
that role.

## Container images

Image tags have no leading `v`:

```console
$ cosign verify "ghcr.io/ai-assistant-kira/fleetwire-api:${VERSION#v}" \
    --certificate-identity "https://github.com/Ai-assistant-KIRA/fleetwire/.github/workflows/release.yml@refs/tags/$VERSION" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The same command works for `fleetwire-bridge` and `fleetwire-ingest`.
