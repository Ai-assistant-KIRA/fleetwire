# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""Deploy the bundle on MicroK8s and check that a real agent's readings are stored.

Needs the four charms packed and the service images pushed (charms/build.sh
does both and prints the FLEETWIRE_IMAGES value to export), `make build` done
in the repository, Go on the PATH, and a Juju controller on MicroK8s:

    cd charms && ./build.sh && FLEETWIRE_IMAGES=... tox -e integration

The agent runs on the test host and reaches the API and the broker through
`kubectl port-forward`, the way an operator's laptop would.

The deployment is its own test, marked juju_setup, so the data test can run
again against a model that is already up:

    pytest tests/integration --juju-model <prefix> --no-juju-setup --no-juju-teardown
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator

import jubilant
import pytest
import yaml

CHARMS = pathlib.Path(__file__).resolve().parents[2]
REPO = CHARMS.parent
BIN = REPO / "bin"
# For example localhost:32000/fleetwire-{service}:<commit>. There is no
# default: a reused tag would run whatever image the node cached last.
IMAGES = os.environ.get("FLEETWIRE_IMAGES", "")
FLEETWIRE = ("fleetwire-api-k8s", "fleetwire-ingest-k8s", "fleetwire-bridge-k8s", "mosquitto-k8s")
DEPLOY_TIMEOUT = 40 * 60


def run(*args: str | pathlib.Path, cwd: pathlib.Path | None = None) -> str:
    # Only this test's own commands with its own arguments run here.
    cmd = [str(a) for a in args]
    return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd).stdout  # noqa: S603


def files(pki: pathlib.Path, keys: dict[str, str]) -> dict[str, str]:
    """Secret content: each key's value is the text of a PKI file."""
    return {key: (pki / name).read_text() for key, name in keys.items()}


def charm_failed(status: jubilant.Status) -> bool:
    """A hook failed in one of fleetwire's charms.

    Juju also shows "error" while Kubernetes backs off an image pull; the
    kubelet retries those, so they are not failures (the timeout catches a
    pull that never works).
    """
    for app in FLEETWIRE:
        for unit in status.apps[app].units.values():
            ws = unit.workload_status
            if ws.current == "error" and ws.message.startswith("hook failed"):
                return True
    return False


@contextlib.contextmanager
def deployable_bundle(model: str) -> Iterator[pathlib.Path]:
    """The bundle with absolute charm paths and the test images, where Juju can read it.

    A Juju snap cannot read /tmp, and Jubilant copies a bundle file into the
    snap's own directory, which breaks the bundle's relative charm paths. So
    the charms and the bundle are copied there first.
    """
    snap = "/snap/" in (shutil.which("juju") or "")
    base = pathlib.Path("~/snap/juju/common").expanduser() if snap else pathlib.Path("/tmp")  # noqa: S108
    out = base / f"fleetwire-{model}"
    out.mkdir(parents=True, exist_ok=True)
    try:
        bundle = yaml.safe_load((CHARMS / "bundle.yaml").read_text())
        for app in FLEETWIRE:
            spec = bundle["applications"][app]
            charm = CHARMS / spec["charm"]
            shutil.copy(charm, out / charm.name)
            spec["charm"] = str(out / charm.name)
        for app, resource, service in (
            ("fleetwire-api-k8s", "api-image", "api"),
            ("fleetwire-ingest-k8s", "ingest-image", "ingest"),
            ("fleetwire-bridge-k8s", "bridge-image", "bridge"),
        ):
            bundle["applications"][app]["resources"][resource] = IMAGES.format(service=service)
        path = out / "bundle.yaml"
        path.write_text(yaml.safe_dump(bundle))
        yield path
    finally:
        shutil.rmtree(out, ignore_errors=True)


@contextlib.contextmanager
def port_forward(model: str, target: str, local: int, remote: int) -> Iterator[None]:
    proc = subprocess.Popen(  # noqa: S603 (fixed program and arguments)
        ["microk8s", "kubectl", "-n", model, "port-forward", target, f"{local}:{remote}"],  # noqa: S607
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while True:
            with contextlib.suppress(OSError), socket.create_connection(("127.0.0.1", local), timeout=1):
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"port-forward to {target}:{remote} did not come up")
            time.sleep(1)
        yield
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def all_active(status: jubilant.Status) -> bool:
    return jubilant.all_active(status, *FLEETWIRE, "postgresql-k8s", "kafka-k8s")


def model_name(juju: jubilant.Juju) -> str:
    return (juju.model or "").split(":")[-1]


@pytest.mark.juju_setup
def test_deploy(juju: jubilant.Juju, tmp_path: pathlib.Path):
    assert "{service}" in IMAGES, "set FLEETWIRE_IMAGES, for example localhost:32000/fleetwire-{service}:<commit>"
    model = model_name(juju)
    pki = tmp_path / "pki"
    # Server certificates name the in-cluster services (what the charms hand
    # to each other) and localhost (the port-forwards).
    hosts = ["localhost", "127.0.0.1", *(f"{app}.{model}.svc.cluster.local" for app in FLEETWIRE)]
    run("go", "run", "./scripts/dev-ca", "-out", pki, "-hosts", ",".join(hosts), cwd=REPO)

    secrets = {
        "fleetwire-api-k8s": {
            **files(
                pki,
                {
                    "tls-cert": "api.pem",
                    "tls-key": "api.key",
                    "ca-bundle": "ca-bundle.pem",
                    "device-trust": "device-trust.pem",
                    "device-ca-cert": "device-ca.pem",
                    "device-ca-key": "device-ca.key",
                    "service-trust": "service-trust.pem",
                },
            ),
            "keyring-root-key": (pki / "keyring-root.key").read_bytes().hex(),
        },
        "fleetwire-ingest-k8s": files(
            pki, {"tls-cert": "ingest.pem", "tls-key": "ingest.key", "ca-bundle": "ca-bundle.pem"}
        ),
        "fleetwire-bridge-k8s": files(
            pki, {"tls-cert": "bridge.pem", "tls-key": "bridge.key", "ca-bundle": "ca-bundle.pem"}
        ),
        "mosquitto-k8s": files(
            pki,
            {
                "tls-cert": "mosquitto.pem",
                "tls-key": "mosquitto.key",
                "device-trust": "device-trust.pem",
                "service-trust": "service-trust.pem",
                "ca-bundle": "ca-bundle.pem",
            },
        ),
    }
    with deployable_bundle(model) as bundle:
        juju.deploy(bundle, trust=True)
    for app, content in secrets.items():
        uri = juju.add_secret(f"{app}-pki", content)
        juju.grant_secret(uri, app)
        juju.config(app, {"pki-secret": str(uri)})
    juju.config(
        "fleetwire-api-k8s",
        {"device-broker-url": "tls://localhost:18883", "device-api-url": "https://localhost:18444"},
    )
    juju.wait(all_active, error=charm_failed, timeout=DEPLOY_TIMEOUT, delay=10)


def test_agent_readings_are_stored(juju: jubilant.Juju, tmp_path: pathlib.Path):
    model = model_name(juju)
    juju.wait(all_active, error=charm_failed, timeout=10 * 60, delay=10)
    # The CA that signed the API and broker certificates, from the API's secret.
    ca = tmp_path / "ca-bundle.pem"
    ca.write_text(juju.show_secret("fleetwire-api-k8s-pki", reveal=True).content["ca-bundle"])
    operator = juju.run("fleetwire-api-k8s/0", "create-operator-token", {"name": "integration-test"}).results["token"]
    agent_dir = tmp_path / "agent"
    agent_dir.mkdir()
    with (
        port_forward(model, "svc/fleetwire-api-k8s", 18443, 8443),
        port_forward(model, "svc/fleetwire-api-k8s", 18444, 8444),
        port_forward(model, "svc/mosquitto-k8s", 18883, 8883),
    ):

        def fw(token: str, *args: str) -> object:
            return json.loads(
                run(
                    BIN / "fwctl",
                    "-url",
                    "https://localhost:18443",
                    "-ca-file",
                    ca,
                    "-token",
                    token,
                    "-o",
                    "json",
                    *args,
                )
            )

        tenant = fw(operator, "tenant", "create", "-name", "integration")["id"]
        token = fw(
            operator,
            "token",
            "create",
            "-tenant",
            tenant,
            "-name",
            "integration",
            "-scopes",
            "devices:read,devices:admin,readings:read",
        )["token"]
        enroll = fw(token, "enroll-token", "create", "-count", "1")[0]["token"]
        (agent_dir / "agent.json").write_text(
            json.dumps(
                {
                    "enroll": {"url": "https://localhost:18444", "token": enroll, "ca-bundle": ca.read_text()},
                    "collect": {"interval": "1s"},
                    "metrics": {"listen": "127.0.0.1:19465"},
                }
            )
        )
        with (agent_dir / "agent.log").open("w") as log:
            agent = subprocess.Popen(  # noqa: S603 (our own binary)
                [BIN / "fleetwire-agent", "run", "-data-dir", agent_dir, "-config", agent_dir / "agent.json"],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        try:
            status: dict = {}
            deadline = time.monotonic() + 300
            while status.get("acked", 0) < 20:
                assert time.monotonic() < deadline, (
                    f"no 20 readings acknowledged in 5 minutes: {status}\n"
                    + (agent_dir / "agent.log").read_text()[-4000:]
                )
                time.sleep(5)
                with contextlib.suppress(subprocess.CalledProcessError, json.JSONDecodeError):
                    status = json.loads(
                        run(
                            BIN / "fleetwire-agent",
                            "status",
                            "-json",
                            "-data-dir",
                            agent_dir,
                            "-set",
                            "metrics.listen=127.0.0.1:19465",
                        )
                    )
            (device,) = fw(token, "device", "list")
            assert device["acked_seq"] >= 20
            readings = fw(token, "device", "readings", device["id"], "-limit", "10000")
            stored = sorted(r["seq"] for r in readings)[: device["acked_seq"]]
            # Every acknowledged seq is stored, once, decrypted by the API.
            assert stored == list(range(1, device["acked_seq"] + 1))
        finally:
            agent.terminate()
            agent.wait(timeout=30)
