# Deploy with Juju on MicroK8s

This guide deploys the backend on MicroK8s in one Ubuntu VM: Charmed
PostgreSQL, Charmed Kafka, and the four fleetwire charms. The design is in
[ADR-0012](../adr/0012-juju-charms.md).

You need a VM with 4 vCPUs, 8 GiB RAM and 40 GB free disk, Docker (to build
the images), Go (to create the development PKI) and snap.

## 1. MicroK8s and Juju

```console
$ sudo snap install microk8s --classic
$ sudo microk8s enable dns hostpath-storage registry
$ sudo snap install juju
$ mkdir -p ~/.kube && sudo microk8s config > ~/.kube/config
$ juju add-k8s mk8s --client
$ juju bootstrap mk8s fleetwire-ctl
$ juju add-model fleetwire
```

Juju 3.6 bootstraps the `microk8s` cloud only on the strictly confined
MicroK8s snap; with the classic snap it stops with "can only work with
strictly confined microk8s". `juju add-k8s` registers the cluster from its
kubeconfig instead, and works with either. The `registry` add-on serves
`localhost:32000`, where the images go.

## 2. Build the charms and images

```console
$ make build
$ charms/build.sh
```

`build.sh` runs `charmcraft pack` in each charm directory and pushes
`localhost:32000/fleetwire-{api,ingest,bridge}:<commit>`. Set `REGISTRY` and
`TAG` to push elsewhere. charmcraft builds the charms for the host's
architecture; on arm64, point the bundle at the `_arm64.charm` files. Use a new tag for every build: a node that already
has an image under a tag does not pull it again. Released versions are on
GHCR, which the bundle uses by default.

charmcraft builds in LXD containers: `sudo snap install lxd && lxd init --auto`.
If `lxd init --auto` fails, initialise LXD from a preseed with a `dir`
storage pool and a bridge on a free subnet:

```yaml
networks:
  - name: lxdbr0
    type: bridge
    config: {ipv4.address: 10.181.34.1/24, ipv4.nat: "true", ipv6.address: none}
storage_pools:
  - {name: default, driver: dir}
profiles:
  - name: default
    devices:
      eth0: {name: eth0, network: lxdbr0, type: nic}
      root: {path: /, pool: default, type: disk}
```

```console
$ lxd init --preseed < lxd-preseed.yaml
```

On a host that also runs Docker, Docker sets the `FORWARD` policy to `DROP`,
so the build containers have no network. Let the bridge through (the rules
do not survive a reboot):

```console
$ sudo iptables -I DOCKER-USER -i lxdbr0 -j ACCEPT
$ sudo iptables -I DOCKER-USER -o lxdbr0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
```

## 3. Certificates and keys

Each charm reads its certificates from a Juju user secret. For a trial,
create a development PKI whose server certificates name the in-cluster
services (`<application>.<model>.svc.cluster.local`) and every address
devices will use:

```console
$ m=fleetwire
$ go run ./scripts/dev-ca -out pki -hosts \
    localhost,fleetwire-api-k8s.$m.svc.cluster.local,mosquitto-k8s.$m.svc.cluster.local,broker.example.com,api.example.com
```

Create one secret per application, holding only what that workload needs.
`juju add-secret` prints each secret's URI:

```console
$ api_pki=$(juju add-secret fleetwire-api-pki \
    tls-cert#file=pki/api.pem tls-key#file=pki/api.key ca-bundle#file=pki/ca-bundle.pem \
    device-trust#file=pki/device-trust.pem device-ca-cert#file=pki/device-ca.pem \
    device-ca-key#file=pki/device-ca.key service-trust#file=pki/service-trust.pem \
    keyring-root-key=$(xxd -p -c 64 pki/keyring-root.key))
$ ingest_pki=$(juju add-secret fleetwire-ingest-pki \
    tls-cert#file=pki/ingest.pem tls-key#file=pki/ingest.key ca-bundle#file=pki/ca-bundle.pem)
$ bridge_pki=$(juju add-secret fleetwire-bridge-pki \
    tls-cert#file=pki/bridge.pem tls-key#file=pki/bridge.key ca-bundle#file=pki/ca-bundle.pem)
$ broker_pki=$(juju add-secret mosquitto-pki \
    tls-cert#file=pki/mosquitto.pem tls-key#file=pki/mosquitto.key ca-bundle#file=pki/ca-bundle.pem \
    device-trust#file=pki/device-trust.pem service-trust#file=pki/service-trust.pem)
```

The keyring root key goes in as 64 hex characters; the charm writes it back
as 32 bytes with mode 0600. In production the certificates come from your
own CA, and the device CA key is the most sensitive item: whoever has it can
mint device certificates.

## 4. Deploy and relate

The bundle deploys everything with one Kafka and one PostgreSQL unit. An
overlay points it at the images you pushed. Save this as `images.yaml`:

```yaml
applications:
  fleetwire-api-k8s: {resources: {api-image: localhost:32000/fleetwire-api:<commit>}}
  fleetwire-ingest-k8s: {resources: {ingest-image: localhost:32000/fleetwire-ingest:<commit>}}
  fleetwire-bridge-k8s: {resources: {bridge-image: localhost:32000/fleetwire-bridge:<commit>}}
```

Then deploy, grant each secret to its application, set it, and tell the API
how devices reach the broker and the device listener:

```console
$ (cd charms && juju deploy ./bundle.yaml --overlay ../images.yaml --trust)
$ juju grant-secret fleetwire-api-pki fleetwire-api-k8s && juju config fleetwire-api-k8s pki-secret=$api_pki
$ juju grant-secret fleetwire-ingest-pki fleetwire-ingest-k8s && juju config fleetwire-ingest-k8s pki-secret=$ingest_pki
$ juju grant-secret fleetwire-bridge-pki fleetwire-bridge-k8s && juju config fleetwire-bridge-k8s pki-secret=$bridge_pki
$ juju grant-secret mosquitto-pki mosquitto-k8s && juju config mosquitto-k8s pki-secret=$broker_pki
$ juju config fleetwire-api-k8s device-broker-url=tls://broker.example.com:8883 \
    device-api-url=https://api.example.com:8444
$ juju status --watch 5s
```

While something is missing, `juju status` says what: a missing relation
(with the `juju integrate` command that adds it), an unreadable secret (with
the `juju grant-secret` command), or the workload it is waiting for.
Everything turns `active` once Kafka and PostgreSQL are up, which takes most
of the time.

The API leader runs the database migrations with the owner's login (its
`database` relation). The API service and ingest log in with their own
relation users, requested with the extra user role `charmed_dml`, which stay
inside row-level security and cannot switch to the owner. Ingest waits until
the API reports over the `ingest` relation that the schema exists, and stops
while the API has migrations due again (after an upgrade or a new `database`
relation); batches wait in Kafka meanwhile.

Give fleetwire a PostgreSQL of its own: `charmed_dml` reads and writes every
database of the cluster that its members can connect to, so on a shared
PostgreSQL the runtime logins could read other applications' data. The
bundle also turns on the charm's `btree_gist` plugin, which the chunk
overlap constraint needs.

## 5. First operator token and a device

```console
$ juju run fleetwire-api-k8s/0 create-operator-token name=alice
```

The token is in the task's results, shown once; anyone with read access to
this model's tasks can read it, as with any credential action. From here,
[enroll a device](enroll-a-device.md) as with the Compose stack.

## 6. Expose the device and tenant ports

Devices outside the cluster reach the broker's device listener (8883) and
the API's device listener (8444); tenants and `fwctl` use the API's public
listener (8443). Do not expose the Kubernetes services Juju creates for the
applications. Ingest and the bridge reach internal listeners through them
under the names in the certificates, so `mosquitto-k8s` also carries the
broker's service listener (8884, no CRL check) and `fleetwire-api-k8s` the
keyring listener (8445). Add separate services that select the same pods and
list only the external ports. Save this as `external.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fleetwire-broker-external
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: mosquitto-k8s
  ports:
    - {name: mqtt-devices, port: 8883, targetPort: 8883}
---
apiVersion: v1
kind: Service
metadata:
  name: fleetwire-api-external
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: fleetwire-api-k8s
  ports:
    - {name: public, port: 8443, targetPort: 8443}
    - {name: device, port: 8444, targetPort: 8444}
```

Give MetalLB a free address range on the VM's network, then apply it:

```console
$ microk8s enable metallb:<first-address>-<last-address>
$ microk8s kubectl -n fleetwire apply -f external.yaml
$ microk8s kubectl -n fleetwire get svc fleetwire-broker-external fleetwire-api-external
```

Without a load balancer, use `type: NodePort`. Put the external names in
the certificates and in `device-broker-url` and `device-api-url`. For a
quick test from the VM, port-forwards to the Juju services work, because
they stay on the VM:

```console
$ microk8s kubectl -n fleetwire port-forward svc/fleetwire-api-k8s 18444:8444 &
$ microk8s kubectl -n fleetwire port-forward svc/mosquitto-k8s 18883:8883 &
```

The admin listeners (9090: plain HTTP health and metrics) are on no
service. Scrape them from the pods, for example by pod name through the
headless `<application>-endpoints` service Juju creates.

## Limits of this deployment

- **One API unit.** The local keyring keeps the tenant KEKs on the unit's
  storage, so the charm blocks if the application is scaled up. Ingest and
  the bridge scale out.
- **Broker-side revocation lags** by up to the update-status interval
  (5 minutes by default, `juju model-config update-status-hook-interval`).
  Ingest drops a revoked device's data at once.
- **One Kafka broker** in the bundle, with `kafka-replication: 1`. For three
  brokers, `juju add-unit kafka-k8s -n 2` and set `kafka-replication: 3`
  before ingest first starts.

## Integration test

`charms/tests/integration/test_bundle.py` does all of the above in a
temporary model and checks that every reading a real agent got acknowledged
is stored:

```console
$ cd charms && FLEETWIRE_IMAGES='localhost:32000/fleetwire-{service}:<commit>' tox -e integration
```
