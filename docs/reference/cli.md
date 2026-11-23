# Command-line reference

Every binary prints its configuration reference with `config-docs` (agent:
`config docs`); [configuration](configuration.md) is generated from those.

## fwctl

```
fwctl [-url URL] [-token TOKEN] [-ca-file FILE] [-o table|json] <group> <command> [flags] [args]
fwctl version
```

Environment: `FWCTL_URL`, `FWCTL_TOKEN`, `FWCTL_CA_FILE`, `FWCTL_OUTPUT`.
Prefer `FWCTL_TOKEN` to `-token`, which other users of the machine can see
in the process list. The token's prefix picks the API: `fwo_` reaches
operator endpoints, `fwt_` the tenant's own.

- The URL must be `https://`. Plain `http://` is accepted only to a
  loopback address (`localhost`, `127.0.0.0/8`, `::1`), for a local proxy
  or tunnel that ends TLS; anywhere else it would send the token in clear
  text.
- Each command takes only its own flags; any other flag is an error.
  Flags may follow the arguments, and `--` ends them.
- Lists follow cursors to the end. `-limit N` stops after N items and asks
  the API for pages of N. With `-o json` a list is a JSON array, `[]` when
  empty.
- An API call times out after 2 minutes. An export download has no overall
  deadline and fails only when no data arrives for 2 minutes.

| Command | Token | Does |
|---|---|---|
| `tenant create -name N [-retention-days D]` | operator | create a tenant and its keys |
| `tenant list [-limit N]` | operator | every tenant |
| `tenant destroy ID -yes` | operator | destroy keys first, then data |
| `tenant show`, `tenant set-retention DAYS` | tenant | own settings |
| `token create -name N -scopes a,b [-tenant ID]` | either | new API token, shown once |
| `token list [-limit N]`, `token show ID`, `token revoke ID` | tenant | API tokens |
| `enroll-token create [-count N] [-ttl 15m] [-tenant ID]` | either | one-time enrollment tokens |
| `device list [-status S] [-limit N]`, `device show ID` | tenant | devices |
| `device revoke ID` | either | revoke |
| `device delete ID -yes` | tenant | revoke and delete all readings |
| `device readings ID [-from] [-to] [-metric] [-limit]` | tenant | decrypted readings |
| `device gaps ID [-limit N]` | tenant | reported gaps |
| `export create [-device ID]`, `export status ID`, `export download ID -out F` | tenant | exports, digest-checked |
| `audit list [-limit N]` | either | audit log |
| `job show ID` | tenant | a background job, such as the `job_id` of `device delete` |

## fleetwire-agent

| Command | Does |
|---|---|
| `run` | the daemon |
| `config check` | validate the configuration (the snap's configure hook) |
| `config docs` | print the configuration reference |
| `status [-json]` | ask the daemon, or read the files read-only if it is not running |
| `version` | print the version, commit and Go version |
| `drop-privileges CMD...` | in the snap, as root: become `snap_daemon` with no supplementary groups or capabilities, then run CMD (the command chain of both apps runs it) |
| `prepare-state` | in the snap, as root: create `$SNAP_DATA/state` for `snap_daemon`, moving the `identity/` and `wal/` of v0.1.1 into it (the install and post-refresh hooks and the daemon's start run it) |

`status` reaches the daemon at `metrics.listen`, on loopback when that
address is a wildcard such as `:9465`. Both its plain and its JSON output
include the agent's version; when the daemon is not running, that is the
version of the installed binary. In the snap the files belong to
`snap_daemon`, so reading them needs sudo; with sudo, the command runs as
`snap_daemon`, not as root.

Outside a snap: `-data-dir DIR`, `-config file.json`, `-set key=value`.

## fleetwire-api

`serve`, `migrate` (owner URL), `create-operator-token -name N` (owner URL),
`healthcheck`, `config-docs`, `version`.

## fleetwire-bridge, fleetwire-ingest

`run` (default), `healthcheck`, `config-docs`, `version`; ingest also
`ensure-topics`.

## Test tools

- `fleetwire-loadgen run -devices N -state-dir DIR -token fwt_... [-interval 10s] [-readings 1] [-duration 10m] [-ramp 200/s] [-metrics-listen ADDR [-pprof]]`;
  `-h` lists the uplink and WAL settings given to every device.
  `-metrics-listen` serves `/metrics` and `/status`, the device counts as
  JSON (`devices`, `started`, `running`, `open_failures`, `run_failures`).
  It exits 1, after writing `results.jsonl`, when a device could not be
  started; that device is not in the results. `fleetwire-loadgen version`
  prints the version.
- `fleetwire-checker -results results.jsonl [-json-out FILE]` or `-device ID -last-seq N [-gaps a-b,c-d]`.
  It exits 1 when a seq is missing or duplicated, when a reported gap was
  not recorded, or when a seq above the device's last seq is stored
  (`beyond_last_seq`, a device that lost its state).
- `fleetwire-chaos run [-scenario a,b] [-devices 2000] [-steady 60s] [-after 60s]`, `fleetwire-chaos list`
- `scripts/throughput.sh`: raises the offered load step by step and reports
  the highest rate the backend keeps up with.
