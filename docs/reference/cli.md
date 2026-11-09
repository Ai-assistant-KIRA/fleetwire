# Command-line reference

Every binary prints its configuration reference with `config-docs` (agent:
`config docs`); [configuration](configuration.md) is generated from those.

## fwctl

```
fwctl [-url URL] [-token TOKEN] [-ca-file FILE] [-o table|json] <group> <command> [flags] [args]
```

Environment: `FWCTL_URL`, `FWCTL_TOKEN`, `FWCTL_CA_FILE`, `FWCTL_OUTPUT`.
The token's prefix picks the API: `fwo_` reaches operator endpoints, `fwt_`
the tenant's own. Lists follow cursors to the end (`-limit N` stops early).

| Command | Token | Does |
|---|---|---|
| `tenant create -name N [-retention-days D]` | operator | create a tenant and its keys |
| `tenant list` | operator | every tenant |
| `tenant destroy ID -yes` | operator | destroy keys first, then data |
| `tenant show`, `tenant set-retention DAYS` | tenant | own settings |
| `token create -name N -scopes a,b [-tenant ID]` | either | new API token, shown once |
| `token list`, `token revoke ID` | tenant | API tokens |
| `enroll-token create [-count N] [-ttl 15m] [-tenant ID]` | either | one-time enrollment tokens |
| `device list [-status S]`, `device show ID` | tenant | devices |
| `device revoke ID` | either | revoke |
| `device delete ID -yes` | tenant | revoke and delete all readings |
| `device readings ID [-from] [-to] [-metric] [-limit]` | tenant | decrypted readings |
| `device gaps ID` | tenant | reported gaps |
| `export create [-device ID]`, `export status ID`, `export download ID -out F` | tenant | exports, digest-checked |
| `audit list [-limit N]` | either | audit log |

## fleetwire-agent

| Command | Does |
|---|---|
| `run` | the daemon |
| `config check` | validate the configuration (the snap's configure hook) |
| `config docs` | print the configuration reference |
| `status [-json]` | ask the daemon, or read the files read-only if it is not running |

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
- `fleetwire-checker -results results.jsonl [-json-out FILE]` or `-device ID -last-seq N [-gaps a-b,c-d]`
- `fleetwire-chaos run [-scenario a,b] [-devices 2000] [-steady 60s] [-after 60s]`, `fleetwire-chaos list`
- `scripts/throughput.sh`: raises the offered load step by step and reports
  the highest rate the backend keeps up with.
