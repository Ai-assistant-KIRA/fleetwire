# Security policy

## Reporting a vulnerability

Please do not open a public issue. Report it privately through
[GitHub security advisories](https://github.com/Ai-assistant-KIRA/fleetwire/security/advisories/new),
or by e-mail to redaalaarabi@gmail.com with "fleetwire security" in the
subject.

Include what you found, how to reproduce it, and what an attacker could do
with it. You will get an answer within 5 working days. Once a fix is
released, the advisory credits you unless you prefer otherwise.

## Supported versions

Only the latest release receives security fixes.

## Scope

The [threat model](docs/explanation/threat-model.md) lists what fleetwire
defends against and which risks are accepted. Reports about accepted risks
are still welcome when they show the risk is worse than described.

## Handling secrets

- Private keys are never committed. `scripts/dev-ca` writes development
  keys into the git-ignored `deploy/compose/secrets/`.
- Tokens have recognisable prefixes (`fwe_`, `fwt_`, `fwo_`) so secret
  scanners can find leaked ones. Revoke a leaked token with `fwctl token
  revoke` or by revoking the device.
