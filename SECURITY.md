# Security

## Reporting a vulnerability

Please report suspected vulnerabilities privately through the repository's
GitHub Security Advisories. Include the affected crate and version, backend
and feature flags, a minimal reproduction, and the impact you observed. Do
not include production credentials or tenant data in a report.

We will acknowledge a report when it is received, investigate it, and
coordinate a fix or mitigation with the reporter. A public issue is appropriate
for ordinary bugs, documentation mistakes, or questions that do not expose a
security boundary.

## Security boundaries

Keepsake stores relation state and lifecycle audit events. It does not
authenticate users, validate identity-provider tokens, choose a tenant for a
request, enforce authorization policy, or configure database row-level
security. Those responsibilities belong to the application and its identity
provider.

Tenant identifiers in the core contract are explicit, validated values. The
matching SQLx and Dovecote releases must also bind the tenant in every query,
event, delivery, and migration operation. A core-only update is not a
multi-tenant deployment recipe; do not deploy an older 2.x SQLx schema as
tenant-isolated storage without the coordinated tenant-aware migration track.

RLS or connection-session tenant settings can provide useful defense in depth,
but they are not a substitute for explicit predicates. Applications must take
care with pooled connections, transaction boundaries, logs, caches, exports,
and background workers.

## Disclosure and assurance

Keepsake does not currently claim an independent security audit, certification,
or regulatory compliance. The project gate runs compiler, lint, test,
dependency-advisory, dependency license-policy, and dependency-source checks;
those checks are evidence of the checked revision, not a guarantee that
deployments are secure. The license allowlist is a repository policy for the
observed graph and should be reviewed when dependencies change; it is not legal
advice.

## Temporary MySQL RSA advisory exception

The optional MySQL SQLx path inherits SQLx's `mysql-rsa` feature so a non-TLS
connection can complete `sha256_password` or full `caching_sha2_password`
authentication. SQLx encrypts the password with the server's public key;
Keepsake does not accept private keys or perform RSA decryption or signing.

RustSec `RUSTSEC-2023-0071` concerns timing leakage in RSA private-key
operations, and no patched `rsa` release is currently available. The
repository's targeted cargo-deny exception applies only to this transitive
SQLx path and only when the optional MySQL graph is enabled. Prefer TLS for
deployed connections. Review by 2026-12-31, or immediately when SQLx or `rsa`
ships a replacement/fix, the authentication policy changes, or private-key RSA
use is introduced; remove the exception at that review.
