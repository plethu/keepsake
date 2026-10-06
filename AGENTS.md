@~/.config/agents/AGENTS.md

# Keepsake

Published Rust relation-lifecycle library. Read `README.md`, `CONTRIBUTING.md`,
`docs/operations/versioning.md` and the relevant migration or transaction guide
before changing public or durable contracts. Use the global `public-library`
skill for release decisions.

- Stabilize the current published major lines. Preserve project names, release
  history and historical migration bytes; package versions and stored-data
  versions remain separate contracts.
- Kumite uses relation state for moderation and authorization. Where available,
  exercise affected expiry, revocation, retry, audit and caller-owned transaction
  workflows before publication. Keep game policy outside Keepsake; neither
  builds nor ordinary tests should require that private repo.
- Follow the existing contribution and versioning gates, including public API
  comparison and real backend evidence for affected contracts. A permitted
  major bump alone does not establish that publishing the break is worthwhile.
