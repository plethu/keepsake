# Lifecycle Model

Every keepsake is either active, revoked, or expired. The active state only
means the relation currently applies. Revoked and expired states are terminal
states with timestamps that explain how the relation ended.

The core model represents that lifecycle with Rust types, then lowers it into a
flat record for JSON and SQL. That split rejects invalid timestamp combinations
at the model boundary and leaves the storage record easy to query.

| Type | Use |
| --- | --- |
| `Keepsake` | Validated relation assignment used by application and adapter code. |
| `KeepsakeRecord` | Flat serde and storage record that converts into `Keepsake`. |
| `KeepsakeLifecycle` | Typed lifecycle payload carried by `Keepsake`. |
| `LifecycleState` | State discriminant: `applied`, `revoked`, or `expired`. |
| `ExpiryCause` | Expiry reason: `timed` or `fulfilled`. |

`Keepsake` serializes as `KeepsakeRecord`. APIs and persistence use the flat
record. Rust callers use typed lifecycle accessors such as
`state()`, `lifecycle()`, `is_active()`, `ended_at()`, `revoked_at()`,
`expired_at()`, and `fulfilled_at()`.

## Record Shape

`KeepsakeRecord` contains:

- `id`, `subject`, and `relation_id`;
- `state`;
- the copied `expiry` policy from apply time;
- `applied_at`;
- `expires_at`, `fulfilled_at`, and `revoked_at`;
- opaque string metadata.

The record includes the copied expiry policy from apply time. Replay and expiry
decisions use that copied policy even after a relation definition changes.

## Valid Combinations

Conversion from `KeepsakeRecord` to `Keepsake` validates the lifecycle rules
also enforced by the SQL schema:

| State | Required fields | Rejected fields |
| --- | --- | --- |
| `applied` | no terminal timestamp | `fulfilled_at`, `revoked_at` |
| `revoked` | `revoked_at` | `fulfilled_at` |
| timed `expired` | `ExpiryPolicy::At` and matching `expires_at` | `fulfilled_at`, `revoked_at` |
| fulfilled `expired` | `ExpiryPolicy::WhenFulfilled` and `fulfilled_at` | `expires_at`, `revoked_at` |

`ExpiryPolicy::ManualOnly` rows have no expired state. Timed expiry uses the
policy timestamp as the expiry timestamp. Fulfillment expiry uses `fulfilled_at`
as the expiry timestamp.

## Subject Validation

`SubjectRef`, `ActorRef`, relation kinds, and relation names reject empty or
whitespace-only identifiers. Repository apply paths validate the subject before
starting lifecycle writes. Invalid subjects fail without a persisted row.
