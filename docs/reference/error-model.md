# Error Model

Keepsake keeps validation, provider, and adapter failures typed so callers can
handle them without parsing strings.

| Layer | Error shape |
| --- | --- |
| Core crate | `KeepsakeError` for validation and lifecycle model failures. |
| Provider traits | Associated error types supplied by the implementation. |
| Audit sinks | Associated error types for durable audit write failures. |
| SQLx adapter | Repository errors for SQL, migration, serialization, and adapter failures. |

Application code should convert these errors at its boundary: HTTP responses,
job failures, command handlers, or domain-specific error enums. Keep durable
audit write failures visible instead of folding them into generic lifecycle
errors.
