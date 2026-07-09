# Fulfillment Projections

Fulfillment-based expiry is for relations that end when application progress
reaches a target. A course entitlement might expire after required lessons are
complete. A review hold might expire after two approvals.

Keepsake evaluates the snapshot. It does not consume every domain event or
replace the application's workflow engine.

Choose the projection source based on write volume and cost:

| Source | Use when |
| --- | --- |
| SQL view | the state is cheap to derive at read time |
| Materialized view | aggregation is expensive but refreshes are controlled |
| App-owned projection table | events are frequent and writes need their own pipeline |
| Built-in counters | the v0 counter model fits and the default feature is enabled |

Pass the current fulfillment state into the lifecycle path when applying or
evaluating the relation. Keep the event stream, rebuild policy, and correction
workflow in application code.

The SQLx adapter includes simple counter projection rows behind the default
`fulfillment-counters` feature. Disable that feature when fulfillment state is
entirely application-owned through views, materialized views, event projections,
or service lookups.
