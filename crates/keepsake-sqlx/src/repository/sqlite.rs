use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use keepsake::{
    ActiveRelation, ActiveRelationSource, ApplyKeepsake, AuditDecision, AuditEvent, ExpiryPolicy,
    FulfillmentSnapshot, Keepsake, KeepsakeId, KeepsakeRecord, RelationDefinition, RelationId,
    RelationKey, RelationSpec, RevokeBySubject, RevokeKeepsake, SubjectRef,
};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use super::support::{
    AuditEventParts, apply_event, audit_event_record, expires_at, filter_active_relations_by_ids,
    filter_active_relations_by_keys, parse_state, parse_uuid, revoke_by_subject_event,
    revoke_event,
};
use super::{
    AppliedKeepsake, AuditCursor, AuditEventRecord, FulfilledExpiryCandidate, MembershipCursor,
    RelationCache, RepositoryError, RepositoryResult, SqliteKeepsakeRepository,
    TimedExpiryCandidate, validate_limit,
};

impl<C> SqliteKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?6
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(format_timestamp(at))
        .fetch_one(&self.pool)
        .await?;
        let relation = relation_from_row(&row)?;
        self.relation_cache.remove_by_id(relation.id).await;
        Ok(relation)
    }

    /// Inserts or updates a typed relation spec by its natural relation key.
    pub async fn upsert_relation_spec<Spec>(
        &self,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition>
    where
        Spec: RelationSpec,
    {
        let relation = RelationDefinition::from_spec::<Spec>(at)?;
        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?6
            where keepsake_relation_definitions.id = excluded.id
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(format_timestamp(at))
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let stored_relation_id = sqlx::query_scalar::<_, String>(
                r"
                select id
                from keepsake_relation_definitions
                where kind = ?1 and key = ?2
                ",
            )
            .bind(relation.key.kind())
            .bind(relation.key.name())
            .fetch_one(&mut *tx)
            .await?;
            return Err(RepositoryError::RelationSpecIdMismatch {
                kind: relation.key.kind().to_owned(),
                name: relation.key.name().to_owned(),
                expected_relation_id: relation.id,
                stored_relation_id: parse_uuid(&stored_relation_id)?,
            });
        };

        tx.commit().await?;
        let relation = relation_from_row(&row)?;
        self.relation_cache.remove_by_id(relation.id).await;
        Ok(relation)
    }

    /// Looks up a relation definition by stable id.
    pub async fn relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_id(relation_id).await {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?1
            ",
        )
        .bind(relation_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Looks up a relation definition by its natural relation key.
    pub async fn relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_key(key).await {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ?1 and key = ?2
            ",
        )
        .bind(key.kind())
        .bind(key.name())
        .fetch_optional(&self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Enables or disables a relation.
    pub async fn set_relation_enabled(
        &self,
        relation_id: RelationId,
        enabled: bool,
        at: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_relation_definitions
            set enabled = ?2, updated_at = ?3
            where id = ?1
            ",
        )
        .bind(relation_id.to_string())
        .bind(enabled)
        .bind(format_timestamp(at))
        .execute(&self.pool)
        .await?;
        let changed = result.rows_affected() == 1;
        if changed {
            self.relation_cache.remove_by_id(relation_id).await;
        }
        Ok(changed)
    }

    /// Applies a command idempotently and records its audit event atomically.
    pub async fn apply(&self, command: &ApplyKeepsake) -> RepositoryResult<AppliedKeepsake> {
        command.subject.validate()?;
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let relation = relation_for_update_tx(&mut tx, command.relation_id).await?;
        if let Some(existing) =
            active_keepsake_for_subject_relation_tx(&mut tx, &command.subject, command.relation_id)
                .await?
        {
            record_audit_event_tx(&mut tx, &apply_event(command, &existing, true)).await?;
            tx.commit().await?;
            return Ok(AppliedKeepsake {
                keepsake: existing,
                duplicate_prevented: true,
            });
        }

        if !relation.enabled {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let metadata = serde_json::to_string(&command.metadata)?;
        let expires_at_column = expires_at(&relation.expiry).map(format_timestamp);
        let at = format_timestamp(command.at);
        let result = sqlx::query(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?1, ?2, ?3, ?4, 'applied', ?5, ?6, ?7, ?8, ?6, ?6)
            on conflict (subject_kind, subject_id, relation_id) where state = 'applied'
            do nothing
            ",
        )
        .bind(command.id.to_string())
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(command.relation_id.to_string())
        .bind(expiry_policy)
        .bind(&at)
        .bind(expires_at_column)
        .bind(metadata)
        .execute(&mut *tx)
        .await?;

        let (keepsake, duplicate_prevented) = if result.rows_affected() == 0 {
            let existing = active_keepsake_for_subject_relation_tx(
                &mut tx,
                &command.subject,
                command.relation_id,
            )
            .await?
            .ok_or(RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            })?;
            (existing, true)
        } else {
            let keepsake = keepsake_by_id_tx(&mut tx, command.id).await?.ok_or(
                RepositoryError::RelationDefinitionMissing {
                    relation_id: command.relation_id,
                },
            )?;
            (keepsake, false)
        };

        record_audit_event_tx(
            &mut tx,
            &apply_event(command, &keepsake, duplicate_prevented),
        )
        .await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented,
        })
    }

    /// Revokes an active keepsake from a command and records its audit event atomically.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> RepositoryResult<bool> {
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let revoked = revoke_tx(&mut tx, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            record_audit_event_tx(&mut tx, &revoke_event(command, keepsake)).await?;
        }
        tx.commit().await?;
        Ok(revoked.is_some())
    }

    /// Revokes the active keepsake for a subject and relation pair.
    ///
    /// Returns the revoked keepsake id, or `None` when no active keepsake exists
    /// for the pair. The active uniqueness invariant guarantees at most one match.
    pub async fn revoke_by_subject(
        &self,
        command: &RevokeBySubject,
    ) -> RepositoryResult<Option<KeepsakeId>> {
        command.subject.validate()?;
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let revoked =
            revoke_by_subject_tx(&mut tx, &command.subject, command.relation_id, command.at)
                .await?;
        let revoked_id = revoked.as_ref().map(Keepsake::id);
        if let Some(keepsake) = &revoked {
            record_audit_event_tx(&mut tx, &revoke_by_subject_event(command, keepsake)).await?;
        }
        tx.commit().await?;
        Ok(revoked_id)
    }

    /// Appends an explicit audit event without mutating lifecycle state.
    pub async fn append_audit_event(&self, event: &AuditEvent) -> RepositoryResult<i64> {
        event.subject.validate()?;
        event.actor.validate()?;

        let mut tx = self.pool.begin().await?;
        let audit_event_id = record_audit_event_tx(&mut tx, event).await?;
        tx.commit().await?;
        Ok(audit_event_id)
    }

    /// Reads audit events for a keepsake in stable `(occurred_at, id)` order.
    pub async fn audit_events_for_keepsake(
        &self,
        keepsake_id: Uuid,
        after: Option<&AuditCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where keepsake_id = ?1
              and (
                ?2 is null
                or (occurred_at, id) > (?2, ?3)
              )
            order by occurred_at, id
            limit ?4
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(after.map(|cursor| format_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        hydrate_audit_records(&self.pool, rows).await
    }

    /// Reads audit events for a relation in stable `(occurred_at, id)` order.
    pub async fn audit_events_for_relation(
        &self,
        relation_id: RelationId,
        after: Option<&AuditCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where relation_id = ?1
              and (
                ?2 is null
                or (occurred_at, id) > (?2, ?3)
              )
            order by occurred_at, id
            limit ?4
            ",
        )
        .bind(relation_id.to_string())
        .bind(after.map(|cursor| format_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        hydrate_audit_records(&self.pool, rows).await
    }

    /// Returns active keepsakes for a subject.
    pub async fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<Keepsake>> {
        let rows = sqlx::query(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where subject_kind = ?1 and subject_id = ?2 and state = 'applied'
            order by relation_id, id
            ",
        )
        .bind(subject.kind())
        .bind(subject.id())
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(keepsake_from_row).collect()
    }

    /// Returns active keepsakes for a subject with their relation definitions.
    pub async fn active_relations_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        let rows = active_relation_rows_for_subject(&self.pool, subject).await?;
        let mut active = Vec::with_capacity(rows.len());
        for (keepsake, relation) in rows {
            self.relation_cache.store(&relation).await;
            active.push(ActiveRelation::new(keepsake, relation)?);
        }
        Ok(active)
    }

    /// Returns active keepsakes for a subject, filtered by relation ids.
    pub async fn active_relations_for_subject_by_ids(
        &self,
        subject: &SubjectRef,
        relation_ids: &[RelationId],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if relation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let active = self.active_relations_for_subject(subject).await?;
        Ok(filter_active_relations_by_ids(active, relation_ids))
    }

    /// Returns active keepsakes for a subject, filtered by relation keys.
    pub async fn active_relations_for_subject_by_keys(
        &self,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let active = self.active_relations_for_subject(subject).await?;
        Ok(filter_active_relations_by_keys(active, keys))
    }

    /// Scans active memberships for a relation in stable order.
    pub async fn active_membership_scan(
        &self,
        relation_id: RelationId,
        limit: i64,
    ) -> RepositoryResult<Vec<Keepsake>> {
        self.active_membership_scan_after(relation_id, None, limit)
            .await
    }

    /// Scans active memberships after a keyset cursor in stable order.
    pub async fn active_membership_scan_after(
        &self,
        relation_id: RelationId,
        after: Option<&MembershipCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<Keepsake>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where relation_id = ?1
              and state = 'applied'
              and (
                ?2 is null
                or (subject_kind, subject_id, id) > (?2, ?3, ?4)
              )
            order by subject_kind, subject_id, id
            limit ?5
            ",
        )
        .bind(relation_id.to_string())
        .bind(after.map(|cursor| cursor.subject_kind.as_str()))
        .bind(after.map(|cursor| cursor.subject_id.as_str()))
        .bind(after.map(|cursor| cursor.keepsake_id.to_string()))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(keepsake_from_row).collect()
    }

    /// Lists due timed expiry candidates in stable batch order.
    pub async fn due_timed_expiry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?1
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?2
            ",
        )
        .bind(format_timestamp(now))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Reads the persisted fulfillment snapshot (counters and checklist) for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let mut tx = self.pool.begin().await?;
        let snapshot = fulfillment_snapshot_tx(&mut tx, keepsake_id).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Lists fulfillment expiry candidates in stable batch order.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.state = 'applied'
              and r.enabled
              and json_extract(k.expiry_policy, '$.type') = 'when_fulfilled'
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?1
            ",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(fulfilled_expiry_candidate_from_row)
            .collect()
    }

    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let target = u64::try_from(limit).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let mut expired = 0;
        let mut tx = self.pool.begin().await?;
        let mut after = None;
        while expired < target {
            let remaining = i64::try_from(target - expired)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let candidates =
                due_fulfilled_expiry_after_tx(&mut tx, after.as_ref(), remaining).await?;
            if candidates.is_empty() {
                break;
            }
            after = candidates.last().map(FulfilledExpiryCursor::from);
            for candidate in candidates {
                let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
                    continue;
                };
                let snapshot = fulfillment_snapshot_tx(&mut tx, candidate.keepsake_id).await?;
                if policy.is_fulfilled(&snapshot) {
                    let result = sqlx::query(
                        r"
                        update keepsakes
                        set state = 'expired', fulfilled_at = ?2, updated_at = ?2
                        where id = ?1
                          and state = 'applied'
                          and exists (
                            select 1
                            from keepsake_relation_definitions r
                            where r.id = keepsakes.relation_id and r.enabled
                          )
                        ",
                    )
                    .bind(candidate.keepsake_id.to_string())
                    .bind(format_timestamp(now))
                    .execute(&mut *tx)
                    .await?;
                    expired += result.rows_affected();
                }
            }
        }
        tx.commit().await?;
        Ok(expired)
    }

    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: DateTime<Utc>, limit: i64) -> RepositoryResult<u64> {
        let candidates = self.due_timed_expiry(now, limit).await?;
        let mut expired = 0;
        let mut tx = self.pool.begin().await?;
        for candidate in candidates {
            let result = sqlx::query(
                r"
                update keepsakes
                set state = 'expired', updated_at = ?2
                where id = ?1
                  and state = 'applied'
                  and exists (
                    select 1
                    from keepsake_relation_definitions r
                    where r.id = keepsakes.relation_id and r.enabled
                  )
                ",
            )
            .bind(candidate.keepsake_id.to_string())
            .bind(format_timestamp(now))
            .execute(&mut *tx)
            .await?;
            expired += result.rows_affected();
        }
        tx.commit().await?;
        Ok(expired)
    }

    /// Upserts a simple fulfillment counter projection.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (keepsake_id, key, value, observed_at)
            values (?1, ?2, ?3, ?4)
            on conflict (keepsake_id, key) do update set
                value = excluded.value,
                observed_at = excluded.observed_at
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(value)
        .bind(format_timestamp(observed_at))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically adds `delta` to a fulfillment counter and returns the new value.
    ///
    /// Unlike [`upsert_counter_projection`](Self::upsert_counter_projection), the
    /// increment is computed in the database, so concurrent writers cannot lose
    /// updates to a read-modify-write race.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn increment_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        delta: i64,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<i64> {
        let row = sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (keepsake_id, key, value, observed_at)
            values (?1, ?2, ?3, ?4)
            on conflict (keepsake_id, key) do update set
                value = value + excluded.value,
                observed_at = excluded.observed_at
            returning value
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(delta)
        .bind(format_timestamp(observed_at))
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("value")?)
    }

    /// Upserts a checklist item completion projection.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_checklist_projection(
        &self,
        keepsake_id: Uuid,
        item: &str,
        complete: bool,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        sqlx::query(
            r"
            insert into keepsake_fulfillment_checklist
                (keepsake_id, item, complete, observed_at)
            values (?1, ?2, ?3, ?4)
            on conflict (keepsake_id, item) do update set
                complete = excluded.complete,
                observed_at = excluded.observed_at
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(item)
        .bind(i64::from(complete))
        .bind(format_timestamp(observed_at))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl<C> ActiveRelationSource for SqliteKeepsakeRepository<C>
where
    C: RelationCache,
{
    type Error = RepositoryError;

    async fn active_relations_for_subject<'a>(
        &'a self,
        subject: &'a SubjectRef,
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        self.active_relations_for_subject(subject).await
    }

    async fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        self.active_relations_for_subject_by_ids(subject, relation_ids)
            .await
    }

    async fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        self.active_relations_for_subject_by_keys(subject, keys)
            .await
    }
}

async fn record_audit_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    event: &AuditEvent,
) -> RepositoryResult<i64> {
    let decision = serde_json::to_string(&event.decision)?;
    let audit_event_id = sqlx::query_scalar::<_, i64>(
        r"
        insert into keepsake_audit_events
            (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
             event_type, decision, occurred_at)
        values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        returning id
        ",
    )
    .bind(event.keepsake_id.to_string())
    .bind(event.relation_id.to_string())
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(decision)
    .bind(format_timestamp(event.at))
    .fetch_one(&mut **tx)
    .await?;

    if event.context.attributes.is_empty() {
        return Ok(audit_event_id);
    }

    let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
        "insert into keepsake_audit_context_attributes (audit_event_id, key, value) ",
    );
    builder.push_values(&event.context.attributes, |mut row, (key, value)| {
        row.push_bind(audit_event_id)
            .push_bind(key.as_str())
            .push_bind(value.as_str());
    });
    builder.build().execute(&mut **tx).await?;

    Ok(audit_event_id)
}

async fn hydrate_audit_records(
    pool: &sqlx::SqlitePool,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> RepositoryResult<Vec<AuditEventRecord>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids = rows
        .iter()
        .map(|row| row.try_get::<i64, _>("id"))
        .collect::<Result<Vec<i64>, _>>()?;
    let mut attributes = audit_attributes_by_event(pool, &ids).await?;
    rows.into_iter()
        .map(|row| {
            let id = row.try_get::<i64, _>("id")?;
            let decision = serde_json::from_str::<AuditDecision>(row.try_get("decision")?)?;
            audit_event_record(AuditEventParts {
                id,
                event_type: row.try_get("event_type")?,
                at: parse_timestamp(row.try_get("occurred_at")?)?,
                actor_kind: row.try_get("actor_kind")?,
                actor_id: row.try_get("actor_id")?,
                keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
                subject_kind: row.try_get("subject_kind")?,
                subject_id: row.try_get("subject_id")?,
                relation_id: parse_uuid(row.try_get("relation_id")?)?,
                decision,
                attributes: attributes.remove(&id).unwrap_or_default(),
            })
        })
        .collect()
}

async fn audit_attributes_by_event(
    pool: &sqlx::SqlitePool,
    ids: &[i64],
) -> RepositoryResult<BTreeMap<i64, BTreeMap<String, String>>> {
    let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
        "select audit_event_id, key, value from keepsake_audit_context_attributes \
         where audit_event_id in (",
    );
    let mut separated = builder.separated(", ");
    for id in ids {
        separated.push_bind(id);
    }
    builder.push(")");
    let rows = builder
        .build_query_as::<(i64, String, String)>()
        .fetch_all(pool)
        .await?;
    let mut attributes = BTreeMap::<i64, BTreeMap<String, String>>::new();
    for (event_id, key, value) in rows {
        attributes.entry(event_id).or_default().insert(key, value);
    }
    Ok(attributes)
}

#[cfg(feature = "fulfillment-counters")]
async fn due_fulfilled_expiry_after_tx(
    tx: &mut Transaction<'_, Sqlite>,
    after: Option<&FulfilledExpiryCursor>,
    limit: i64,
) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
    let after_relation_id = after.map(|cursor| cursor.relation_id.to_string());
    let after_keepsake_id = after.map(|cursor| cursor.keepsake_id.to_string());
    let rows = sqlx::query(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.state = 'applied'
          and r.enabled
          and json_extract(k.expiry_policy, '$.type') = 'when_fulfilled'
          and (
            ?1 is null
            or (k.relation_id, k.subject_kind, k.subject_id, k.id) > (?1, ?2, ?3, ?4)
          )
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit ?5
        ",
    )
    .bind(after_relation_id.as_deref())
    .bind(after.map(|cursor| cursor.subject_kind.as_str()))
    .bind(after.map(|cursor| cursor.subject_id.as_str()))
    .bind(after_keepsake_id.as_deref())
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    rows.iter()
        .map(fulfilled_expiry_candidate_from_row)
        .collect()
}

async fn relation_for_update_tx(
    tx: &mut Transaction<'_, Sqlite>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = ?1
        ",
    )
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where subject_kind = ?1 and subject_id = ?2 and relation_id = ?3 and state = 'applied'
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where id = ?1
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn revoke_tx(
    tx: &mut Transaction<'_, Sqlite>,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?2, updated_at = ?2
        where id = ?1 and state = 'applied'
        returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(keepsake_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, Sqlite>,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?4, updated_at = ?4
        where subject_kind = ?1 and subject_id = ?2 and relation_id = ?3 and state = 'applied'
        returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn active_relation_rows_for_subject(
    pool: &sqlx::SqlitePool,
    subject: &SubjectRef,
) -> RepositoryResult<Vec<(Keepsake, RelationDefinition)>> {
    let rows = sqlx::query(
        r"
        select
            k.id,
            k.subject_kind,
            k.subject_id,
            k.relation_id,
            k.state,
            k.expiry_policy,
            k.applied_at,
            k.expires_at,
            k.fulfilled_at,
            k.revoked_at,
            k.metadata,
            r.id as relation_definition_id,
            r.kind as relation_kind,
            r.key as relation_key,
            r.enabled as relation_enabled,
            r.expiry_policy as relation_expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.subject_kind = ?1 and k.subject_id = ?2 and k.state = 'applied'
        order by k.relation_id, k.id
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(|row| {
            Ok((
                keepsake_from_row(row)?,
                relation_definition_from_active_row(row)?,
            ))
        })
        .collect()
}

#[cfg(feature = "fulfillment-counters")]
async fn fulfillment_snapshot_tx(
    tx: &mut Transaction<'_, Sqlite>,
    keepsake_id: Uuid,
) -> RepositoryResult<FulfillmentSnapshot> {
    let counter_rows = sqlx::query(
        r"
        select key, value
        from keepsake_fulfillment_counters
        where keepsake_id = ?1
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;

    let mut counters = BTreeMap::new();
    for row in counter_rows {
        counters.insert(row.try_get("key")?, row.try_get("value")?);
    }

    let checklist_rows = sqlx::query(
        r"
        select item, complete
        from keepsake_fulfillment_checklist
        where keepsake_id = ?1
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;

    let mut checklist = BTreeMap::new();
    for row in checklist_rows {
        checklist.insert(
            row.try_get("item")?,
            row.try_get::<i64, _>("complete")? != 0,
        );
    }
    Ok(FulfillmentSnapshot {
        counters,
        checklist,
    })
}

fn relation_from_row(row: &sqlx::sqlite::SqliteRow) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(RelationDefinition::new(
        parse_uuid(row.try_get("id")?)?,
        RelationKey::new(
            row.try_get::<String, _>("kind")?,
            row.try_get::<String, _>("key")?,
        )?,
        row.try_get("enabled")?,
        expiry,
    )?)
}

fn keepsake_from_row(row: &sqlx::sqlite::SqliteRow) -> RepositoryResult<Keepsake> {
    let metadata = serde_json::from_str::<BTreeMap<String, String>>(row.try_get("metadata")?)?;
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(KeepsakeRecord {
        id: parse_uuid(row.try_get("id")?)?,
        subject: SubjectRef::new(
            row.try_get::<String, _>("subject_kind")?,
            row.try_get::<String, _>("subject_id")?,
        )?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        state: parse_state(row.try_get("state")?)?,
        expiry,
        applied_at: parse_timestamp(row.try_get("applied_at")?)?,
        expires_at: optional_timestamp(row.try_get("expires_at")?)?,
        fulfilled_at: optional_timestamp(row.try_get("fulfilled_at")?)?,
        revoked_at: optional_timestamp(row.try_get("revoked_at")?)?,
        metadata,
    }
    .try_into()?)
}

fn relation_definition_from_active_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("relation_expiry_policy")?)?;
    Ok(RelationDefinition::new(
        parse_uuid(row.try_get("relation_definition_id")?)?,
        RelationKey::new(
            row.try_get::<String, _>("relation_kind")?,
            row.try_get::<String, _>("relation_key")?,
        )?,
        row.try_get("relation_enabled")?,
        expiry,
    )?)
}

fn timed_expiry_candidate_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<TimedExpiryCandidate> {
    Ok(TimedExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        due_at: parse_timestamp(row.try_get("due_at")?)?,
    })
}

#[cfg(feature = "fulfillment-counters")]
fn fulfilled_expiry_candidate_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<FulfilledExpiryCandidate> {
    Ok(FulfilledExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        expiry_policy: serde_json::from_str(row.try_get("expiry_policy")?)?,
    })
}

#[cfg(feature = "fulfillment-counters")]
#[derive(Debug, Clone)]
struct FulfilledExpiryCursor {
    relation_id: Uuid,
    subject_kind: String,
    subject_id: String,
    keepsake_id: Uuid,
}

#[cfg(feature = "fulfillment-counters")]
impl From<&FulfilledExpiryCandidate> for FulfilledExpiryCursor {
    fn from(candidate: &FulfilledExpiryCandidate) -> Self {
        Self {
            relation_id: candidate.relation_id,
            subject_kind: candidate.subject_kind.clone(),
            subject_id: candidate.subject_id.clone(),
            keepsake_id: candidate.keepsake_id,
        }
    }
}

fn parse_timestamp(value: &str) -> RepositoryResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?
        .with_timezone(&Utc))
}

#[allow(clippy::needless_pass_by_value)]
fn optional_timestamp(value: Option<String>) -> RepositoryResult<Option<DateTime<Utc>>> {
    value.as_deref().map(parse_timestamp).transpose()
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}
