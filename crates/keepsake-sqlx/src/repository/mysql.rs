use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDateTime, Utc};
use keepsake::{
    ActiveRelation, ActiveRelationSource, ApplyKeepsake, AuditEvent, ExpiryPolicy,
    FulfillmentSnapshot, Keepsake, KeepsakeRecord, RelationDefinition, RelationId, RelationKey,
    RelationSpec, RevokeKeepsake, SubjectRef,
};
use sqlx::{MySql, Row, Transaction};
use uuid::Uuid;

use super::support::{apply_event, expires_at, parse_state, parse_uuid, revoke_event};
use super::{
    AppliedKeepsake, FulfilledExpiryCandidate, MembershipCursor, MySqlKeepsakeRepository,
    RelationCache, RepositoryError, RepositoryResult, TimedExpiryCandidate, validate_limit,
};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?)
            on duplicate key update
                enabled = values(enabled),
                expiry_policy = values(expiry_policy),
                updated_at = values(updated_at)
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(naive_timestamp(at))
        .bind(naive_timestamp(at))
        .execute(&self.pool)
        .await?;

        let relation = self.relation_by_key(&relation.key).await?.ok_or(
            RepositoryError::RelationDefinitionMissing {
                relation_id: relation.id,
            },
        )?;
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
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ? and `key` = ?
            for update
            ",
        )
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = existing {
            let stored = relation_from_row(&row)?;
            if stored.id != relation.id {
                return Err(RepositoryError::RelationSpecIdMismatch {
                    kind: relation.key.kind().to_owned(),
                    name: relation.key.name().to_owned(),
                    expected_relation_id: relation.id,
                    stored_relation_id: stored.id,
                });
            }
            sqlx::query(
                r"
                update keepsake_relation_definitions
                set enabled = ?, expiry_policy = ?, updated_at = ?
                where id = ?
                ",
            )
            .bind(relation.enabled)
            .bind(serde_json::to_value(&relation.expiry)?)
            .bind(naive_timestamp(at))
            .bind(relation.id.to_string())
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                r"
                insert into keepsake_relation_definitions
                    (id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?)
                ",
            )
            .bind(relation.id.to_string())
            .bind(relation.key.kind())
            .bind(relation.key.name())
            .bind(relation.enabled)
            .bind(serde_json::to_value(&relation.expiry)?)
            .bind(naive_timestamp(at))
            .bind(naive_timestamp(at))
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?
            ",
        )
        .bind(relation.id.to_string())
        .fetch_one(&mut *tx)
        .await?;
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
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?
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
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ? and `key` = ?
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
            set enabled = ?, updated_at = ?
            where id = ?
            ",
        )
        .bind(enabled)
        .bind(naive_timestamp(at))
        .bind(relation_id.to_string())
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

        sqlx::query(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?, ?, ?, ?, 'applied', ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(command.id.to_string())
        .bind(&command.subject.kind)
        .bind(&command.subject.id)
        .bind(command.relation_id.to_string())
        .bind(serde_json::to_value(&relation.expiry)?)
        .bind(naive_timestamp(command.at))
        .bind(expires_at(&relation.expiry).map(naive_timestamp))
        .bind(serde_json::to_value(&command.metadata)?)
        .bind(naive_timestamp(command.at))
        .bind(naive_timestamp(command.at))
        .execute(&mut *tx)
        .await?;

        let keepsake = keepsake_by_id_tx(&mut tx, command.id).await?.ok_or(
            RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            },
        )?;
        record_audit_event_tx(&mut tx, &apply_event(command, &keepsake, false)).await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented: false,
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

    /// Appends an explicit audit event without mutating lifecycle state.
    pub async fn append_audit_event(&self, event: &AuditEvent) -> RepositoryResult<i64> {
        event.subject.validate()?;
        event.actor.validate()?;

        let mut tx = self.pool.begin().await?;
        let audit_event_id = record_audit_event_tx(&mut tx, event).await?;
        tx.commit().await?;
        Ok(audit_event_id)
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
            where subject_kind = ? and subject_id = ? and state = 'applied'
            order by relation_id, id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
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
        let requested = relation_ids.iter().copied().collect::<BTreeSet<_>>();
        Ok(self
            .active_relations_for_subject(subject)
            .await?
            .into_iter()
            .filter(|active| requested.contains(&active.relation().id))
            .collect())
    }

    /// Returns active keepsakes for a subject, filtered by relation keys.
    pub async fn active_relations_for_subject_by_keys(
        &self,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        let requested = keys.iter().collect::<BTreeSet<_>>();
        Ok(self
            .active_relations_for_subject(subject)
            .await?
            .into_iter()
            .filter(|active| requested.contains(&active.relation().key))
            .collect())
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
            where relation_id = ?
              and state = 'applied'
              and (
                ? is null
                or (subject_kind, subject_id, id) > (?, ?, ?)
              )
            order by subject_kind, subject_id, id
            limit ?
            ",
        )
        .bind(relation_id.to_string())
        .bind(after.map(|cursor| cursor.subject_kind.as_str()))
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
              and k.expires_at <= ?
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Reads the persisted fulfillment counter snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let rows = sqlx::query(
            r"
            select `key`, value
            from keepsake_fulfillment_counters
            where keepsake_id = ?
            ",
        )
        .bind(keepsake_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        counters_from_rows(&rows)
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
            where k.state = 'applied'
              and json_unquote(json_extract(k.expiry_policy, '$.type')) = 'when_fulfilled'
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
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
        let candidates = self.due_fulfilled_expiry(limit).await?;
        let mut expired = 0;
        let mut tx = self.pool.begin().await?;
        for candidate in candidates {
            let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
                continue;
            };
            let snapshot = fulfillment_snapshot_tx(&mut tx, candidate.keepsake_id).await?;
            if policy.is_fulfilled(&snapshot) {
                let result = sqlx::query(
                    r"
                    update keepsakes
                    set state = 'expired', fulfilled_at = ?, updated_at = ?
                    where id = ?
                      and state = 'applied'
                      and exists (
                        select 1
                        from keepsake_relation_definitions r
                        where r.id = keepsakes.relation_id and r.enabled
                      )
                    ",
                )
                .bind(naive_timestamp(now))
                .bind(naive_timestamp(now))
                .bind(candidate.keepsake_id.to_string())
                .execute(&mut *tx)
                .await?;
                expired += result.rows_affected();
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
                set state = 'expired', updated_at = ?
                where id = ?
                  and state = 'applied'
                  and exists (
                    select 1
                    from keepsake_relation_definitions r
                    where r.id = keepsakes.relation_id and r.enabled
                  )
                ",
            )
            .bind(naive_timestamp(now))
            .bind(candidate.keepsake_id.to_string())
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
                (keepsake_id, `key`, value, observed_at)
            values (?, ?, ?, ?)
            on duplicate key update
                value = values(value),
                observed_at = values(observed_at)
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(value)
        .bind(naive_timestamp(observed_at))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl<C> ActiveRelationSource for MySqlKeepsakeRepository<C>
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
    tx: &mut Transaction<'_, MySql>,
    event: &AuditEvent,
) -> RepositoryResult<i64> {
    let result = sqlx::query(
        r"
        insert into keepsake_audit_events
            (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
             event_type, decision, occurred_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
    )
    .bind(event.keepsake_id.to_string())
    .bind(event.relation_id.to_string())
    .bind(&event.subject.kind)
    .bind(&event.subject.id)
    .bind(&event.actor.kind)
    .bind(&event.actor.id)
    .bind(event.event_type.as_str())
    .bind(serde_json::to_value(&event.decision)?)
    .bind(naive_timestamp(event.at))
    .execute(&mut **tx)
    .await?;
    let audit_event_id = i64::try_from(result.last_insert_id())
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;

    for (key, value) in &event.context.attributes {
        sqlx::query(
            r"
            insert into keepsake_audit_context_attributes
                (audit_event_id, `key`, value)
            values (?, ?, ?)
            ",
        )
        .bind(audit_event_id)
        .bind(key)
        .bind(value)
        .execute(&mut **tx)
        .await?;
    }

    Ok(audit_event_id)
}

async fn relation_for_update_tx(
    tx: &mut Transaction<'_, MySql>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select id, kind, `key`, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = ?
        for update
        ",
    )
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, MySql>,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where subject_kind = ? and subject_id = ? and relation_id = ? and state = 'applied'
        for update
        ",
    )
    .bind(&subject.kind)
    .bind(&subject.id)
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, MySql>,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where id = ?
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

async fn revoke_tx(
    tx: &mut Transaction<'_, MySql>,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let keepsake = keepsake_by_id_tx(tx, keepsake_id).await?;
    let Some(keepsake) = keepsake.filter(Keepsake::is_active) else {
        return Ok(None);
    };
    sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?, updated_at = ?
        where id = ? and state = 'applied'
        ",
    )
    .bind(naive_timestamp(at))
    .bind(naive_timestamp(at))
    .bind(keepsake_id.to_string())
    .execute(&mut **tx)
    .await?;
    keepsake_by_id_tx(tx, keepsake.id()).await
}

async fn active_relation_rows_for_subject(
    pool: &sqlx::MySqlPool,
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
            r.`key` as relation_key,
            r.enabled as relation_enabled,
            r.expiry_policy as relation_expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.subject_kind = ? and k.subject_id = ? and k.state = 'applied'
        order by k.relation_id, k.id
        ",
    )
    .bind(&subject.kind)
    .bind(&subject.id)
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
    tx: &mut Transaction<'_, MySql>,
    keepsake_id: Uuid,
) -> RepositoryResult<FulfillmentSnapshot> {
    let rows = sqlx::query(
        r"
        select `key`, value
        from keepsake_fulfillment_counters
        where keepsake_id = ?
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;
    counters_from_rows(&rows)
}

fn relation_from_row(row: &sqlx::mysql::MySqlRow) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
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

fn keepsake_from_row(row: &sqlx::mysql::MySqlRow) -> RepositoryResult<Keepsake> {
    let metadata = serde_json::from_value::<BTreeMap<String, String>>(row.try_get("metadata")?)?;
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(KeepsakeRecord {
        id: parse_uuid(row.try_get("id")?)?,
        subject: SubjectRef {
            kind: row.try_get("subject_kind")?,
            id: row.try_get("subject_id")?,
        },
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        state: parse_state(row.try_get("state")?)?,
        expiry,
        applied_at: utc_timestamp(row.try_get("applied_at")?),
        expires_at: optional_utc_timestamp(row.try_get("expires_at")?),
        fulfilled_at: optional_utc_timestamp(row.try_get("fulfilled_at")?),
        revoked_at: optional_utc_timestamp(row.try_get("revoked_at")?),
        metadata,
    }
    .try_into()?)
}

fn relation_definition_from_active_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("relation_expiry_policy")?)?;
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
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<TimedExpiryCandidate> {
    Ok(TimedExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        due_at: utc_timestamp(row.try_get("due_at")?),
    })
}

#[cfg(feature = "fulfillment-counters")]
fn fulfilled_expiry_candidate_from_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<FulfilledExpiryCandidate> {
    Ok(FulfilledExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        expiry_policy: serde_json::from_value(row.try_get("expiry_policy")?)?,
    })
}

#[cfg(feature = "fulfillment-counters")]
fn counters_from_rows(rows: &[sqlx::mysql::MySqlRow]) -> RepositoryResult<FulfillmentSnapshot> {
    let mut counters = BTreeMap::new();
    for row in rows {
        counters.insert(row.try_get("key")?, row.try_get("value")?);
    }
    Ok(FulfillmentSnapshot {
        counters,
        checklist: BTreeMap::new(),
    })
}

const fn naive_timestamp(value: DateTime<Utc>) -> NaiveDateTime {
    value.naive_utc()
}

const fn utc_timestamp(value: NaiveDateTime) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(value, Utc)
}

fn optional_utc_timestamp(value: Option<NaiveDateTime>) -> Option<DateTime<Utc>> {
    value.map(utc_timestamp)
}
