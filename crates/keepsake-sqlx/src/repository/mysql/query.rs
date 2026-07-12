use std::collections::BTreeSet;

use keepsake::{
    ActiveRelation, ActiveRelationSource, Keepsake, RelationDefinition, RelationId, RelationKey,
    SubjectRef,
};
use sqlx::{MySql, QueryBuilder};

use crate::repository::{
    MembershipCursor, MySqlKeepsakeRepository, RelationCache, RepositoryError, RepositoryResult,
    validate_limit,
};

use super::rows::{keepsake_from_row, relation_definition_from_active_row};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
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
    ///
    /// Missing and duplicate requested ids are ignored.
    pub async fn active_relations_for_subject_by_ids(
        &self,
        subject: &SubjectRef,
        relation_ids: &[RelationId],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if relation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let relation_ids = relation_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut query = QueryBuilder::<MySql>::new(ACTIVE_RELATION_SELECT);
        query
            .push(" where k.subject_kind = ")
            .push_bind(subject.kind())
            .push(" and k.subject_id = ")
            .push_bind(subject.id())
            .push(" and k.state = 'applied' and k.relation_id in (");
        {
            let mut separated = query.separated(", ");
            for relation_id in relation_ids {
                separated.push_bind(relation_id.to_string());
            }
        }
        query.push(") order by k.relation_id, k.id");

        let rows = query.build().fetch_all(&self.pool).await?;
        self.active_relations_from_rows(&rows).await
    }

    /// Returns active keepsakes for a subject, filtered by relation keys.
    ///
    /// Missing and duplicate requested keys are ignored.
    pub async fn active_relations_for_subject_by_keys(
        &self,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let keys = keys
            .iter()
            .map(|key| (key.kind(), key.name()))
            .collect::<BTreeSet<_>>();
        let mut query = QueryBuilder::<MySql>::new(ACTIVE_RELATION_SELECT);
        query
            .push(" where k.subject_kind = ")
            .push_bind(subject.kind())
            .push(" and k.subject_id = ")
            .push_bind(subject.id())
            .push(" and k.state = 'applied' and (");
        {
            let mut separated = query.separated(" or ");
            for (kind, name) in keys {
                separated
                    .push("(r.kind = ")
                    .push_bind_unseparated(kind)
                    .push_unseparated(" and r.`key` = ")
                    .push_bind_unseparated(name)
                    .push_unseparated(")");
            }
        }
        query.push(") order by k.relation_id, k.id");

        let rows = query.build().fetch_all(&self.pool).await?;
        self.active_relations_from_rows(&rows).await
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

    async fn active_relations_from_rows(
        &self,
        rows: &[sqlx::mysql::MySqlRow],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let relation = relation_definition_from_active_row(row)?;
            self.relation_cache.store(&relation).await;
            active.push(ActiveRelation::new(keepsake_from_row(row)?, relation)?);
        }
        Ok(active)
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

pub(super) async fn active_relation_rows_for_subject(
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

const ACTIVE_RELATION_SELECT: &str = r"
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
";
