use keepsake::{
    ActiveRelation, ActiveRelationSource, Keepsake, RelationId, RelationKey, SubjectRef,
};

use super::{
    ActiveRelationRow, AppliedKeepsakeRow, KeepsakeRepository, MembershipCursor, RelationCache,
    RepositoryError, RepositoryResult, validate_limit,
};

impl<C> KeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Returns active keepsakes for a subject.
    pub async fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<Keepsake>> {
        let rows = sqlx::query_as::<_, AppliedKeepsakeRow>(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where subject_kind = $1 and subject_id = $2 and state = 'applied'
            order by relation_id, id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(AppliedKeepsakeRow::try_into_keepsake)
            .collect()
    }

    /// Returns active keepsakes for a subject with their relation definitions.
    pub async fn active_relations_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        let rows = sqlx::query_as::<_, ActiveRelationRow>(
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
            where k.subject_kind = $1 and k.subject_id = $2 and k.state = 'applied'
            order by k.relation_id, k.id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .fetch_all(&self.pool)
        .await?;

        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let active_relation = row.try_into_active_relation()?;
            self.relation_cache.store(active_relation.relation()).await;
            active.push(active_relation);
        }
        Ok(active)
    }

    /// Returns active keepsakes for a subject, filtered by relation ids.
    ///
    /// This is the bounded variant of [`Self::active_relations_for_subject`] for
    /// request paths that use typed relation specs or another stable relation-id
    /// catalogue. Missing ids are ignored, duplicate requested ids do not
    /// duplicate output rows, and disabled relation definitions are still
    /// returned when their keepsake is active.
    pub async fn active_relations_for_subject_by_ids(
        &self,
        subject: &SubjectRef,
        relation_ids: &[RelationId],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if relation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let requested_relation_ids = relation_ids.to_vec();
        let rows = sqlx::query_as::<_, ActiveRelationRow>(
            r"
            with requested_relation_ids(id) as (
                select distinct id
                from unnest($3::uuid[]) as requested(id)
            )
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
            from requested_relation_ids requested
            join keepsake_relation_definitions r
              on r.id = requested.id
            join keepsakes k
              on k.relation_id = r.id
             and k.subject_kind = $1
             and k.subject_id = $2
             and k.state = 'applied'
            order by k.relation_id, k.id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .bind(&requested_relation_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let active_relation = row.try_into_active_relation()?;
            self.relation_cache.store(active_relation.relation()).await;
            active.push(active_relation);
        }
        Ok(active)
    }

    /// Returns active keepsakes for a subject, filtered by relation keys.
    ///
    /// This is the bounded variant of [`Self::active_relations_for_subject`] for
    /// request paths that know the small set of relation keys they care about.
    /// Missing keys are ignored, and disabled relation definitions are still
    /// returned when their keepsake is active.
    pub async fn active_relations_for_subject_by_keys(
        &self,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let kinds = keys
            .iter()
            .map(|key| key.kind().to_owned())
            .collect::<Vec<String>>();
        let names = keys
            .iter()
            .map(|key| key.name().to_owned())
            .collect::<Vec<String>>();

        let rows = sqlx::query_as::<_, ActiveRelationRow>(
            r"
            with requested_relation_keys(kind, key) as (
                select distinct kind, key
                from unnest($3::text[], $4::text[]) as requested(kind, key)
            )
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
            from requested_relation_keys requested
            join keepsake_relation_definitions r
              on r.kind = requested.kind and r.key = requested.key
            join keepsakes k
              on k.relation_id = r.id
             and k.subject_kind = $1
             and k.subject_id = $2
             and k.state = 'applied'
            order by k.relation_id, k.id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .bind(&kinds)
        .bind(&names)
        .fetch_all(&self.pool)
        .await?;

        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let active_relation = row.try_into_active_relation()?;
            self.relation_cache.store(active_relation.relation()).await;
            active.push(active_relation);
        }
        Ok(active)
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
        let rows = sqlx::query_as::<_, AppliedKeepsakeRow>(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where relation_id = $1
              and state = 'applied'
              and (
                $2::text is null
                or (subject_kind, subject_id, id) > ($2, $3, $4)
              )
            order by subject_kind, subject_id, id
            limit $5
            ",
        )
        .bind(relation_id)
        .bind(after.map(|cursor| cursor.subject_kind.as_str()))
        .bind(after.map(|cursor| cursor.subject_id.as_str()))
        .bind(after.map(|cursor| cursor.keepsake_id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(AppliedKeepsakeRow::try_into_keepsake)
            .collect()
    }
}

impl<C> ActiveRelationSource for KeepsakeRepository<C>
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
