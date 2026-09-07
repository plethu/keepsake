//! Canonical comparisons for published SQL artifacts and catalog deparser forms.

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(super) fn normalize_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    for line in sql.lines() {
        let line = line.split_once("--").map_or(line, |(line, _)| line);
        if !line.trim().is_empty() {
            if !normalized.is_empty() {
                normalized.push(' ');
            }

            normalized.push_str(line.trim());
        }
    }

    normalized
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn default_sql(sql: &str) -> String {
    normalize_sql(sql)
        .replace("_utf8mb4", "")
        .replace("_utf8mb3", "")
        .replace("\\'", "'")
        .trim_matches('(')
        .trim_matches(')')
        .trim_matches('\'')
        .to_owned()
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(super) fn compact_sql(sql: &str) -> String {
    normalize_sql(sql)
        .replace([' ', '\n', '`'], "")
        .replace("_utf8mb4", "")
        .replace("_utf8mb3", "")
        .replace("_latin1", "")
        .replace("\\'", "'")
}

#[cfg(feature = "mysql")]
pub(super) fn normalize_mysql_generated_expression(expression: &str) -> String {
    let mut normalized = compact_sql(expression).replace("\\'", "'");
    // MySQL's generated-column deparser wraps the whole CASE and its WHEN
    // predicate, and makes the implicit ELSE NULL explicit. Those are
    // equivalent representations of the migration's expression, while the
    // function calls and predicates inside them remain byte-for-byte strict.
    normalized = strip_sql_outer_groups(&normalized).to_owned();

    if let Some(predicate) = normalized.strip_prefix("casewhen(") {
        normalized = format!("casewhen{}", predicate.replacen(")then", "then", 1));
    }

    while normalized.starts_with("casewhen(") {
        normalized = normalized.replacen("casewhen(", "casewhen", 1);
    }
    // The catalog may also parenthesize one atomic predicate inside the CASE
    // condition. These deparser boundaries surround atomic predicates; no
    // parentheses containing an AND/OR expression are removed.
    normalized
        .replace(")and(", "and")
        .replace(")then", "then")
        .replace("elsenullend", "end")
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn sql_group_end(expression: &str) -> Option<usize> {
    if !expression.starts_with('(') {
        return None;
    }

    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (offset, character) in expression.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        if quoted && character == '\\' {
            escaped = true;
            continue;
        }

        if character == '\'' {
            quoted = !quoted;
            continue;
        }

        if quoted {
            continue;
        }

        match character {
            '(' => depth = depth.checked_add(1)?,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return offset.checked_add(character.len_utf8());
                }
            }
            _ => {}
        }
    }

    None
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn strip_sql_outer_groups(mut expression: &str) -> &str {
    while sql_group_end(expression) == Some(expression.len()) {
        let Some(inner) = expression
            .strip_prefix('(')
            .and_then(|text| text.strip_suffix(')'))
        else {
            break;
        };

        expression = inner;
    }

    expression
}

#[cfg(any(feature = "mysql", feature = "postgres"))]
pub(super) fn artifact_check_expression(artifact: &str, marker: &str) -> Option<String> {
    let artifact = normalize_sql(artifact);
    let marker_start = artifact.find(&normalize_sql(marker))?;
    let suffix = artifact.get(marker_start..)?;
    let check = suffix.get(suffix.find("check")?..)?;
    let expression = check.get(check.find('(')?..)?;
    expression
        .get(..sql_group_end(expression)?)
        .map(str::to_owned)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn identifier_check_matches(actual: &str, expected: &str) -> bool {
    // These artifacts contain only AND-connected length/trim predicates, no
    // literals or arithmetic. Catalogs add grouping parentheses to individual
    // predicates. Removing those groups is safe only for this narrow contract;
    // keep every operand, operator, and conjunction in the comparison.
    if actual.contains(['\'', '"']) || actual.contains("--") || actual.contains("/*") {
        return false;
    }

    if !balanced_identifier_groups(actual) || !balanced_identifier_groups(expected) {
        return false;
    }

    let normalize =
        |expression: &str| normalize_check_expression(expression).replace(['(', ')'], "");
    normalize(actual) == normalize(expected)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn identifier_check_from_artifact(
    artifact: &str,
    table: &str,
    name: &str,
) -> Option<String> {
    // Include the table in the marker so a valid constraint on the wrong table
    // cannot satisfy another table's identifier contract.
    artifact_check_expression(
        artifact,
        &format!("alter table {table} add constraint {name} check"),
    )
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(super) fn normalize_check_expression(expression: &str) -> String {
    let compact = compact_sql(expression);
    let expression = compact
        .strip_prefix("check(")
        .map_or(compact.as_str(), |_| {
            compact.strip_prefix("check").unwrap_or(&compact)
        });

    let mut normalized = strip_sql_outer_groups(expression).to_owned();
    // PostgreSQL annotates inferred literal types and deparses IN as ANY.
    for cast in ["::text", "::timestamptz", "::timestampwithtimezone"] {
        normalized = normalized.replace(cast, "");
    }
    normalize_in_list(&normalized)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn normalize_in_list(expression: &str) -> String {
    let mut normalized = expression.to_owned();
    while let Some(in_offset) = normalized.find("in(") {
        let Some(prefix) = normalized.get(..in_offset) else {
            break;
        };

        let Some(left_start) = in_list_operand_start(prefix) else {
            break;
        };

        let Some(expression) = normalized
            .get(in_offset..)
            .and_then(|text| text.strip_prefix("in"))
        else {
            break;
        };

        let Some(group_end) = sql_group_end(expression) else {
            break;
        };

        let Some(group) = expression.get(..group_end) else {
            break;
        };

        let Some(values) = group
            .strip_prefix('(')
            .and_then(|text| text.strip_suffix(')'))
        else {
            break;
        };

        let Some(left) = prefix.get(left_start..) else {
            break;
        };

        let Some(tail) = expression.get(group_end..) else {
            break;
        };

        let Some(head) = prefix.get(..left_start) else {
            break;
        };

        normalized = format!("{head}{left}=any(array[{values}]){tail}");
    }

    normalized
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn in_list_operand_start(prefix: &str) -> Option<usize> {
    if prefix.ends_with(')') {
        return grouped_operand_start(prefix);
    }

    prefix
        .char_indices()
        .rev()
        .take_while(|(_, character)| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '\'' | '-' | '>')
        })
        .last()
        .map(|(offset, _)| offset)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn grouped_operand_start(prefix: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, character) in prefix.char_indices().rev() {
        match character {
            ')' => depth = depth.checked_add(1)?,
            '(' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
    }

    None
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
pub(super) fn artifact_object_sql<'a>(
    artifact: &'a str,
    kind: &str,
    name: &str,
) -> Option<&'a str> {
    let lower = artifact.to_ascii_lowercase();
    let marker = format!("create {kind} {name}");
    let start = lower.find(&marker)?;
    let remainder = lower.get(start..)?;
    let terminator = if kind == "trigger" { "\nend;" } else { ";" };

    let end = remainder.find(terminator)?.checked_add(terminator.len())?;
    artifact.get(start..)?.get(..end)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn balanced_identifier_groups(expression: &str) -> bool {
    expression
        .chars()
        .try_fold(0_usize, |depth, character| match character {
            '(' => depth.checked_add(1),
            ')' => depth.checked_sub(1),
            _ => Some(depth),
        })
        == Some(0)
}
