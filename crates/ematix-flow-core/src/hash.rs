//! Canonical encoding + SHA-256 for SCD2 row hashing.
//!
//! Encoding (per column): `coalesce(value, "\x00NULL\x00")`. Columns are
//! joined by `\x01`. SHA-256 over the resulting UTF-8 bytes. The Postgres
//! equivalent uses `pgcrypto.digest(...)` over the same byte sequence so
//! the same row produces the same hash whether computed in Rust or in SQL.

use sha2::{Digest, Sha256};

const NULL_MARKER: &[u8] = b"\x00NULL\x00";
const SEPARATOR: u8 = 0x01;

pub fn canonical_encoding(values: &[Option<&str>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push(SEPARATOR);
        }
        match v {
            None => buf.extend_from_slice(NULL_MARKER),
            Some(s) => buf.extend_from_slice(s.as_bytes()),
        }
    }
    buf
}

pub fn sha256_row_hash(values: &[Option<&str>]) -> [u8; 32] {
    let encoded = canonical_encoding(values);
    let mut hasher = Sha256::new();
    hasher.update(&encoded);
    hasher.finalize().into()
}

/// Build the Postgres SQL expression that produces the equivalent of
/// `sha256_row_hash` for a row from `prefix` (e.g. `q.` or `_stage_x.`).
/// The Postgres expression is:
///   digest(
///     coalesce(col1::text, E'\\x00NULL\\x00') || E'\\x01' ||
///     coalesce(col2::text, E'\\x00NULL\\x00') || ..., 'sha256'
///   )
pub fn postgres_digest_expression(columns: &[String], prefix: &str) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| {
            format!(
                "coalesce({prefix}{c}::text, E'\\\\x00NULL\\\\x00')",
                prefix = prefix,
                c = c,
            )
        })
        .collect();
    format!("digest({}, 'sha256')", parts.join(" || E'\\\\x01' || "))
}

#[cfg(test)]
mod tests {
    use crate::hash::{canonical_encoding, sha256_row_hash};

    #[test]
    fn null_marker_is_distinguishable_from_empty_and_value() {
        let null_only = canonical_encoding(&[None]);
        let empty = canonical_encoding(&[Some("")]);
        let value = canonical_encoding(&[Some("NULL")]);
        assert_ne!(null_only, empty);
        assert_ne!(null_only, value);
    }

    #[test]
    fn separator_keeps_concatenations_distinct() {
        // ("ab", "c") and ("a", "bc") would collide without a separator.
        let a = canonical_encoding(&[Some("ab"), Some("c")]);
        let b = canonical_encoding(&[Some("a"), Some("bc")]);
        assert_ne!(a, b);
    }

    #[test]
    fn hash_is_stable_for_identical_input() {
        let a = sha256_row_hash(&[Some("alice"), Some("a@x.com")]);
        let b = sha256_row_hash(&[Some("alice"), Some("a@x.com")]);
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn hash_changes_with_value() {
        let a = sha256_row_hash(&[Some("alice"), Some("a@x.com")]);
        let b = sha256_row_hash(&[Some("alice"), Some("a2@x.com")]);
        assert_ne!(a, b);
    }

    #[test]
    fn hash_distinguishes_null_from_value() {
        let with_null = sha256_row_hash(&[Some("alice"), None]);
        let with_value = sha256_row_hash(&[Some("alice"), Some("x")]);
        assert_ne!(with_null, with_value);
    }

    #[test]
    fn hash_distinguishes_value_to_null() {
        // 'x' → NULL must be detected as a change.
        let before = sha256_row_hash(&[Some("alice"), Some("x")]);
        let after = sha256_row_hash(&[Some("alice"), None]);
        assert_ne!(before, after);
    }

    #[test]
    fn hash_is_order_dependent_in_inputs() {
        // We don't sort columns inside the hash — the caller chooses the
        // canonical order. This test pins that contract.
        let ab = sha256_row_hash(&[Some("a"), Some("b")]);
        let ba = sha256_row_hash(&[Some("b"), Some("a")]);
        assert_ne!(ab, ba);
    }
}
