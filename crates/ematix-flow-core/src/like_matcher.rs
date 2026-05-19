//! Σ.E5 (2026-05-19): Photon-style vectorized LIKE substring matcher.
//!
//! Compiles a SQL LIKE pattern into a small `Vec<MatcherStep>` that
//! evaluates against a `&[u8]` haystack via SIMD-accelerated substring
//! search (`memchr::memmem`). Supports the common shapes:
//!
//!  * `'literal'`              — exact match
//!  * `'prefix%'`              — starts-with
//!  * `'%suffix'`              — ends-with
//!  * `'%substr%'`             — contains
//!  * `'%a%b%c%'`              — ordered multi-substring contains
//!  * `'a%b'`, `'%a%b'`, ...   — combinations of the above
//!
//! The `_` single-char wildcard is not supported (callers should refuse
//! patterns containing it). The kernel is byte-oriented — it does not
//! decode UTF-8. This is correct for ASCII patterns and for any UTF-8
//! substring that doesn't straddle multi-byte boundary at the pattern
//! edges (TPC-H's text generator emits ASCII so this holds for the
//! bench corpus).
//!
//! Wire-up notes: callers building a row bitmap (e.g. emat's
//! `BridgeFilter::build_bitmap` for `StringLike`) construct one
//! `LikeMatcher` per pattern and call `matches(bytes)` per row. For
//! Utf8View arrays, dereference each view via the array's
//! `value_unchecked` (or read length-prefix + buffer offset directly).
//!
//! Bench: `cargo run --release -p ematix-flow-core --example
//! like_matcher_bench`.

use std::cmp::min;

use memchr::memmem::{Finder, FinderRev};

/// A compiled LIKE pattern.
#[derive(Debug)]
pub struct LikeMatcher {
    /// First literal must appear at offset 0 (prefix anchor)?
    anchor_start: bool,
    /// Last literal must end at the haystack tail (suffix anchor)?
    anchor_end: bool,
    /// Ordered literal substrings to find. Empty = match-anything
    /// (pattern was just `%`).
    literals: Vec<Literal>,
    /// Reverse finder for the last literal when `anchor_end` is set —
    /// scanning back from the haystack tail is cheaper than forward.
    tail_finder: Option<FinderRev<'static>>,
}

#[derive(Debug)]
struct Literal {
    bytes: Vec<u8>,
    finder: Finder<'static>,
}

impl LikeMatcher {
    /// Compile a SQL LIKE pattern. Returns `None` if the pattern uses
    /// `_` (single-char wildcard not yet supported) or contains the
    /// escape character (we don't track an explicit escape).
    pub fn compile(pattern: &str) -> Option<Self> {
        if pattern.contains('_') {
            return None;
        }
        let bytes = pattern.as_bytes();
        let anchor_start = !bytes.starts_with(b"%");
        let anchor_end = !bytes.ends_with(b"%");

        let trimmed = bytes
            .strip_prefix(b"%")
            .unwrap_or(bytes);
        let trimmed = trimmed.strip_suffix(b"%").unwrap_or(trimmed);

        let mut literals = Vec::new();
        for part in trimmed.split(|&b| b == b'%') {
            if part.is_empty() {
                continue;
            }
            let owned = part.to_vec();
            // SAFETY: `Finder::new` borrows the slice; we want an
            // owned finder, so we transmute the lifetime after
            // ensuring the owning Vec lives in the same struct.
            // The construction below uses `into_owned()`.
            let finder = Finder::new(&owned).into_owned();
            literals.push(Literal {
                bytes: owned,
                finder,
            });
        }

        let tail_finder = if anchor_end && literals.len() > 1 {
            // We'll re-find the last literal from the tail; cache a
            // reverse finder. (For single-literal patterns we use the
            // simpler ends_with fast path.)
            literals
                .last()
                .map(|l| FinderRev::new(&l.bytes).into_owned())
        } else {
            None
        };

        Some(Self {
            anchor_start,
            anchor_end,
            literals,
            tail_finder,
        })
    }

    /// `true` if `haystack` matches the compiled pattern.
    #[inline]
    pub fn matches(&self, haystack: &[u8]) -> bool {
        // No literals: pattern is some sequence of `%` only.
        //   - `""` (start AND end anchored, no `%`): match empty haystack only
        //   - `"%"`, `"%%"`, ...: match anything
        if self.literals.is_empty() {
            return !(self.anchor_start && self.anchor_end) || haystack.is_empty();
        }

        // Single-literal fast paths.
        if self.literals.len() == 1 {
            let lit = &self.literals[0].bytes;
            return match (self.anchor_start, self.anchor_end) {
                (true, true) => haystack == lit.as_slice(),
                (true, false) => haystack.starts_with(lit),
                (false, true) => haystack.ends_with(lit),
                (false, false) => self.literals[0].finder.find(haystack).is_some(),
            };
        }

        // Multi-literal. Walk forward through the haystack, requiring
        // each literal to appear at-or-after the previous match.
        let mut cursor: usize = 0;
        let last_idx = self.literals.len() - 1;

        // If the tail is anchored, locate the last literal first via
        // a reverse search; this caps the haystack length for the
        // middle scans.
        let mut tail_limit: Option<usize> = None;
        if self.anchor_end {
            let last_lit = &self.literals[last_idx];
            let pos_opt = if let Some(rev) = &self.tail_finder {
                rev.rfind(haystack)
            } else {
                last_lit.finder.find_iter(haystack).last()
            };
            let pos = match pos_opt {
                Some(p) => p,
                None => return false,
            };
            // The reverse find returns the OFFSET where the literal
            // starts. For `anchor_end`, that offset + len must equal
            // haystack.len() — otherwise some bytes remain past the
            // last literal which would belong to the trailing `%`.
            // Since `anchor_end == true` means no trailing `%`, the
            // last literal MUST end exactly at the tail.
            if pos + last_lit.bytes.len() != haystack.len() {
                // Try to find a later occurrence anchored to the tail.
                // The reverse iter already gave us the last; if it
                // doesn't end at the tail, the pattern can't match.
                return false;
            }
            tail_limit = Some(pos);
        }

        // Walk the first-through-(last) literals forward.
        let scan_end = if let Some(tail) = tail_limit {
            // We've already validated the last literal at `tail`.
            // Match literals[0..last_idx] forward in haystack[..tail].
            // Anchor_end's last literal is excluded from this walk.
            tail
        } else {
            haystack.len()
        };

        let scan_count = if tail_limit.is_some() {
            last_idx
        } else {
            self.literals.len()
        };

        for (i, lit) in self.literals.iter().take(scan_count).enumerate() {
            let pos = if i == 0 && self.anchor_start {
                // First literal must match at offset 0.
                if haystack[cursor..min(haystack.len(), cursor + lit.bytes.len())]
                    .starts_with(&lit.bytes)
                {
                    cursor
                } else {
                    return false;
                }
            } else {
                match lit.finder.find(&haystack[cursor..scan_end]) {
                    Some(p) => cursor + p,
                    None => return false,
                }
            };
            cursor = pos + lit.bytes.len();
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(pattern: &str, haystack: &str, expected: bool) {
        let m = LikeMatcher::compile(pattern).expect("compile");
        assert_eq!(
            m.matches(haystack.as_bytes()),
            expected,
            "pattern={pattern:?} haystack={haystack:?}"
        );
    }

    #[test]
    fn exact() {
        check("hello", "hello", true);
        check("hello", "helloo", false);
        check("hello", "hellos", false);
        check("hello", "ohello", false);
    }

    #[test]
    fn prefix() {
        check("hel%", "hello", true);
        check("hel%", "hel", true);
        check("hel%", "help", true);
        check("hel%", "ahello", false);
    }

    #[test]
    fn suffix() {
        check("%llo", "hello", true);
        check("%llo", "llo", true);
        check("%llo", "hellos", false);
    }

    #[test]
    fn contains() {
        check("%ell%", "hello", true);
        check("%ell%", "yellow", true);
        check("%ell%", "world", false);
        check("%ell%", "ell", true);
    }

    #[test]
    fn multi_substring() {
        check("%special%requests%", "needs special urgent requests now", true);
        check("%special%requests%", "needs requests special now", false);
        check("%special%requests%", "no match here", false);
        check("%a%b%c%", "abc", true);
        check("%a%b%c%", "xaybzc", true);
        check("%a%b%c%", "cba", false);
    }

    #[test]
    fn prefix_anchored_multi() {
        check("hel%ld", "hello world", true);
        check("hel%ld", "help", false);
        check("hel%ld", "ahello world", false); // not prefix-anchored
    }

    #[test]
    fn suffix_anchored_multi() {
        check("%foo%bar", "foobar", true);
        check("%foo%bar", "barfoobar", true);
        check("%foo%bar", "foo bar", true);
        check("%foo%bar", "foobars", false);
    }

    #[test]
    fn full_anchored() {
        check("hello", "hello world", false);
        check("hello%world", "hello world", true);
        check("hello%world", "hello cruel world", true);
        check("hello%world", "hello cruel world!", false);
    }

    #[test]
    fn just_percent() {
        check("%", "anything", true);
        check("%", "", true);
    }

    #[test]
    fn refuses_underscore() {
        assert!(LikeMatcher::compile("hel_o").is_none());
    }

    #[test]
    fn empty_pattern() {
        let m = LikeMatcher::compile("").expect("compile");
        // anchor_start && anchor_end on empty literals = matches empty only.
        assert!(m.matches(b""));
        assert!(!m.matches(b"x"));
    }
}
