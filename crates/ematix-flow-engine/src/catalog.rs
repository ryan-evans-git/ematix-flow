//! P3 catalog: the binder's name-resolution surface. Maps a table name to
//! its parquet path and column schema — `column name → (parquet leaf index,
//! engine logical type)`.
//!
//! Schemas are **registered by the caller** (a harness, a session layer, or
//! later a parquet-metadata reader) — never hardcoded in engine code, per
//! the no-TPC-H-in-the-engine rule. Leaf indices address the native scan
//! ([`crate::scan_native`]); the binder turns names into positions so the
//! bound plan never carries a name past the catalog boundary.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::vector::LogicalType;

/// One column of a registered table.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    /// Parquet leaf index (schema order) — what the native scan decodes by.
    pub leaf: usize,
    pub ty: LogicalType,
}

/// A registered table: where it lives and what its columns are.
#[derive(Clone, Debug, PartialEq)]
pub struct TableDef {
    pub path: PathBuf,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    /// Look a column up by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// The binder's table registry.
#[derive(Default, Clone, Debug)]
pub struct Catalog {
    tables: HashMap<String, TableDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `name` → (`path`, `columns` as `(name, leaf, type)`).
    /// Re-registering a name replaces the previous definition.
    pub fn register_table(
        &mut self,
        name: &str,
        path: impl Into<PathBuf>,
        columns: &[(&str, usize, LogicalType)],
    ) {
        self.tables.insert(
            name.to_string(),
            TableDef {
                path: path.into(),
                columns: columns
                    .iter()
                    .map(|&(n, leaf, ty)| ColumnDef {
                        name: n.to_string(),
                        leaf,
                        ty,
                    })
                    .collect(),
            },
        );
    }

    /// Look a table up by name.
    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }
}
