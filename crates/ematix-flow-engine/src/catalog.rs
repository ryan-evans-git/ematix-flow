//! P3 catalog: the binder's name-resolution surface. Maps a table name to
//! its parquet path and column schema — `column name → (parquet leaf index,
//! engine logical type)`.
//!
//! Schemas are **registered by the caller** — explicitly via
//! [`Catalog::register_table`], or derived from a file's own footer via
//! [`Catalog::register_parquet`] — never hardcoded in engine code, per the
//! no-benchmark-in-the-engine rule. Leaf indices address the native scan
//! ([`crate::scan_native`]); the binder turns names into positions so the
//! bound plan never carries a name past the catalog boundary.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use parquet::basic::{ConvertedType, LogicalType as PqLogical, Type as PhysType};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::vector::LogicalType;

/// One column of a registered table.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    /// Parquet leaf index (schema order) — what the native scan decodes by.
    pub leaf: usize,
    pub ty: LogicalType,
    /// `Some(s)` when the file stores this column as an INT32/INT64-backed
    /// parquet `DECIMAL(p, s)` — the decode divides by `10^s` into the
    /// engine's `Float64`. `None` = storage matches `ty` directly.
    pub dec_scale: Option<u8>,
    /// The column may contain NULLs (parquet `optional`) — the decode
    /// reads definition levels into the vector's validity.
    pub nullable: bool,
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
                        dec_scale: None,
                        nullable: false,
                    })
                    .collect(),
            },
        );
    }

    /// Register `name` with a schema derived from `path`'s own parquet
    /// footer: every leaf becomes a column (leaf index = schema order),
    /// with the engine logical type, decimal scale, and nullability read
    /// from the file metadata. Re-registering a name replaces it.
    pub fn register_parquet(&mut self, name: &str, path: impl Into<PathBuf>) -> Result<(), String> {
        let path = path.into();
        let file = File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let reader = SerializedFileReader::new(file).map_err(|e| format!("parquet open: {e}"))?;
        let descr = reader.metadata().file_metadata().schema_descr();
        let mut columns = Vec::with_capacity(descr.num_columns());
        for leaf in 0..descr.num_columns() {
            let col = descr.column(leaf);
            let phys = col.physical_type();
            let logical = col.logical_type_ref();
            let converted = col.converted_type();
            let dec_of = |scale: i32| -> Result<Option<u8>, String> {
                u8::try_from(scale).map(Some).map_err(|_| {
                    format!("column {}: unsupported decimal scale {scale}", col.name())
                })
            };
            let (ty, dec_scale) = match (phys, logical) {
                (PhysType::INT32, Some(PqLogical::Date)) => (LogicalType::Date32, None),
                (PhysType::INT32, Some(PqLogical::Decimal { scale, .. })) => {
                    (LogicalType::Float64, dec_of(*scale)?)
                }
                (PhysType::INT32, _) if converted == ConvertedType::DATE => {
                    (LogicalType::Date32, None)
                }
                (PhysType::INT32, _) => (LogicalType::Int32, None),
                (PhysType::INT64, Some(PqLogical::Decimal { scale, .. })) => {
                    (LogicalType::Float64, dec_of(*scale)?)
                }
                (PhysType::INT64, _) => (LogicalType::Int64, None),
                (PhysType::DOUBLE, _) => (LogicalType::Float64, None),
                (PhysType::BYTE_ARRAY, _) => (LogicalType::Utf8, None),
                (other, _) => {
                    return Err(format!(
                        "column {}: unsupported parquet physical type {other} in {}",
                        col.name(),
                        path.display()
                    ));
                }
            };
            columns.push(ColumnDef {
                name: col.name().to_string(),
                leaf,
                ty,
                dec_scale,
                nullable: col.self_type().is_optional(),
            });
        }
        self.tables
            .insert(name.to_string(), TableDef { path, columns });
        Ok(())
    }

    /// Look a table up by name.
    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }
}
