# Task #557 — Arrow-native warehouse adapters (design)

## Goal

Drop pandas from the `[snowflake]` / `[bigquery]` / `[redshift]` /
`[warehouses]` extras and from the runtime call paths in
`python/ematix_flow/warehouses.py`. The current adapters route
Arrow → pandas → SDK; the pandas hop is a real perf bottleneck for
ematix-flow's "Rust + Arrow, no pandas" pitch.

Memory: see `feedback_no_pandas_in_warehouse_path.md`.

## Per-warehouse plan

### Snowflake — PUT + COPY INTO via parquet staging

Today's path (`snowflake_write_arrow`):
```python
df = table.to_pandas()                  # ← pandas dep
success, _, n, _ = write_pandas(client, df, table_name=...)
```

Arrow-native path:
```python
import tempfile
import pyarrow.parquet as pq

# 1. Write Arrow → parquet to a temp file. Pure pyarrow.
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
    pq.write_table(table, tmp.name)
    parquet_path = tmp.name

# 2. PUT to the table's session stage. Snowflake auto-creates @%table.
cur = client.cursor()
cur.execute(f"PUT file://{parquet_path} @%{quote_id(table_name)} "
            "OVERWRITE = TRUE AUTO_COMPRESS = FALSE")

# 3. COPY INTO. MATCH_BY_COLUMN_NAME handles column ordering.
cur.execute(
    f"COPY INTO {quote_id(table_name)} "
    f"FROM @%{quote_id(table_name)}/{basename(parquet_path)} "
    "FILE_FORMAT = (TYPE = PARQUET) "
    "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
    "PURGE = TRUE"
)

# 4. Read row count from cursor.rowcount or last result.
```

For `create_if_not_exists=True`, derive DDL from the Arrow schema:

```python
def arrow_schema_to_snowflake_ddl(table_name: str, schema: pa.Schema) -> str:
    cols = []
    for field in schema:
        sql_type = arrow_to_snowflake_type(field.type)
        nullable = "" if field.nullable else " NOT NULL"
        cols.append(f"{quote_id(field.name)} {sql_type}{nullable}")
    return (
        f"CREATE TABLE IF NOT EXISTS {quote_id(table_name)} "
        f"({', '.join(cols)})"
    )

def arrow_to_snowflake_type(t: pa.DataType) -> str:
    # Snowflake's PARQUET file format auto-handles type coercion, so
    # we just need approximate target types. Conservative mapping:
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        return "NUMBER(10)"
    if pa.types.is_int64(t):
        return "NUMBER(19)"
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t):
        return "NUMBER(10)"
    if pa.types.is_uint64(t):
        return "NUMBER(20)"
    if pa.types.is_float32(t):
        return "FLOAT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR(16777216)"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP_NTZ" if t.tz is None else "TIMESTAMP_TZ"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "BINARY"
    if pa.types.is_decimal(t):
        return f"NUMBER({t.precision},{t.scale})"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return "ARRAY"
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return "OBJECT"
    # Unknown → VARIANT (Snowflake's JSON-shaped catch-all).
    return "VARIANT"
```

**Identifier quoting** — Snowflake folds unquoted identifiers to
uppercase. To preserve case from the Arrow schema, double-quote
every identifier on write (`quote_id(name) = f'"{name}"'`).

**Cleanup** — `delete=False` on the temp file because Windows can't
delete an open file; explicit `os.unlink(parquet_path)` in a finally.

### BigQuery — Write Storage API (gRPC + Arrow IPC)

Today's path:
```python
df = table.to_pandas()                  # ← pandas dep
job = client.load_table_from_dataframe(df, table_name)
job.result()
```

Arrow-native path uses the Storage Write API directly:
```python
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest, ProtoSchema, ProtoRows,
)

# Approach A — Arrow IPC streaming (preferred):
# Use bigquery_storage's WriteSession with TYPE_ARROW. Send the
# Arrow table as IPC framed batches; BigQuery converts server-side.
```

Two complexities here:
1. The Storage Write API still wants you to call `Append`-shaped
   protobuf messages, even with the Arrow type. The pyarrow IPC
   conversion is one helper function but not zero.
2. For tables that don't exist yet, `load_table_from_uri` + parquet
   staging on GCS is simpler. We can ship both paths and let the
   adapter pick based on a `staging_bucket=` parameter.

**Pragmatic slice 1 for BigQuery:** stage to GCS as parquet, then
`load_table_from_uri` with `source_format="PARQUET"`. No pandas, no
Storage Write API complexity. Faster than `load_table_from_dataframe`
for >100MB tables anyway.

### Redshift — S3 staging + COPY (extend existing merge-mode path)

Today's append-mode path goes through pandas + `redshift-connector`'s
DataFrame loader. Merge mode already stages to S3 and runs `COPY FROM`.

Arrow-native append:
```python
import boto3
import pyarrow.parquet as pq

# 1. Write Arrow → parquet to S3 staging (already configured via
#    target.s3_staging_bucket per existing merge-mode contract).
key = f"ematix-flow-staging/{uuid.uuid4()}.parquet"
buf = io.BytesIO()
pq.write_table(table, buf)
boto3.client("s3").put_object(
    Bucket=target.s3_staging_bucket, Key=key, Body=buf.getvalue()
)

# 2. COPY FROM. IAM role attached to the cluster (the supported
#    auth mode for production today) handles credentials.
cur.execute(
    f"COPY {quote_id(table_name)} "
    f"FROM 's3://{target.s3_staging_bucket}/{key}' "
    "IAM_ROLE default FORMAT AS PARQUET"
)

# 3. Cleanup S3 staging object.
boto3.client("s3").delete_object(Bucket=target.s3_staging_bucket, Key=key)
```

Effectively the merge-mode path generalised to append. The
`s3_staging_bucket` becomes mandatory for Redshift writes (it was
already mandatory for merge mode).

## Test strategy

Each warehouse needs a unit-test layer that:
1. Monkey-patches the SDK's `cursor.execute` to capture issued SQL.
2. Asserts the SQL contains the expected PUT/COPY/CREATE statements.
3. Asserts pandas is **never imported** (via `sys.modules` snapshot before and after).

Integration tests against real warehouses are gated on env credentials
(skip unless `EMATIX_FLOW_SNOWFLAKE_TEST_ACCOUNT` etc. is set).

## Migration

- Slice 1 (this design + Snowflake implementation): land Snowflake's
  Arrow-native path. Drop `pandas` from the `[snowflake]` extra.
- Slice 2: Redshift S3+COPY append path. Drop `pandas` from
  `[redshift]`.
- Slice 3: BigQuery GCS+`load_table_from_uri` path. Drop `pandas`
  from `[bigquery]`.
- Slice 4: Drop `pandas` from `[warehouses]` aggregate and from CI's
  install line. Verify the existing test suite still passes without
  pandas installed.

## Open questions for review

1. **Snowflake type fidelity** — `arrow_to_snowflake_type()` maps
   conservatively (e.g. `int32` → `NUMBER(10)`, not `NUMBER(10,0)`).
   For users with strict downstream type expectations, do we expose a
   user-provided override (`column_types={col: "NUMBER(38,9)"}` on
   `WarehouseTarget.snowflake_table(...)`) or auto-promote
   everything to `VARIANT` and let the user cast in SQL?
2. **BigQuery without GCS staging** — Storage Write API works without
   an external staging bucket but adds gRPC dependency complexity. Is
   the simpler GCS+`load_table_from_uri` path acceptable as the v1, or
   should slice 1 go straight to Storage Write API?
3. **Backward-compat shim** — keep the existing pandas-based
   functions as deprecated aliases (`snowflake_write_arrow_pandas`)
   for one release, or hard-cut at v0.5.0?

## Status

Design doc only. Implementation pending review of the above.
