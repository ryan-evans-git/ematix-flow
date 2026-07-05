#!/usr/bin/env bash
# Register the 8 TPC-H tables with the Hive (Glue) catalog for a given
# scale factor. Idempotent: re-running issues CREATE SCHEMA IF NOT EXISTS
# + CREATE TABLE IF NOT EXISTS, so a partially-failed run is recoverable.
#
# Why this script exists: Trino's Hive connector treats `external_location`
# as a *directory*, but our pipeline uploads each TPC-H table as a single
# file at s3://$BENCH_BUCKET/tpch-data/sf{N}/<table>.parquet. We reorganise
# once into s3://$BENCH_BUCKET/tpch-data/sf{N}/<table>/<table>.parquet so
# the directory shape matches what Hive expects. PySpark + ematix don't
# care which layout we use; this is the cheapest concession.
#
# Usage (run from coordinator only):
#   register-tables.sh --sf {10|100}
#
# Required env:
#   BENCH_BUCKET   S3 bucket (no s3:// prefix)
#   AWS_REGION     (optional; falls back to AWS CLI default)
set -euo pipefail

SF=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        -h|--help) sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//;/^set -euo/d'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done
if [[ "$SF" != "10" && "$SF" != "100" ]]; then
    echo "--sf must be 10 or 100 (got: ${SF:-<empty>})" >&2
    exit 2
fi
if [[ -z "${BENCH_BUCKET:-}" ]]; then
    echo "BENCH_BUCKET env var is required" >&2; exit 2
fi

SCHEMA="tpch_sf${SF}"
# DATA_PREFIX selects the S3 layout: tpch-data (single file per table) or
# tpch-data-parted (K parts per table). external_location points at the
# per-table dir either way, so Trino reads whatever files are in it.
DATA_PREFIX="${DATA_PREFIX:-tpch-data}"
SRC="s3://${BENCH_BUCKET}/${DATA_PREFIX}/sf${SF}"
TABLES=(region nation supplier customer part partsupp orders lineitem)

# --- 1. ensure each table lives under <table>/ subdir -----------------------
# Skip the move if the destination already exists. `aws s3 ls` exits non-zero
# if the prefix is empty, so we test stdout.
for t in "${TABLES[@]}"; do
    dst_prefix="${DATA_PREFIX}/sf${SF}/${t}/"
    src_key="${DATA_PREFIX}/sf${SF}/${t}.parquet"
    has_dir="$(aws s3 ls "s3://${BENCH_BUCKET}/${dst_prefix}" 2>/dev/null | head -n 1 || true)"
    has_file="$(aws s3 ls "s3://${BENCH_BUCKET}/${src_key}" 2>/dev/null | head -n 1 || true)"
    if [[ -n "$has_dir" ]]; then
        echo "    ok  s3://${BENCH_BUCKET}/${dst_prefix} (already populated)"
    elif [[ -n "$has_file" ]]; then
        echo "    mv  s3://${BENCH_BUCKET}/${src_key} -> ${dst_prefix}${t}.parquet"
        aws s3 mv "s3://${BENCH_BUCKET}/${src_key}" \
                  "s3://${BENCH_BUCKET}/${dst_prefix}${t}.parquet"
    else
        echo "!! neither ${src_key} nor ${dst_prefix} exists in ${BENCH_BUCKET}" >&2
        exit 1
    fi
done

# --- 2. write DDL to a temp file + execute via trino CLI --------------------
DDL="$(mktemp -t tpch-trino-ddl.XXXXXX.sql)"
trap 'rm -f "$DDL"' EXIT

# TPC-H spec § 1.4.1. DECIMAL(15,2)/DECIMAL(12,2) columns are written as
# Float64 by tpch_generate.rs (see comment there); we declare them as
# DOUBLE in Trino to match the parquet physical type. Date columns are
# Date32 → DATE. Integer keys are i64 → BIGINT, p_size/ps_availqty/
# l_linenumber/o_shippriority are i32 → INTEGER.
cat >"$DDL" <<EOF
CREATE SCHEMA IF NOT EXISTS hive.${SCHEMA} WITH (location = 's3://${BENCH_BUCKET}/glue-warehouse/${SCHEMA}/');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.region (
    r_regionkey BIGINT,
    r_name      VARCHAR,
    r_comment   VARCHAR
) WITH (external_location = '${SRC}/region/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.nation (
    n_nationkey BIGINT,
    n_name      VARCHAR,
    n_regionkey BIGINT,
    n_comment   VARCHAR
) WITH (external_location = '${SRC}/nation/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.supplier (
    s_suppkey   BIGINT,
    s_name      VARCHAR,
    s_address   VARCHAR,
    s_nationkey BIGINT,
    s_phone     VARCHAR,
    s_acctbal   DOUBLE,
    s_comment   VARCHAR
) WITH (external_location = '${SRC}/supplier/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.customer (
    c_custkey    BIGINT,
    c_name       VARCHAR,
    c_address    VARCHAR,
    c_nationkey  BIGINT,
    c_phone      VARCHAR,
    c_acctbal    DOUBLE,
    c_mktsegment VARCHAR,
    c_comment    VARCHAR
) WITH (external_location = '${SRC}/customer/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.part (
    p_partkey     BIGINT,
    p_name        VARCHAR,
    p_mfgr        VARCHAR,
    p_brand       VARCHAR,
    p_type        VARCHAR,
    p_size        INTEGER,
    p_container   VARCHAR,
    p_retailprice DOUBLE,
    p_comment     VARCHAR
) WITH (external_location = '${SRC}/part/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.partsupp (
    ps_partkey    BIGINT,
    ps_suppkey    BIGINT,
    ps_availqty   INTEGER,
    ps_supplycost DOUBLE,
    ps_comment    VARCHAR
) WITH (external_location = '${SRC}/partsupp/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.orders (
    o_orderkey      BIGINT,
    o_custkey       BIGINT,
    o_orderstatus   VARCHAR,
    o_totalprice    DOUBLE,
    o_orderdate     DATE,
    o_orderpriority VARCHAR,
    o_clerk         VARCHAR,
    o_shippriority  INTEGER,
    o_comment       VARCHAR
) WITH (external_location = '${SRC}/orders/', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.${SCHEMA}.lineitem (
    l_orderkey      BIGINT,
    l_partkey       BIGINT,
    l_suppkey       BIGINT,
    l_linenumber    INTEGER,
    l_quantity      DOUBLE,
    l_extendedprice DOUBLE,
    l_discount      DOUBLE,
    l_tax           DOUBLE,
    l_returnflag    VARCHAR,
    l_linestatus    VARCHAR,
    l_shipdate      DATE,
    l_commitdate    DATE,
    l_receiptdate   DATE,
    l_shipinstruct  VARCHAR,
    l_shipmode      VARCHAR,
    l_comment       VARCHAR
) WITH (external_location = '${SRC}/lineitem/', format = 'PARQUET');
EOF

echo "==> registering schema hive.${SCHEMA} (8 tables)"
trino --server http://localhost:8080 --catalog hive --file "$DDL"

# --- 3. sanity-check: row count from region (cheap, always 5) ---------------
echo "==> sanity check"
trino --server http://localhost:8080 --catalog hive --schema "${SCHEMA}" \
    --execute "SELECT count(*) FROM region;"
echo "==> done"
