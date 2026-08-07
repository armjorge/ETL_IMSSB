#!/usr/bin/env bash
# Recreate AWS S3 (+ IAM) and Snowflake storage integration / stage in one go.
#
# Safe to re-run (idempotent where possible):
#   1) Terraform: bucket, prefixes, Snowflake IAM role, Snowpipe SNS topics
#   2) Snowflake: STORAGE INTEGRATION + trust update + STAGE + LIST
#   3) (optional) Snowpipe AUTO_INGEST — after Iceberg tables exist
#
# Customize layout / names:
#   cp infra/infra.env.example infra/infra.env
#   # edit BUCKET_NAME, ROOT_PREFIX, SOURCE_FOLDERS, Snowflake object names…
#   ./scripts/recreate_infra.sh
#
# Or override one-off:
#   ROOT_PREFIX=imss_bienestar_dev ./scripts/recreate_infra.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/infra/infra.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
fi

: "${BUCKET_NAME:=so3-data}"
: "${ROOT_PREFIX:=imss_bienestar}"
: "${S3_URL:=s3://${BUCKET_NAME}/${ROOT_PREFIX}/}"
export BUCKET_NAME ROOT_PREFIX S3_URL
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_PROFILE="${AWS_PROFILE:-}"
export SNOWFLAKE_ROLE_NAME="${SNOWFLAKE_ROLE_NAME:-snowflake-s3-imss-bienestar}"
export SOURCE_FOLDERS="${SOURCE_FOLDERS:-camunda,sagi,invoicing,payments,banking,institution_status}"
export SNOWSQL_CONN="${SNOWSQL_CONN:-eseotres}"
export SF_DATABASE="${SF_DATABASE:-ESEOTRES_PHARMA}"
export SF_SCHEMA="${SF_SCHEMA:-SRC_IMSS_BIENESTAR}"
export SF_ROLE="${SF_ROLE:-SYSADMIN}"
export INTEGRATION_NAME="${INTEGRATION_NAME:-s3_imss_bienestar}"
export STAGE_NAME="${STAGE_NAME:-eseotres_sources}"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  Recreate IMSS Bienestar infra (S3 + Snowflake)      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo "  S3 URL     : ${S3_URL}"
echo "  SnowSQL -c : ${SNOWSQL_CONN}"
echo "  Stage      : ${SF_DATABASE}.${SF_SCHEMA}.${STAGE_NAME}"
echo

# --- 1) AWS bucket + IAM ----------------------------------------------------
if command -v terraform >/dev/null 2>&1; then
  "${ROOT}/infra/apply.sh" apply
else
  echo "⚠️  terraform not on PATH — skipping bucket create."
  echo "   Ensure s3://${BUCKET_NAME}/${ROOT_PREFIX}/ already exists, then continuing…"
  aws s3 ls "${S3_URL}" >/dev/null \
    || { echo "❌ Cannot list ${S3_URL}. Install terraform or create the bucket first."; exit 1; }
fi

# --- 2) Snowflake integration + stage ---------------------------------------
echo
"${ROOT}/snowflake/setup_s3_stage.sh"

# --- 3) Snowpipe (needs Iceberg tables with etl_file_name) -------------------
if [[ "${SETUP_SNOWPIPES:-0}" == "1" ]]; then
  echo
  echo "→ SETUP_SNOWPIPES=1 — creating AUTO_INGEST pipes…"
  "${ROOT}/snowflake/setup_snowpipes.sh"
else
  echo
  echo "ℹ️  Snowpipe skipped (Iceberg tables must exist first)."
  echo "   After src_iceberg_tables.sql:  ./snowflake/setup_snowpipes.sh"
  echo "   Or re-run with:                SETUP_SNOWPIPES=1 ./scripts/recreate_infra.sh"
fi

echo
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Recreate complete                                   ║"
echo "╚══════════════════════════════════════════════════════╝"
echo "  LIST: snowsql -c ${SNOWSQL_CONN} -q \"USE SCHEMA ${SF_DATABASE}.${SF_SCHEMA}; LIST @${STAGE_NAME};\""
echo
echo "To change the S3 key prefix later:"
echo "  1) Edit infra/infra.env → ROOT_PREFIX=… (and optionally BUCKET_NAME)"
echo "  2) Align Streamlit config.yml → s3.root_prefix"
echo "  3) Re-run: ./scripts/recreate_infra.sh"
