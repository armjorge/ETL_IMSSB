#!/usr/bin/env bash
# Create / refresh SRC_IMSS_BIENESTAR + stage eseotres_sources from terminal vars.
# Secrets are prompted (or read from env), written only to a 0600 temp SQL file,
# executed, then deleted — never committed and never passed on the process argv.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="${ROOT}/snowflake/source_imss_bienestar.sql"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Missing template: $TEMPLATE" >&2
  exit 1
fi

if ! command -v envsubst >/dev/null 2>&1; then
  echo "envsubst is required (gettext). On macOS: brew install gettext && brew link --force gettext" >&2
  exit 1
fi

if ! command -v snowsql >/dev/null 2>&1; then
  for candidate in \
    "${HOME}/.local/bin/snowsql" \
    "${HOME}/bin/snowsql" \
    "/Applications/SnowSQL.app/Contents/MacOS/snowsql"
  do
    if [[ -x "$candidate" ]]; then
      export PATH="$(dirname "$candidate"):${PATH}"
      break
    fi
  done
fi

if ! command -v snowsql >/dev/null 2>&1; then
  echo "snowsql not found on PATH. Install Snowflake SnowSQL or add it to PATH." >&2
  echo "  Expected: /Applications/SnowSQL.app/Contents/MacOS/snowsql" >&2
  exit 1
fi

prompt() {
  local var="$1" label="$2" default="${3:-}"
  local current="${!var:-}"
  if [[ -n "$current" ]]; then
    printf -v "$var" '%s' "$current"
    echo "  $label = (from env)"
    return
  fi
  if [[ -n "$default" ]]; then
    read -r -p "  $label [$default]: " input
    printf -v "$var" '%s' "${input:-$default}"
  else
    read -r -p "  $label: " input
    printf -v "$var" '%s' "$input"
  fi
}

prompt_secret() {
  local var="$1" label="$2"
  local current="${!var:-}"
  if [[ -n "$current" ]]; then
    printf -v "$var" '%s' "$current"
    echo "  $label = (from env)"
    return
  fi
  read -r -s -p "  $label: " input
  echo
  printf -v "$var" '%s' "$input"
}

echo "=== Snowflake stage: IMSS Bienestar S3 ==="
echo "Template: $TEMPLATE"
echo

prompt SF_DATABASE   "Database"              "ESEOTRES_PHARMA"
prompt SF_SCHEMA     "Schema"                "SRC_IMSS_BIENESTAR"
prompt STAGE_NAME    "Stage name"            "eseotres_sources"
prompt S3_URL        "S3 URL"                "s3://so3-data/imss_bienestar/"
prompt SNOWSQL_CONN  "SnowSQL connection (-c, blank = default)" ""

# Ensure trailing slash on S3 URL
[[ "$S3_URL" == */ ]] || S3_URL="${S3_URL}/"

echo
echo "Auth mode for the stage:"
echo "  1) STORAGE_INTEGRATION (recommended)"
echo "  2) AWS access key + secret (temporary; avoid long-lived keys)"
read -r -p "  Choose [1/2]: " AUTH_MODE
AUTH_MODE="${AUTH_MODE:-1}"

AUTH_CLAUSE=""
case "$AUTH_MODE" in
  1)
    prompt STORAGE_INTEGRATION "Storage integration name" ""
    if [[ -z "$STORAGE_INTEGRATION" ]]; then
      echo "Storage integration name is required for mode 1." >&2
      exit 1
    fi
    AUTH_CLAUSE="STORAGE_INTEGRATION = ${STORAGE_INTEGRATION}"
    ;;
  2)
    prompt_secret AWS_KEY_ID     "AWS_KEY_ID"
    prompt_secret AWS_SECRET_KEY "AWS_SECRET_KEY"
    if [[ -z "$AWS_KEY_ID" || -z "$AWS_SECRET_KEY" ]]; then
      echo "AWS_KEY_ID and AWS_SECRET_KEY are required for mode 2." >&2
      exit 1
    fi
    # Single-quote secrets for SQL; reject values that would break quoting
    if [[ "$AWS_KEY_ID" == *"'"* || "$AWS_SECRET_KEY" == *"'"* ]]; then
      echo "AWS credentials must not contain single quotes." >&2
      exit 1
    fi
    AUTH_CLAUSE="CREDENTIALS = (AWS_KEY_ID='${AWS_KEY_ID}' AWS_SECRET_KEY='${AWS_SECRET_KEY}')"
    ;;
  *)
    echo "Invalid auth mode: $AUTH_MODE" >&2
    exit 1
    ;;
esac

echo
echo "Will create:"
echo "  ${SF_DATABASE}.${SF_SCHEMA}.${STAGE_NAME}"
echo "  URL = ${S3_URL}"
echo "  AUTH = ${AUTH_CLAUSE%%=*}"   # show mode only, not secrets
read -r -p "Proceed? [y/N]: " OK
[[ "${OK:-}" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

TMP="$(mktemp -t imssb_stage.XXXXXX.sql)"
chmod 600 "$TMP"
cleanup() {
  # Best-effort wipe of rendered SQL (may contain credentials)
  if [[ -f "$TMP" ]]; then
    if command -v shred >/dev/null 2>&1; then
      shred -u "$TMP" 2>/dev/null || rm -f "$TMP"
    else
      rm -f "$TMP"
    fi
  fi
}
trap cleanup EXIT

export SF_DATABASE SF_SCHEMA STAGE_NAME S3_URL AUTH_CLAUSE
# Only substitute known placeholders (avoid eating unrelated $ in comments if any)
envsubst '${SF_DATABASE} ${SF_SCHEMA} ${STAGE_NAME} ${S3_URL} ${AUTH_CLAUSE}' \
  < "$TEMPLATE" > "$TMP"

SNOWSQL_ARGS=(-o variable_substitution=false -o echo=true -f "$TMP")
if [[ -n "${SNOWSQL_CONN:-}" ]]; then
  SNOWSQL_ARGS=(-c "$SNOWSQL_CONN" "${SNOWSQL_ARGS[@]}")
fi

echo
echo "Running snowsql…"
snowsql "${SNOWSQL_ARGS[@]}"

echo
echo "Done. Verify in Snowsight:"
echo "  USE SCHEMA ${SF_DATABASE}.${SF_SCHEMA};"
echo "  LIST @${STAGE_NAME};"
echo "  LIST @${STAGE_NAME}/camunda/;"
echo
echo "Next: Iceberg tables over this stage (external volume + CREATE ICEBERG TABLE)."
