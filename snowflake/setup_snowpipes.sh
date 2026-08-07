#!/usr/bin/env bash
# Create Snowpipe AUTO_INGEST pipes for Iceberg SRC tables, wired to SNS topics
# from Terraform (infra/snowpipe_sns.tf).
#
# Prerequisites:
#   - ./infra/apply.sh          (SNS topics + S3 notifications)
#   - ./snowflake/setup_s3_stage.sh
#   - Iceberg tables exist (snowflake/iceberg_tables.sql/src_iceberg_tables.sql)
#   - snowsql + envsubst
#
# Usage:
#   ./snowflake/setup_snowpipes.sh
#   SNOWSQL_CONN=eseotres ./snowflake/setup_snowpipes.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="${ROOT}/infra"
PIPE_SQL="${ROOT}/snowflake/create_snowpipes.sql"
ENV_FILE="${INFRA}/infra.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
fi

: "${SNOWSQL_CONN:=eseotres}"
: "${SF_DATABASE:=ESEOTRES_PHARMA}"
: "${SF_SCHEMA:=SRC_IMSS_BIENESTAR}"
: "${SF_ROLE:=SYSADMIN}"
: "${STAGE_NAME:=eseotres_sources}"

find_snowsql() {
  if command -v snowsql >/dev/null 2>&1; then
    return 0
  fi
  for candidate in \
    "${HOME}/.local/bin/snowsql" \
    "${HOME}/bin/snowsql" \
    "/Applications/SnowSQL.app/Contents/MacOS/snowsql"
  do
    if [[ -x "$candidate" ]]; then
      export PATH="$(dirname "$candidate"):${PATH}"
      return 0
    fi
  done
  return 1
}

die() { echo "❌ $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

need envsubst
need python3
find_snowsql || die "snowsql not found on PATH"

load_sns_arns_from_terraform() {
  command -v terraform >/dev/null 2>&1 || return 1
  [[ -d "${INFRA}/.terraform" ]] || return 1
  (
    cd "$INFRA"
    terraform output -json snowpipe_sns_topic_arns 2>/dev/null
  )
}

echo "=== IMSS Bienestar: Snowpipe + SNS setup ==="
echo "  connection : ${SNOWSQL_CONN}"
echo "  database   : ${SF_DATABASE}.${SF_SCHEMA}"
echo "  role       : ${SF_ROLE}"
echo "  stage      : ${STAGE_NAME}"
echo

SNS_JSON="${SNOWPIPE_SNS_ARNS_JSON:-}"
if [[ -z "$SNS_JSON" ]]; then
  echo "→ Reading snowpipe_sns_topic_arns from terraform output…"
  SNS_JSON="$(load_sns_arns_from_terraform)" \
    || die "Could not read terraform output snowpipe_sns_topic_arns. Run ./infra/apply.sh first, or set SNOWPIPE_SNS_ARNS_JSON."
fi

eval "$(python3 -c '
import json, os, sys
raw = sys.argv[1]
arns = json.loads(raw)
need = ("camunda", "payments", "invoicing", "sagi")
missing = [k for k in need if k not in arns or not arns[k]]
if missing:
    sys.stderr.write("Missing SNS ARNs for: %s\n" % ", ".join(missing))
    sys.stderr.write("Got keys: %s\n" % sorted(arns))
    sys.exit(1)
for k in need:
    # Export shell-safe assignments
    print(f"SNS_ARN_{k.upper()}={json.dumps(arns[k])}")
' "$SNS_JSON")"

echo "  SNS camunda   : ${SNS_ARN_CAMUNDA}"
echo "  SNS payments  : ${SNS_ARN_PAYMENTS}"
echo "  SNS invoicing : ${SNS_ARN_INVOICING}"
echo "  SNS sagi      : ${SNS_ARN_SAGI}"
echo

TMP="$(mktemp -t imssb_pipes.XXXXXX.sql)"
cleanup() { rm -f "$TMP"; }
trap cleanup EXIT

export SF_DATABASE SF_SCHEMA SF_ROLE STAGE_NAME
export SNS_ARN_CAMUNDA SNS_ARN_PAYMENTS SNS_ARN_INVOICING SNS_ARN_SAGI

envsubst '${SF_DATABASE} ${SF_SCHEMA} ${SF_ROLE} ${STAGE_NAME} ${SNS_ARN_CAMUNDA} ${SNS_ARN_PAYMENTS} ${SNS_ARN_INVOICING} ${SNS_ARN_SAGI}' \
  < "$PIPE_SQL" > "$TMP"

echo "→ Snowflake: CREATE PIPES (AUTO_INGEST + AWS_SNS_TOPIC)…"
snowsql -c "$SNOWSQL_CONN" \
  -o variable_substitution=false \
  -o echo=true \
  -o friendly=true \
  -o timing=false \
  -f "$TMP"

echo
echo "✅ Done."
echo "  Pipes: pipe_camunda_orders, pipe_payments, pipe_invoicing, pipe_sagi"
echo "  Upload a .csv under s3://…/{camunda|payments|invoicing|sagi}/ to trigger append."
echo
echo "Status:"
echo "  snowsql -c ${SNOWSQL_CONN} -q \"USE SCHEMA ${SF_DATABASE}.${SF_SCHEMA}; SELECT SYSTEM\\\$PIPE_STATUS('pipe_camunda_orders');\""
