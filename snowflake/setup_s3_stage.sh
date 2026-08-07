#!/usr/bin/env bash
# End-to-end: IAM role → Snowflake storage integration → trust update
# → external stage → LIST files under s3://{bucket}/{root_prefix}/
#
# Prefer the master recreate script:
#   ./scripts/recreate_infra.sh
#
# Prerequisites:
#   - aws CLI authenticated (can manage IAM + see the bucket)
#   - snowsql connection (default: -c eseotres) with ACCOUNTADMIN + SYSADMIN
#   - envsubst (gettext)
#   - terraform optional (if present, manages IAM; else AWS CLI is used)
#
# Knobs: infra/infra.env (from infra.env.example) or env vars.
#
# Usage:
#   ./snowflake/setup_s3_stage.sh
#   SNOWSQL_CONN=eseotres ROOT_PREFIX=imss_bienestar ./snowflake/setup_s3_stage.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="${ROOT}/infra"
INT_SQL="${ROOT}/snowflake/create_storage_integration.sql"
STAGE_SQL="${ROOT}/snowflake/create_stage_and_list.sql"
TFVARS_SNOW="${INFRA}/terraform.tfvars.snowflake"
ENV_FILE="${INFRA}/infra.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
fi

# Defaults (override with infra.env or env)
: "${SNOWSQL_CONN:=eseotres}"
: "${SF_DATABASE:=ESEOTRES_PHARMA}"
: "${SF_SCHEMA:=SRC_IMSS_BIENESTAR}"
: "${SF_ROLE:=SYSADMIN}"
: "${STAGE_NAME:=eseotres_sources}"
: "${INTEGRATION_NAME:=s3_imss_bienestar}"
: "${AUTO_APPROVE:=1}"
: "${SNOWFLAKE_ROLE_NAME:=snowflake-s3-imss-bienestar}"
: "${BUCKET_NAME:=so3-data}"
: "${ROOT_PREFIX:=imss_bienestar}"
: "${S3_URL:=s3://${BUCKET_NAME}/${ROOT_PREFIX}/}"

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

ensure_iam_bootstrap() {
  # Sets AWS_ROLE_ARN. Prefers Terraform; falls back to AWS CLI.
  if command -v terraform >/dev/null 2>&1; then
    echo "→ Terraform: ensure Snowflake IAM role (bootstrap trust)…"
    cd "$INFRA"
    [[ -d .terraform ]] || terraform init
    local tf_args=(-var="enable_snowflake_s3_access=true")
    if [[ "$AUTO_APPROVE" == "1" ]]; then
      terraform apply -auto-approve "${tf_args[@]}"
    else
      terraform apply "${tf_args[@]}"
    fi
    AWS_ROLE_ARN="$(terraform output -raw snowflake_iam_role_arn 2>/dev/null || true)"
    cd "$ROOT"
    [[ -n "$AWS_ROLE_ARN" ]] || die "terraform output snowflake_iam_role_arn is empty"
    return
  fi

  echo "→ AWS CLI: ensure Snowflake IAM role (terraform not on PATH)…"
  local account_id policy_name policy_arn trust_file policy_file
  account_id="$(aws sts get-caller-identity --query Account --output text)"
  policy_name="${SNOWFLAKE_ROLE_NAME}-policy"
  trust_file="$(mktemp -t imssb_trust.XXXXXX.json)"
  policy_file="$(mktemp -t imssb_policy.XXXXXX.json)"

  cat > "$trust_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::${account_id}:root" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId": "0000" } }
  }]
}
EOF

  cat > "$policy_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ObjectAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:GetObjectVersion",
        "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}/${ROOT_PREFIX}/*"
    },
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}",
      "Condition": {
        "StringLike": { "s3:prefix": ["${ROOT_PREFIX}/*"] }
      }
    }
  ]
}
EOF

  if ! aws iam get-role --role-name "$SNOWFLAKE_ROLE_NAME" >/dev/null 2>&1; then
    aws iam create-role \
      --role-name "$SNOWFLAKE_ROLE_NAME" \
      --description "Assumed by Snowflake storage integration for IMSS Bienestar S3" \
      --assume-role-policy-document "file://${trust_file}" >/dev/null
  else
    aws iam update-assume-role-policy \
      --role-name "$SNOWFLAKE_ROLE_NAME" \
      --policy-document "file://${trust_file}" >/dev/null
  fi

  if policy_arn="$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='${policy_name}'].Arn | [0]" --output text 2>/dev/null)" \
    && [[ -n "$policy_arn" && "$policy_arn" != "None" ]]; then
    aws iam create-policy-version \
      --policy-arn "$policy_arn" \
      --policy-document "file://${policy_file}" \
      --set-as-default >/dev/null 2>&1 || \
    aws iam create-policy \
      --policy-name "$policy_name" \
      --policy-document "file://${policy_file}" >/dev/null
  else
    policy_arn="$(aws iam create-policy \
      --policy-name "$policy_name" \
      --policy-document "file://${policy_file}" \
      --query 'Policy.Arn' --output text)"
  fi

  policy_arn="$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='${policy_name}'].Arn | [0]" --output text)"
  aws iam attach-role-policy --role-name "$SNOWFLAKE_ROLE_NAME" --policy-arn "$policy_arn" >/dev/null 2>&1 || true
  AWS_ROLE_ARN="arn:aws:iam::${account_id}:role/${SNOWFLAKE_ROLE_NAME}"
  rm -f "$trust_file" "$policy_file"
}

# Optional extra ExternalIds (comma-separated), e.g. Iceberg external volume:
#   SNOWFLAKE_EXTRA_EXTERNAL_IDS='UJ94228_SFCRole=4_…'
# Also preserved from an existing infra/terraform.tfvars.snowflake when present.
update_iam_trust() {
  local iam_user_arn="$1" external_id="$2"
  local trust_file extra_ids_hcl="" extra_csv="${SNOWFLAKE_EXTRA_EXTERNAL_IDS:-}"
  local -a extra_ids=()

  # Preserve extras already written by a previous Iceberg trust update
  if [[ -f "$TFVARS_SNOW" ]]; then
    local from_file
    from_file="$(python3 -c '
import re, sys
text = open(sys.argv[1]).read()
m = re.search(r"snowflake_external_ids\s*=\s*\[(.*?)\]", text, re.S)
if not m:
    sys.exit(0)
print(",".join(re.findall(r"\"([^\"]+)\"", m.group(1))))
' "$TFVARS_SNOW" 2>/dev/null || true)"
    if [[ -n "$from_file" ]]; then
      [[ -n "$extra_csv" ]] && extra_csv="${extra_csv},${from_file}" || extra_csv="$from_file"
    fi
  fi

  if [[ -n "$extra_csv" ]]; then
    local IFS=','
    # shellcheck disable=SC2206
    extra_ids=($extra_csv)
  fi

  # Dedupe while dropping the primary integration id if it appears in extras
  local -a uniq_extras=()
  local e
  for e in "${extra_ids[@]}"; do
    e="$(echo "$e" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$e" || "$e" == "$external_id" ]] && continue
    local seen=0 u
    for u in "${uniq_extras[@]+"${uniq_extras[@]}"}"; do
      [[ "$u" == "$e" ]] && { seen=1; break; }
    done
    [[ $seen -eq 0 ]] && uniq_extras+=("$e")
  done

  if ((${#uniq_extras[@]})); then
    extra_ids_hcl="["
    local first=1
    for e in "${uniq_extras[@]}"; do
      [[ $first -eq 1 ]] && first=0 || extra_ids_hcl+=", "
      extra_ids_hcl+="\"${e}\""
    done
    extra_ids_hcl+="]"
  else
    extra_ids_hcl="[]"
  fi

  trust_file="$(mktemp -t imssb_trust2.XXXXXX.json)"
  python3 -c '
import json, sys
principal, primary, out = sys.argv[1], sys.argv[2], sys.argv[3]
extras = sys.argv[4:]
ids = [primary] + [e for e in extras if e and e != primary]
seen, ordered = set(), []
for i in ids:
    if i not in seen:
        seen.add(i)
        ordered.append(i)
stmts = [{
    "Sid": f"SnowflakeAssume{n}",
    "Effect": "Allow",
    "Principal": {"AWS": principal},
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": ext}},
} for n, ext in enumerate(ordered)]
with open(out, "w") as f:
    json.dump({"Version": "2012-10-17", "Statement": stmts}, f, indent=2)
print("Trust ExternalIds:", ", ".join(ordered), file=sys.stderr)
' "$iam_user_arn" "$external_id" "$trust_file" "${uniq_extras[@]+"${uniq_extras[@]}"}"

  if command -v terraform >/dev/null 2>&1; then
    echo "→ Terraform: update IAM trust policy with Snowflake principal…"
    cd "$INFRA"
    cat > "$TFVARS_SNOW" <<EOF
enable_snowflake_s3_access = true
snowflake_iam_user_arn     = "${iam_user_arn}"
snowflake_external_id      = "${external_id}"
snowflake_external_ids     = ${extra_ids_hcl}
EOF
    local apply_files=()
    [[ -f terraform.tfvars ]] && apply_files+=(-var-file=terraform.tfvars)
    apply_files+=(-var-file="$TFVARS_SNOW")
    if [[ "$AUTO_APPROVE" == "1" ]]; then
      terraform apply -auto-approve "${apply_files[@]}"
    else
      terraform apply "${apply_files[@]}"
    fi
    cd "$ROOT"
  else
    echo "→ AWS CLI: update IAM trust policy with Snowflake principal…"
    aws iam update-assume-role-policy \
      --role-name "$SNOWFLAKE_ROLE_NAME" \
      --policy-document "file://${trust_file}" >/dev/null
  fi
  rm -f "$trust_file"
}

run_snowsql_file() {
  local file="$1"
  snowsql -c "$SNOWSQL_CONN" \
    -o variable_substitution=false \
    -o echo=true \
    -o friendly=true \
    -o timing=false \
    -f "$file"
}

run_snowsql_query() {
  local q="$1"
  snowsql -c "$SNOWSQL_CONN" \
    -o variable_substitution=false \
    -o output_format=csv \
    -o header=true \
    -o timing=false \
    -o friendly=false \
    -q "$q"
}

render_sql() {
  local template="$1" dest="$2"
  envsubst '${SF_DATABASE} ${SF_SCHEMA} ${SF_ROLE} ${STAGE_NAME} ${INTEGRATION_NAME} ${S3_URL} ${AWS_ROLE_ARN}' \
    < "$template" > "$dest"
}

parse_desc_integration() {
  # Reads DESC INTEGRATION CSV on stdin; prints IAM user ARN then external ID
  python3 -c '
import csv, sys
rows = list(csv.DictReader(sys.stdin))

def get(row, *keys):
    lower = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        if k.lower() in lower and lower[k.lower()] is not None:
            return lower[k.lower()]
    return None

props = {}
for row in rows:
    name = get(row, "property", "name")
    value = get(row, "property_value", "value")
    if name:
        props[str(name).strip().upper()] = (value or "").strip()

iam = props.get("STORAGE_AWS_IAM_USER_ARN", "")
ext = props.get("STORAGE_AWS_EXTERNAL_ID", "")
if not iam or not ext:
    sys.stderr.write("Could not parse DESC INTEGRATION. Got keys: %s\n" % sorted(props))
    sys.stderr.write("Raw rows: %s\n" % rows)
    sys.exit(1)
print(iam)
print(ext)
'
}

[[ "$S3_URL" == */ ]] || S3_URL="${S3_URL}/"

echo "=== IMSS Bienestar: Snowflake ↔ S3 stage setup ==="
echo "  connection : ${SNOWSQL_CONN}"
echo "  database   : ${SF_DATABASE}.${SF_SCHEMA}"
echo "  role       : ${SF_ROLE}"
echo "  integration: ${INTEGRATION_NAME}"
echo "  stage      : ${STAGE_NAME}"
echo "  s3 url     : ${S3_URL}"
echo

need aws
need envsubst
need python3
find_snowsql || die "snowsql not found on PATH"

echo "→ Checking AWS identity…"
aws sts get-caller-identity >/dev/null || die "AWS CLI not authenticated. Run: aws login"
aws s3 ls "$S3_URL" >/dev/null || die "Cannot list ${S3_URL} — check bucket/prefix permissions"

ensure_iam_bootstrap
export AWS_ROLE_ARN
echo "  IAM role: ${AWS_ROLE_ARN}"

TMP_INT="$(mktemp -t imssb_int.XXXXXX.sql)"
TMP_STAGE="$(mktemp -t imssb_stage.XXXXXX.sql)"
cleanup() { rm -f "$TMP_INT" "$TMP_STAGE"; }
trap cleanup EXIT

export SF_DATABASE SF_SCHEMA SF_ROLE STAGE_NAME INTEGRATION_NAME S3_URL
render_sql "$INT_SQL" "$TMP_INT"

echo
echo "→ Snowflake: CREATE STORAGE INTEGRATION (ACCOUNTADMIN)…"
run_snowsql_file "$TMP_INT"

echo
echo "→ Snowflake: DESC INTEGRATION (parse IAM user + external id)…"
DESC_OUT="$(run_snowsql_query "USE ROLE ACCOUNTADMIN; DESC INTEGRATION ${INTEGRATION_NAME};")"
# Drop snowsql chatter lines; keep CSV
DESC_CSV="$(printf '%s\n' "$DESC_OUT" | python3 -c '
import sys
lines = sys.stdin.read().splitlines()
start = 0
for i, line in enumerate(lines):
    low = line.lower()
    if "property" in low and "property_value" in low:
        start = i
        break
    if line.startswith("\"property\"") or line.startswith("property,"):
        start = i
        break
print("\n".join(lines[start:]))
')"

PARSED="$(printf '%s\n' "$DESC_CSV" | parse_desc_integration)"
SF_IAM_USER_ARN="$(printf '%s\n' "$PARSED" | sed -n '1p')"
SF_EXTERNAL_ID="$(printf '%s\n' "$PARSED" | sed -n '2p')"
echo "  STORAGE_AWS_IAM_USER_ARN = ${SF_IAM_USER_ARN}"
echo "  STORAGE_AWS_EXTERNAL_ID  = ${SF_EXTERNAL_ID}"

update_iam_trust "$SF_IAM_USER_ARN" "$SF_EXTERNAL_ID"

echo
echo "→ Waiting a few seconds for IAM to propagate…"
sleep 8

render_sql "$STAGE_SQL" "$TMP_STAGE"
echo "→ Snowflake: CREATE STAGE + LIST…"
run_snowsql_file "$TMP_STAGE"

echo
echo "✅ Done."
echo "  Stage: ${SF_DATABASE}.${SF_SCHEMA}.${STAGE_NAME}"
echo "  Integration: ${INTEGRATION_NAME}"
echo "  S3: ${S3_URL}"
echo
echo "Re-check anytime:"
echo "  snowsql -c ${SNOWSQL_CONN} -q \"USE SCHEMA ${SF_DATABASE}.${SF_SCHEMA}; LIST @${STAGE_NAME};\""
