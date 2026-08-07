#!/usr/bin/env bash
# Apply / recreate AWS infra for IMSS Bienestar:
#   - S3 bucket + source folder placeholders
#   - IAM role/policy for Snowflake storage integration (bootstrap trust)
#
# Usage:
#   ./infra/apply.sh              # plan + apply
#   ./infra/apply.sh plan         # plan only
#   ./infra/apply.sh destroy      # destroy (careful)
#
# Knobs: copy infra/infra.env.example → infra/infra.env (or export vars).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="${ROOT}/infra"
ENV_FILE="${INFRA}/infra.env"
ACTION="${1:-apply}"

die() { echo "❌ $*" >&2; exit 1; }

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
  echo "Loaded ${ENV_FILE}"
elif [[ -f "${INFRA}/infra.env.example" ]]; then
  echo "No infra/infra.env — using defaults (see infra/infra.env.example to customize)."
fi

: "${AWS_REGION:=us-east-1}"
: "${BUCKET_NAME:=so3-data}"
: "${ROOT_PREFIX:=imss_bienestar}"
: "${SNOWFLAKE_ROLE_NAME:=snowflake-s3-imss-bienestar}"
: "${SOURCE_FOLDERS:=camunda,sagi,invoicing,payments,banking,institution_status}"

command -v terraform >/dev/null 2>&1 || die "terraform not on PATH. Install Terraform >= 1.5."
command -v aws >/dev/null 2>&1 || die "aws CLI not on PATH."

echo "→ AWS identity…"
aws sts get-caller-identity >/dev/null || die "AWS not authenticated. Try: aws login"

# Export credentials for Terraform when using aws login short-lived sessions
if command -v aws >/dev/null 2>&1; then
  if eval "$(aws configure export-credentials --format env 2>/dev/null)"; then
    echo "  Exported short-lived credentials for Terraform"
  fi
fi

cd "$INFRA"
[[ -d .terraform ]] || terraform init

# Build -var list from env (overrides terraform.tfvars when both exist)
TF_VARS=(
  -var="aws_region=${AWS_REGION}"
  -var="bucket_name=${BUCKET_NAME}"
  -var="root_prefix=${ROOT_PREFIX}"
  -var="enable_snowflake_s3_access=true"
  -var="snowflake_role_name=${SNOWFLAKE_ROLE_NAME}"
)

# source_folders as HCL list
IFS=',' read -r -a folders <<< "$SOURCE_FOLDERS"
folder_hcl="["
for i in "${!folders[@]}"; do
  f="$(echo "${folders[$i]}" | xargs)"
  [[ -n "$f" ]] || continue
  [[ "$folder_hcl" == "[" ]] || folder_hcl+=", "
  folder_hcl+="\"${f}\""
done
folder_hcl+="]"
TF_VARS+=(-var="source_folders=${folder_hcl}")

if [[ -n "${AWS_PROFILE:-}" ]]; then
  TF_VARS+=(-var="aws_profile=${AWS_PROFILE}")
fi

# Keep Snowflake trust values if a previous setup wrote them
EXTRA=()
[[ -f terraform.tfvars.snowflake ]] && EXTRA+=(-var-file=terraform.tfvars.snowflake)
[[ -f terraform.tfvars ]] && EXTRA=(-var-file=terraform.tfvars "${EXTRA[@]}")

echo "=== Terraform ${ACTION} ==="
echo "  bucket      : ${BUCKET_NAME}"
echo "  root_prefix : ${ROOT_PREFIX}"
echo "  region      : ${AWS_REGION}"
echo "  iam role    : ${SNOWFLAKE_ROLE_NAME}"
echo "  folders     : ${SOURCE_FOLDERS}"
echo

case "$ACTION" in
  plan)
    terraform plan "${EXTRA[@]}" "${TF_VARS[@]}"
    ;;
  apply)
    terraform apply -auto-approve "${EXTRA[@]}" "${TF_VARS[@]}"
    echo
    echo "✅ Infra applied."
    terraform output
    echo
    echo "S3 root: s3://${BUCKET_NAME}/${ROOT_PREFIX}/"
    echo "Next: ./snowflake/setup_s3_stage.sh   (or ./scripts/recreate_infra.sh)"
    ;;
  destroy)
    read -r -p "Destroy bucket ${BUCKET_NAME} and related IAM? Type yes: " conf
    [[ "$conf" == "yes" ]] || die "Aborted."
    terraform destroy -auto-approve "${EXTRA[@]}" "${TF_VARS[@]}"
    ;;
  *)
    die "Unknown action: ${ACTION} (use plan|apply|destroy)"
    ;;
esac
