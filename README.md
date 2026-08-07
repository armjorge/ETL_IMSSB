# ETL IMSSB

Extract configured Excel sources to dated CSV files and upload them to S3. Configuration is managed from a Streamlit dashboard.

## What this phase does

Streamlit dashboard to configure **dataset schemas** and take clean CSV snapshots into S3.

Each dataset has:

- `dataset_name` — human label (editable)
- `prefix` — subfolder name (lowercase, no spaces/special chars)
- Excel `file_path` (often a synced cloud folder), `sheet`, `columns`

**Take snapshot** (one dataset) or **Snapshot all ready** (all green datasets in order) writes:

```text
{MAIN_PATH}/imssb_files/{prefix}/{prefix} dd-mm-yyyy hh mm.csv
s3://so3-data/imss_bienestar/{prefix}/
```

CSV format: all text, pipe `|` separator, `"` / `'` removed (minimal cleanup only).

`MAIN_PATH`, S3 settings, Camunda/SAGI credentials, and datasets all persist in `{MAIN_PATH}/imssb_files/config.yml` (gitignored). Camunda/SAGI extractors come in a later phase.

S3 layout:

```
s3://so3-data/imss_bienestar/camunda/
s3://so3-data/imss_bienestar/sagi/
s3://so3-data/imss_bienestar/invoicing/
s3://so3-data/imss_bienestar/payments/
s3://so3-data/imss_bienestar/banking/
s3://so3-data/imss_bienestar/institution_status/
```

## Prerequisites

- Python 3.13+ (project uses `uv` / `.venv`)
- AWS CLI authenticated with profile `default` (account `747289880051`), region `us-east-1`
- Terraform >= 1.5 (for infrastructure; optional if you create the bucket from Streamlit)

## 1. Install dependencies

```bash
cd /path/to/ETL_IMSSB
uv sync
# or: python -m venv .venv && source .venv/bin/activate && pip install -e .
```

## 2. Create the S3 bucket (Terraform)

If you authenticate with `aws login`, export credentials for Terraform first (the AWS provider does not always pick up login sessions):

```bash
eval "$(aws configure export-credentials --format env)"
```

Then:

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # edit if needed
terraform init
terraform plan
terraform apply
```

Defaults:

- Bucket: `so3-data`
- Region: `us-east-1`
- Root prefix: `imss_bienestar`
- Credential chain: env vars / shared AWS config (optional `aws_profile` in tfvars)

Outputs include `bucket_name`, `bucket_arn`, `root_prefix`, and the source prefix URIs.

Alternatively, open the Streamlit **S3** tab, set the bucket name, and click **Create bucket if missing**.

## 3. Local config

Runtime config is stored at:

```text
{MAIN_PATH}/imssb_files/config.yml
```

With default `MAIN_PATH=.` that is `imssb_files/config.yml` (the whole `imssb_files/` folder is gitignored).

On first run the dashboard creates it from `config.example.yaml` (or migrates an old root `config.yaml` if present). **Paths → Save paths** stores `MAIN_PATH` in that same file; dataset create/save/snapshot also auto-saves so Streamlit reloads keep your setup.

Optional env overrides (`.env`):

```bash
cp .env.example .env
# AWS_REGION=us-east-1
# AWS_PROFILE=default
# MAIN_PATH=.
```

## 4. Run the Streamlit dashboard

```bash
uv run streamlit run app.py
# or: .venv/bin/streamlit run app.py
```

Dashboard tabs:

1. **Paths** — set and save `MAIN_PATH` (persisted in `config.yml`).
2. **S3** — link an existing bucket or create one if missing.
3. **Datasets** — create/edit schemas; green validation for file/sheet/columns; **Take snapshot** per dataset or **Snapshot all ready** for every green dataset in order.
4. **Camunda / SAGI** — edit URL, user, password (extraction later).

Snapshot example:

```
{MAIN_PATH}/imssb_files/payments/payments 06-08-2026 18 30.csv
s3://so3-data/imss_bienestar/payments/payments 06-08-2026 18 30.csv
```

## Project layout (relevant)

```
app.py                      # Streamlit dashboard
config.example.yaml         # Template (safe to commit)
imssb_files/                # Local data + config.yml (gitignored)
infra/                      # Terraform for so3-data
modules/config.py           # Load/save config.yml under MAIN_PATH
modules/datasets.py         # Dataset schema helpers
modules/source_validation.py
modules/s3_client.py
modules/xlsx_extract.py     # Snapshot → pipe CSV → S3
modules/helpers.py          # load_and_concat (reused)
main.py                     # Legacy full ETL CLI
```

## Legacy CLI

`main.py` still contains the older interactive ETL menu (Camunda/SAGI Selenium downloads, SQL load, BI). Prefer the Streamlit dataset snapshot path for Excel → CSV → S3. Orchestration of remaining sources comes later.
