# ETL IMSSB

Extract configured Excel / web sources to dated files, upload them to S3, and expose that S3 prefix to Snowflake via an external stage.

## What this phase does

Streamlit dashboard to configure **dataset schemas**, take clean CSV snapshots into S3, and run Camunda/SAGI browser extracts.

Each dataset has:

- `dataset_name` — human label (editable)
- `folder` — S3/local upload subfolder (lowercase, no spaces/special chars)
- `prefix` — optional filename tag (empty OK; e.g. `fantasmas`)
- Excel `file_path` (often a synced cloud folder), `sheet`, `columns`

**Take snapshot** writes:

```text
{MAIN_PATH}/imssb_files/{folder}/{folder} [prefix] dd-mm-yyyy hh mm.csv
s3://{bucket}/{root_prefix}/{folder}/
```

Defaults: `bucket=so3-data`, `root_prefix=imss_bienestar`. CSV: all text, pipe `|` separator, minimal quote cleanup.

Runtime config lives in `{MAIN_PATH}/imssb_files/config.yml` (gitignored).

## Prerequisites

- Python 3.13+ (`uv`)
- AWS CLI authenticated (account `747289880051`), region `us-east-1`
- Terraform >= 1.5 (recommended for bucket recreate; Snowflake IAM can fall back to AWS CLI)
- SnowSQL (`brew install --cask snowflake-snowsql`)
  - Connection in `~/.snowsql/config` (e.g. `eseotres`, JWT key-pair)
  - Needs `ACCOUNTADMIN` (storage integration) and `SYSADMIN` (stage)

## Recreate everything (S3 + Snowflake stage)

One command after a loss / new account / new prefix:

```bash
cp infra/infra.env.example infra/infra.env   # edit knobs (see below)
./scripts/recreate_infra.sh
```

That runs:

1. `./infra/apply.sh` — S3 bucket, source folders, Snowflake IAM role/policy  
2. `./snowflake/setup_s3_stage.sh` — `STORAGE INTEGRATION` + IAM trust + stage + `LIST`

Or step-by-step:

```bash
./infra/apply.sh              # plan|apply|destroy
./snowflake/setup_s3_stage.sh
```

Verify:

```bash
snowsql -c eseotres -q "USE SCHEMA ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR; LIST @eseotres_sources; LIST @eseotres_sources/camunda/;"
```

### Changing the S3 key prefix / bucket

All infra names hang off `infra/infra.env` (gitignored). Start from the example:

```bash
cp infra/infra.env.example infra/infra.env
```

Important knobs:

| Variable | Default | Meaning |
|---|---|---|
| `BUCKET_NAME` | `so3-data` | S3 bucket |
| `ROOT_PREFIX` | `imss_bienestar` | Key prefix under the bucket (`s3://bucket/prefix/`) |
| `SOURCE_FOLDERS` | `camunda,sagi,…` | Placeholder folders under the prefix |
| `SNOWFLAKE_ROLE_NAME` | `snowflake-s3-imss-bienestar` | IAM role Snowflake assumes |
| `INTEGRATION_NAME` | `s3_imss_bienestar` | Snowflake storage integration |
| `STAGE_NAME` | `eseotres_sources` | Snowflake stage |
| `SF_DATABASE` / `SF_SCHEMA` | `ESEOTRES_PHARMA` / `SRC_IMSS_BIENESTAR` | Target schema |
| `SNOWSQL_CONN` | `eseotres` | SnowSQL `-c` connection name |

After changing `ROOT_PREFIX` / `BUCKET_NAME`:

1. Align Streamlit `imssb_files/config.yml` → `s3.bucket` and `s3.root_prefix`
2. Re-run `./scripts/recreate_infra.sh`
3. Re-upload or re-snapshot data into the new prefix

One-off override without editing the file:

```bash
ROOT_PREFIX=imss_bienestar_dev ./scripts/recreate_infra.sh
```

## App setup

```bash
uv sync
cp .env.example .env          # optional
uv run streamlit run app.py
```

Tabs: **Paths** · **S3** · **Datasets** · **Camunda / SAGI**.

## Project layout

```
scripts/recreate_infra.sh     # master: S3 + Snowflake stage
infra/
  apply.sh                    # terraform apply (bucket + IAM)
  infra.env.example           # knobs (copy → infra.env)
  main.tf / snowflake_iam.tf  # bucket + Snowflake IAM
snowflake/
  setup_s3_stage.sh           # integration + stage + LIST
  create_*.sql                # SQL templates
app.py                        # Streamlit dashboard
imssb_files/                  # local data + config.yml (gitignored)
modules/                      # extractors, S3, datasets, …
```

## Legacy CLI

`main.py` still has the older interactive ETL menu. Prefer Streamlit for Excel → CSV → S3 and Camunda/SAGI web extract. Iceberg tables over the stage are next.
