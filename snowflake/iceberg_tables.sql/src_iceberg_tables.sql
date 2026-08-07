-- =============================================================================
-- SRC_IMSS_BIENESTAR — Iceberg tables over staged pipe CSVs
-- =============================================================================
-- Prerequisites:
--   ./snowflake/setup_s3_stage.sh
--     → stage @ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR.eseotres_sources
--     → file format ff_imssb_pipe_csv (pipe '|', SKIP_HEADER = 1)
--   External volume for Iceberg Parquet + metadata (see below)
--   Auto-ingest (optional): ./snowflake/setup_snowpipes.sh after terraform SNS
--
-- Landing CSVs (read-only source for COPY) — files sit directly under each folder:
--   @ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR.eseotres_sources/camunda/
--   @ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR.eseotres_sources/payments/
--   @ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR.eseotres_sources/invoicing/
--   @ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR.eseotres_sources/sagi/
--
-- Iceberg storage (Snowflake-managed catalog writes Parquet here):
--   s3://so3-data/imss_bienestar/iceberg/{camunda_orders|payments|invoicing|sagi}/
--
-- etl_file_name = basename of the staged object (METADATA$FILENAME), not a CSV column.
-- =============================================================================

USE ROLE SYSADMIN;

USE DATABASE ESEOTRES_PHARMA;

USE SCHEMA SRC_IMSS_BIENESTAR;

-- -----------------------------------------------------------------------------
-- External volume (ACCOUNTADMIN once). Same IAM role as the stage integration,
-- but a *different* STORAGE_AWS_EXTERNAL_ID — both must be in the role trust:
--   integration : DESC INTEGRATION s3_imss_bienestar
--   volume      : DESC EXTERNAL VOLUME ev_imss_bienestar
-- Persist extras in infra/terraform.tfvars.snowflake → snowflake_external_ids.
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE EXTERNAL VOLUME IF NOT EXISTS ev_imss_bienestar
  STORAGE_LOCATIONS = (
    (
      NAME = 'imss_bienestar_s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://so3-data/imss_bienestar/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::747289880051:role/snowflake-s3-imss-bienestar'
    )
  )
  ALLOW_WRITES = TRUE
  COMMENT = 'Iceberg external volume for SRC_IMSS_BIENESTAR';

GRANT USAGE ON EXTERNAL VOLUME ev_imss_bienestar TO ROLE SYSADMIN;

DESC EXTERNAL VOLUME ev_imss_bienestar;

USE ROLE SYSADMIN;

USE SCHEMA ESEOTRES_PHARMA.SRC_IMSS_BIENESTAR;

-- -----------------------------------------------------------------------------
-- camunda_orders — all columns STRING (text) for initial load
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE camunda_orders (
  numero_orden_suministro                 STRING,
  numero_contrato                         STRING,
  rfc_proveedor                           STRING,
  razon_social                            STRING,
  clave_medicamento                       STRING,
  medicamento                             STRING,
  precio_unitario                         STRING,
  cantidad_solicitada                     STRING,
  almacen_entrega                         STRING,
  clues_destino                           STRING,
  entidad_destino                         STRING,
  nombre_unidad                           STRING,
  descripcion_estatus_orden_suministro    STRING,
  fecha_autorizacion                      STRING,
  fecha_limite_entrega                    STRING,
  numero_procedimiento                    STRING,
  orden_remision                          STRING,
  estatus                                 STRING,
  etl_file_name                           STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/camunda_orders/'
  COMMENT = 'Camunda supply orders; all text. Source: @eseotres_sources/camunda';

COPY INTO camunda_orders (
  numero_orden_suministro,
  numero_contrato,
  rfc_proveedor,
  razon_social,
  clave_medicamento,
  medicamento,
  precio_unitario,
  cantidad_solicitada,
  almacen_entrega,
  clues_destino,
  entidad_destino,
  nombre_unidad,
  descripcion_estatus_orden_suministro,
  fecha_autorizacion,
  fecha_limite_entrega,
  numero_procedimiento,
  orden_remision,
  estatus,
  etl_file_name
)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
    REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/camunda/
)
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv'
  LOAD_MODE = FULL_INGEST
  ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) AS row_count FROM camunda_orders;
SELECT * FROM camunda_orders LIMIT 10;

-- -----------------------------------------------------------------------------
-- payments — CSV has its own file_name ($5); etl_file_name = S3 basename
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE payments (
  folio_fiscal    STRING,
  referencia      STRING,
  importe         STRING,
  clc             STRING,
  file_name       STRING,
  etl_file_name   STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/payments/'
  COMMENT = 'Payments extracts; all text. Source: @eseotres_sources/payments';

COPY INTO payments (
  folio_fiscal,
  referencia,
  importe,
  clc,
  file_name,
  etl_file_name
)
FROM (
  SELECT
    $1, $2, $3, $4, $5,
    REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/payments/
)
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv'
  LOAD_MODE = FULL_INGEST
  ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) AS row_count FROM payments;
SELECT * FROM payments LIMIT 10;

-- -----------------------------------------------------------------------------
-- invoicing — all columns STRING (text) for initial load
-- CSV header "UUID Descripción" → uuid_descripcion (positional COPY)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE invoicing (
  referencia         STRING,
  factura            STRING,
  total              STRING,
  uuid_descripcion   STRING,
  uuid               STRING,
  folio              STRING,
  fecha              STRING,
  nombre             STRING,
  rfc                STRING,
  descripcion        STRING,
  cantidad           STRING,
  importe            STRING,
  etl_file_name      STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/invoicing/'
  COMMENT = 'Invoicing extracts; all text. Source: @eseotres_sources/invoicing';

COPY INTO invoicing (
  referencia,
  factura,
  total,
  uuid_descripcion,
  uuid,
  folio,
  fecha,
  nombre,
  rfc,
  descripcion,
  cantidad,
  importe,
  etl_file_name
)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/invoicing/
)
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv'
  LOAD_MODE = FULL_INGEST
  ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) AS row_count FROM invoicing;
SELECT * FROM invoicing LIMIT 10;

-- -----------------------------------------------------------------------------
-- sagi — skip CSV col $1 (Unnamed: 0); etl_file_name = S3 basename
-- Header: Unnamed: 0|Número de oficio|Proveedor|RFC|Número de contrato|
--         Orden de suministro|Número de factura|Folio fiscal|Total|CLUES|
--         Estado de la factura|Opciones
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE sagi (
  numero_de_oficio        STRING,
  proveedor               STRING,
  rfc                     STRING,
  numero_de_contrato      STRING,
  orden_de_suministro     STRING,
  numero_de_factura       STRING,
  folio_fiscal            STRING,
  total                   STRING,
  clues                   STRING,
  estado_de_la_factura    STRING,
  opciones                STRING,
  etl_file_name           STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/sagi/'
  COMMENT = 'SAGI extracts; all text. Source: @eseotres_sources/sagi';

COPY INTO sagi (
  numero_de_oficio,
  proveedor,
  rfc,
  numero_de_contrato,
  orden_de_suministro,
  numero_de_factura,
  folio_fiscal,
  total,
  clues,
  estado_de_la_factura,
  opciones,
  etl_file_name
)
FROM (
  SELECT
    $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/sagi/
)
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv'
  LOAD_MODE = FULL_INGEST
  ON_ERROR = 'ABORT_STATEMENT';

SELECT COUNT(*) AS row_count FROM sagi;
SELECT * FROM sagi LIMIT 10;
