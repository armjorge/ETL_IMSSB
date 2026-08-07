-- =============================================================================
-- Snowpipe AUTO_INGEST for Iceberg SRC tables (expanded by setup_snowpipes.sh)
-- =============================================================================
-- Prerequisites:
--   - Stage + ff_imssb_pipe_csv (setup_s3_stage.sh)
--   - Iceberg tables with etl_file_name column (src_iceberg_tables.sql)
--   - SNS topics from terraform (infra/snowpipe_sns.tf)
--
-- etl_file_name = basename of METADATA$FILENAME (S3 object name only).
-- =============================================================================

USE ROLE ${SF_ROLE};

USE DATABASE ${SF_DATABASE};

USE SCHEMA ${SF_SCHEMA};

-- Migrate legacy file_name → etl_file_name (idempotent where possible)
ALTER ICEBERG TABLE camunda_orders ADD COLUMN IF NOT EXISTS etl_file_name STRING;
ALTER ICEBERG TABLE invoicing ADD COLUMN IF NOT EXISTS etl_file_name STRING;
ALTER ICEBERG TABLE payments ADD COLUMN IF NOT EXISTS etl_file_name STRING;
ALTER ICEBERG TABLE payments ADD COLUMN IF NOT EXISTS file_name STRING;

CREATE OR REPLACE PIPE pipe_camunda_orders
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = '${SNS_ARN_CAMUNDA}'
  COMMENT = 'Append Camunda CSVs → camunda_orders (etl_file_name = basename)'
  AS
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
    FROM @${STAGE_NAME}/camunda/
  )
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv';

CREATE OR REPLACE PIPE pipe_payments
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = '${SNS_ARN_PAYMENTS}'
  COMMENT = 'Append payments CSVs → payments (file_name=CSV $5; etl_file_name=basename)'
  AS
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
    FROM @${STAGE_NAME}/payments/
  )
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv';

CREATE OR REPLACE PIPE pipe_invoicing
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = '${SNS_ARN_INVOICING}'
  COMMENT = 'Append invoicing CSVs → invoicing (etl_file_name = basename)'
  AS
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
    FROM @${STAGE_NAME}/invoicing/
  )
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv';

CREATE OR REPLACE PIPE pipe_sagi
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = '${SNS_ARN_SAGI}'
  COMMENT = 'Append SAGI CSVs → sagi (skip Unnamed:0; etl_file_name = basename)'
  AS
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
    FROM @${STAGE_NAME}/sagi/
  )
  FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
  PATTERN = '.*[.]csv';

ALTER PIPE pipe_camunda_orders SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE pipe_payments SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE pipe_invoicing SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE pipe_sagi SET PIPE_EXECUTION_PAUSED = FALSE;

SELECT SYSTEM$PIPE_STATUS('pipe_camunda_orders');
SELECT SYSTEM$PIPE_STATUS('pipe_payments');
SELECT SYSTEM$PIPE_STATUS('pipe_invoicing');
SELECT SYSTEM$PIPE_STATUS('pipe_sagi');
