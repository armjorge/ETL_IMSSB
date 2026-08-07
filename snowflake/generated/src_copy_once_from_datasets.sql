-- =============================================================================
-- One-shot COPY INTO from stage (backfill)
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

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
    archivo,
    etl_file_name
)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
      REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/invoicing/
)
FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
PATTERN = '.*invoicing [0-9].*[.]csv';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

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
PATTERN = '.*payments [0-9].*[.]csv';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

COPY INTO camunda (
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
PATTERN = '.*camunda [0-9].*[.]csv';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

COPY INTO camunda_fantasmas (
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
PATTERN = '.*fantasmas.*[.]csv';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

COPY INTO penalties (
    orden_de_suministro,
    pena,
    oficio,
    etl_file_name
)
FROM (
  SELECT
    $1, $2, $3,
      REGEXP_REPLACE(METADATA$FILENAME, '^.*/', '')
  FROM @eseotres_sources/penalties/
)
FILE_FORMAT = (FORMAT_NAME = ff_imssb_pipe_csv)
PATTERN = '.*penalties [0-9].*[.]csv';


