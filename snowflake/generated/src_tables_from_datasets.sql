-- =============================================================================
-- Iceberg tables from app datasets (IF NOT EXISTS)
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

CREATE ICEBERG TABLE IF NOT EXISTS invoicing (
  referencia STRING,
  factura STRING,
  total STRING,
  uuid_descripcion STRING,
  uuid STRING,
  folio STRING,
  fecha STRING,
  nombre STRING,
  rfc STRING,
  descripcion STRING,
  cantidad STRING,
  importe STRING,
  archivo STRING,
  etl_file_name STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/invoicing/'
  COMMENT = 'App dataset: PAQ_INSABI_2023-2026. Source folder @eseotres_sources/invoicing/';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

CREATE ICEBERG TABLE IF NOT EXISTS payments (
  folio_fiscal STRING,
  referencia STRING,
  importe STRING,
  clc STRING,
  file_name STRING,
  etl_file_name STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/payments/'
  COMMENT = 'App dataset: payments 2025-2026. Source folder @eseotres_sources/payments/';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

CREATE ICEBERG TABLE IF NOT EXISTS camunda (
  numero_orden_suministro STRING,
  numero_contrato STRING,
  rfc_proveedor STRING,
  razon_social STRING,
  clave_medicamento STRING,
  medicamento STRING,
  precio_unitario STRING,
  cantidad_solicitada STRING,
  almacen_entrega STRING,
  clues_destino STRING,
  entidad_destino STRING,
  nombre_unidad STRING,
  descripcion_estatus_orden_suministro STRING,
  fecha_autorizacion STRING,
  fecha_limite_entrega STRING,
  numero_procedimiento STRING,
  orden_remision STRING,
  estatus STRING,
  etl_file_name STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/camunda/'
  COMMENT = 'App dataset: camunda 2023-2025. Source folder @eseotres_sources/camunda/';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

CREATE ICEBERG TABLE IF NOT EXISTS camunda_fantasmas (
  numero_orden_suministro STRING,
  numero_contrato STRING,
  rfc_proveedor STRING,
  razon_social STRING,
  clave_medicamento STRING,
  medicamento STRING,
  precio_unitario STRING,
  cantidad_solicitada STRING,
  almacen_entrega STRING,
  clues_destino STRING,
  entidad_destino STRING,
  nombre_unidad STRING,
  descripcion_estatus_orden_suministro STRING,
  fecha_autorizacion STRING,
  fecha_limite_entrega STRING,
  numero_procedimiento STRING,
  orden_remision STRING,
  estatus STRING,
  etl_file_name STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/camunda_fantasmas/'
  COMMENT = 'App dataset: camunda fantasmas. Source folder @eseotres_sources/camunda/';


USE DATABASE ESEOTRES_PHARMA;
USE SCHEMA SRC_IMSS_BIENESTAR;

CREATE ICEBERG TABLE IF NOT EXISTS penalties (
  orden_de_suministro STRING,
  pena STRING,
  oficio STRING,
  etl_file_name STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ev_imss_bienestar'
  BASE_LOCATION = 'iceberg/penalties/'
  COMMENT = 'App dataset: penalties letters. Source folder @eseotres_sources/penalties/';


