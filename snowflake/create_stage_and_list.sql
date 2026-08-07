-- =============================================================================
-- Schema + pipe CSV format + external stage + LIST (verifies S3 access)
-- Expanded by setup_s3_stage.sh via envsubst.
-- =============================================================================

USE ROLE ${SF_ROLE};

USE DATABASE ${SF_DATABASE};

CREATE SCHEMA IF NOT EXISTS ${SF_SCHEMA};

USE SCHEMA ${SF_SCHEMA};

CREATE FILE FORMAT IF NOT EXISTS ff_imssb_pipe_csv
  TYPE = CSV
  FIELD_DELIMITER = '|'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  ESCAPE = '\\'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('')
  ENCODING = 'UTF8'
  COMMENT = 'IMSS Bienestar pipe-separated snapshot CSVs';

CREATE STAGE IF NOT EXISTS ${STAGE_NAME}
  URL = '${S3_URL}'
  STORAGE_INTEGRATION = ${INTEGRATION_NAME}
  DIRECTORY = (ENABLE = TRUE)
  FILE_FORMAT = ff_imssb_pipe_csv
  COMMENT = 'S3 root for IMSS Bienestar extracts (camunda/, payments/, invoicing/, sagi/, …)';

-- Ensure stage points at the integration even if it already existed
ALTER STAGE ${STAGE_NAME} SET
  URL = '${S3_URL}'
  STORAGE_INTEGRATION = ${INTEGRATION_NAME}
  DIRECTORY = (ENABLE = TRUE)
  FILE_FORMAT = ff_imssb_pipe_csv;

ALTER STAGE ${STAGE_NAME} REFRESH;

LIST @${STAGE_NAME};

LIST @${STAGE_NAME}/camunda/;

LIST @${STAGE_NAME}/payments/;
