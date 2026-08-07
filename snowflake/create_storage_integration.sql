-- =============================================================================
-- Create STORAGE INTEGRATION for IMSS Bienestar S3 (ACCOUNTADMIN required)
-- Expanded by setup_s3_stage.sh via envsubst.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS ${INTEGRATION_NAME}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
  STORAGE_ALLOWED_LOCATIONS = ('${S3_URL}');

GRANT USAGE ON INTEGRATION ${INTEGRATION_NAME} TO ROLE SYSADMIN;

DESC INTEGRATION ${INTEGRATION_NAME};
