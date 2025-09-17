"""
Transform module for IMSS Bienestar data integration.
Handles data cleaning, validation, and transformation operations.
"""
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Callable
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class DataTransformer:
    """Base class for data transformation operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataTransformer.
        
        Args:
            config: Configuration dictionary for transformations
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply basic cleaning operations to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        try:
            self.logger.info("Applying basic data cleaning")
            
            # Remove completely empty rows and columns
            df_cleaned = df.dropna(how='all').dropna(axis=1, how='all')
            
            # Strip whitespace from string columns
            string_columns = df_cleaned.select_dtypes(include=['object']).columns
            for col in string_columns:
                if df_cleaned[col].dtype == 'object':
                    df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
                    # Replace empty strings with NaN
                    df_cleaned[col] = df_cleaned[col].replace('', np.nan)
            
            self.logger.info(f"Cleaned DataFrame: {len(df)} -> {len(df_cleaned)} rows")
            return df_cleaned
        except Exception as e:
            self.logger.error(f"Error in basic data cleaning: {str(e)}")
            raise
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names (lowercase, underscores, no spaces).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized column names
        """
        try:
            self.logger.info("Standardizing column names")
            
            # Convert to lowercase and replace spaces/special chars with underscores
            new_columns = []
            for col in df.columns:
                new_col = str(col).lower()
                new_col = re.sub(r'[^\w\s]', '', new_col)  # Remove special characters
                new_col = re.sub(r'\s+', '_', new_col)     # Replace spaces with underscores
                new_col = re.sub(r'_+', '_', new_col)      # Replace multiple underscores with single
                new_col = new_col.strip('_')               # Remove leading/trailing underscores
                new_columns.append(new_col)
            
            df.columns = new_columns
            self.logger.info(f"Standardized {len(new_columns)} column names")
            return df
        except Exception as e:
            self.logger.error(f"Error standardizing column names: {str(e)}")
            raise
    
    def validate_data_types(self, df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Validate and convert data types according to mapping.
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to desired data types
            
        Returns:
            DataFrame with validated data types
        """
        try:
            self.logger.info("Validating and converting data types")
            
            for column, dtype in type_mapping.items():
                if column in df.columns:
                    try:
                        if dtype == 'datetime':
                            df[column] = pd.to_datetime(df[column], errors='coerce')
                        elif dtype == 'numeric':
                            df[column] = pd.to_numeric(df[column], errors='coerce')
                        else:
                            df[column] = df[column].astype(dtype)
                        
                        self.logger.debug(f"Converted {column} to {dtype}")
                    except Exception as e:
                        self.logger.warning(f"Failed to convert {column} to {dtype}: {str(e)}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error in data type validation: {str(e)}")
            raise
    
    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df: Input DataFrame
            subset: List of columns to consider for duplicate detection
            
        Returns:
            DataFrame with duplicates removed
        """
        try:
            initial_count = len(df)
            df_deduped = df.drop_duplicates(subset=subset, keep='first')
            duplicates_removed = initial_count - len(df_deduped)
            
            self.logger.info(f"Removed {duplicates_removed} duplicate rows")
            return df_deduped
        except Exception as e:
            self.logger.error(f"Error removing duplicates: {str(e)}")
            raise


class IMSSBienestarTransformer(DataTransformer):
    """Specialized transformer for IMSS Bienestar data."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.imss_config = self.config.get('imss_bienestar', {})
    
    def transform_patient_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform patient data according to IMSS Bienestar standards.
        
        Args:
            df: Raw patient data DataFrame
            
        Returns:
            Transformed patient data DataFrame
        """
        try:
            self.logger.info("Transforming IMSS Bienestar patient data")
            
            # Apply base transformations
            df_transformed = self.clean_dataframe(df)
            df_transformed = self.standardize_column_names(df_transformed)
            
            # IMSS-specific transformations
            df_transformed = self._standardize_patient_identifiers(df_transformed)
            df_transformed = self._standardize_dates(df_transformed)
            df_transformed = self._validate_medical_data(df_transformed)
            
            # Remove duplicates based on patient identifier
            if 'patient_id' in df_transformed.columns:
                df_transformed = self.remove_duplicates(df_transformed, subset=['patient_id'])
            
            return df_transformed
        except Exception as e:
            self.logger.error(f"Error transforming patient data: {str(e)}")
            raise
    
    def transform_medical_services_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform medical services data according to IMSS Bienestar standards.
        
        Args:
            df: Raw medical services data DataFrame
            
        Returns:
            Transformed medical services data DataFrame
        """
        try:
            self.logger.info("Transforming IMSS Bienestar medical services data")
            
            # Apply base transformations
            df_transformed = self.clean_dataframe(df)
            df_transformed = self.standardize_column_names(df_transformed)
            
            # Medical services specific transformations
            df_transformed = self._standardize_service_codes(df_transformed)
            df_transformed = self._validate_service_data(df_transformed)
            
            return df_transformed
        except Exception as e:
            self.logger.error(f"Error transforming medical services data: {str(e)}")
            raise
    
    def _standardize_patient_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize patient identifiers (CURP, NSS, etc.)."""
        try:
            # Standardize CURP (Clave Única de Registro de Población)
            if 'curp' in df.columns:
                df['curp'] = df['curp'].astype(str).str.upper().str.strip()
                # Basic CURP format validation (18 characters)
                df['curp_valid'] = df['curp'].str.match(r'^[A-Z]{4}\d{6}[HM][A-Z]{5}\d{2}$')
            
            # Standardize NSS (Número de Seguridad Social)
            if 'nss' in df.columns:
                df['nss'] = df['nss'].astype(str).str.replace('-', '').str.strip()
                # Basic NSS format validation (11 digits)
                df['nss_valid'] = df['nss'].str.match(r'^\d{11}$')
            
            return df
        except Exception as e:
            self.logger.error(f"Error standardizing patient identifiers: {str(e)}")
            raise
    
    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize date columns."""
        try:
            date_columns = ['fecha_nacimiento', 'fecha_servicio', 'fecha_registro', 'birth_date', 'service_date', 'registration_date']
            
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Add validation flag
                    df[f'{col}_valid'] = df[col].notna()
            
            return df
        except Exception as e:
            self.logger.error(f"Error standardizing dates: {str(e)}")
            raise
    
    def _validate_medical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate medical data fields."""
        try:
            # Age validation
            if 'edad' in df.columns or 'age' in df.columns:
                age_col = 'edad' if 'edad' in df.columns else 'age'
                df[age_col] = pd.to_numeric(df[age_col], errors='coerce')
                df[f'{age_col}_valid'] = (df[age_col] >= 0) & (df[age_col] <= 120)
            
            # Gender standardization
            if 'sexo' in df.columns or 'gender' in df.columns:
                gender_col = 'sexo' if 'sexo' in df.columns else 'gender'
                df[gender_col] = df[gender_col].astype(str).str.upper().str.strip()
                df[gender_col] = df[gender_col].map({'M': 'M', 'F': 'F', 'MASCULINO': 'M', 'FEMENINO': 'F', 'HOMBRE': 'M', 'MUJER': 'F'})
            
            return df
        except Exception as e:
            self.logger.error(f"Error validating medical data: {str(e)}")
            raise
    
    def _standardize_service_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize medical service codes."""
        try:
            # Standardize service codes
            if 'codigo_servicio' in df.columns or 'service_code' in df.columns:
                service_col = 'codigo_servicio' if 'codigo_servicio' in df.columns else 'service_code'
                df[service_col] = df[service_col].astype(str).str.upper().str.strip()
            
            return df
        except Exception as e:
            self.logger.error(f"Error standardizing service codes: {str(e)}")
            raise
    
    def _validate_service_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate medical service data."""
        try:
            # Validate service costs
            if 'costo' in df.columns or 'cost' in df.columns:
                cost_col = 'costo' if 'costo' in df.columns else 'cost'
                df[cost_col] = pd.to_numeric(df[cost_col], errors='coerce')
                df[f'{cost_col}_valid'] = df[cost_col] >= 0
            
            return df
        except Exception as e:
            self.logger.error(f"Error validating service data: {str(e)}")
            raise