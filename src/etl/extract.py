"""
Extract module for IMSS Bienestar data integration.
Handles data extraction from various sources including databases, APIs, and files.
"""
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class DataExtractor:
    """Base class for data extraction operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataExtractor.
        
        Args:
            config: Configuration dictionary for data sources
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            **kwargs: Additional arguments for pandas.read_csv
            
        Returns:
            DataFrame with extracted data
        """
        try:
            self.logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            self.logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting data from CSV {file_path}: {str(e)}")
            raise
    
    def extract_from_excel(self, file_path: str, sheet_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data from Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet to read (default: first sheet)
            **kwargs: Additional arguments for pandas.read_excel
            
        Returns:
            DataFrame with extracted data
        """
        try:
            self.logger.info(f"Extracting data from Excel: {file_path}, sheet: {sheet_name}")
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            self.logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting data from Excel {file_path}: {str(e)}")
            raise
    
    def extract_from_directory(self, directory_path: str, file_pattern: str = "*.csv") -> List[pd.DataFrame]:
        """
        Extract data from multiple files in a directory.
        
        Args:
            directory_path: Path to the directory containing files
            file_pattern: Pattern to match files (default: "*.csv")
            
        Returns:
            List of DataFrames, one for each file
        """
        try:
            self.logger.info(f"Extracting data from directory: {directory_path}")
            directory = Path(directory_path)
            files = list(directory.glob(file_pattern))
            
            if not files:
                self.logger.warning(f"No files found matching pattern {file_pattern} in {directory_path}")
                return []
            
            dataframes = []
            for file_path in files:
                if file_pattern.endswith('.csv'):
                    df = self.extract_from_csv(str(file_path))
                elif file_pattern.endswith('.xlsx') or file_pattern.endswith('.xls'):
                    df = self.extract_from_excel(str(file_path))
                else:
                    # Default to CSV
                    df = self.extract_from_csv(str(file_path))
                
                df['source_file'] = file_path.name
                dataframes.append(df)
            
            self.logger.info(f"Successfully extracted data from {len(dataframes)} files")
            return dataframes
        except Exception as e:
            self.logger.error(f"Error extracting data from directory {directory_path}: {str(e)}")
            raise


class IMSSBienestrarExtractor(DataExtractor):
    """Specialized extractor for IMSS Bienestar data sources."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.imss_config = self.config.get('imss_bienestar', {})
    
    def extract_patient_data(self, source_path: str) -> pd.DataFrame:
        """
        Extract patient data from IMSS Bienestar files.
        
        Args:
            source_path: Path to patient data file
            
        Returns:
            DataFrame with patient information
        """
        try:
            self.logger.info("Extracting IMSS Bienestar patient data")
            
            # Determine file type and extract accordingly
            if source_path.endswith('.csv'):
                df = self.extract_from_csv(source_path, encoding='utf-8')
            elif source_path.endswith(('.xlsx', '.xls')):
                df = self.extract_from_excel(source_path)
            else:
                raise ValueError(f"Unsupported file format: {source_path}")
            
            # Add metadata
            df['extraction_timestamp'] = pd.Timestamp.now()
            df['data_source'] = 'IMSS_Bienestar'
            
            return df
        except Exception as e:
            self.logger.error(f"Error extracting IMSS Bienestar patient data: {str(e)}")
            raise
    
    def extract_medical_services_data(self, source_path: str) -> pd.DataFrame:
        """
        Extract medical services data from IMSS Bienestar files.
        
        Args:
            source_path: Path to medical services data file
            
        Returns:
            DataFrame with medical services information
        """
        try:
            self.logger.info("Extracting IMSS Bienestar medical services data")
            
            if source_path.endswith('.csv'):
                df = self.extract_from_csv(source_path, encoding='utf-8')
            elif source_path.endswith(('.xlsx', '.xls')):
                df = self.extract_from_excel(source_path)
            else:
                raise ValueError(f"Unsupported file format: {source_path}")
            
            # Add metadata
            df['extraction_timestamp'] = pd.Timestamp.now()
            df['data_source'] = 'IMSS_Bienestar_Services'
            
            return df
        except Exception as e:
            self.logger.error(f"Error extracting IMSS Bienestar medical services data: {str(e)}")
            raise