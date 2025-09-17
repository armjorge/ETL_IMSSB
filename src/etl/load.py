"""
Load module for IMSS Bienestar data integration.
Handles data loading to various destinations including files, databases, and APIs.
"""
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
import os
from pathlib import Path
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class DataLoader:
    """Base class for data loading operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataLoader.
        
        Args:
            config: Configuration dictionary for data destinations
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str, **kwargs) -> str:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            output_path: Path where to save the CSV file
            **kwargs: Additional arguments for pandas.to_csv
            
        Returns:
            Path to the saved file
        """
        try:
            self.logger.info(f"Saving DataFrame to CSV: {output_path}")
            
            # Ensure output directory exists
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Default CSV parameters
            csv_params = {
                'index': False,
                'encoding': 'utf-8',
                **kwargs
            }
            
            df.to_csv(output_path, **csv_params)
            self.logger.info(f"Successfully saved {len(df)} rows to {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Error saving DataFrame to CSV {output_path}: {str(e)}")
            raise
    
    def save_to_excel(self, df: pd.DataFrame, output_path: str, sheet_name: str = 'Sheet1', **kwargs) -> str:
        """
        Save DataFrame to Excel file.
        
        Args:
            df: DataFrame to save
            output_path: Path where to save the Excel file
            sheet_name: Name of the sheet
            **kwargs: Additional arguments for pandas.to_excel
            
        Returns:
            Path to the saved file
        """
        try:
            self.logger.info(f"Saving DataFrame to Excel: {output_path}")
            
            # Ensure output directory exists
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Default Excel parameters
            excel_params = {
                'index': False,
                'sheet_name': sheet_name,
                **kwargs
            }
            
            df.to_excel(output_path, **excel_params)
            self.logger.info(f"Successfully saved {len(df)} rows to {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Error saving DataFrame to Excel {output_path}: {str(e)}")
            raise
    
    def save_to_json(self, df: pd.DataFrame, output_path: str, orient: str = 'records', **kwargs) -> str:
        """
        Save DataFrame to JSON file.
        
        Args:
            df: DataFrame to save
            output_path: Path where to save the JSON file
            orient: JSON orientation ('records', 'index', 'values', etc.)
            **kwargs: Additional arguments for pandas.to_json
            
        Returns:
            Path to the saved file
        """
        try:
            self.logger.info(f"Saving DataFrame to JSON: {output_path}")
            
            # Ensure output directory exists
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Default JSON parameters
            json_params = {
                'orient': orient,
                'date_format': 'iso',
                'indent': 2,
                **kwargs
            }
            
            df.to_json(output_path, **json_params)
            self.logger.info(f"Successfully saved {len(df)} rows to {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Error saving DataFrame to JSON {output_path}: {str(e)}")
            raise
    
    def save_multiple_formats(self, df: pd.DataFrame, base_path: str, formats: List[str]) -> Dict[str, str]:
        """
        Save DataFrame to multiple formats.
        
        Args:
            df: DataFrame to save
            base_path: Base path without extension
            formats: List of formats ('csv', 'excel', 'json')
            
        Returns:
            Dictionary mapping format to saved file path
        """
        try:
            saved_files = {}
            
            for format_type in formats:
                if format_type.lower() == 'csv':
                    file_path = f"{base_path}.csv"
                    saved_files['csv'] = self.save_to_csv(df, file_path)
                elif format_type.lower() in ['excel', 'xlsx']:
                    file_path = f"{base_path}.xlsx"
                    saved_files['excel'] = self.save_to_excel(df, file_path)
                elif format_type.lower() == 'json':
                    file_path = f"{base_path}.json"
                    saved_files['json'] = self.save_to_json(df, file_path)
                else:
                    self.logger.warning(f"Unsupported format: {format_type}")
            
            return saved_files
        except Exception as e:
            self.logger.error(f"Error saving multiple formats: {str(e)}")
            raise


class IMSSBienestarLoader(DataLoader):
    """Specialized loader for IMSS Bienestar data."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.imss_config = self.config.get('imss_bienestar', {})
        self.output_config = self.imss_config.get('output', {})
    
    def save_processed_patient_data(self, df: pd.DataFrame, output_dir: str = "output/patients") -> Dict[str, str]:
        """
        Save processed patient data with timestamp and validation report.
        
        Args:
            df: Processed patient DataFrame
            output_dir: Output directory for patient data
            
        Returns:
            Dictionary with paths to saved files
        """
        try:
            self.logger.info("Saving processed IMSS Bienestar patient data")
            
            # Create timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"imss_bienestar_patients_{timestamp}"
            base_path = os.path.join(output_dir, base_filename)
            
            # Ensure output directory exists
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save in multiple formats
            saved_files = self.save_multiple_formats(df, base_path, ['csv', 'excel'])
            
            # Generate and save validation report
            validation_report = self._generate_validation_report(df)
            report_path = os.path.join(output_dir, f"validation_report_{timestamp}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(validation_report, f, indent=2, default=str)
            
            saved_files['validation_report'] = report_path
            
            self.logger.info(f"Successfully saved patient data to {len(saved_files)} files")
            return saved_files
        except Exception as e:
            self.logger.error(f"Error saving processed patient data: {str(e)}")
            raise
    
    def save_processed_services_data(self, df: pd.DataFrame, output_dir: str = "output/services") -> Dict[str, str]:
        """
        Save processed medical services data with timestamp and validation report.
        
        Args:
            df: Processed services DataFrame
            output_dir: Output directory for services data
            
        Returns:
            Dictionary with paths to saved files
        """
        try:
            self.logger.info("Saving processed IMSS Bienestar medical services data")
            
            # Create timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"imss_bienestar_services_{timestamp}"
            base_path = os.path.join(output_dir, base_filename)
            
            # Ensure output directory exists
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save in multiple formats
            saved_files = self.save_multiple_formats(df, base_path, ['csv', 'excel'])
            
            # Generate and save validation report
            validation_report = self._generate_validation_report(df)
            report_path = os.path.join(output_dir, f"services_validation_report_{timestamp}.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(validation_report, f, indent=2, default=str)
            
            saved_files['validation_report'] = report_path
            
            self.logger.info(f"Successfully saved services data to {len(saved_files)} files")
            return saved_files
        except Exception as e:
            self.logger.error(f"Error saving processed services data: {str(e)}")
            raise
    
    def save_etl_summary(self, summary_data: Dict[str, Any], output_dir: str = "output/reports") -> str:
        """
        Save ETL process summary report.
        
        Args:
            summary_data: Dictionary containing ETL process summary
            output_dir: Output directory for reports
            
        Returns:
            Path to the saved summary report
        """
        try:
            self.logger.info("Saving ETL process summary")
            
            # Ensure output directory exists
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_path = os.path.join(output_dir, f"etl_summary_{timestamp}.json")
            
            # Add metadata
            summary_data.update({
                'generated_at': datetime.now().isoformat(),
                'etl_version': '1.0.0',
                'data_source': 'IMSS_Bienestar'
            })
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, default=str)
            
            self.logger.info(f"ETL summary saved to {summary_path}")
            return summary_path
        except Exception as e:
            self.logger.error(f"Error saving ETL summary: {str(e)}")
            raise
    
    def _generate_validation_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate validation report for processed data.
        
        Args:
            df: Processed DataFrame
            
        Returns:
            Dictionary containing validation statistics
        """
        try:
            report = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'data_types': df.dtypes.astype(str).to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'null_percentages': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
                'validation_columns': {}
            }
            
            # Check for validation columns
            validation_cols = [col for col in df.columns if col.endswith('_valid')]
            for col in validation_cols:
                if col in df.columns:
                    valid_count = df[col].sum() if df[col].dtype == 'bool' else 0
                    report['validation_columns'][col] = {
                        'valid_records': int(valid_count),
                        'invalid_records': int(len(df) - valid_count),
                        'validation_rate': round(valid_count / len(df) * 100, 2)
                    }
            
            return report
        except Exception as e:
            self.logger.error(f"Error generating validation report: {str(e)}")
            return {'error': str(e)}