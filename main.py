"""
Main ETL Pipeline for IMSS Bienestar data integration.
Orchestrates the Extract, Transform, Load process.
"""
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import argparse

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from etl.extract import IMSSBienestrarExtractor
from etl.transform import IMSSBienestarTransformer
from etl.load import IMSSBienestarLoader
from utils.config import ConfigManager
from utils.logger import setup_logging


class IMSSBienestarETL:
    """Main ETL pipeline for IMSS Bienestar data integration."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.get_config()
        
        # Setup logging
        setup_logging(self.config.get('logging', {}))
        self.logger = logging.getLogger(__name__)
        
        # Initialize ETL components
        self.extractor = IMSSBienestrarExtractor(self.config)
        self.transformer = IMSSBienestarTransformer(self.config)
        self.loader = IMSSBienestarLoader(self.config)
        
        # ETL metrics
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'duration': None,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'files_processed': 0,
            'errors': []
        }
    
    def run_full_pipeline(self, input_sources: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Args:
            input_sources: List of dictionaries with 'type' and 'path' keys
                          [{'type': 'patients', 'path': '/path/to/patients.csv'}, ...]
            
        Returns:
            Dictionary containing pipeline execution results
        """
        try:
            self.logger.info("Starting IMSS Bienestar ETL pipeline")
            self.metrics['start_time'] = datetime.now()
            
            results = {
                'patient_data': None,
                'services_data': None,
                'saved_files': {},
                'summary': {}
            }
            
            # Process each input source
            for source in input_sources:
                source_type = source.get('type')
                source_path = source.get('path')
                
                if not source_path or not Path(source_path).exists():
                    error_msg = f"Source file not found: {source_path}"
                    self.logger.error(error_msg)
                    self.metrics['errors'].append(error_msg)
                    continue
                
                self.logger.info(f"Processing {source_type} data from {source_path}")
                
                try:
                    if source_type == 'patients':
                        results['patient_data'] = self._process_patient_data(source_path)
                    elif source_type == 'services':
                        results['services_data'] = self._process_services_data(source_path)
                    else:
                        self.logger.warning(f"Unknown source type: {source_type}")
                        continue
                    
                    self.metrics['files_processed'] += 1
                    
                except Exception as e:
                    error_msg = f"Error processing {source_type} data from {source_path}: {str(e)}"
                    self.logger.error(error_msg)
                    self.metrics['errors'].append(error_msg)
            
            # Generate summary report
            self.metrics['end_time'] = datetime.now()
            self.metrics['duration'] = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
            
            results['summary'] = self._generate_summary()
            
            # Save summary report
            summary_path = self.loader.save_etl_summary(results['summary'])
            results['saved_files']['summary'] = summary_path
            
            self.logger.info("IMSS Bienestar ETL pipeline completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Fatal error in ETL pipeline: {str(e)}")
            self.metrics['errors'].append(str(e))
            raise
    
    def _process_patient_data(self, source_path: str) -> Dict[str, Any]:
        """Process patient data through the ETL pipeline."""
        try:
            # Extract
            self.logger.info("Extracting patient data")
            raw_data = self.extractor.extract_patient_data(source_path)
            self.metrics['records_extracted'] += len(raw_data)
            
            # Transform
            self.logger.info("Transforming patient data")
            transformed_data = self.transformer.transform_patient_data(raw_data)
            self.metrics['records_transformed'] += len(transformed_data)
            
            # Load
            self.logger.info("Loading patient data")
            saved_files = self.loader.save_processed_patient_data(transformed_data)
            self.metrics['records_loaded'] += len(transformed_data)
            
            return {
                'data': transformed_data,
                'saved_files': saved_files,
                'record_count': len(transformed_data)
            }
            
        except Exception as e:
            self.logger.error(f"Error processing patient data: {str(e)}")
            raise
    
    def _process_services_data(self, source_path: str) -> Dict[str, Any]:
        """Process medical services data through the ETL pipeline."""
        try:
            # Extract
            self.logger.info("Extracting services data")
            raw_data = self.extractor.extract_medical_services_data(source_path)
            self.metrics['records_extracted'] += len(raw_data)
            
            # Transform
            self.logger.info("Transforming services data")
            transformed_data = self.transformer.transform_medical_services_data(raw_data)
            self.metrics['records_transformed'] += len(transformed_data)
            
            # Load
            self.logger.info("Loading services data")
            saved_files = self.loader.save_processed_services_data(transformed_data)
            self.metrics['records_loaded'] += len(transformed_data)
            
            return {
                'data': transformed_data,
                'saved_files': saved_files,
                'record_count': len(transformed_data)
            }
            
        except Exception as e:
            self.logger.error(f"Error processing services data: {str(e)}")
            raise
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate ETL pipeline execution summary."""
        return {
            'pipeline_info': {
                'version': '1.0.0',
                'execution_date': self.metrics['start_time'].isoformat(),
                'duration_seconds': self.metrics['duration']
            },
            'data_processing': {
                'files_processed': self.metrics['files_processed'],
                'records_extracted': self.metrics['records_extracted'],
                'records_transformed': self.metrics['records_transformed'],
                'records_loaded': self.metrics['records_loaded']
            },
            'quality_metrics': {
                'success_rate': (self.metrics['files_processed'] / max(self.metrics['files_processed'] + len(self.metrics['errors']), 1)) * 100,
                'error_count': len(self.metrics['errors']),
                'errors': self.metrics['errors']
            }
        }


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="IMSS Bienestar ETL Pipeline")
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--patients', type=str, help='Path to patient data file')
    parser.add_argument('--services', type=str, help='Path to services data file')
    parser.add_argument('--input-dir', type=str, help='Directory containing input files')
    
    args = parser.parse_args()
    
    # Initialize ETL pipeline
    etl = IMSSBienestarETL(args.config)
    
    # Prepare input sources
    input_sources = []
    
    if args.patients:
        input_sources.append({'type': 'patients', 'path': args.patients})
    
    if args.services:
        input_sources.append({'type': 'services', 'path': args.services})
    
    if args.input_dir:
        # Auto-discover files in input directory
        input_dir = Path(args.input_dir)
        if input_dir.exists():
            for file_path in input_dir.glob('*'):
                if file_path.is_file():
                    filename = file_path.name.lower()
                    if 'patient' in filename or 'paciente' in filename:
                        input_sources.append({'type': 'patients', 'path': str(file_path)})
                    elif 'service' in filename or 'servicio' in filename:
                        input_sources.append({'type': 'services', 'path': str(file_path)})
    
    if not input_sources:
        print("No input sources specified. Use --patients, --services, or --input-dir")
        return
    
    # Run ETL pipeline
    try:
        results = etl.run_full_pipeline(input_sources)
        print(f"\nETL Pipeline completed successfully!")
        print(f"Files processed: {results['summary']['data_processing']['files_processed']}")
        print(f"Records processed: {results['summary']['data_processing']['records_loaded']}")
        print(f"Duration: {results['summary']['pipeline_info']['duration_seconds']:.2f} seconds")
        
        if results['summary']['quality_metrics']['errors']:
            print(f"Errors encountered: {len(results['summary']['quality_metrics']['errors'])}")
            for error in results['summary']['quality_metrics']['errors']:
                print(f"  - {error}")
        
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())