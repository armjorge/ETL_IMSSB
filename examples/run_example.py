#!/usr/bin/env python3
"""
Example script demonstrating IMSS Bienestar ETL pipeline usage.
"""
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import IMSSBienestarETL
from utils.logger import setup_logging
import pandas as pd
import json


def create_sample_data():
    """Create sample IMSS Bienestar data for demonstration."""
    
    # Create sample patient data
    patients_data = {
        'CURP': ['ABCD123456HDFRYT01', 'EFGH789012MDFRZX02', 'IJKL345678HDFRYU03'],
        'NSS': ['12345678901', '23456789012', '34567890123'],
        'Nombre': ['Juan Carlos', 'María Elena', 'Pedro Luis'],
        'Apellido_Paterno': ['García', 'Rodríguez', 'Hernández'],
        'Apellido_Materno': ['López', 'Martínez', 'Pérez'],
        'Fecha_Nacimiento': ['1985-03-15', '1990-07-20', '1975-11-10'],
        'Sexo': ['M', 'F', 'M'],
        'Edad': [39, 34, 49],
        'Estado': ['Activo', 'Activo', 'Inactivo'],
        'Unidad_Medica': ['UMF 001', 'UMF 002', 'UMF 003']
    }
    
    # Create sample services data
    services_data = {
        'Codigo_Servicio': ['CONS001', 'LAB001', 'RAD001', 'CONS002', 'MED001'],
        'Descripcion': ['Consulta General', 'Laboratorio Clínico', 'Radiografía', 'Consulta Especialidad', 'Medicamentos'],
        'Fecha_Servicio': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
        'NSS_Paciente': ['12345678901', '12345678901', '23456789012', '34567890123', '12345678901'],
        'Costo': [150.00, 300.00, 450.00, 250.00, 75.00],
        'Unidad_Medica': ['UMF 001', 'UMF 001', 'UMF 002', 'UMF 003', 'UMF 001'],
        'Medico': ['Dr. Pérez', 'Lab. Central', 'Dr. Gómez', 'Dra. Ruiz', 'Farmacia']
    }
    
    # Create DataFrames
    patients_df = pd.DataFrame(patients_data)
    services_df = pd.DataFrame(services_data)
    
    # Create sample data directory
    sample_dir = Path('sample_data')
    sample_dir.mkdir(exist_ok=True)
    
    # Save sample data
    patients_df.to_csv(sample_dir / 'imss_bienestar_pacientes.csv', index=False, encoding='utf-8')
    services_df.to_csv(sample_dir / 'imss_bienestar_servicios.csv', index=False, encoding='utf-8')
    
    print("Sample data created:")
    print(f"- {sample_dir / 'imss_bienestar_pacientes.csv'} ({len(patients_df)} records)")
    print(f"- {sample_dir / 'imss_bienestar_servicios.csv'} ({len(services_df)} records)")
    
    return sample_dir


def run_example_etl():
    """Run example ETL process with sample data."""
    print("=" * 60)
    print("IMSS Bienestar ETL Pipeline - Example Usage")
    print("=" * 60)
    
    # Create sample data
    sample_dir = create_sample_data()
    
    # Initialize ETL pipeline with configuration
    config_path = "config/default.json"
    etl = IMSSBienestarETL(config_path)
    
    # Define input sources
    input_sources = [
        {
            'type': 'patients',
            'path': str(sample_dir / 'imss_bienestar_pacientes.csv')
        },
        {
            'type': 'services', 
            'path': str(sample_dir / 'imss_bienestar_servicios.csv')
        }
    ]
    
    try:
        # Run ETL pipeline
        print("\nStarting ETL pipeline...")
        results = etl.run_full_pipeline(input_sources)
        
        # Display results
        print("\n" + "=" * 60)
        print("ETL PIPELINE RESULTS")
        print("=" * 60)
        
        summary = results['summary']
        print(f"Duration: {summary['pipeline_info']['duration_seconds']:.2f} seconds")
        print(f"Files processed: {summary['data_processing']['files_processed']}")
        print(f"Records extracted: {summary['data_processing']['records_extracted']}")
        print(f"Records transformed: {summary['data_processing']['records_transformed']}")
        print(f"Records loaded: {summary['data_processing']['records_loaded']}")
        print(f"Success rate: {summary['quality_metrics']['success_rate']:.1f}%")
        
        if summary['quality_metrics']['errors']:
            print(f"\nErrors encountered ({len(summary['quality_metrics']['errors'])}):")
            for error in summary['quality_metrics']['errors']:
                print(f"  - {error}")
        else:
            print("\n✅ No errors encountered!")
        
        print("\n" + "=" * 60)
        print("OUTPUT FILES GENERATED")
        print("=" * 60)
        
        # List output files
        output_dir = Path('output')
        if output_dir.exists():
            for file_path in output_dir.rglob('*'):
                if file_path.is_file():
                    size_kb = file_path.stat().st_size / 1024
                    print(f"  {file_path} ({size_kb:.1f} KB)")
        
        print("\n🎉 ETL Pipeline completed successfully!")
        print("\nNext steps:")
        print("1. Review the generated files in the 'output' directory")
        print("2. Check validation reports for data quality metrics")
        print("3. Use your own data files by modifying the input_sources")
        
    except Exception as e:
        print(f"\n❌ ETL Pipeline failed: {str(e)}")
        return 1
    
    return 0


def show_data_requirements():
    """Display expected data format and requirements."""
    print("\n" + "=" * 60)
    print("EXPECTED DATA FORMATS")
    print("=" * 60)
    
    print("\nPATIENT DATA (CSV/Excel):")
    print("Required columns:")
    print("- CURP: Clave Única de Registro de Población (18 characters)")
    print("- NSS: Número de Seguridad Social (11 digits)")
    print("- Nombre: Patient first name")
    print("- Apellido_Paterno: Paternal surname")
    print("- Apellido_Materno: Maternal surname")
    print("- Fecha_Nacimiento: Birth date (YYYY-MM-DD)")
    print("- Sexo: Gender (M/F)")
    print("- Edad: Age (numeric)")
    
    print("\nSERVICES DATA (CSV/Excel):")
    print("Required columns:")
    print("- Codigo_Servicio: Service code")
    print("- Descripcion: Service description")
    print("- Fecha_Servicio: Service date (YYYY-MM-DD)")
    print("- NSS_Paciente: Patient NSS (links to patient data)")
    print("- Costo: Service cost (numeric)")
    print("- Unidad_Medica: Medical unit")
    
    print("\nSUPPORTED FILE FORMATS:")
    print("- CSV files (.csv) with UTF-8 encoding")
    print("- Excel files (.xlsx, .xls)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="IMSS Bienestar ETL Pipeline Example")
    parser.add_argument('--show-format', action='store_true', help='Show expected data formats')
    parser.add_argument('--create-sample', action='store_true', help='Create sample data only')
    
    args = parser.parse_args()
    
    if args.show_format:
        show_data_requirements()
    elif args.create_sample:
        create_sample_data()
    else:
        sys.exit(run_example_etl())