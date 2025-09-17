# ETL_IMSSB - IMSS Bienestar Data Integration ETL Pipeline

A comprehensive Extract, Transform, Load (ETL) pipeline specifically designed for processing IMSS Bienestar (Instituto Mexicano del Seguro Social - Bienestar) data. This pipeline handles healthcare and social security data integration with built-in data validation, cleaning, and standardization features.

## Features

- **Multi-format Data Extraction**: Support for CSV, Excel files
- **Advanced Data Transformation**: 
  - CURP (Clave Única de Registro de Población) validation
  - NSS (Número de Seguridad Social) standardization
  - Medical data validation and cleaning
  - Date standardization and validation
- **Flexible Data Loading**: Output to CSV, Excel, JSON formats
- **Data Quality Reports**: Comprehensive validation reporting
- **Configurable Processing**: JSON/YAML configuration support
- **Robust Logging**: Detailed logging with rotating file support
- **Error Handling**: Graceful error handling with detailed reporting

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run Example with Sample Data

```bash
python examples/run_example.py
```

This will:
- Create sample IMSS Bienestar data
- Run the complete ETL pipeline
- Generate processed files in the `output/` directory
- Display processing summary and statistics

### 3. Use Your Own Data

```bash
python main.py --patients /path/to/patient_data.csv --services /path/to/services_data.csv
```

Or process all files in a directory:

```bash
python main.py --input-dir /path/to/data/directory
```

## Project Structure

```
ETL_IMSSB/
├── src/                    # Source code
│   ├── etl/               # ETL modules
│   │   ├── extract.py     # Data extraction
│   │   ├── transform.py   # Data transformation
│   │   └── load.py        # Data loading
│   └── utils/             # Utility modules
│       ├── config.py      # Configuration management
│       └── logger.py      # Logging setup
├── config/                # Configuration files
│   └── default.json       # Default configuration
├── examples/              # Example scripts
│   └── run_example.py     # Example usage
├── main.py               # Main ETL pipeline
└── requirements.txt      # Python dependencies
```

## Data Format Requirements

### Patient Data Format

Required columns for patient data:
- **CURP**: Clave Única de Registro de Población (18 characters)
- **NSS**: Número de Seguridad Social (11 digits)
- **Nombre**: Patient first name
- **Apellido_Paterno**: Paternal surname
- **Apellido_Materno**: Maternal surname
- **Fecha_Nacimiento**: Birth date (YYYY-MM-DD format)
- **Sexo**: Gender (M/F)
- **Edad**: Age (numeric)

Example:
```csv
CURP,NSS,Nombre,Apellido_Paterno,Apellido_Materno,Fecha_Nacimiento,Sexo,Edad
ABCD123456HDFRYT01,12345678901,Juan Carlos,García,López,1985-03-15,M,39
```

### Medical Services Data Format

Required columns for services data:
- **Codigo_Servicio**: Service code
- **Descripcion**: Service description
- **Fecha_Servicio**: Service date (YYYY-MM-DD format)
- **NSS_Paciente**: Patient NSS (links to patient data)
- **Costo**: Service cost (numeric)
- **Unidad_Medica**: Medical unit

Example:
```csv
Codigo_Servicio,Descripcion,Fecha_Servicio,NSS_Paciente,Costo,Unidad_Medica
CONS001,Consulta General,2024-01-15,12345678901,150.00,UMF 001
```

## Configuration

The pipeline uses JSON configuration files. Default configuration is in `config/default.json`.

### Key Configuration Sections

- **logging**: Logging level, format, handlers
- **imss_bienestar**: IMSS-specific settings
- **processing**: Performance and processing options

Example configuration:

```json
{
  "logging": {
    "level": "INFO",
    "handlers": ["console", "file"]
  },
  "imss_bienestar": {
    "validation": {
      "required_columns": {
        "patients": ["curp", "nombre", "apellido_paterno"],
        "services": ["codigo_servicio", "descripcion", "costo"]
      }
    }
  }
}
```

## ETL Pipeline Components

### Extract Module (`src/etl/extract.py`)
- **DataExtractor**: Base class for data extraction
- **IMSSBienestrarExtractor**: Specialized for IMSS Bienestar data
- Supports CSV, Excel file extraction
- Handles multiple files and directories

### Transform Module (`src/etl/transform.py`)
- **DataTransformer**: Base transformation operations
- **IMSSBienestarTransformer**: IMSS-specific transformations
- CURP and NSS validation
- Date standardization
- Medical data validation

### Load Module (`src/etl/load.py`)
- **DataLoader**: Base loading operations
- **IMSSBienestarLoader**: IMSS-specific loading
- Multiple output formats (CSV, Excel, JSON)
- Validation report generation

## Usage Examples

### Basic Usage

```python
from main import IMSSBienestarETL

# Initialize ETL pipeline
etl = IMSSBienestarETL('config/default.json')

# Define input sources
input_sources = [
    {'type': 'patients', 'path': 'data/patients.csv'},
    {'type': 'services', 'path': 'data/services.csv'}
]

# Run pipeline
results = etl.run_full_pipeline(input_sources)
```

### Command Line Usage

```bash
# Process specific files
python main.py --patients data/patients.csv --services data/services.csv

# Process directory
python main.py --input-dir data/

# Use custom configuration
python main.py --config custom_config.json --patients data/patients.csv

# Show data format requirements
python examples/run_example.py --show-format
```

## Output Files

The pipeline generates several types of output files:

### Processed Data Files
- `output/patients/imss_bienestar_patients_YYYYMMDD_HHMMSS.csv`
- `output/patients/imss_bienestar_patients_YYYYMMDD_HHMMSS.xlsx`
- `output/services/imss_bienestar_services_YYYYMMDD_HHMMSS.csv`
- `output/services/imss_bienestar_services_YYYYMMDD_HHMMSS.xlsx`

### Validation Reports
- `output/patients/validation_report_YYYYMMDD_HHMMSS.json`
- `output/services/services_validation_report_YYYYMMDD_HHMMSS.json`

### Summary Reports
- `output/reports/etl_summary_YYYYMMDD_HHMMSS.json`

## Data Validation Features

### CURP Validation
- Format validation (18 characters)
- Pattern matching for Mexican CURP format

### NSS Validation
- Format validation (11 digits)
- Standardization (removes dashes, formatting)

### Medical Data Validation
- Age validation (0-120 years)
- Gender standardization (M/F)
- Service cost validation (non-negative)

### Date Validation
- Multiple date format support
- Date range validation
- Invalid date detection and flagging

## Logging

The pipeline provides comprehensive logging:

- **Console Output**: Real-time processing information
- **File Logging**: Detailed logs saved to `logs/etl.log`
- **Rotating Files**: Automatic log rotation (10MB max, 5 backups)
- **Configurable Levels**: DEBUG, INFO, WARNING, ERROR

## Error Handling

- **Graceful Error Recovery**: Pipeline continues processing other files if one fails
- **Error Threshold**: Configurable error tolerance
- **Detailed Error Reports**: Comprehensive error logging and reporting
- **Data Quality Metrics**: Success rates and error statistics

## Performance Considerations

- **Memory Management**: Configurable chunk processing for large files
- **File Format Support**: Optimized for CSV and Excel processing
- **Parallel Processing**: Optional parallel processing (configurable)
- **Resource Monitoring**: Memory usage tracking and limits

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Make your changes
4. Add tests if applicable
5. Commit your changes (`git commit -am 'Add new feature'`)
6. Push to the branch (`git push origin feature/new-feature`)
7. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions, issues, or contributions, please:
1. Check the documentation and examples
2. Review existing issues in the repository
3. Create a new issue with detailed information about your problem or suggestion

## Version History

- **v1.0.0**: Initial release with core ETL functionality
  - Basic Extract, Transform, Load operations
  - IMSS Bienestar data validation
  - Configuration management
  - Example usage and documentation