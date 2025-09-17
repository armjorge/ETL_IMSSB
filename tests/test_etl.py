"""
Basic tests for IMSS Bienestar ETL Pipeline components.
"""
import unittest
import pandas as pd
import sys
from pathlib import Path
import tempfile
import os

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from etl.extract import DataExtractor, IMSSBienestrarExtractor
from etl.transform import DataTransformer, IMSSBienestarTransformer
from etl.load import DataLoader, IMSSBienestarLoader


class TestDataExtractor(unittest.TestCase):
    """Test cases for data extraction functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.extractor = DataExtractor()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_csv_extraction(self):
        """Test CSV file extraction."""
        # Create test CSV file
        test_data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
        test_df = pd.DataFrame(test_data)
        csv_path = os.path.join(self.temp_dir, 'test.csv')
        test_df.to_csv(csv_path, index=False)
        
        # Extract data
        result = self.extractor.extract_from_csv(csv_path)
        
        # Verify results
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result.columns), ['col1', 'col2'])
        self.assertEqual(result['col1'].tolist(), [1, 2, 3])


class TestDataTransformer(unittest.TestCase):
    """Test cases for data transformation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.transformer = IMSSBienestarTransformer()
    
    def test_column_name_standardization(self):
        """Test column name standardization."""
        # Create test DataFrame with various column names
        test_data = {
            'Column With Spaces': [1, 2, 3],
            'UPPERCASE_COLUMN': ['A', 'B', 'C'],
            'Mixed-Case.Column!': [4, 5, 6]
        }
        df = pd.DataFrame(test_data)
        
        # Standardize column names
        result = self.transformer.standardize_column_names(df)
        
        # Verify standardization
        expected_columns = ['column_with_spaces', 'uppercase_column', 'mixedcasecolumn']
        self.assertEqual(list(result.columns), expected_columns)
    
    def test_curp_validation(self):
        """Test CURP validation functionality."""
        # Create test DataFrame with CURP data (using lowercase as the transformer standardizes)
        test_data = {
            'curp': ['ABCD123456HDFRYT01', 'INVALID_CURP', 'EFGH789012MDFRZX02']
        }
        df = pd.DataFrame(test_data)
        
        # Apply CURP standardization
        result = self.transformer._standardize_patient_identifiers(df)
        
        # Verify CURP validation
        self.assertTrue('curp_valid' in result.columns)
        # First and third should be valid format, second should be invalid
        self.assertTrue(result['curp_valid'].iloc[0])
        self.assertFalse(result['curp_valid'].iloc[1])
        self.assertTrue(result['curp_valid'].iloc[2])


class TestDataLoader(unittest.TestCase):
    """Test cases for data loading functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.loader = IMSSBienestarLoader()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_csv_saving(self):
        """Test CSV file saving."""
        # Create test DataFrame
        test_data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']}
        df = pd.DataFrame(test_data)
        
        # Save to CSV
        csv_path = os.path.join(self.temp_dir, 'test_output.csv')
        result_path = self.loader.save_to_csv(df, csv_path)
        
        # Verify file was created and path is returned
        self.assertTrue(os.path.exists(result_path))
        self.assertEqual(result_path, csv_path)
        
        # Verify content
        loaded_df = pd.read_csv(result_path)
        pd.testing.assert_frame_equal(df, loaded_df)
    
    def test_validation_report_generation(self):
        """Test validation report generation."""
        # Create test DataFrame with validation columns
        test_data = {
            'col1': [1, 2, 3],
            'col2': ['A', 'B', 'C'],
            'col1_valid': [True, False, True]
        }
        df = pd.DataFrame(test_data)
        
        # Generate validation report
        report = self.loader._generate_validation_report(df)
        
        # Verify report structure
        self.assertIn('total_rows', report)
        self.assertIn('total_columns', report)
        self.assertIn('validation_columns', report)
        self.assertEqual(report['total_rows'], 3)
        self.assertEqual(report['total_columns'], 3)
        
        # Verify validation column metrics
        self.assertIn('col1_valid', report['validation_columns'])
        validation_metrics = report['validation_columns']['col1_valid']
        self.assertEqual(validation_metrics['valid_records'], 2)
        self.assertEqual(validation_metrics['invalid_records'], 1)
        self.assertEqual(validation_metrics['validation_rate'], 66.67)


class TestIMSSBienestarExtractor(unittest.TestCase):
    """Test cases for IMSS Bienestar specific extraction functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.extractor = IMSSBienestrarExtractor()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_patient_data_extraction(self):
        """Test patient data extraction with metadata."""
        # Create test patient CSV
        patient_data = {
            'CURP': ['ABCD123456HDFRYT01'],
            'NSS': ['12345678901'],
            'Nombre': ['Juan'],
            'Apellido_Paterno': ['García']
        }
        test_df = pd.DataFrame(patient_data)
        csv_path = os.path.join(self.temp_dir, 'patients.csv')
        test_df.to_csv(csv_path, index=False)
        
        # Extract patient data
        result = self.extractor.extract_patient_data(csv_path)
        
        # Verify metadata was added
        self.assertIn('extraction_timestamp', result.columns)
        self.assertIn('data_source', result.columns)
        self.assertEqual(result['data_source'].iloc[0], 'IMSS_Bienestar')


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)