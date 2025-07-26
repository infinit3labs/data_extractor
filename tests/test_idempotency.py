"""
Tests for idempotent operations and recovery capabilities.
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from data_extractor.core import DataExtractor
from data_extractor.databricks import DatabricksDataExtractor


class TestIdempotentOperations:
    """Test idempotent operations in data extraction."""

    @pytest.fixture
    def test_config(self):
        """Test configuration."""
        return {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
        }

    @pytest.fixture
    def mock_spark_session(self):
        """Mock Spark session."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        return mock_spark

    def test_file_integrity_validation(self, test_config):
        """Test file integrity validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            # Test non-existent file
            assert not extractor._is_file_valid("/nonexistent/file.parquet")

            # Test invalid file
            invalid_file = os.path.join(temp_dir, "invalid.parquet")
            with open(invalid_file, "w") as f:
                f.write("not a parquet file")
            assert not extractor._is_file_valid(invalid_file)

    def test_checksum_calculation(self, test_config):
        """Test checksum calculation for file integrity."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            # Create test file
            test_file = os.path.join(temp_dir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test content")

            # Calculate checksum
            checksum1 = extractor._calculate_file_checksum(test_file)
            checksum2 = extractor._calculate_file_checksum(test_file)

            # Checksums should be consistent
            assert checksum1 == checksum2
            assert checksum1 != ""

    def test_metadata_save_and_load(self, test_config):
        """Test extraction metadata save and load."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            # Create test output file
            output_path = os.path.join(temp_dir, "test.parquet")
            with open(output_path, "w") as f:
                f.write("test data")

            # Create test config
            table_config = {
                "source_name": "test_source",
                "table_name": "test_table",
                "is_full_extract": True,
            }

            start_time = datetime.now()
            end_time = datetime.now()

            # Save metadata
            extractor._save_extraction_metadata(
                output_path, 100, start_time, end_time, table_config
            )

            # Load metadata
            metadata = extractor._load_extraction_metadata(output_path)

            assert metadata is not None
            assert metadata["record_count"] == 100
            assert metadata["extraction_complete"] is True
            assert metadata["table_config"] == table_config

    @patch("data_extractor.core.SparkSession")
    def test_idempotent_extraction_skip(self, mock_spark_session, test_config):
        """Test that completed extractions are skipped."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            # Mock Spark
            mock_spark = Mock()
            mock_spark_session.builder = Mock()
            mock_spark_session.builder.appName.return_value = mock_spark_session.builder
            mock_spark_session.builder.config.return_value = mock_spark_session.builder
            mock_spark_session.builder.getOrCreate.return_value = mock_spark

            # Create existing output file and metadata
            extraction_date = datetime(2023, 12, 1)
            run_id = "20231201_120000"
            output_path = extractor._get_output_path(
                "test_source", "test_table", extraction_date, run_id
            )

            # Create directory and file
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                f.write("existing data")

            # Create metadata indicating completed extraction
            table_config = {"source_name": "test_source", "table_name": "test_table"}
            extractor._save_extraction_metadata(
                output_path, 50, extraction_date, extraction_date, table_config
            )

            # Attempt extraction - should be skipped
            with patch.object(extractor, "_is_file_valid", return_value=True):
                result = extractor.extract_table(
                    source_name="test_source",
                    table_name="test_table",
                    extraction_date=extraction_date,
                    run_id=run_id,
                    is_full_extract=True,
                )

            # Should succeed without calling Spark
            assert result is True
            mock_spark.read.format.assert_not_called()

    @patch("data_extractor.core.SparkSession")
    def test_force_reprocess_overrides_idempotency(
        self, mock_spark_session, test_config, mock_spark_session_setup
    ):
        """Test that force_reprocess overrides idempotency checks."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            # Setup mock Spark
            mock_spark = mock_spark_session_setup
            mock_spark_session.builder = Mock()
            mock_spark_session.builder.appName.return_value = mock_spark_session.builder
            mock_spark_session.builder.config.return_value = mock_spark_session.builder
            mock_spark_session.builder.getOrCreate.return_value = mock_spark

            # Create existing output file
            extraction_date = datetime(2023, 12, 1)
            run_id = "20231201_120000"
            output_path = extractor._get_output_path(
                "test_source", "test_table", extraction_date, run_id
            )
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            with patch.object(extractor, "_is_file_valid", return_value=True):
                with patch("pathlib.Path.mkdir"):
                    # Force reprocess should override idempotency
                    result = extractor.extract_table(
                        source_name="test_source",
                        table_name="test_table",
                        extraction_date=extraction_date,
                        run_id=run_id,
                        is_full_extract=True,
                        force_reprocess=True,
                    )

            # Should call Spark even with existing file
            assert result is True
            mock_spark.read.format.assert_called()

    @patch("data_extractor.core.SparkSession")
    def test_parallel_extraction_with_force_reprocess(
        self, mock_spark_session, test_config, mock_spark_session_setup
    ):
        """Test parallel extraction with force reprocess."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(
                **test_config, output_base_path=temp_dir, max_workers=2
            )

            # Setup mock Spark
            mock_spark = mock_spark_session_setup
            mock_spark_session.builder = Mock()
            mock_spark_session.builder.appName.return_value = mock_spark_session.builder
            mock_spark_session.builder.config.return_value = mock_spark_session.builder
            mock_spark_session.builder.getOrCreate.return_value = mock_spark

            table_configs = [
                {
                    "source_name": "test_source",
                    "table_name": "table1",
                    "is_full_extract": True,
                },
                {
                    "source_name": "test_source",
                    "table_name": "table2",
                    "is_full_extract": True,
                },
            ]

            with patch("pathlib.Path.mkdir"):
                results = extractor.extract_tables_parallel(
                    table_configs, force_reprocess=True
                )

            # All tables should be processed
            assert len(results) == 2
            assert all(results.values())

    def test_output_path_consistency(self, test_config):
        """Test that output paths are consistent for idempotent operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = DataExtractor(**test_config, output_base_path=temp_dir)

            source_name = "test_source"
            table_name = "test_table"
            extraction_date = datetime(2023, 12, 1)
            run_id = "20231201_120000"

            # Get path multiple times
            path1 = extractor._get_output_path(
                source_name, table_name, extraction_date, run_id
            )
            path2 = extractor._get_output_path(
                source_name, table_name, extraction_date, run_id
            )

            # Paths should be identical
            assert path1 == path2
            assert "test_source" in path1
            assert "test_table" in path1
            assert "202312" in path1  # year-month
            assert "01" in path1  # day
            assert "20231201_120000.parquet" in path1

    @pytest.fixture
    def mock_spark_session_setup(self):
        """Setup mock Spark session for tests."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_read = Mock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_df
        mock_spark.read = mock_read

        return mock_spark


class TestDatabricksIdempotency:
    """Test idempotent operations in DatabricksDataExtractor."""

    @pytest.fixture
    def test_config(self):
        """Test configuration."""
        return {
            "oracle_host": "localhost",
            "oracle_port": "1521",
            "oracle_service": "XE",
            "oracle_user": "test_user",
            "oracle_password": "test_password",
            "output_base_path": "/dbfs/data",
        }

    def test_databricks_inheritance(self, test_config):
        """Test that DatabricksDataExtractor inherits idempotency features."""
        extractor = DatabricksDataExtractor(**test_config)

        # Should have inherited idempotency methods
        assert hasattr(extractor, "_is_extraction_needed")
        assert hasattr(extractor, "_calculate_file_checksum")
        assert hasattr(extractor, "_save_extraction_metadata")
        assert hasattr(extractor, "_load_extraction_metadata")

    def test_unity_catalog_output_path(self, test_config):
        """Test Unity Catalog volume path handling."""
        extractor = DatabricksDataExtractor(
            **test_config, unity_catalog_volume="catalog/schema/volume"
        )

        extraction_date = datetime(2023, 12, 1)
        run_id = "20231201_120000"

        output_path = extractor._get_output_path(
            "test_source", "test_table", extraction_date, run_id
        )

        # Should use Unity Catalog volume
        assert output_path.startswith("/Volumes/catalog/schema/volume")
        assert "test_source" in output_path
        assert "test_table" in output_path

    def test_databricks_path_normalization(self, test_config):
        """Test Databricks path normalization."""
        # Create extractor with mocked Databricks environment
        with patch.object(
            DatabricksDataExtractor, "_is_databricks_environment", return_value=True
        ):
            extractor = DatabricksDataExtractor(**test_config)

        # Test various path types
        assert extractor._normalize_output_path("/dbfs/data") == "/dbfs/data"
        assert (
            extractor._normalize_output_path("s3://bucket/path") == "s3://bucket/path"
        )
        assert extractor._normalize_output_path("data").startswith("/dbfs/")

    @patch("data_extractor.databricks.SparkSession")
    def test_databricks_force_reprocess(self, mock_spark_session, test_config):
        """Test force reprocess in Databricks mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Override output_base_path in test_config
            test_config_with_temp = test_config.copy()
            test_config_with_temp["output_base_path"] = temp_dir
            extractor = DatabricksDataExtractor(**test_config_with_temp)

            # Mock existing Spark session
            mock_spark = Mock()
            mock_spark_session.getActiveSession.return_value = mock_spark

            # Mock DataFrame
            mock_df = Mock()
            mock_df.count.return_value = 50
            mock_df.coalesce.return_value = mock_df
            mock_df.write.mode.return_value.parquet = Mock()

            mock_read = Mock()
            mock_read.format.return_value = mock_read
            mock_read.option.return_value = mock_read
            mock_read.load.return_value = mock_df
            mock_spark.read = mock_read

            with patch("pathlib.Path.mkdir"):
                result = extractor.extract_table(
                    source_name="test_source",
                    table_name="test_table",
                    is_full_extract=True,
                    force_reprocess=True,
                )

            assert result is True
            mock_spark.read.format.assert_called()
