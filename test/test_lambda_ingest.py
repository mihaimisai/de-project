import os
import pytest
import boto3
import logging
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq
from moto import mock_aws
import pg8000
import pyarrow as pa
import src.lambda_ingestion
from unittest import mock
from unittest.mock import patch, MagicMock

# Import functions and constants from lambda_ingestion.
from src.lambda_ingestion import (
    s3_client,
    timestamp_data_retrival,
    ingest_data_to_s3,
    load_credentials_for_pg_access,
    S3_INGESTION_BUCKET,
    REGION,
)

class TestTimeStampDataRetrival:
    @mock_aws
    def test_timestamp_return_in_str(self):
        table_name = 'test-table'
        bucket_name = "test-bucket"
        mock_client = boto3.client("s3")
        file_key = f"time_stamp_{table_name}.txt"
        file_content = "Hello Carlo!"
        mock_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2' })
        mock_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
        result = timestamp_data_retrival(mock_client, bucket_name,table_name)

        assert result == "Hello Carlo!"
    @mock_aws
    def test_NO_timestamp_available(self):
        table_name = 'test-table'
        bucket_name = "test-bucket"
        mock_client = boto3.client("s3")
        mock_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2' })
        result = timestamp_data_retrival(mock_client, bucket_name,table_name)

        assert result == None
# Dummy data for mocking database response
dummy_data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
dummy_df = pd.DataFrame(dummy_data)

class TestLambdaIngestion:
    @mock_aws
    @pytest.mark.skip
    def test_ingest_success(self, caplog):
        """
        Test successful ingestion scenario with logging verification.
        """
        caplog.set_level(logging.INFO)
        
        with mock.patch("pg8000.connect", return_value=mock.Mock()), \
             mock.patch("pandas.read_sql", return_value=dummy_df), \
             mock.patch("src.lambda_ingestion.load_credentials_for_pg_access", return_value=("host", "5432", "db", "user", "pass")), \
             mock.patch("boto3.client") as mock_s3:
            
            # Create a simulated S3 bucket
            s3 = mock_s3.return_value
            s3.create_bucket.return_value = None
            
            table_name = "test_table"
            ingest_data_to_s3(table_name)
            
            # Verify logs
            assert "Successfully fetched data from table" in caplog.text
            assert "Successfully converted data from table" in caplog.text
            assert "Successfully uploaded Parquet file to S3" in caplog.text
    @pytest.mark.skip
    def test_missing_credentials(self, caplog):
        """
        Test missing credentials logging.
        """
        caplog.set_level(logging.ERROR)
        
        with mock.patch("src.lambda_ingestion.load_credentials_for_pg_access", return_value=(None, None, None, None, None)):
            ingest_data_to_s3("test_table")
        
        assert "Error loading PostgreSQL credentials" in caplog.text
    @pytest.mark.skip
    def test_pg_connection_failure(self, caplog):
        """
        Test handling of PostgreSQL connection failure.
        """
        caplog.set_level(logging.ERROR)

        with mock.patch("src.lambda_ingestion.load_credentials_for_pg_access", return_value=("host", "5432", "db", "user", "pass")), \
             mock.patch("pg8000.connect", side_effect=Exception("DB connection failed")):
            
            ingest_data_to_s3("test_table")
        
        assert "Error connecting to PostgreSQL" in caplog.text
    @pytest.mark.skip
    def test_s3_upload_failure(self, caplog):
        """
        Test handling of S3 upload failure with logging.
        """
        caplog.set_level(logging.ERROR)

        with mock.patch("src.lambda_ingestion.load_credentials_for_pg_access", return_value=("host", "5432", "db", "user", "pass")), \
             mock.patch("pg8000.connect", return_value=mock.Mock()), \
             mock.patch("pandas.read_sql", return_value=dummy_df), \
             mock.patch("boto3.client") as mock_s3:
            
            mock_s3.return_value.upload_fileobj.side_effect = Exception("S3 upload failed")
            ingest_data_to_s3("test_table")
        
        assert "Error uploading Parquet file to S3" in caplog.text
