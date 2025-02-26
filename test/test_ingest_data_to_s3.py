from src.utils.ingest_data_to_s3 import ingest_data_to_s3
from unittest.mock import patch
from moto import mock_aws
import pandas as pd
import boto3
import pytest
from src.utils.connect_to_db import connect_to_db, close_db


@pytest.fixture()
def db():
    db = connect_to_db()
    yield db
    close_db(db)


@pytest.fixture()
@mock_aws
def client():
    client = boto3.client("s3", region_name="eu-west-2")
    return client


class TestIngestDataToS3:

    def test_ingest_data_successfully_connecting_to_client(
        self,
    ):
        """Test a successful ingestion where all steps work correctly."""
        mock_client = client
        # Create the required buckets in the mocked S3 environment.
        mock_client.create_bucket(
            Bucket="your-timestamp-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.create_bucket(
            Bucket="your-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with caplog.at_level(logging.INFO):
            # Call the function with a valid table name
            # and no time_stamp filter.
            ingest_data_to_s3(
                s3_client=mock_client,
                table_name="test_table",
                time_stamp=None,
                S3_INGESTION_BUCKET="your-ingestion-bucket",
                S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
            )

        # Check that the expected log messages were emitted.
        assert "Connecting to PostgreSQL database:" in caplog.text
        assert "Successfully fetched data from table:" in caplog.text
        assert "Successfully converted data from table" in caplog.text

        # Verify that our dummy functions were called.
        mock_upload_time_stamp.assert_called_once()
        mock_aws_data_upload.assert_called_once()

    @mock_aws
    @patch(
        "src.utils.ingest_data_to_s3.load_credentials_for_pg_access",
        return_value=(None, None, None, None, None),
    )
    def test_missing_credentials(self, mock_load_creds, caplog):
        """Test that ingestion fails when PostgreSQL credentials are missing."""  # noqa: E501
        mock_client = boto3.client("s3", region_name="eu-west-2")

        with caplog.at_level(logging.ERROR):
            result = ingest_data_to_s3(
                s3_client=mock_client,
                table_name="test_table",
                time_stamp=None,
                S3_INGESTION_BUCKET="your-ingestion-bucket",
                S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
            )
        # The function should return early (None) after logging the error.
        assert result is None
        assert (
            "One or more PostgreSQL credentials are missing." in caplog.text
            or "Error loading PostgreSQL credentials:" in caplog.text
        )

    @mock_aws
    @patch(
        "src.utils.load_credentials_for_pg_access.load_credentials_for_pg_access",  # noqa: E501
        side_effect=dummy_load_credentials,
    )
    @patch(
        "src.utils.ingest_data_to_s3.pg8000.connect",
        side_effect=Exception("Connection failed"),
    )
    def test_pg_connection_failure(
        self, mock_pg_connect, mock_load_creds, caplog
    ):  # noqa: E501
        """Test that ingestion fails when the PostgreSQL connection cannot be established."""  # noqa: E501
        mock_client = boto3.client("s3", region_name="eu-west-2")
        with caplog.at_level(logging.ERROR):
            result = ingest_data_to_s3(
                s3_client=mock_client,
                table_name="test_table",
                time_stamp=None,
                S3_INGESTION_BUCKET="your-ingestion-bucket",
                S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
            )
        assert result is None
        assert (
            "Error connecting to PostgreSQL or executing query for table"
            in caplog.text  # noqa: E501
        )

    @mock_aws
    @patch(
        "src.utils.load_credentials_for_pg_access.load_credentials_for_pg_access",  # noqa: E501
        side_effect=dummy_load_credentials,
    )
    @patch(
        "src.utils.ingest_data_to_s3.pg8000.connect",
        return_value=DummyConnection(),  # noqa: E501
    )  # noqa: E501
    @patch(
        "src.utils.ingest_data_to_s3.pd.read_sql",
        side_effect=Exception("SQL error"),  # noqa: E501
    )
    def test_sql_query_failure(
        self, mock_read_sql, mock_pg_connect, mock_load_creds, caplog
    ):
        """Test that ingestion fails when the SQL query execution fails."""
        mock_client = boto3.client("s3", region_name="eu-west-2")

        with caplog.at_level(logging.ERROR):
            result = ingest_data_to_s3(
                s3_client=mock_client,
                table_name="test_table",
                time_stamp=None,
                S3_INGESTION_BUCKET="your-ingestion-bucket",
                S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
            )
        assert result is None
        assert (
            "Error connecting to PostgreSQL or executing query for table"
            in caplog.text  # noqa: E501
        )

    @mock_aws
    @patch(
        "src.utils.ingest_data_to_s3.load_credentials_for_pg_access",
        side_effect=dummy_load_credentials,
    )
    @patch(
        "src.utils.ingest_data_to_s3.pg8000.connect", return_value=DummyConnection()
    )  # noqa: E501
    @patch("src.utils.ingest_data_to_s3.pd.read_sql", return_value=dummy_df)
    @patch(
        "src.lambda_function.pq.write_table",
        side_effect=Exception("Parquet conversion error"),
    )
    def test_parquet_conversion_failure(
        self, mock_write_table, mock_read_sql, mock_pg_connect, mock_load_creds, caplog
    ):
        """Test that ingestion fails when converting
        the DataFrame to Parquet format fails."""
        mock_client = boto3.client("s3", region_name="eu-west-2")
        with caplog.at_level(logging.ERROR):
            result = ingest_data_to_s3(
                s3_client=mock_client,
                table_name="test_table",
                time_stamp=None,
                S3_INGESTION_BUCKET="your-ingestion-bucket",
                S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
            )
        assert result is None
        assert "Error converting DataFrame to Parquet for table" in caplog.text
