import pytest
import boto3
import logging
import pandas as pd
from io import BytesIO
from moto import mock_aws
from unittest.mock import patch, call
from src.utils.ingest_data_to_s3 import ingest_data_to_s3
from src.utils.load_credentials_for_pg_access import (
    load_credentials_for_pg_access,
)  # noqa: E501
from src.utils.s3_data_upload import s3_data_upload
from src.utils.timestamp_data_retrival import timestamp_data_retrival
from src.utils.upload_time_stamp import upload_time_stamp

# Import functions and constants from lambda_ingestion.
from src.lambda_function import (  # noqa: F401
    process_all_tables,
    S3_INGESTION_BUCKET,
    S3_TIMESTAMP_BUCKET,
    REGION,
)


class TestLoadCredentials:
    def test_load_credentials_for_pg_access_success(self):
        [
            PG_HOST,
            PG_PORT,
            PG_DATABASE,
            PG_USER,
            PG_PASSWORD,
        ] = load_credentials_for_pg_access()
        assert (
            PG_HOST
            == "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"  # noqa: E501
        )
        assert PG_PORT == "5432"
        assert PG_DATABASE == "totesys"
        assert PG_USER == "project_team_2"
        assert PG_PASSWORD == "eOVWx4L4xDGPJKC"


class TestTimeStampDataRetrival:
    @mock_aws
    def test_timestamp_return_in_str(self):
        table_name = "test-table"
        bucket_name = "test-bucket"
        mock_client = boto3.client("s3", region_name="eu-west-2")
        file_key = f"time_stamp_{table_name}.txt"
        file_content = "Hello world!"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.put_object(
            Bucket=bucket_name, Key=file_key, Body=file_content
        )  # noqa: E501
        result = timestamp_data_retrival(mock_client, bucket_name, table_name)

        assert result == "Hello world!"

    @mock_aws
    def test_NO_timestamp_available(self):
        table_name = "test-table"
        bucket_name = "test-bucket"
        mock_client = boto3.client("s3", region_name="eu-west-2")
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = timestamp_data_retrival(mock_client, bucket_name, table_name)

        assert result is None


class TestUploadDataStamp:
    @mock_aws
    def test_upload_time_stamp_success(self, caplog):
        """Test successful upload"""
        bucket_name = "test-bucket"
        table_name = "test_table"
        mock_client = boto3.client("s3", region_name="eu-west-2")
        # Create a mock S3 bucket
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with caplog.at_level(logging.INFO):  # Capture logs at INFO level
            timestamp = upload_time_stamp(mock_client, bucket_name, table_name)

        assert isinstance(timestamp, str)
        assert (
            len(timestamp) == 19
        )  # Ensure correct timestamp format (YYYY-MM-DD HH:MM:SS)

        # Check if the correct log message was recorded
        expected_log = f"Successfully uploaded time_stamp_{table_name}.txt file to S3 bucket '{bucket_name}'"  # noqa: E501
        assert expected_log in caplog.text

        # Verify file exists in S3
        response = mock_client.list_objects_v2(Bucket=bucket_name)
        s3_keys = [obj["Key"] for obj in response.get("Contents", [])]
        expected_s3_key = f"time_stamp_{table_name}.txt"
        assert expected_s3_key in s3_keys

    @mock_aws
    def test_upload_time_stamp_invalid_bucket(self, caplog):
        """Test uploading to a non-existent bucket"""
        bucket_name = "non-existent-bucket"
        table_name = "test_table"
        mock_client = boto3.client("s3", region_name="eu-west-2")

        with caplog.at_level(logging.ERROR), pytest.raises(Exception):
            upload_time_stamp(mock_client, bucket_name, table_name)

        # Verify error log message
        expected_log = f"Error uploading time_stamp_{table_name}.txt to S3 bucket: '{bucket_name}'"  # noqa: E501
        assert expected_log in caplog.text


class TestS3DataUpload:
    @mock_aws
    def test_s3_data_upload_success(self, caplog):
        """Test that a Parquet file is successfully uploaded."""
        bucket_name = "test-bucket"
        table_name = "test_table"
        mock_client = boto3.client("s3", region_name="eu-west-2")

        # Create the bucket in the mock environment
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Create a BytesIO buffer with dummy Parquet data
        data = b"dummy parquet data"
        buffer = BytesIO(data)

        with caplog.at_level(logging.INFO):
            s3_data_upload(mock_client, bucket_name, table_name, buffer)

        # Verify that the file now exists in the bucket
        response = mock_client.list_objects_v2(Bucket=bucket_name)
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        expected_key = f"ingestion/{table_name}.parquet"
        assert expected_key in keys

        # Verify that the correct success log message was recorded
        expected_log = (
            f"Successfully uploaded Parquet file to S3 bucket '{bucket_name}' "
            f"for table '{table_name}'"
        )
        assert expected_log in caplog.text

    @mock_aws
    def test_s3_data_upload_error(self, caplog):
        """Test that an exception is raised and logged when S3 upload fails."""
        bucket_name = "test-bucket"
        table_name = "test_table"
        mock_client = boto3.client("s3", region_name="eu-west-2")
        # Create the bucket so that it exists in moto
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Create a BytesIO buffer with dummy data
        data = b"dummy parquet data"
        buffer = BytesIO(data)

        # Define a helper function that simulates an S3 error.
        def fake_upload_fileobj():
            raise ClientError(  # noqa: F821
                {
                    "Error": {
                        "Code": "TestError",
                        "Message": "Simulated S3 error",
                    }  # noqa: E501
                },  # noqa: E501
                "upload_fileobj",
            )

        # Use unittest.mock.patch to replace upload_fileobj
        # with our fake function.
        with patch.object(
            mock_client, "upload_fileobj", side_effect=fake_upload_fileobj
        ):
            # If you want your function to re-raise
            # the exception (after logging),
            # the following test will pass. Otherwise,
            # if the function swallows the error,
            # you should verify the log message only.
            with caplog.at_level(logging.ERROR), pytest.raises(Exception):
                s3_data_upload(mock_client, bucket_name, table_name, buffer)

            # Verify that the error log message was recorded.
            expected_log = (
                f"Error uploading Parquet file to S3 for table '{table_name}':"
            )
            assert expected_log in caplog.text


# Dummy connection class for pg8000
class DummyConnection:
    def close(self):
        pass


# Dummy credentials function: returns valid credentials
def dummy_load_credentials():
    return ("dummy_host", 5432, "dummy_db", "dummy_user", "dummy_pass")


# Dummy DataFrame to simulate a query result
dummy_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})


# Dummy upload_time_stamp: simply returns a dummy timestamp string.
def dummy_upload_time_stamp(client, bucket_name, table_name):
    return "2025-02-24 12:00:00"


# Dummy s3_data_upload: do nothing (its behavior is tested elsewhere)
def dummy_s3_data_upload(client, bucket_name, table_name, buffer):
    pass


class TestIngestDataToS3:
    @mock_aws
    @patch(
        "src.utils.load_credentials_for_pg_access.load_credentials_for_pg_access",  # noqa: E501
        side_effect=dummy_load_credentials,
    )
    @patch(
        "src.utils.ingest_data_to_s3.pg8000.connect",
        return_value=DummyConnection(),  # noqa: E501
    )  # noqa: E501
    @patch("src.utils.ingest_data_to_s3.pd.read_sql", return_value=dummy_df)
    @patch(
        "src.utils.ingest_data_to_s3.upload_time_stamp",
        side_effect=dummy_upload_time_stamp,
    )
    @patch(
        "src.utils.ingest_data_to_s3.s3_data_upload",
        side_effect=dummy_s3_data_upload,  # noqa: E501
    )
    def test_ingest_data_success(
        self,
        mock_aws_data_upload,
        mock_upload_time_stamp,
        mock_read_sql,
        mock_pg_connect,
        mock_load_creds,
        caplog,
    ):
        """Test a successful ingestion where all steps work correctly."""
        mock_client = boto3.client("s3", region_name="eu-west-2")
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
    # fmt: off
    @patch(
        "src.utils.ingest_data_to_s3.pg8000.connect", 
        return_value=DummyConnection())
    # fmt:on
    @patch("src.utils.ingest_data_to_s3.pd.read_sql", return_value=dummy_df)
    @patch(
        "src.lambda_function.pq.write_table",
        side_effect=Exception("Parquet conversion error"),
    )
    # fmt: off
    def test_parquet_conversion_failure(
        self, 
        mock_write_table, 
        mock_read_sql, 
        mock_pg_connect, 
        mock_load_creds, 
        caplog
    ):
    # fmt: on
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


class TestProcessAllTables:
    # List of tables that are expected to be processed.
    EXPECTED_TABLES = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    # Mock function to simulate successful ingestion of data to S3.
    def dummy_ingest_data_to_s3(
        self, client, table, time_stamp, ingestion_bucket, timestamp_bucket
    ):
        return f"Processed {table}"  # Returns a success message for any table.

    # Mock function that simulates an exception
    # when processing the "department" table.
    def dummy_ingest_data_to_s3_with_exception(
        self, client, table, time_stamp, ingestion_bucket, timestamp_bucket
    ):
        if table == "department":
            raise Exception(
                "Simulated error in department"
            )  # Simulates failure for "department".
        return f"Processed {table}"  # Returns success for other tables.

    @mock_aws  # Mocks AWS services for the test.
    def test_process_all_tables_success(self):
        """Test that all tables are successfully
        processed without exceptions."""

        # Create a mock S3 client with the specified AWS region.
        mock_client = boto3.client("s3", region_name="eu-west-2")

        # Define the timestamp and bucket names.
        time_stamp = "2025-02-24 12:00:00"
        ingestion_bucket = "ingestion-bucket"
        timestamp_bucket = "timestamp-bucket"

        # Mock the `ingest_data_to_s3` function
        # to use the dummy success function.
        with patch(
            "src.lambda_function.ingest_data_to_s3",
            side_effect=self.dummy_ingest_data_to_s3,
        ) as mock_ingest:

            # Call the function under test.
            process_all_tables(
                mock_client, time_stamp, ingestion_bucket, timestamp_bucket
            )

            # Verify that ingest_data_to_s3 was
            # called once for each expected table.
            assert mock_ingest.call_count == len(self.EXPECTED_TABLES)

            # Construct the expected function call arguments.
            # fmt: off
            expected_calls = [
                call(
                    mock_client, 
                    table, time_stamp, 
                    ingestion_bucket, 
                    timestamp_bucket
                )  # noqa: E501
                for table in self.EXPECTED_TABLES
            ]
            # fmt: on

            # Check that all expected calls were made in order.
            mock_ingest.assert_has_calls(expected_calls, any_order=False)

    @mock_aws  # Mocks AWS services for the test.
    def test_process_all_tables_exception(self):
        """Test that an exception is raised when
        processing the 'department' table."""

        # Create a mock S3 client with the specified AWS region.
        mock_client = boto3.client("s3", region_name="eu-west-2")

        # Define the timestamp and bucket names.
        time_stamp = "2025-02-24 12:00:00"
        ingestion_bucket = "ingestion-bucket"
        timestamp_bucket = "timestamp-bucket"

        # Mock the `ingest_data_to_s3` function to use
        # the exception-raising version.
        with patch(
            "src.lambda_function.ingest_data_to_s3",
            side_effect=self.dummy_ingest_data_to_s3_with_exception,
        ) as mock_ingest:

            # Expect an exception when calling the function.
            with pytest.raises(
                Exception, match="Simulated error in department"
            ):  # noqa: E501
                process_all_tables(
                    mock_client, time_stamp, ingestion_bucket, timestamp_bucket
                )

            # Verify that ingest_data_to_s3 was called up
            # until the "department" table caused an error.
            assert (
                mock_ingest.call_count == 3
            )  # Should process three tables before failing.
