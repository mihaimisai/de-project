import boto3
import pytest
import pandas as pd
import io
from moto import mock_aws
from src.load.function.utils.get_parquet_v2 import (
    get_recent_parquet_files_from_s3,
)  # Adjust import as needed
import logging
from testfixtures import LogCapture
from pprint import pprint


@pytest.fixture
def mock_s3_bucket():
    """Fixture to create a mock S3 bucket and
    return the client and bucket name."""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3_client.create_bucket(Bucket=bucket_name)
        yield s3_client, bucket_name


@pytest.fixture
def test_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


@pytest.fixture
def sample_parquet_data():
    """Fixture to create sample Parquet data."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    }
    df = pd.DataFrame(data)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer.getvalue()


def test_get_recent_parquet_files_from_s3(
    mock_s3_bucket, sample_parquet_data, test_logger
):
    """Test that the function retrieves the most recent
    files and extracts correct table names."""
    s3_client, bucket_name = mock_s3_bucket

    file_keys = [
        "First/2024/12/03/00:00.parquet",
        "Second/2024/12/03/01:00.parquet",
        "Third/2024/12/03/02:00.parquet",
        "Fourth/2024/12/03/03:00.parquet",
        "Fifth/2024/12/03/03:00.parquet",
        "Sixth/2024/12/03/03:00.parquet",
        "Seventh/2024/12/03/03:00.parquet",
        "Eight/2024/12/03/03:00.parquet",
    ]

    for key in file_keys:
        s3_client.put_object(
            Bucket=bucket_name, Key=key, Body=sample_parquet_data
        )

    # Run the function
    df_dict = get_recent_parquet_files_from_s3(
        bucket_name, client=s3_client, logger=test_logger
    )
    pprint(df_dict)

    # Assertions
    assert len(df_dict) == 7
    assert isinstance(
        df_dict["First"], pd.DataFrame
    )  # Ensure it's a DataFrame


def test_logs_when_successfully_retrieved(
    mock_s3_bucket, sample_parquet_data, test_logger
):
    """Test that the function retrieves the most recent
    files and extracts correct table names."""
    s3_client, bucket_name = mock_s3_bucket

    # Upload mock Parquet files to S3
    file_keys = [
        "First/2024/12/03/00:00.parquet",
        "Second/2024/12/03/01:00.parquet",
        "Third/2024/12/03/02:00.parquet",
        "Fourth/2024/12/03/03:00.parquet",
        "Fifth/2024/12/03/03:00.parquet",
        "Sixth/2024/12/03/03:00.parquet",
        "Seventh/2024/12/03/03:00.parquet",
        "Eight/2024/12/03/03:00.parquet",
    ]

    for key in file_keys:
        s3_client.put_object(
            Bucket=bucket_name, Key=key, Body=sample_parquet_data
        )

    with LogCapture("test_logger", level=logging.INFO) as logstream:
        get_recent_parquet_files_from_s3(
            bucket_name, client=s3_client, logger=test_logger
        )

    assert logstream[0] == (
        "test_logger",
        "INFO",
        "Successfully loaded transformed parquet files.",
    )


def test_logs_error_when_bucket_empty(
    mock_s3_bucket, sample_parquet_data, test_logger
):
    """Test that the function retrieves the most
    recent files and extracts correct table names."""
    s3_client, bucket_name = mock_s3_bucket

    # Upload mock Parquet files to S3

    with LogCapture("test_logger", level=logging.INFO) as logstream:
        try:
            get_recent_parquet_files_from_s3(
                bucket_name, client=s3_client, logger=test_logger
            )
        except Exception as e:
            test_logger.error(f"{e}")

    assert logstream[0] == (
        "test_logger",
        "ERROR",
        "Error loading transformed parquet files: Processed Bucket Empty.",
    )


def test_raises_error_when_bucket_empty(
    mock_s3_bucket, sample_parquet_data, test_logger
):
    """Test that the function retrieves the most recent
    files and extracts correct table names."""
    s3_client, bucket_name = mock_s3_bucket

    with pytest.raises(ValueError):
        get_recent_parquet_files_from_s3(
            bucket_name, client=s3_client, logger=test_logger
        )
