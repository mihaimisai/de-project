from src.transform.function.transform_handler_fn import get_latest_files
import boto3
from moto import mock_aws
import pytest
from unittest.mock import MagicMock


@pytest.fixture
def s3_client():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        yield s3_client


def test_get_latest_files_returns_the_only_existing_files(s3_client):

    bucket_name = "test-bucket"
    mock_logger = MagicMock()

    test_files = {
        "table1/2025/02/03/12:00:00.csv": "file 1",
        "table2/2025/02/03/12:05:00.csv": "file 2",
        "table3/2025/02/03/12:02:00.csv": "file 3",
    }

    for key, content in test_files.items():
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)

    expected_files = {
        "table1": "table1/2025/02/03/12:00:00.csv",
        "table2": "table2/2025/02/03/12:05:00.csv",
        "table3": "table3/2025/02/03/12:02:00.csv",
    }

    result = get_latest_files(s3_client, "test-bucket", mock_logger)

    assert len(result) == 3
    assert result == expected_files

    mock_logger.info.assert_called_once_with(
        f"Succesfully retrieved files from bucket:{bucket_name}"  # noqa 501
    )


def test_get_latest_files_returns_the_most_recent_files(s3_client):

    bucket_name = "test-bucket"
    mock_logger = MagicMock()

    test_files = {
        "table1/2025/02/03/2025-03-04 12:00:00.csv": "file 1",
        "table1/2025/02/03/2025-03-04 12:05:00.csv": "file 2",
        "table2/2025/02/03/2025-03-04 12:05:00.csv": "file 3",
        "table3/2025/02/03/2025-03-04 12:02:00.csv": "file 4",
        "table3/2025/02/03/2025-03-04 13:00:00.csv": "file 5",
    }

    for key, content in test_files.items():
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)

    expected_files = {
        "table1": "table1/2025/02/03/2025-03-04 12:05:00.csv",
        "table2": "table2/2025/02/03/2025-03-04 12:05:00.csv",
        "table3": "table3/2025/02/03/2025-03-04 13:00:00.csv",
    }

    result = get_latest_files(s3_client, "test-bucket", mock_logger)

    assert len(result) == 3
    assert result == expected_files

    mock_logger.info.assert_called_once_with(
        f"Succesfully retrieved files from bucket:{bucket_name}"  # noqa 501
    )


def test_error_get_latest_files(s3_client):

    bucket_name = "test-bucket"
    mock_logger = MagicMock()

    s3_client.list_objects_v2 = MagicMock(side_effect=Exception("S3 error occurred"))

    with pytest.raises(Exception):
        get_latest_files(s3_client, bucket_name, mock_logger)

    mock_logger.error.assert_called_once_with(
        f"Error getting files from bucket:{bucket_name}: S3 error occurred"  # noqa 501
    )
