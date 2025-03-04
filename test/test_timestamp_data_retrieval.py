# import botocore.exceptions
from src.ingestion.function.utils.timestamp_data_retrival import (
    timestamp_data_retrival,
)
import boto3
from moto import mock_aws
import logging
from testfixtures import LogCapture
import pytest
import botocore
from unittest.mock import MagicMock


@pytest.fixture
def test_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


class TestTimeStampDataRetrieval:
    @mock_aws
    def test_retrieves_and_reads_data_successfully(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        body = "testing,testing"
        key = "time_stamp_users.txt"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},  # noqa
        )
        test_client.put_object(Bucket=bucket_name, Body=body, Key=key)
        time_stamp = timestamp_data_retrival(
            test_client, bucket_name, "users", test_logger
        )

        assert time_stamp == body

    @mock_aws
    def test_info_logged_on_successful_retrieval(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        body = "testing,testing"
        key = "time_stamp_users.txt"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},  # noqa
        )
        test_client.put_object(Bucket=bucket_name, Body=body, Key=key)

        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            timestamp_data_retrival(test_client, bucket_name, "users", test_logger)

        for log in logstream:
            assert log == (
                "test_logger",
                "INFO",
                "Successfully retrieved time_stamp_users.txt file from S3 bucket 'Test_bucket'",  # noqa
            )

    @mock_aws
    def test_none_returned_when_no_key(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_Bucket"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},  # noqa
        )

        test_client.get_object = MagicMock()
        test_client.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        result = timestamp_data_retrival(
            test_client, bucket_name, bucket_name, test_logger
        )

        assert result is None

    @mock_aws
    def test_error_logged_when_file_not_found(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},  # noqa
        )
        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            try:
                timestamp_data_retrival(test_client, bucket_name, "users", test_logger)
            except Exception:
                print("Exception in test func")

        for log in logstream:
            assert log == (
                "test_logger",
                "ERROR",
                "Error retrieving time_stamp_users.txt from S3 bucket: 'Test_bucket': An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.",  # noqa
            )
