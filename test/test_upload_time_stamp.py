from src.utils.upload_time_stamp import upload_time_stamp
from moto import mock_aws
import logging
import boto3
from testfixtures import LogCapture
import pytest
from freezegun import freeze_time
from pprint import pprint


@pytest.fixture
def test_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


class TestUploadTimeStamp:
    @mock_aws
    @freeze_time("2025-01-01 01:00:00")
    def test_upload_timestamp_success_info_logged(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        table_name = "test-users"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2"
            },  # noqa
        )

        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            upload_time_stamp(
                test_client, bucket_name, table_name, test_logger
            )

        for log in logstream:
            assert log == (
                "test_logger",
                "INFO",
                "Successfully uploaded 2025-01-01 01:00:00_test-users.txt file to S3 bucket 'Test_bucket'",  # noqa
            )

    @mock_aws
    @freeze_time("2025-01-01 01:00:00")
    def test_upload_timestamp_error_logged_when_no_bucket(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        table_name = "test-users"
        with LogCapture(level=logging.INFO) as logstream:
            logstream.clear()
            try:
                upload_time_stamp(
                    test_client, bucket_name, table_name, test_logger
                )
            except Exception:
                print("Exception in test func")

        for log in logstream:
            assert log == (
                "test_logger",
                "ERROR",
                "Error uploading time_stamp_test-users.txt to S3 bucket: 'Test_bucket': An error occurred (NoSuchBucket) when calling the PutObject operation: The specified bucket does not exist",  # noqa
            )

    @mock_aws
    def test_if_wrong_logger_fails(self):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        table_name = "test-users"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2"
            },  # noqa
        )
        with pytest.raises(AttributeError):
            upload_time_stamp(
                test_client, bucket_name, table_name, "test_logger"
            )

    def test_if_wrong_client_fails(self, test_logger):
        bucket_name = "Test_bucket"
        table_name = "test-users"
        with pytest.raises(Exception):
            upload_time_stamp(
                "test_client", bucket_name, table_name, test_logger
            )

    @mock_aws
    @freeze_time("2025-01-01 01:00:00")
    def test_upload_timestamp_to_bucket_success(self, test_logger):
        test_client = boto3.client("s3")
        bucket_name = "Test_bucket"
        table_name = "test-users"
        test_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        upload_time_stamp(test_client, bucket_name, table_name, test_logger)
        result = test_client.list_objects(Bucket=bucket_name)
        pprint(result["Contents"][0]["Key"])

        assert result["Contents"][0]["Key"] == "time_stamp_test-users.txt"
