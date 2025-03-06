import os
import pytest
import pandas as pd
import boto3
from moto import mock_aws
from unittest.mock import MagicMock

from src.transform.function.utils.parquet_upload import upload_df_to_s3


# Fixture to provide a dummy logger using MagicMock
@pytest.fixture
def dummy_logger():
    return MagicMock()


# Fixture to provide a simple DataFrame for testing
@pytest.fixture
def dummy_df():
    return pd.DataFrame({"a": [1, 2, 3]})


# Fixture to provide an S3 client wrapped in moto's generic AWS mock.
@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


# 1) Test successful upload to S3 with proper log messages and file removal.
def test_success_upload(dummy_df, dummy_logger, s3_client):
    file_key = "test_table"
    bucket_name = "dummy-bucket"

    # Create the bucket in the mocked AWS environment.
    s3_client.create_bucket(Bucket=bucket_name)

    # Call the function
    # (this should perform parquet conversion, upload, and file removal)
    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    # Check that the local file was removed.
    assert not os.path.exists(file_key)

    # Verify that logger.info was called with messages indicating success.
    info_calls = [call_arg[0][0] for call_arg in dummy_logger.info.call_args_list] # noqa
    assert any("Successfully uploaded DataFrame" in msg for msg in info_calls)
    assert any("Successfully removed local copy" in msg for msg in info_calls)


# 2) Test that a failure in Parquet conversion
# raises an error and logs properly.


def test_parquet_conversion_error(dummy_df, dummy_logger, s3_client, monkeypatch): # noqa
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch df.to_parquet to raise an error.
    def failing_to_parquet(*args, **kwargs):
        raise Exception("Parquet conversion error")

    monkeypatch.setattr(dummy_df, "to_parquet", failing_to_parquet)

    with pytest.raises(Exception, match="Parquet conversion error"):
        upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name) # noqa

    # Assert that logger.error was called with the expected message.
    dummy_logger.error.assert_called_with(
        f"Failed to convert DataFrame to Parquet for {file_key}: Parquet conversion error" # noqa
    )
    # The file should not exist since conversion failed.
    assert not os.path.exists(file_key)


# 3) Test that a failure during S3 upload raises an error,
# logs properly, and the file remains.


def test_s3_upload_error(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"

    # Create the bucket so that the parquet conversion succeeds.
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch the S3 client's upload_file to simulate an upload failure.
    def failing_upload_file(file_name, bucket, s3_key):
        raise Exception("S3 upload failed")

    monkeypatch.setattr(s3_client, "upload_file", failing_upload_file)

    with pytest.raises(Exception, match="S3 upload failed"):
        upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name) # noqa

    dummy_logger.error.assert_called_with(
        f"Failed to upload file {file_key} to S3 bucket {bucket_name}: S3 upload failed" # noqa
    )
    # Since upload failed, the file should remain.
    assert os.path.exists(file_key)
    os.remove(file_key)  # Clean up


# 4) Test that file removal is successful with proper logging.


def test_file_removal_success(dummy_df, dummy_logger, s3_client):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    # Verify that the file was removed.
    assert not os.path.exists(file_key)
    info_calls = [call_arg[0][0] for call_arg in dummy_logger.info.call_args_list] # noqa
    assert any("Successfully removed local copy" in msg for msg in info_calls)


# 5) Test that a failure in file removal logs
# a warning and leaves the file on disk.

def test_file_removal_failure(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch os.remove to simulate a removal failure.
    def fake_remove(file):
        raise Exception("Removal failed")

    monkeypatch.setattr(os, "remove", fake_remove)

    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    dummy_logger.warning.assert_called_with(
        f"File {file_key} could not be removed after upload: Removal failed"
    )
    # The file should still exist due to the removal failure.
    assert os.path.exists(file_key)
    # manually undo mokeypatch to remove the created file file_key
    monkeypatch.undo()
    os.remove(file_key)
