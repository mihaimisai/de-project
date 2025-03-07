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


# Fixture to provide an S3 client wrapped in moto's AWS mock.
@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


def get_uploaded_keys(file_key, s3_client, bucket_name):
    """
    Helper function: lists object keys in the bucket that start with file_key.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    return []


# 1) Test successful upload to S3 with proper log messages.
def test_success_upload(dummy_df, dummy_logger, s3_client):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Call the function which now uses an in-memory buffer.
    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    # Since we no longer save to disk, verify that no local file exists.

    assert not os.path.exists(file_key)

    # Verify that an object was uploaded to S3.
    keys = get_uploaded_keys(file_key, s3_client, bucket_name)
    assert len(keys) >= 1

    # Verify that logger.info was called with the success message.
    info_calls = [args[0] for args, kwargs in dummy_logger.info.call_args_list]
    assert any("Successfully uploaded DataFrame" in msg for msg in info_calls)


# 2) Test that a failure in Parquet conversion
# raises an error and logs properly.
def test_parquet_conversion_error(
    dummy_df, dummy_logger, s3_client, monkeypatch
):  # noqa
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch df.to_parquet to raise an error.
    def failing_to_parquet(*args, **kwargs):
        raise Exception("Parquet conversion error")

    monkeypatch.setattr(dummy_df, "to_parquet", failing_to_parquet)

    with pytest.raises(Exception, match="Parquet conversion error"):
        upload_df_to_s3(
            s3_client, dummy_df, file_key, dummy_logger, bucket_name
        )  # noqa

    dummy_logger.error.assert_called_with(
        f"Failed to convert DataFrame to Parquet for {file_key}: Parquet conversion error"  # noqa
    )
    # No local file should exist.

    assert not os.path.exists(file_key)


# 3) Test that a failure during S3 upload raises an error,
# logs properly, and no local file exists.
def test_s3_upload_error(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch s3_client.upload_fileobj to simulate an upload failure.
    def failing_upload_fileobj(fileobj, bucket, s3_key):
        raise Exception("S3 upload failed")

    monkeypatch.setattr(s3_client, "upload_fileobj", failing_upload_fileobj)

    with pytest.raises(Exception, match="S3 upload failed"):
        upload_df_to_s3(
            s3_client, dummy_df, file_key, dummy_logger, bucket_name
        )  # noqa

    dummy_logger.error.assert_called_with(
        f"Failed to upload file {file_key} to S3 bucket {bucket_name}: S3 upload failed"  # noqa
    )
    # No local file should exist.

    assert not os.path.exists(file_key)
