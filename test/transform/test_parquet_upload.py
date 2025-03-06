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

# Helper to build the temporary file path.
def get_tmp_file_path(file_key):
    return f"/tmp/{file_key}"

# Helper: Clean up the temp file if it exists.
def cleanup_tmp_file(file_key):
    path = get_tmp_file_path(file_key)
    if os.path.exists(path):
        os.remove(path)

# 1) Test successful upload to S3 with proper log messages and file removal.
def test_success_upload(dummy_df, dummy_logger, s3_client):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    cleanup_tmp_file(file_key)
    s3_client.create_bucket(Bucket=bucket_name)

    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)
    
    # Check that the local file was removed from /tmp.
    assert not os.path.exists(get_tmp_file_path(file_key))

    # Verify that logger.info was called with success messages.
    info_calls = [args[0] for args, kwargs in dummy_logger.info.call_args_list]
    assert any("Successfully uploaded DataFrame" in msg for msg in info_calls)
    assert any("Successfully removed local copy" in msg for msg in info_calls)

# 2) Test that a failure in Parquet conversion raises an error and logs properly.
def test_parquet_conversion_error(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    cleanup_tmp_file(file_key)
    s3_client.create_bucket(Bucket=bucket_name)

    def failing_to_parquet(*args, **kwargs):
        raise Exception("Parquet conversion error")
    monkeypatch.setattr(dummy_df, "to_parquet", failing_to_parquet)

    with pytest.raises(Exception, match="Parquet conversion error"):
        upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    dummy_logger.error.assert_called_with(
        f"Failed to convert DataFrame to Parquet for {file_key}: Parquet conversion error"
    )
    assert not os.path.exists(get_tmp_file_path(file_key))

# 3) Test that a failure during S3 upload raises an error, logs properly, and the file remains.
def test_s3_upload_error(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    cleanup_tmp_file(file_key)
    s3_client.create_bucket(Bucket=bucket_name)

    def failing_upload_file(file_name, bucket, s3_key):
        raise Exception("S3 upload failed")
    monkeypatch.setattr(s3_client, "upload_file", failing_upload_file)

    with pytest.raises(Exception, match="S3 upload failed"):
        upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    dummy_logger.error.assert_called_with(
        f"Failed to upload file {file_key} to S3 bucket {bucket_name}: S3 upload failed"
    )
    # Since upload failed, the file should remain.
    assert os.path.exists(get_tmp_file_path(file_key))
    cleanup_tmp_file(file_key)

# 4) Test that file removal is successful with proper logging.
def test_file_removal_success(dummy_df, dummy_logger, s3_client):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    cleanup_tmp_file(file_key)
    s3_client.create_bucket(Bucket=bucket_name)

    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    # Verify that the file was removed from /tmp.
    assert not os.path.exists(get_tmp_file_path(file_key))
    info_calls = [args[0] for args, kwargs in dummy_logger.info.call_args_list]
    assert any("Successfully removed local copy" in msg for msg in info_calls)

# 5) Test that a failure in file removal logs a warning and leaves the file on disk.
def test_file_removal_failure(dummy_df, dummy_logger, s3_client, monkeypatch):
    file_key = "test_table"
    bucket_name = "dummy-bucket"
    cleanup_tmp_file(file_key)
    s3_client.create_bucket(Bucket=bucket_name)

    # Monkeypatch os.remove to simulate a removal failure.
    def fake_remove(file):
        raise Exception("Removal failed")
    monkeypatch.setattr(os, "remove", fake_remove)

    upload_df_to_s3(s3_client, dummy_df, file_key, dummy_logger, bucket_name)

    dummy_logger.warning.assert_called_with(
        f"File {file_key} could not be removed after upload: Removal failed"
    )
    # The file should still exist in /tmp.
    assert os.path.exists(get_tmp_file_path(file_key))
    monkeypatch.undo()
    cleanup_tmp_file(file_key)
