import pytest
from io import BytesIO
import pandas as pd
from moto import mock_aws
from testfixtures import LogCapture
from unittest.mock import MagicMock

from src.transform.function.utils.parquet_upload import upload_df_to_s3


class TestParquetUpload:
    @pytest.fixture
    def s3_client(self):
        """Fixture that provides a mocked S3 client."""
        s3_client = MagicMock()
        s3_client.exceptions = type(
            "DummyExceptions", (), {"NoSuchKey": KeyError}
        )  # noqa
        return s3_client

    @mock_aws
    def test_upload_df_to_s3_existing_file(self, s3_client):
        """Test behavior when the file already exists in S3."""
        existing_df = pd.DataFrame({"col": [1, 2]})
        existing_buffer = BytesIO()
        existing_df.to_parquet(existing_buffer, index=False)
        existing_bytes = existing_buffer.getvalue()

        s3_client.get_object.return_value = {"Body": BytesIO(existing_bytes)}

        new_df = pd.DataFrame({"col": [3, 4]})
        file_key = "test_existing.parquet"
        transform_bucket_name = "dummy-bucket"

        # Capture logs using LogCapture
        with LogCapture() as log_capture:
            upload_df_to_s3(
                s3_client,
                new_df,
                file_key,
                transform_bucket_name=transform_bucket_name,  # noqa
            )  # noqa

            # Convert log entries to a list of messages
            log_messages = [record.msg for record in log_capture.records]

        # Expected log messages
        expected_messages = [
            f"Trying to read the file {file_key} from bucket {transform_bucket_name}",  # noqa
            f"Successfully read the existing {file_key} file",
            "Appended new data to the existing DataFrame",
            f"Successfully uploaded the updated DataFrame to {file_key} in bucket {transform_bucket_name}",  # noqa
        ]

        # Verify that each expected log message was logged
        for expected_message in expected_messages:
            assert expected_message in log_messages

    @mock_aws
    def test_upload_df_to_s3_no_existing_file(self, s3_client):
        """Test behavior when the file does not exist in S3."""
        s3_client.get_object.side_effect = s3_client.exceptions.NoSuchKey(
            "No such key"
        )  # noqa

        new_df = pd.DataFrame({"col": [5, 6]})
        file_key = "test_new.parquet"
        transform_bucket_name = "dummy-bucket"

        # Capture logs using LogCapture
        with LogCapture() as log_capture:
            upload_df_to_s3(
                s3_client,
                new_df,
                file_key,
                transform_bucket_name=transform_bucket_name,  # noqa
            )  # noqa

            # Convert log entries to a list of messages
            log_messages = [record.msg for record in log_capture.records]

        # Expected log messages
        expected_messages = [
            f"Trying to read the file {file_key} from bucket {transform_bucket_name}",  # noqa
            "File not found. Using the new DataFrame",
            f"Successfully uploaded the updated DataFrame to {file_key} in bucket {transform_bucket_name}",  # noqa
        ]

        # Verify that each expected log message was logged
        for expected_message in expected_messages:
            assert expected_message in log_messages
