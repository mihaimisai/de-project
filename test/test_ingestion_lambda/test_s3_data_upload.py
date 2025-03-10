import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.function.utils.s3_data_upload import s3_data_upload
from freezegun import freeze_time


@patch("src.ingestion.function.utils.s3_client.s3_client")
def test_s3_data_upload_success_with_time_stamp(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"

    s3_data_upload(
        mock_s3_client, bucket_name, table_name, csv_data, mock_logger
    )

    mock_s3_client.put_object.assert_called_once()

    mock_logger.info.assert_called_once_with(
        f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa 501
    )


@patch("src.ingestion.function.utils.s3_client.s3_client")
def test_s3_data_upload_success_without_time_stamp(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"

    s3_data_upload(
        mock_s3_client, bucket_name, table_name, csv_data, mock_logger
    )

    s3_key = mock_s3_client.put_object.call_args[1]["Key"]
    assert s3_key is not None

    mock_logger.info.assert_called_once_with(
        f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'"  # noqa 501
    )


@freeze_time("2025-01-01 11:00:00")
@patch("src.ingestion.function.utils.s3_client.s3_client")
def test_s3_data_upload_success_returns_time_stamp(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"

    result = s3_data_upload(
        mock_s3_client, bucket_name, table_name, csv_data, mock_logger
    )

    assert isinstance(result, str)
    assert result == "2025-01-01 11:00:00"


@patch("src.ingestion.function.utils.s3_client.s3_client")
def test_s3_data_upload_failed(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"

    mock_s3_client.put_object.side_effect = Exception("S3 Upload Error")

    with pytest.raises(Exception, match="S3 Upload Error"):
        s3_data_upload(
            mock_s3_client, bucket_name, table_name, csv_data, mock_logger
        )

    mock_logger.error.assert_called_once_with(
        f"Error uploading csv file to S3 for table '{table_name}': S3 Upload Error"  # noqa 501
    )
