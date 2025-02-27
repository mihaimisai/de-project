import pytest
from unittest.mock import patch, MagicMock
from src.utils.s3_data_upload import s3_data_upload


@patch("src.utils.s3_client.s3_client")
def test_s3_data_upload_success(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"
    time_stamp = "20240221"
    s3_data_upload(
        mock_s3_client,
        bucket_name,
        table_name,
        csv_data,
        mock_logger,
        time_stamp,
    )

    expected_s3_key = f"{table_name}/{time_stamp}.csv"

    mock_s3_client.put_object.assert_called_once_with(
        Bucket=bucket_name, Body=csv_data, Key=expected_s3_key
    )

    mock_logger.info.assert_called_once_with(
        f"Successfully uploaded csv file to S3 bucket '{bucket_name}' for table '{table_name}'" # noqa 501
    )


@patch("src.utils.s3_client.s3_client")
def test_s3_data_upload_failed(mock_s3_client):
    mock_logger = MagicMock()

    bucket_name = "test-bucket"
    table_name = "test-table"
    csv_data = b"sample,data"
    time_stamp = "20240221"

    mock_s3_client.put_object.side_effect = Exception("S3 Upload Error")

    with pytest.raises(Exception, match="S3 Upload Error"):
        s3_data_upload(
            mock_s3_client,
            bucket_name,
            table_name,
            csv_data,
            mock_logger,
            time_stamp,
        )

    mock_logger.error.assert_called_once_with(
        f"Error uploading csv file to S3 for table '{table_name}': S3 Upload Error" # noqa 501
    )
