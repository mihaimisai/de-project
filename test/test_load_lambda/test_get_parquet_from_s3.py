from src.load.function.utils.get_parquet_from_s3 import get_parquet_from_s3
import pytest
from moto import mock_aws
import boto3
import pandas as pd
import io
import logging
from unittest.mock import patch
from testfixtures import LogCapture
from botocore.exceptions import ClientError


@pytest.fixture
def test_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


@pytest.fixture
def test_parquet():
    data = {
        "name": ["shea", "anna", "carlo"],
        "age": [22, 25, 24],
        "city": ["Manchester", "Cambridge", "London"],
    }
    df = pd.DataFrame(data)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer


@pytest.fixture
def test_parquet_two():
    data = {
        "name": ["mihai", "ahmad", "rob"],
        "age": [22, 25, 24],
        "city": ["Manchester", "Cambridge", "London"],
    }
    df = pd.DataFrame(data)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer


class TestGetParquetFromS3:
    @mock_aws
    @patch(
        "src.load.function.utils.get_parquet_from_s3.table_list",
        return_value=["fact_test"],
    )
    def test_raises_error_when_NoSuchKey(self, test_logger, test_parquet):
        mock_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with pytest.raises(ClientError):
            get_parquet_from_s3(test_logger, mock_client, bucket_name)

    @mock_aws
    @patch(
        "src.load.function.utils.get_parquet_from_s3.table_list",
        return_value=["fact_test"],
    )
    def test_retrieves_dataframe_from_bucket(self, test_logger, test_parquet):
        mock_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.put_object(
            Bucket=bucket_name, Key="fact_test", Body=test_parquet.getvalue()
        )
        result = get_parquet_from_s3(test_logger, mock_client, bucket_name)

        assert isinstance(result, dict)
        assert isinstance(result["fact_test"], pd.DataFrame)

    @mock_aws
    @patch(
        "src.load.function.utils.get_parquet_from_s3.table_list",
        return_value=["fact_test", "fact_test_two"],
    )
    def test_retrieves_multiple_dataframes_from_bucket(
        self, test_logger, test_parquet, test_parquet_two
    ):
        mock_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.put_object(
            Bucket=bucket_name, Key="fact_test", Body=test_parquet.getvalue()
        )
        mock_client.put_object(
            Bucket=bucket_name,
            Key="fact_test_two",
            Body=test_parquet_two.getvalue(),
        )
        result = get_parquet_from_s3(test_logger, mock_client, bucket_name)

        assert len(result) == 2

    @mock_aws
    def test_logs_info_when_retrieved(self, test_logger, test_parquet):
        mock_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.put_object(
            Bucket=bucket_name, Key="fact_test", Body=test_parquet.getvalue()
        )

        with patch(
            "src.load.function.utils.get_parquet_from_s3.table_list"
        ) as mock_get:
            mock_get.return_value = ["fact_test"]
            with LogCapture("test_logger", level=logging.INFO) as logstream:
                get_parquet_from_s3(
                    logger=test_logger,
                    client=mock_client,
                    s3_processed_bucket=bucket_name,
                )

        assert logstream[0] == (
            "test_logger",
            "INFO",
            "Parquet data successfully retrieved within load lambda.",
        )

    @mock_aws
    def test_logs_error_when_fails_to_find_obj(
        self, test_logger, test_parquet
    ):
        mock_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with patch(
            "src.load.function.utils.get_parquet_from_s3.table_list"
        ) as mock_get:
            mock_get.return_value = ["fact_test"]
            with LogCapture("test_logger", level=logging.INFO) as logstream:
                try:
                    get_parquet_from_s3(
                        logger=test_logger,
                        client=mock_client,
                        s3_processed_bucket=bucket_name,
                    )
                except Exception as e:
                    test_logger.info(f"testing... {e}")
        assert logstream[0] == (
            "test_logger",
            "ERROR",
            "Error loading in the parquet data within Load Lambda: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.",  # noqa
        )
