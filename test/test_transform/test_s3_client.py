import pytest
from unittest.mock import patch
from botocore.exceptions import NoCredentialsError
from moto import mock_aws
from src.transform.function.utils.s3_client import s3_client


@mock_aws
def test_s3_client():

    client = s3_client()

    assert hasattr(client, "list_buckets")


@mock_aws
def test_error_s3_client():
    with patch("boto3.client", side_effect=NoCredentialsError):
        with pytest.raises(NoCredentialsError):
            s3_client().list_buckets()
