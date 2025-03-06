from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError
from src.transform.function.utils.list_s3 import ingested_data_retrival

# import pandas as pd
import io


@pytest.fixture
def mock_s3_client():
    mock_client = MagicMock()
    csv_data = "id,name,age\n1,Alice,30\n2,Bob,25"
    mock_response = {"Body": io.BytesIO(csv_data.encode("utf-8"))}
    mock_client.get_object.return_value = mock_response
    return mock_client


@pytest.fixture
def mock_logger():
    return MagicMock()


# def test_ingested_data_retrival_success(mock_s3_client, mock_logger):
#     bucket_name = "test-bucket"
#     files_dict = {"users": "data/users.csv"}

#     result = ingested_data_retrival(
#         mock_s3_client, files_dict, mock_logger, bucket_name
#     )

#     assert isinstance(result, dict)
#     assert "users" in result
#     assert isinstance(result["users"], pd.DataFrame)

#     expected_df = pd.DataFrame(
#         {"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}
#     )
#     pd.testing.assert_frame_equal(result["users"], expected_df)
#     assert result["users"].iloc[0]["id"] == 1


def test_file_not_found(mock_logger):
    mock_s3_client = MagicMock()
    error_response = {
        "Error": {
            "Code": "NoSuchKey",
            "Message": "The specified key does not exist.",
        }
    }
    mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")

    files_dict = {"table1": "table1/test.csv", "table2": "table2/another.csv"}
    result = ingested_data_retrival(
        mock_s3_client, files_dict, mock_logger, "test-bucket"
    )
    assert result == {}
    mock_logger.error.assert_any_call("File not found: table1/test.csv.")
