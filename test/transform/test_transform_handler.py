from src.transform.function.transform_handler_fn import transform_handler
from unittest.mock import patch


def test_transform_handler_called_files_dict():

    with patch(
        "src.transform.function.transform_handler_fn.get_latest_files"
    ) as mock_files:
        with patch(
            "src.transform.function.transform_handler_fn.ingested_data_retrival"  # noqa 501
        ):
            result = transform_handler({}, {})
            mock_files.assert_called_once()

    expected_result = {
        "statusCode": 200,
        "body": "Data transformation complete",
    }
    assert result == expected_result


def test_transform_handler_called_list_s3():

    with patch("src.transform.function.transform_handler_fn.get_latest_files"):
        with patch(
            "src.transform.function.transform_handler_fn.ingested_data_retrival"  # noqa 501
        ) as mock_list:
            result = transform_handler({}, {})
            mock_list.assert_called_once()

    expected_result = {
        "statusCode": 200,
        "body": "Data transformation complete",
    }
    assert result == expected_result
