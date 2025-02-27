from src.ingestion_handler import ingestion_handler
from unittest.mock import patch, MagicMock


@patch("src.ingestion_handler.s3_client")
def test_ingestion_handler(mock_s3_client):

    mock_s3_client.return_value = MagicMock()

    event = {}
    context = {}

    with patch(
        "src.ingestion_handler.process_all_tables"
    ) as mock_process_tables:
        response = ingestion_handler(event, context)

    mock_s3_client.assert_called_once()

    mock_process_tables.assert_called_once_with(mock_s3_client.return_value)

    assert response == {"statusCode": 200, "body": "Data ingestion complete"}