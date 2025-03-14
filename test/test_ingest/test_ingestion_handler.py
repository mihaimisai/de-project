from src.ingest.function.ingest_handler_fn import ingest_handler
from unittest.mock import patch, MagicMock


@patch("src.ingest.function.ingest_handler_fn.s3_client")
def test_ingest_handler(mock_s3_client):

    mock_s3_client.return_value = MagicMock()

    event = {}
    context = {}

    with patch(
        "src.ingest.function.ingest_handler_fn.process_all_tables"
    ) as mock_process_tables:
        response = ingest_handler(event, context)

    mock_s3_client.assert_called_once()

    mock_process_tables.assert_called_once_with(mock_s3_client.return_value)

    assert response == {"statusCode": 200, "body": "Data ingestion complete"}
