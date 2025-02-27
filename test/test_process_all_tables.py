from unittest.mock import patch, MagicMock
from src.ingestion.utils.process_all_tables import process_all_tables


@patch("src.utils.process_all_tables.ingest_data_to_s3")
@patch("src.utils.process_all_tables.s3_client")
@patch("src.utils.process_all_tables.logger")
def test_process_all_tables(
    mock_logger, mock_s3_client, mock_ingest_data_to_s3
):
    """Test that all tables are processed
    and ingest_data_to_s3 is properly mocked"""

    mock_s3_client.return_value = MagicMock()
    mock_ingest_data_to_s3.return_value = None

    process_all_tables(mock_s3_client.return_value, mock_logger)

    expected_tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    assert mock_ingest_data_to_s3.call_count == len(expected_tables)

    for table in expected_tables:
        mock_ingest_data_to_s3.assert_any_call(
            mock_s3_client.return_value, mock_logger, table, None, None
        )
