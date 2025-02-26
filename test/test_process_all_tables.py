import os
import pytest
from unittest.mock import patch, MagicMock
from src.utils.process_all_tables import process_all_tables  


@patch("src.utils.ingest_data_to_s3.ingest_data_to_s3")
@patch("src.utils.s3_client.s3_client") 
def test_process_all_tables(mock_s3_client, mock_ingest_data_to_s3):
    """Test that all tables are processed and ingest_data_to_s3 is called"""

    mock_s3_client.return_value = MagicMock()
    logger_mock = MagicMock()
    
    process_all_tables(mock_s3_client.return_value, logger_mock)

    expected_tables = [
        "counterparty", "currency", "department", "design", "staff",
        "sales_order", "address", "payment", "purchase_order",
        "payment_type", "transaction"
    ]

    for table in expected_tables:
        mock_ingest_data_to_s3.assert_any_call(
            mock_s3_client.return_value, logger_mock, table, 
            "test-ingestion-bucket", "test-timestamp-bucket"
        )

    assert mock_ingest_data_to_s3.call_count == len(expected_tables)
