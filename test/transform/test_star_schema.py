import pytest
from moto import mock_aws
from unittest.mock import patch, MagicMock
from src.transform.function.utils.star_schema import star_schema
from src.transform.function.utils.s3_client import s3_client
import logging

class TestStarschema:

    @pytest.fixture
    def mock_s3_client(self):
        with mock_aws():
            yield s3_client()

    @patch("src.transform.function.utils.star_schema.transform_dim_transaction")
    @patch("src.transform.function.utils.star_schema.transform_dim_payment_type")
    @patch("src.transform.function.utils.star_schema.transform_dim_counterparty")
    @patch("src.transform.function.utils.star_schema.transform_dim_currency")
    @patch("src.transform.function.utils.star_schema.transform_dim_date")
    @patch("src.transform.function.utils.star_schema.transform_dim_design")
    @patch("src.transform.function.utils.star_schema.transform_dim_location")
    @patch("src.transform.function.utils.star_schema.transform_dim_staff")
    @patch("src.transform.function.utils.star_schema.transform_fact_payment")
    @patch("src.transform.function.utils.star_schema.transform_fact_purchase_order")
    @patch("src.transform.function.utils.star_schema.transform_fact_sales_order")
    @patch("src.transform.function.utils.star_schema.ingested_data_retrival")
    def test_star_schema(
        self,
        mock_ingested_data_retrival,
        mock_transform_fact_sales_order,
        mock_transform_fact_purchase_order,
        mock_transform_fact_payment,
        mock_transform_dim_staff,
        mock_transform_dim_location,
        mock_transform_dim_design,
        mock_transform_dim_date,
        mock_transform_dim_currency,
        mock_transform_dim_counterparty,
        mock_transform_dim_payment_type,
        mock_transform_dim_transaction,
        mock_s3_client,
    ):
        # Set up a logger
        logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        # Prepare ingested data with all required keys
        mock_ingested_data_retrival.return_value = {
            "df_sales_order": MagicMock(),
            "df_purchase_order": MagicMock(),
            "df_payment": MagicMock(),
            "df_staff": MagicMock(),
            "df_department": MagicMock(),
            "df_address": MagicMock(),
            "df_design": MagicMock(),
            "df_currency": MagicMock(),
            "df_counterparty": MagicMock(),
            "df_payment_type": MagicMock(),
            "df_transaction": MagicMock(),
        }

        # Set mocks for all transform functions
        mock_transform_fact_sales_order.return_value = "fact_sales_order"
        mock_transform_fact_purchase_order.return_value = "fact_purchase_order"
        mock_transform_fact_payment.return_value = "fact_payment"
        mock_transform_dim_staff.return_value = "dim_staff"
        mock_transform_dim_location.return_value = "dim_location"
        mock_transform_dim_design.return_value = "dim_design"
        mock_transform_dim_date.return_value = "dim_date"
        mock_transform_dim_currency.return_value = "dim_currency"
        mock_transform_dim_counterparty.return_value = "dim_counterparty"
        mock_transform_dim_payment_type.return_value = "dim_payment_type"
        mock_transform_dim_transaction.return_value = "dim_transaction"

        # Call the function under test
        result = star_schema(mock_s3_client, "test-bucket", logger, {})

        # Define expected result
        expected_result = {
            "fact_sales_order": "fact_sales_order",
            "fact_purchase_order": "fact_purchase_order",
            "fact_payment": "fact_payment",
            "dim_staff": "dim_staff",
            "dim_location": "dim_location",
            "dim_design": "dim_design",
            "dim_date": "dim_date",
            "dim_currency": "dim_currency",
            "dim_counterparty": "dim_counterparty",
            "dim_payment_type": "dim_payment_type",
            "dim_transaction": "dim_transaction",
        }

        # Assert that the result matches the expected dictionary
        assert result == expected_result

        # Verify each mocked function was called exactly once
        mock_ingested_data_retrival.assert_called_once()
        mock_transform_fact_sales_order.assert_called_once()
        mock_transform_fact_purchase_order.assert_called_once()
        mock_transform_fact_payment.assert_called_once()
        mock_transform_dim_staff.assert_called_once()
        mock_transform_dim_location.assert_called_once()
        mock_transform_dim_design.assert_called_once()
        mock_transform_dim_date.assert_called_once()
        mock_transform_dim_currency.assert_called_once()
        mock_transform_dim_counterparty.assert_called_once()
        mock_transform_dim_payment_type.assert_called_once()
        mock_transform_dim_transaction.assert_called_once()
