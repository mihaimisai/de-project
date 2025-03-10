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

    @patch(
        "src.transform.function.utils.star_schema.ingested_data_retrival"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_fact_sales_order"  # noqa
    )
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_staff"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_location"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_design"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_date"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_currency"
    )  # noqa
    @patch(
        "src.transform.function.utils.star_schema.transform_dim_counterparty"  # noqa
    )
    def test_star_schema(
        self,
        mock_transform_dim_counterparty,
        mock_transform_dim_currency,
        mock_transform_dim_date,
        mock_transform_dim_design,
        mock_transform_dim_location,
        mock_transform_dim_staff,
        mock_transform_fact_sales_order,
        mock_ingested_data_retrival,
        mock_s3_client,
    ):
        # Mock input data
        logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        mock_ingested_data_retrival.return_value = {
            "df_sales_order": MagicMock(),
            "df_staff": MagicMock(),
            "df_department": MagicMock(),
            "df_address": MagicMock(),
            "df_design": MagicMock(),
            "df_currency": MagicMock(),
            "df_counterparty": MagicMock(),
        }

        # Mock transform functions
        mock_transform_fact_sales_order.return_value = "fact_sales_order"
        mock_transform_dim_staff.return_value = "dim_staff"
        mock_transform_dim_location.return_value = "dim_location"
        mock_transform_dim_design.return_value = "dim_design"
        mock_transform_dim_date.return_value = "dim_date"
        mock_transform_dim_currency.return_value = "dim_currency"
        mock_transform_dim_counterparty.return_value = "dim_counterparty"

        # Call function
        result = star_schema(mock_s3_client, "test-bucket", logger, {})

        # Assertions
        assert result == {
            "fact_sales_order": "fact_sales_order",
            "dim_staff": "dim_staff",
            "dim_location": "dim_location",
            "dim_design": "dim_design",
            "dim_date": "dim_date",
            "dim_currency": "dim_currency",
            "dim_counterparty": "dim_counterparty",
        }

        mock_ingested_data_retrival.assert_called_once()
        mock_transform_fact_sales_order.assert_called_once()
        mock_transform_dim_staff.assert_called_once()
        mock_transform_dim_location.assert_called_once()
        mock_transform_dim_design.assert_called_once()
        mock_transform_dim_date.assert_called_once()
        mock_transform_dim_currency.assert_called_once()
        mock_transform_dim_counterparty.assert_called_once()
