import pytest
from moto import mock_aws
from unittest.mock import patch
from src.transformation.function.utils.star_schema import star_schema
from src.transformation.function.utils.list_s3 import ingested_data_retrival


class TestStarschema:
    def test_star_schema(self):
        with mock_aws():
            mock_logger = patch(
                "src.transformation.function.utils.list_s3.logger"
            ).start()

            # Mocking ingested_data_retrival
            mock_dataframes = {
                "df_sales_order": "mock_df_sales_order",
                "df_staff": "mock_df_staff",
                "df_department": "mock_df_department",
                "df_address": "mock_df_address",
                "df_design": "mock_df_design",
                "df_currency": "mock_df_currency",
                "df_counterparty": "mock_df_counterparty",
            }
            mock_dataframes_info = {
                "table_names": [
                    "sales_order",
                    "staff",
                    "department",
                    "address",
                    "design",
                    "currency",
                    "counterparty",
                ]
            }

            with patch(
                "src.transformation.function.utils.list_s3.ingested_data_retrival",
                return_value=(mock_dataframes, mock_dataframes_info),
            ):
                with patch(
                    "src.transformation.function.utils.transform.transform_fact_sales_order",
                    return_value="fact_sales_order_mock",
                ):
                    with patch(
                        "src.transformation.function.utils.transform.transform_dim_staff",
                        return_value="dim_staff_mock",
                    ):
                        with patch(
                            "src.transformation.function.utils.transform.transform_dim_location",
                            return_value="dim_location_mock",
                        ):
                            with patch(
                                "src.transformation.function.utils.transform.transform_dim_design",
                                return_value="dim_design_mock",
                            ):
                                with patch(
                                    "src.transformation.function.utils.transform.transform_dim_date",
                                    return_value="dim_date_mock",
                                ):
                                    with patch(
                                        "src.transformation.function.utils.transform.transform_dim_currency",
                                        return_value="dim_currency_mock",
                                    ):
                                        with patch(
                                            "src.transformation.function.utils.transform.transform_dim_counterparty",
                                            return_value="dim_counterparty_mock",
                                        ):

                                            result = star_schema(
                                                None, logger=mock_logger
                                            )

                                            assert result == {
                                                "fact_sales_order": "fact_sales_order_mock",
                                                "dim_staff": "dim_staff_mock",
                                                "dim_location": "dim_location_mock",
                                                "dim_design": "dim_design_mock",
                                                "dim_date": "dim_date_mock",
                                                "dim_currency": "dim_currency_mock",
                                                "dim_counterparty": "dim_counterparty_mock",
                                            }

                                            mock_logger.info.assert_any_call(
                                                "All required dataframes are available"
                                            )
                                            mock_logger.info.assert_any_call(
                                                "Star-schema data successfully processed"
                                            )

            patch.stopall()
