import pandas as pd
import pytest
import requests
from unittest.mock import patch
from src.transform.function.utils.transform import (
    transform_fact_sales_order,
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_dim_date,
    fetch_exchange_rates,
    transform_dim_currency,
    transform_dim_counterparty,
)

# DummyResponse class to simulate requests.Response
class DummyResponse:
    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self._json_data = json_data

    def json(self):
        return self._json_data

# Dummy functions to simulate API responses
def dummy_get_success(url):
    return DummyResponse(200, {"eur": {"usd": 1.12, "eur": 1.0, "gbp": 0.85}})

def dummy_get_failure(url):
    return DummyResponse(404, {})

class TestFetchExchangeRates:
    def test_fetch_exchange_rates_success(self, monkeypatch):
        # Replace requests.get with the dummy success function
        monkeypatch.setattr(requests, "get", dummy_get_success)
        result = fetch_exchange_rates()
        expected = {"eur": {"usd": 1.12, "eur": 1.0, "gbp": 0.85}}
        assert result == expected

    def test_fetch_exchange_rates_failure(self, monkeypatch):
        # Replace requests.get with the dummy failure function
        monkeypatch.setattr(requests, "get", dummy_get_failure)
        with pytest.raises(Exception) as excinfo:
            fetch_exchange_rates()
        assert "Failed to fetch data from API" in str(excinfo.value)
class TestTransform:
    def test_transform_fact_sales_order(self):
        # Sample input data
        data = {
            "sales_order_id": [101, 102],
            "created_at": ["2025-03-03 10:22:01", "2025-03-03 11:45:30"],
            "last_updated": ["2025-03-03 10:30:00", "2025-03-03 12:00:00"],
            "staff_id": [1, 2],
            "counterparty_id": [201, 202],
            "units_sold": [10, 20],
            "unit_price": [100.123, 200.567],
            "currency_id": [1, 2],
            "design_id": [301, 302],
            "agreed_payment_date": ["2025-03-05", "2025-03-06"],
            "agreed_delivery_date": ["2025-03-10", "2025-03-11"],
            "agreed_delivery_location_id": [401, 402],
        }

        # Convert to DataFrame
        df_sales_order = pd.DataFrame(data)

        # Run the transformation function
        df_fact_sales_order = transform_fact_sales_order(df_sales_order)

        # Expected column names
        expected_columns = [
            "sales_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]

        # Verify columns match
        assert list(df_fact_sales_order.columns) == expected_columns

        # Verify sales_record_id is sequential
        assert list(df_fact_sales_order["sales_record_id"]) == [1, 2]

        # Verify datetime conversions
        assert (
            df_fact_sales_order["created_date"].tolist()
            == [pd.to_datetime("2025-03-03").date()] * 2
        )
        assert (
            df_fact_sales_order["last_updated_date"].tolist()
            == [pd.to_datetime("2025-03-03").date()] * 2
        )

        # Verify time extraction
        assert df_fact_sales_order["created_time"].tolist() == [
            pd.to_datetime("10:22:01").time(),
            pd.to_datetime("11:45:30").time(),
        ]
        assert df_fact_sales_order["last_updated_time"].tolist() == [
            pd.to_datetime("10:30:00").time(),
            pd.to_datetime("12:00:00").time(),
        ]

        # Verify numeric values
        assert df_fact_sales_order["unit_price"].tolist() == [
            100.12,
            200.57,
        ]  # Rounded values

        # Verify agreed dates
        assert df_fact_sales_order["agreed_payment_date"].tolist() == [
            pd.to_datetime("2025-03-05").date(),
            pd.to_datetime("2025-03-06").date(),
        ]
        assert df_fact_sales_order["agreed_delivery_date"].tolist() == [
            pd.to_datetime("2025-03-10").date(),
            pd.to_datetime("2025-03-11").date(),
        ]

        print("All tests passed!")

    def test_transform_dim_staff(self):
        # Sample staff data
        df_staff = pd.DataFrame(
            {
                "staff_id": [1, 2, 3],
                "first_name": ["Alice", "Bob", "Charlie"],
                "last_name": ["Smith", "Jones", "Brown"],
                "department_id": [
                    101,
                    102,
                    103,
                ],  # Note: department_id 103 won't have a match
            }
        )

        # Sample department data
        df_department = pd.DataFrame(
            {
                "department_id": [101, 102],
                "department_name": ["Sales", "Engineering"],
                "location": ["New York", "San Francisco"],
                "email_address": ["sales@example.com", "eng@example.com"],
            }
        )

        # Execute the transformation function
        df_dim_staff = transform_dim_staff(df_staff, df_department)

        # Define expected columns
        expected_columns = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
        # Check that output has the expected columns in the correct order
        assert (
            list(df_dim_staff.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # The output should have the same number of rows as the staff DataFrame
        assert len(df_dim_staff) == len(df_staff), "Row count mismatch."

        # Validate merged data for staff with a matching department
        row_alice = df_dim_staff[df_dim_staff["staff_id"] == 1].iloc[0]
        assert (
            row_alice["department_name"] == "Sales"
        ), "Alice's department_name is incorrect."
        assert (
            row_alice["location"] == "New York"
        ), "Alice's location is incorrect."  # noqa
        assert (
            row_alice["email_address"] == "sales@example.com"
        ), "Alice's email_address is incorrect."

        row_bob = df_dim_staff[df_dim_staff["staff_id"] == 2].iloc[0]
        assert (
            row_bob["department_name"] == "Engineering"
        ), "Bob's department_name is incorrect."
        assert (
            row_bob["location"] == "San Francisco"
        ), "Bob's location is incorrect."  # noqa
        assert (
            row_bob["email_address"] == "eng@example.com"
        ), "Bob's email_address is incorrect."

        # For staff with no matching department (e.g. department_id 103),
        # the department fields should be NaN.
        row_charlie = df_dim_staff[df_dim_staff["staff_id"] == 3].iloc[0]
        assert pd.isna(
            row_charlie["department_name"]
        ), "Expected NaN for Charlie's department_name."
        assert pd.isna(
            row_charlie["location"]
        ), "Expected NaN for Charlie's location."  # noqa
        assert pd.isna(
            row_charlie["email_address"]
        ), "Expected NaN for Charlie's email_address."

    def test_transform_dim_location(self):
        # Create sample input data including an extra column
        # to verify that only required columns are kept.
        data = {
            "address_id": [1, 2, 3],
            "address_line_1": ["123 Main St", "456 Elm St", "789 Oak St"],
            "address_line_2": ["Apt 1", "Suite 200", ""],
            "district": ["District A", "District B", "District C"],
            "city": ["CityA", "CityB", "CityC"],
            "postal_code": ["10001", "20002", "30003"],
            "country": ["USA", "Canada", "UK"],
            "phone": ["111-222-3333", "222-333-4444", "333-444-5555"],
            "extra": [
                "ignore",
                "this",
                "column",
            ],  # Extra column that should not appear in the output.
        }
        df_address = pd.DataFrame(data)

        # Run the transformation function
        df_dim_location = transform_dim_location(df_address)

        # Define expected columns in the output DataFrame
        expected_columns = [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        # Verify the output DataFrame has the correct
        # columns in the right order
        assert (
            list(df_dim_location.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # Verify that the renaming was done correctly:
        # "location_id" should match the original "address_id"
        expected_location_ids = df_address["address_id"].tolist()
        assert (
            df_dim_location["location_id"].tolist() == expected_location_ids
        ), "location_id values do not match address_id values."

        # Verify that other columns are correctly transferred
        for col in expected_columns[1:]:
            assert (
                df_dim_location[col].tolist() == df_address[col].tolist()
            ), f"Column {col} does not match the expected values."

        # Confirm that the extra column is not present in the output
        assert (
            "extra" not in df_dim_location.columns
        ), "Extra column found in output DataFrame."

    def test_transform_dim_design(self):
        # Create sample input data, including an extra column
        data = {
            "design_id": [1, 2, 3],
            "design_name": ["DesignA", "DesignB", "DesignC"],
            "file_location": ["/files/a", "/files/b", "/files/c"],
            "file_name": ["a.pdf", "b.pdf", "c.pdf"],
            "extra_col": [
                "ignore1",
                "ignore2",
                "ignore3",
            ],  # Extra column that should be dropped
        }
        df_design = pd.DataFrame(data)

        # Run the transformation function
        df_dim_design = transform_dim_design(df_design)

        # Define expected columns
        expected_columns = [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]  # noqa

        # Check that output has exactly the
        # expected columns in the correct order
        assert (
            list(df_dim_design.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # Verify that the data for each column is preserved
        for col in expected_columns:
            assert (
                df_dim_design[col].tolist() == df_design[col].tolist()
            ), f"Values for column '{col}' do not match."

        # Confirm that the extra column is not present in the output
        assert (
            "extra_col" not in df_dim_design.columns
        ), "Unexpected extra column found in output DataFrame."

    def test_transform_dim_date(self):
        # Generate the date dimension DataFrame
        df_dim_date = transform_dim_date()

        # Define expected columns in the output DataFrame
        expected_columns = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        # Check that the DataFrame has exactly
        # the expected columns in the right order
        assert (
            list(df_dim_date.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # Define the expected start and end dates as Timestamps
        first_date = pd.Timestamp("2020-01-01")
        last_date = pd.Timestamp("2125-12-31")

        # Verify that the first and last dates in the DataFrame are correct
        assert (
            df_dim_date.iloc[0]["date_id"] == first_date
        ), "The first date is not 2020-01-01."
        assert (
            df_dim_date.iloc[-1]["date_id"] == last_date
        ), "The last date is not 2125-12-31."

        # Check that the total number of rows equals
        # (last_date - first_date).days + 1
        expected_length = (last_date - first_date).days + 1
        assert (
            len(df_dim_date) == expected_length
        ), f"Expected {expected_length} rows, but got {len(df_dim_date)}."

        # Validate attributes for the first date (2020-01-01)
        first_row = df_dim_date.iloc[0]
        # 2020-01-01 is a Wednesday: Monday=1, so Wednesday=3.
        assert first_row["year"] == 2020, "Year for the first row is incorrect."  # noqa
        assert first_row["month"] == 1, "Month for the first row is incorrect."
        assert first_row["day"] == 1, "Day for the first row is incorrect."
        assert (
            first_row["day_of_week"] == 3
        ), "Day of week for 2020-01-01 should be 3 (Wednesday)."
        assert (
            first_row["day_name"] == "Wednesday"
        ), "Day name for 2020-01-01 should be 'Wednesday'."
        assert (
            first_row["month_name"] == "January"
        ), "Month name for the first row is incorrect."
        assert first_row["quarter"] == 1, "Quarter for 2020-01-01 should be 1."

        # Validate attributes for a sample date, e.g., 2021-12-31
        sample_date = pd.Timestamp("2021-12-31")
        sample_row = df_dim_date[df_dim_date["date_id"] == sample_date].iloc[0]
        # 2021-12-31 is a Friday: Monday=1, so Friday=5.
        assert sample_row["year"] == 2021, "Year for 2021-12-31 is incorrect."
        assert sample_row["month"] == 12, "Month for 2021-12-31 is incorrect."
        assert sample_row["day"] == 31, "Day for 2021-12-31 is incorrect."
        assert (
            sample_row["day_of_week"] == 5
        ), "Day of week for 2021-12-31 should be 5 (Friday)."
        assert (
            sample_row["day_name"] == "Friday"
        ), "Day name for 2021-12-31 should be 'Friday'."
        assert (
            sample_row["month_name"] == "December"
        ), "Month name for 2021-12-31 is incorrect."
        assert sample_row["quarter"] == 4, "Quarter for 2021-12-31 should be 4."  # noqa

    @staticmethod
    def dummy_fetch_exchange_rates():
        # Return a complete set of dummy exchange rates in lowercase keys.
        dummy_rates = {
            "usd": 1.12,
            "eur": 1.0,
            "jpy": 110.0,
            "gbp": 0.85,
            "aud": 1.5,
            "cad": 1.4,
            "chf": 0.95,
            "cny": 7.0,
            "hkd": 8.8,
            "nzd": 1.7,
            "sek": 10.0,
            "krw": 1300,
            "sgd": 1.6,
            "nok": 11.0,
            "mxn": 22.0,
            "inr": 80.0,
            "rub": 90.0,
            "zar": 20.0,
            "try": 20.0,
            "brl": 6.0,
            "twd": 32.0,
            "dkk": 7.0,
            "pln": 4.5,
            "thb": 35.0,
            "idr": 16000,
            "huf": 350.0,
            "czk": 25.0,
            "ils": 3.5,
            "clp": 900.0,
            "php": 56.0,
            "aed": 4.0,
            "cop": 4500.0,
            "sar": 4.2,
            "myr": 4.7,
            "ron": 4.9,
            "pen": 3.8,
            "vnd": 26000,
            "egp": 17.0,
            "ngn": 450.0,
            "pkr": 230.0,
            "bdt": 102.0,
            "uah": 36.0,
            "kzt": 510.0,
            "qar": 4.0,
            "kwd": 0.33,
            "omr": 0.38,
            "dzd": 150.0,
            "mad": 10.0,
            "ars": 130.0,
            "lkr": 365.0,
        }
        return {"eur": dummy_rates}

    @patch("src.transform.function.utils.transform.fetch_exchange_rates", new=dummy_fetch_exchange_rates.__func__)
    def test_transform_dim_currency_success(self):
        # Create a sample input DataFrame mimicking your df_currency
        df_currency = pd.DataFrame({
            "currency_id": [1, 2, 3],
            "currency_code": ["GBP", "USD", "EUR"]
        })

        # Call the function under test
        result_df = transform_dim_currency(df_currency)

        # Define the expected output DataFrame
        expected_df = pd.DataFrame({
            "currency_id": [1, 2, 3],
            "currency_code": ["GBP", "USD", "EUR"],
            "currency_name": ["British Pound", "US Dollar", "Euro"],
            "currency_exchange_rate_eur_based": [0.85, 1.12, 1.0]
        })

        # Compare the result with the expected DataFrame
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    def test_transform_dim_counterparty(self):
        # Create sample counterparty data
        df_counterparty = pd.DataFrame(
            {
                "counterparty_id": [1, 2],
                "counterparty_legal_name": ["Acme Corp", "Beta LLC"],
            }
        )

        # Create sample address data
        df_address = pd.DataFrame(
            {
                "address_id": [1],
                "address_line_1": ["123 Road"],
                "address_line_2": ["Suite 1"],
                "district": ["District 1"],
                "city": ["CityA"],
                "postal_code": ["1000"],
                "country": ["CountryA"],
                "phone": ["111-111-1111"],
            }
        )

        # Execute the transformation function
        result = transform_dim_counterparty(df_counterparty, df_address)

        # Define expected output columns
        expected_columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        # Check that the output DataFrame has
        # exactly the expected columns in order
        assert (
            list(result.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # Validate the row with a matching address (counterparty_id = 1)
        row1 = result[result["counterparty_id"] == 1].iloc[0]
        assert row1["counterparty_legal_name"] == "Acme Corp"
        assert row1["counterparty_legal_address_line_1"] == "123 Road"
        assert row1["counterparty_legal_address_line_2"] == "Suite 1"
        assert row1["counterparty_legal_district"] == "District 1"
        assert row1["counterparty_legal_city"] == "CityA"
        assert row1["counterparty_legal_postal_code"] == "1000"
        assert row1["counterparty_legal_country"] == "CountryA"
        assert row1["counterparty_legal_phone_number"] == "111-111-1111"

        # Validate the row without a matching address (counterparty_id = 2)
        row2 = result[result["counterparty_id"] == 2].iloc[0]
        assert row2["counterparty_legal_name"] == "Beta LLC"
        # The address fields should be NaN since there's no matching address
        assert pd.isna(row2["counterparty_legal_address_line_1"])
        assert pd.isna(row2["counterparty_legal_address_line_2"])
        assert pd.isna(row2["counterparty_legal_district"])
        assert pd.isna(row2["counterparty_legal_city"])
        assert pd.isna(row2["counterparty_legal_postal_code"])
        assert pd.isna(row2["counterparty_legal_country"])
        assert pd.isna(row2["counterparty_legal_phone_number"])


