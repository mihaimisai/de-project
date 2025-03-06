import pandas as pd
from src.transform.function.utils.transform import (
    transform_fact_sales_order,
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_dim_date,
    transform_dim_currency,
    transform_dim_counterparty,
)


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

    def test_transform_dim_currency(self):
        # Create sample input data, including an
        # extra column that should be dropped.
        data = {
            "currency_id": [1, 2, 3, 4],
            "currency_code": ["USD", "EUR", "ABC", "JPY"],
            "extra_col": [
                "ignore1",
                "ignore2",
                "ignore3",
                "ignore4",
            ],  # Extra column that should be ignored.
        }
        df_currency = pd.DataFrame(data)

        # Run the transformation function
        result = transform_dim_currency(df_currency)

        # Define the expected columns and check that
        # the output DataFrame has exactly these columns in order.
        expected_columns = ["currency_id", "currency_code", "currency_name"]
        assert (
            list(result.columns) == expected_columns
        ), "Output columns do not match expected columns."

        # Define the expected mapping for currency codes:
        # "USD" -> "US Dollar"
        # "EUR" -> "Euro"
        # "ABC" -> not in mapping, should result in NaN (or None)
        # "JPY" -> "Japanese Yen"
        expected_currency_names = ["US Dollar", "Euro", None, "Japanese Yen"]

        # Check that each row's currency_name matches the expected value.
        for i, expected_name in enumerate(expected_currency_names):
            actual_name = result.loc[i, "currency_name"]
            if expected_name is None:
                # For an unmapped currency code, the value should be NaN.
                assert pd.isna(
                    actual_name
                ), f"Expected NaN for currency_code 'ABC', got {actual_name}"
            else:
                assert (
                    actual_name == expected_name
                ), f"Row {i}: expected '{expected_name}', got '{actual_name}'"

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
