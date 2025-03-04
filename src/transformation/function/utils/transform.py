import pandas as pd
from datetime import datetime


def transform_fact_sales_order(df_sales_order: pd.DataFrame) -> pd.DataFrame:
    # Ensure datetime columns are in datetime format
    df_sales_order["created_at"] = pd.to_datetime(df_sales_order["created_at"])
    df_sales_order["last_updated"] = pd.to_datetime(
        df_sales_order["last_updated"]
    )  # noqa

    # Convert agreed dates from string to datetime
    # (if not already) then extract date part
    df_sales_order["agreed_payment_date"] = pd.to_datetime(
        df_sales_order["agreed_payment_date"]
    ).dt.date
    df_sales_order["agreed_delivery_date"] = pd.to_datetime(
        df_sales_order["agreed_delivery_date"]
    ).dt.date

    # Create the fact table DataFrame
    df_fact_sales_order = pd.DataFrame(
        {
            # Generate a sales_record_id starting from 1; mimics SERIAL
            "sales_record_id": range(1, len(df_sales_order) + 1),
            "sales_order_id": df_sales_order["sales_order_id"],
            "created_date": df_sales_order["created_at"].dt.date,
            "created_time": df_sales_order["created_at"].dt.time,
            "last_updated_date": df_sales_order["last_updated"].dt.date,
            "last_updated_time": df_sales_order["last_updated"].dt.time,
            "sales_staff_id": df_sales_order["staff_id"],
            "counterparty_id": df_sales_order["counterparty_id"],
            "units_sold": df_sales_order["units_sold"],
            # Round unit_price to 2 decimal places to match numeric(10,2)
            "unit_price": df_sales_order["unit_price"].round(2),
            "currency_id": df_sales_order["currency_id"],
            "design_id": df_sales_order["design_id"],
            "agreed_payment_date": df_sales_order["agreed_payment_date"],
            "agreed_delivery_date": df_sales_order["agreed_delivery_date"],
            "agreed_delivery_location_id": df_sales_order[
                "agreed_delivery_location_id"
            ],
        }
    )

    return df_fact_sales_order


def transform_dim_staff(
    df_staff: pd.DataFrame, df_department: pd.DataFrame
) -> pd.DataFrame:
    # Perform the join operation
    df_dim_staff = pd.merge(
        df_staff,
        df_department,
        how="left",
        left_on="department_id",
        right_on="department_id",
    )

    # Select the required columns
    df_dim_staff = df_dim_staff[
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    ]
    return df_dim_staff


def transform_dim_location(df_address: pd.DataFrame) -> pd.DataFrame:
    # Select and rename the required columns
    df_dim_location = df_address.rename(columns={"address_id": "location_id"})[
        [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
    ]
    return df_dim_location


def transform_dim_design(df_design: pd.DataFrame) -> pd.DataFrame:
    # Select the required columns
    df_dim_design = df_design[
        ["design_id", "design_name", "file_location", "file_name"]
    ]
    return df_dim_design


def transform_dim_date() -> pd.DataFrame:

    # Define the start and end dates for the date dimension
    start_date = datetime.strptime("2020-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2125-12-31", "%Y-%m-%d")

    # Generate date range
    date_range = pd.date_range(start_date, end_date, freq="D")

    # Create DataFrame with date attributes
    data = {
        "date_id": date_range,
        "year": date_range.year,
        "month": date_range.month,
        "day": date_range.day,
        "day_of_week": date_range.weekday + 1,  # Monday=1, Sunday=7
        "day_name": date_range.strftime("%A"),
        "month_name": date_range.strftime("%B"),
        "quarter": date_range.quarter,
    }
    df_dim_date = pd.DataFrame(data)
    return df_dim_date


def transform_dim_currency(df_currency: pd.DataFrame) -> pd.DataFrame:

    # Create a dictionary for the currency mapping
    currency_mapping = {
        "USD": "US Dollar",
        "EUR": "Euro",
        "JPY": "Japanese Yen",
        "GBP": "British Pound",
        "AUD": "Australian Dollar",
        "CAD": "Canadian Dollar",
        "CHF": "Swiss Franc",
        "CNY": "Chinese Yuan Renminbi",
        "HKD": "Hong Kong Dollar",
        "NZD": "New Zealand Dollar",
        "SEK": "Swedish Krona",
        "KRW": "South Korean Won",
        "SGD": "Singapore Dollar",
        "NOK": "Norwegian Krone",
        "MXN": "Mexican Peso",
        "INR": "Indian Rupee",
        "RUB": "Russian Ruble",
        "ZAR": "South African Rand",
        "TRY": "Turkish Lira",
        "BRL": "Brazilian Real",
        "TWD": "New Taiwan Dollar",
        "DKK": "Danish Krone",
        "PLN": "Polish Zloty",
        "THB": "Thai Baht",
        "IDR": "Indonesian Rupiah",
        "HUF": "Hungarian Forint",
        "CZK": "Czech Koruna",
        "ILS": "Israeli New Shekel",
        "CLP": "Chilean Peso",
        "PHP": "Philippine Peso",
        "AED": "United Arab Emirates Dirham",
        "COP": "Colombian Peso",
        "SAR": "Saudi Riyal",
        "MYR": "Malaysian Ringgit",
        "RON": "Romanian Leu",
        "PEN": "Peruvian Sol",
        "VND": "Vietnamese Dong",
        "EGP": "Egyptian Pound",
        "NGN": "Nigerian Naira",
        "PKR": "Pakistani Rupee",
        "BDT": "Bangladeshi Taka",
        "UAH": "Ukrainian Hryvnia",
        "KZT": "Kazakhstani Tenge",
        "QAR": "Qatari Riyal",
        "KWD": "Kuwaiti Dinar",
        "OMR": "Omani Rial",
        "DZD": "Algerian Dinar",
        "MAD": "Moroccan Dirham",
        "ARS": "Argentine Peso",
        "LKR": "Sri Lankan Rupee",
    }

    # Add a new column with currency names
    df_currency["currency_name"] = df_currency["currency_code"].map(
        currency_mapping
    )  # noqa

    # Select the required columns
    df_dim_currency = df_currency[
        ["currency_id", "currency_code", "currency_name"]
    ]
    return df_dim_currency


def transform_dim_counterparty(
    df_counterparty: pd.DataFrame, df_address: pd.DataFrame
) -> pd.DataFrame:
    # Perform LEFT JOIN
    df_counterparty = df_counterparty.merge(
        df_address,
        left_on="counterparty_id",
        right_on="address_id",
        how="left",
    ).drop(
        columns=["address_id"]
    )  # noqa Dropping the duplicate ID column

    # Rename columns to match SQL output
    df_counterparty.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        },
        inplace=True,
    )

    # Select only the required columns
    columns_to_keep = [
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

    df_dim_counterparty = df_counterparty[columns_to_keep]
    return df_dim_counterparty
