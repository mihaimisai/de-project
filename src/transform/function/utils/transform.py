import pandas as pd
from datetime import datetime


def transform_fact_sales_order(df_sales_order: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the sales order DataFrame into a fact table DataFrame.
    This function processes the input DataFrame by converting date columns to
    datetime format, extracting date and time components, and creating a new
    DataFrame structured as a fact table for sales orders.
    Args:
        df_sales_order (pd.DataFrame):
        The input DataFrame containing sales order data.
    Returns:
        pd.DataFrame: A transformed DataFrame structured
        as a fact table for sales orders.
    """
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
    """
    Transforms and merges staff and department data into a single DataFrame.
    This function performs a left join operation between the staff
    and department DataFrames on the 'department_id' column.
    It then selects and returns a DataFrame with the following columns:
        'staff_id',
        'first_name',
        'last_name',
        'department_name',
        'location',
        'email_address'
    Parameters:
    df_staff (pd.DataFrame): DataFrame containing staff information.
    df_department (pd.DataFrame): DataFrame containing department information.
    Returns:
    pd.DataFrame: A DataFrame containing the merged and selected staff
    and department data.
    """
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
    """
    Transforms the input DataFrame by selecting and renaming specific columns.

    Args:
        df_address (pd.DataFrame): The input DataFrame containing
        address information.

    Returns:
        pd.DataFrame: A DataFrame with selected columns renamed
        for dimensional location.
    """
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
    """
    Transforms the input DataFrame by selecting specific columns
    related to design.

    Args:
        df_design (pd.DataFrame): The input DataFrame containing design data.

    Returns:
        pd.DataFrame: A DataFrame containing only the selected columns:
                      'design_id',
                      'design_name',
                      'file_location',
                      'file_name'
    """
    # Select the required columns
    df_dim_design = df_design[
        ["design_id", "design_name", "file_location", "file_name"]
    ]
    return df_dim_design


def transform_dim_date() -> pd.DataFrame:
    """
    Generates a DataFrame representing a date dimension table
    with various date attributes.
    The date dimension table includes the following columns:
    - date_id: The date itself.
    - year: The year of the date.
    - month: The month of the date.
    - day: The day of the month.
    - day_of_week: The day of the week (Monday=1, Sunday=7).
    - day_name: The name of the day of the week.
    - month_name: The name of the month.
    - quarter: The quarter of the year.
    The date range spans from January 1, 2020, to December 31, 2125.
    Returns:
        pd.DataFrame: A DataFrame containing the date dimension table.
    """

    # Define the start and end dates for the date dimension
    start_date = datetime.strptime("2020-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2025-12-31", "%Y-%m-%d")

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
    """
    Transforms a DataFrame containing currency information by adding a column
    with the full currency names and selecting specific columns.
    Args:
        df_currency (pd.DataFrame): A DataFrame with currency information.
                                    It must contain the columns
                                    'currency_id' and 'currency_code'.
    Returns:
        pd.DataFrame: A transformed DataFrame with the columns 'currency_id',
                      'currency_code', and 'currency_name'.
    """

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
    ]  # noqa
    return df_dim_currency


def transform_dim_counterparty(
    df_counterparty: pd.DataFrame, df_address: pd.DataFrame
) -> pd.DataFrame:
    """
    Transforms the counterparty DataFrame by performing a left join
    with the address DataFrame and renaming columns to match
    the desired output format.
    Args:
        df_counterparty (pd.DataFrame): DataFrame containing
        counterparty information.
        df_address (pd.DataFrame): DataFrame containing address information.
    Returns:
        pd.DataFrame: Transformed DataFrame with selected columns
        and renamed address fields.
    """
    # Perform LEFT JOIN
    df_counterparty = df_counterparty.merge(
        df_address,
        left_on="counterparty_id",
        right_on="address_id",
        how="left",  # noqa Dropping the duplicate ID column
    ).drop(columns=["address_id"])

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
