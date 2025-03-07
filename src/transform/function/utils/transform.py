import requests
import pandas as pd
from datetime import datetime


def transform_fact_sales_order(df_sales_order: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the sales order DataFrame into a fact table DataFrame for sales orders.
    This function performs the following transformations:
    - Ensures datetime columns ('created_at' and 'last_updated') are in datetime format.
    - Converts agreed dates ('agreed_payment_date' and 'agreed_delivery_date')
    from string to datetime and extracts the date part.
    - Creates a new DataFrame with the following columns:
        - 'sales_record_id': A unique identifier for each sales record, starting from 1.
        - 'sales_order_id': The sales order ID.
        - 'created_date': The date part of the 'created_at' column.
        - 'created_time': The time part of the 'created_at' column.
        - 'last_updated_date': The date part of the 'last_updated' column.
        - 'last_updated_time': The time part of the 'last_updated' column.
        - 'sales_staff_id': The ID of the sales staff.
        - 'counterparty_id': The ID of the counterparty.
        - 'units_sold': The number of units sold.
        - 'unit_price': The unit price, rounded to 2 decimal places.
        - 'currency_id': The ID of the currency.
        - 'design_id': The ID of the design.
        - 'agreed_payment_date': The agreed payment date.
        - 'agreed_delivery_date': The agreed delivery date.
        - 'agreed_delivery_location_id': The ID of the agreed delivery location.
    Args:
        df_sales_order (pd.DataFrame): The input DataFrame containing sales order data.
    Returns:
        pd.DataFrame: The transformed fact table DataFrame for sales orders.
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
    """
    Transforms and merges staff and department data into a dimensional staff DataFrame.
    This function performs a left join operation between the staff and department DataFrames
    on the 'department_id' column. It then selects and returns a DataFrame with the following
    columns: 
        'staff_id',
        'first_name',
        'last_name',
        'department_name',
        'location',
        'email_address'.
    Args:
        df_staff (pd.DataFrame): DataFrame containing staff information.
        df_department (pd.DataFrame): DataFrame containing department information.
    Returns:
        pd.DataFrame: A DataFrame containing the merged and transformed staff data.
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
        df_address (pd.DataFrame): The input DataFrame containing address information.
    Returns:
        pd.DataFrame: A DataFrame with selected columns renamed for dimensional location.
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

# Function to fetch exchange rates
def fetch_exchange_rates():
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/eur.json" # noqa
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch data from API")

def transform_dim_currency(df_currency: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the currency DataFrame by adding currency names and exchange rates.
    This function takes a DataFrame containing currency information, maps the currency codes
    to their respective currency names, fetches the latest exchange rates based on EUR, and
    adds these details as new columns to the DataFrame.
    Args:
        df_currency (pd.DataFrame): A DataFrame containing currency information with at least
                                    the following columns:
                                    - "currency_id": Unique identifier for the currency.
                                    - "currency_code": ISO currency code.
    Returns:
        pd.DataFrame: A transformed DataFrame with the following columns:
                      - "currency_id": Unique identifier for the currency.
                      - "currency_code": ISO currency code.
                      - "currency_name": Full name of the currency.
                      - "currency_exchange_rate_eur_based": Latest exchange rate based on EUR.
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

    # Fetch latest exchange rates EUR based
    exch_rates = fetch_exchange_rates()
    xc = {key: exch_rates['eur'][key.lower()] for key in list(currency_mapping.keys())}

    # Add a new column with currency names using the mapping
    df_currency["currency_name"] = df_currency["currency_code"].map(currency_mapping)
    # Add a new column with the exchange rate using the xc dictionary
    df_currency["currency_exchange_rate_eur_based"] = df_currency["currency_code"].map(xc)

    # Select the required columns
    df_dim_currency = df_currency[
        ["currency_id", "currency_code", "currency_name", "currency_exchange_rate_eur_based"]
    ]
    return df_dim_currency


def transform_dim_counterparty(
    df_counterparty: pd.DataFrame, df_address: pd.DataFrame
) -> pd.DataFrame:
    """
    Transforms and merges counterparty and address dataframes
    to create a dimension table for counterparties. This function
    performs a LEFT JOIN on the `df_counterparty` and `df_address`
    dataframes based on the `counterparty_id` and `address_id` columns,
    respectively. It then renames the address columns to match 
    the required output format and selects only the necessary columns
    for the final dataframe.
    Args:
        df_counterparty (pd.DataFrame): DataFrame containing counterparty information.
        df_address (pd.DataFrame): DataFrame containing address information.
    Returns:
        pd.DataFrame: A transformed DataFrame with the selected and renamed columns.
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

def transform_fact_purchase_order(df_purchase_order: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the purchase order DataFrame into a fact table DataFrame.
    This function processes the input DataFrame by ensuring datetime columns
    are in the correct format, converting agreed dates to datetime and extracting
    the date part, and creating a new DataFrame structured as a fact table
    with specific columns.
    Args:
        df_purchase_order (pd.DataFrame): The input DataFrame containing purchase order data.
    Returns:
        pd.DataFrame: A new DataFrame structured as a fact table with transformed and additional columns.
    """

    # Ensure datetime columns are in datetime format
    df_purchase_order["created_at"] = pd.to_datetime(df_purchase_order["created_at"])
    df_purchase_order["last_updated"] = pd.to_datetime(
        df_purchase_order["last_updated"]
    )  # noqa

    # Convert agreed dates from string to datetime
    # (if not already) then extract date part
    df_purchase_order["agreed_payment_date"] = pd.to_datetime(
        df_purchase_order["agreed_payment_date"]
    ).dt.date
    df_purchase_order["agreed_delivery_date"] = pd.to_datetime(
        df_purchase_order["agreed_delivery_date"]
    ).dt.date

    # Create the fact table DataFrame
    df_fact_purchase_order = pd.DataFrame(
        {
            # Generate a purchase_record_id starting from 1; mimics SERIAL
            "purchase_record_id": range(1, len(df_purchase_order) + 1),
            "purchase_order_id": df_purchase_order["purchase_order_id"],
            "created_date": df_purchase_order["created_at"].dt.date,
            "created_time": df_purchase_order["created_at"].dt.time,
            "last_updated_date": df_purchase_order["last_updated"].dt.date,
            "last_updated_time": df_purchase_order["last_updated"].dt.time,
            "purchase_staff_id": df_purchase_order["staff_id"],
            "counterparty_id": df_purchase_order["counterparty_id"],
            "item_code": df_purchase_order["item_code"],
            "item_quantity": df_purchase_order["item_quantity"],
            # Round unit_price to 2 decimal places to match numeric(10,2)
            "item_unit_price": df_purchase_order["item_unit_price"].round(2),
            "currency_id": df_purchase_order["currency_id"],
            "agreed_payment_date": df_purchase_order["agreed_payment_date"],
            "agreed_delivery_date": df_purchase_order["agreed_delivery_date"],
            "agreed_delivery_location_id": df_purchase_order[
                "agreed_delivery_location_id"
            ],
        }
    )

    return df_fact_purchase_order


def transform_fact_payment(df_payment: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the payment DataFrame into a fact table DataFrame for payments.
    Args:
        df_payment (pd.DataFrame): DataFrame containing payment data with the following columns:
            - payment_id
            - created_at
            - last_updated
            - transaction_id
            - counterparty_id
            - payment_amount
            - currency_id
            - payment_type_id
            - paid
            - payment_date
            - agreed_payment_date
            - agreed_delivery_date
    Returns:
        pd.DataFrame: Transformed DataFrame with the following columns:
            - payment_record_id: Unique identifier for each payment record.
            - payment_id: Original payment ID.
            - created_date: Date part of the created_at timestamp.
            - created_time: Time part of the created_at timestamp.
            - last_updated_date: Date part of the last_updated timestamp.
            - last_updated_time: Time part of the last_updated timestamp.
            - transaction_id: Transaction ID associated with the payment.
            - counterparty_id: Counterparty ID associated with the payment.
            - payment_amount: Payment amount rounded to 2 decimal places.
            - currency_id: Currency ID associated with the payment.
            - payment_type_id: Payment type ID.
            - paid: Payment status.
            - payment_date: Date of the payment.
    """

    # Ensure datetime columns are in datetime format
    df_payment["created_at"] = pd.to_datetime(df_payment["created_at"])
    df_payment["last_updated"] = pd.to_datetime(
        df_payment["last_updated"]
    )  # noqa

    # Convert agreed dates from string to datetime
    # (if not already) then extract date part
    df_payment["agreed_payment_date"] = pd.to_datetime(
        df_payment["agreed_payment_date"]
    ).dt.date
    df_payment["agreed_delivery_date"] = pd.to_datetime(
        df_payment["agreed_delivery_date"]
    ).dt.date

    # Create the fact table DataFrame
    df_fact_payment = pd.DataFrame(
        {
            # Generate a payment_record_id starting from 1; mimics SERIAL
            "payment_record_id": range(1, len(df_payment) + 1),
            "payment_id": df_payment["payment_id"],
            "created_date": df_payment["created_at"].dt.date,
            "created_time": df_payment["created_at"].dt.time,
            "last_updated_date": df_payment["last_updated"].dt.date,
            "last_updated_time": df_payment["last_updated"].dt.time,
            "transaction_id": df_payment["transaction_id"],
            "counterparty_id": df_payment["counterparty_id"],
            # Round unit_price to 2 decimal places to match numeric(10,2)
            "payment_amount": df_payment["payment_amount"].round(2),
            "currency_id": df_payment["currency_id"],
            "payment_type_id": df_payment["payment_type_id"],
            "paid": df_payment["paid"],
            "payment_date": df_payment["payment_date"],
        }
    )

    return df_fact_payment


def transform_dim_payment_type(
    df_payment_type: pd.DataFrame
) -> pd.DataFrame:
    """
    Transforms the payment type DataFrame by ensuring datetime columns are in the correct format
    and creating a new DataFrame with specific columns for a fact table.
    Args:
        df_payment_type (pd.DataFrame): The input DataFrame containing payment type data.
    Returns:
        pd.DataFrame: A transformed DataFrame with the following columns:
            - payment_type_id: The ID of the payment type.
            - payment_type_name: The name of the payment type.
            - created_date: The date when the payment type was created.
            - created_time: The time when the payment type was created.
            - last_updated_date: The date when the payment type was last updated.
            - last_updated_time: The time when the payment type was last updated.
    """

# Ensure datetime columns are in datetime format
    df_payment_type["created_at"] = pd.to_datetime(df_payment_type["created_at"])
    df_payment_type["last_updated"] = pd.to_datetime(
        df_payment_type["last_updated"]
    )  # noqa

    # Create the fact table DataFrame
    df_dim_payment_type = pd.DataFrame(
        {
            # Generate a payment_record_id starting from 1; mimics SERIAL
            "payment_type_id": df_payment_type["payment_type_id"],
            "payment_type_name": df_payment_type["payment_type_name"].dt.date,
            "created_date": df_payment_type["created_at"].dt.date,
            "created_time": df_payment_type["created_at"].dt.time,
            "last_updated_date": df_payment_type["last_updated"].dt.date,
            "last_updated_time": df_payment_type["last_updated"].dt.time,
            
        }
    )

    return df_dim_payment_type



def transform_dim_transaction(
    df_transaction: pd.DataFrame
) -> pd.DataFrame:
    """
    Transforms the transaction DataFrame by converting datetime columns to the appropriate format
    and creating a new DataFrame with specific columns for a fact table.
    Args:
        df_transaction (pd.DataFrame): The input DataFrame containing transaction data.
    Returns:
        pd.DataFrame: A new DataFrame with transformed transaction data, including separate columns
                      for transaction ID, transaction type, created date and time, and last updated
                      date and time.
    """

# Ensure datetime columns are in datetime format
    df_transaction["created_at"] = pd.to_datetime(df_transaction["created_at"])
    df_transaction["last_updated"] = pd.to_datetime(
        df_transaction["last_updated"]
    )  # noqa

    # Create the fact table DataFrame
    df_dim_transaction = pd.DataFrame(
        {
            # Generate a payment_record_id starting from 1; mimics SERIAL
            "transaction_id": df_transaction["transaction_id"],
            "transaction_type": df_transaction["transaction_type"].dt.date,
            "created_date": df_transaction["created_at"].dt.date,
            "created_time": df_transaction["created_at"].dt.time,
            "last_updated_date": df_transaction["last_updated"].dt.date,
            "last_updated_time": df_transaction["last_updated"].dt.time,
            
        }
    )

    return df_dim_transaction


