import os
from pg8000 import Connection
from pg8000.native import identifier

"""
    This file will be run automatically when doing terraform-destroy

    It will access the datawarehouse and clear all rows expect dim_date
"""

PG_HOST = os.environ.get("DB_HOST_DW")
PG_PORT = os.environ.get("DB_PORT_DW")
PG_DATABASE = os.environ.get("DB_DW")
PG_USER = os.environ.get("DB_USER_DW")
PG_PASSWORD = os.environ.get("DB_PASSWORD_DW")


conn = Connection(
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
)

cur = conn.cursor()

tables = [
    "fact_payment",
    "fact_purchase_order",
    "fact_sales_order",
    "dim_counterparty",
    "dim_currency",
    "dim_design",
    "dim_location",
    "dim_payment_type",
    "dim_staff",
    "dim_transaction",
]

for table in tables:
    query = "DELETE FROM "
    query += f"{identifier(table)};"
    cur.execute(query)
    conn.commit()

cur.close()
