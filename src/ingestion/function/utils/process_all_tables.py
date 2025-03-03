import logging
import os
from .ingest_data_to_s3 import ingest_data_to_s3
from .s3_client import s3_client


# S3 Configuration
# names obtained from tf
s3_ingestion_bucket = os.environ.get("ingested_data_bucket")
s3_timestamp_bucket = os.environ.get("timestamp_bucket")
region = "eu-west-2"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Main execution function
def process_all_tables(client, logger=logger):
    """
    Orchestrates the ingestion and transformation of multiple tables.

    Invokes ingest_data_to_s3 function for list of tables.

    Parameters:
        client (): s3 client
        logger (Logger): logger instance (default is logger
        - variable defined in enclosing scope)
    """
    tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    client = s3_client()

    for table in tables:
        ingest_data_to_s3(
            client, logger, table, s3_ingestion_bucket, s3_timestamp_bucket
        )
