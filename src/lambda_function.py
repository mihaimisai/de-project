import logging
from src.utils.ingest_data_to_s3 import ingest_data_to_s3
import pyarrow.parquet as pq

# S3 Configuration
s3_ingestion_bucket = "your-ingestion-bucket"
s3_timestamp_bucket = "your-timestamp-bucket"
# not used
# S3_PROCESSED_BUCKET = "your-processed-bucket"
region = "eu-west-2"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Main execution function


def process_all_tables(
    client,
    # s3_ingestion_bucket,
    # s3_timestamp_bucket,
    time_stamp=None
):
    """
    Orchestrates the ingestion and transformation of multiple tables.
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

    for table in tables:
        ingest_data_to_s3(
            client, table, time_stamp, s3_ingestion_bucket, s3_timestamp_bucket
        )
