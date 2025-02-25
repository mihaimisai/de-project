import logging
from src.utils.ingest_data_to_s3 import ingest_data_to_s3
import pyarrow.parquet as pq  # noqa: F401

# S3 Configuration
S3_INGESTION_BUCKET = "your-ingestion-bucket"
S3_TIMESTAMP_BUCKET = "your-timestamp-bucket"
S3_PROCESSED_BUCKET = "your-processed-bucket"
REGION = "eu-west-2"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Main execution function


def process_all_tables(
    client,
    time_stamp=None,
    S3_INGESTION_BUCKET="your-ingestion-bucket",
    S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
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
            client, table, time_stamp, S3_INGESTION_BUCKET, S3_TIMESTAMP_BUCKET
        )
