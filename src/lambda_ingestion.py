import os
import pg8000
import boto3
import pandas as pd
import logging
from dotenv import load_dotenv
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_credentials_for_pg_access():
    """Load PostgreSQL credentials from .env file."""
    load_dotenv()

    # Retrieve credentials from environment variables
    PG_HOST = os.getenv("PG_HOST")
    PG_PORT = os.getenv("PG_PORT")
    PG_DATABASE = os.getenv("PG_DATABASE")
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")

    logger.info("Credentials for PostgreSQL access successfully loaded")
    return [PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]


# S3 Configuration
S3_INGESTION_BUCKET = "your-ingestion-bucket"
S3_TIMESTAMP_BUCKET = "your-timestamp-bucket"
S3_PROCESSED_BUCKET = "your-processed-bucket"
REGION = "eu-west-2"

def s3_client():
    # Initialize S3 client
    client = boto3.client("s3", region_name=REGION)
    return client
def timestamp_data_retrival(client,
                            S3_TIMESTAMP_BUCKET):
            
    try:
        time_stamp = client.get_object(Bucket = S3_TIMESTAMP_BUCKET, Key = 'time_stamp.txt')# test time_stamp type
        content = time_stamp['Body'].read()
        time_stamp = content.decode("utf-8") 
        return time_stamp
    except:
        time_stamp = None
        return time_stamp

def upload_time_stamp(client, bucket_name):
    try:
        # Define the file path in the S3 bucket
        s3_key_ingestion = "time_stamp.txt"
        # Get the current date and time
        now = datetime.now()

        # Format the output to yyyy-mm-dd hh:mm:ss
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
        # Write the formatted time to a txt file
        with open("time_stamp.txt", "w") as file:
            file.write(formatted_now)
        # Upload the Parquet file to S3
        client.upload_fileobj(file, bucket_name, s3_key_ingestion)
        logger.info(
            f"Successfully uploaded time_stamp.txt file to S3 bucket '{bucket_name}'"  # noqa
        )
        return formatted_now
    except Exception as e:
        logger.error(
            f"Error uploading time_stamp.txt to S3 bucket: '{bucket_name}': {e}" # noqa
        )
def s3_data_upload(client,bucket_name,time_stamp,table_name,buffer):
    try:

        # Define the file path in the S3 bucket
        
        s3_key_ingestion = f"ingestion/{table_name}.parquet"

        # Upload the Parquet file to S3
        s3_client.upload_fileobj(buffer, S3_INGESTION_BUCKET, s3_key_ingestion)
        logger.info(
            f"Successfully uploaded Parquet file to S3 bucket '{S3_INGESTION_BUCKET}' for table '{table_name}'"  # noqa
        )
    except Exception as e:
        logger.error(
            f"Error uploading Parquet file to S3 for table '{table_name}': {e}"
        )





def ingest_data_to_s3(
    s3_client,
    table_name,
    time_stamp = None,
    S3_INGESTION_BUCKET="your-ingestion-bucket",
    S3_TIMESTAMP_BUCKET="your-timestamp-bucket",
    REGION="eu-west-2",
):
    """
    Extracts data from a PostgreSQL table and uploads it to
    the ingestion S3 bucket in Parquet format.
    Logs information at each stage for better observability.
    """
    try:
        # Load database credentials
        PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD = (
            load_credentials_for_pg_access()
        )
        if not all([PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD]):
            logger.error("One or more PostgreSQL credentials are missing.")
            raise ValueError("One or more PostgreSQL credentials are missing.")
    except Exception as e:
        logger.error(f"Error loading PostgreSQL credentials: {e}")
        return

    conn = None
    try:
        # Establish a connection to PostgreSQL database
        logger.info(
            f"Connecting to PostgreSQL database: {PG_DATABASE} on host: {PG_HOST}"  # noqa
        )
        conn = pg8000.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )

        # Query to fetch all records from the given table
        if not time_stamp:
            query = f"SELECT * FROM {table_name};"
        else:
            query = f"SELECT * FROM {table_name} WHERE last_updated > {time_stamp};"
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully fetched data from table: {table_name}")
    except Exception as e:
        logger.error(
            f"Error connecting to PostgreSQL or executing query for table '{table_name}': {e}"  # noqa
        )
        return
    finally:
        if conn:
            # Ensure database connection is closed
            conn.close()

    try:
        # Convert the retrieved data into a PyArrow table format
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()

        # Write the table as a Parquet file to an in-memory buffer
        pq.write_table(table, buffer)
        buffer.seek(0)
        logger.info(
            f"Successfully converted data from table '{table_name}' to Parquet format."  # noqa
        )
    except Exception as e:
        logger.error(
            f"Error converting DataFrame to Parquet for table '{table_name}': {e}"  # noqa
        )
        return
    
    time_stamp = upload_time_stamp(s3_client,S3_TIMESTAMP_BUCKET)

    s3_data_upload(s3_client,S3_INGESTION_BUCKET,time_stamp,table_name,buffer)



# Main execution function
def process_all_tables(
    client,
    time_stamp=None,
    S3_INGESTION_BUCKET="your-ingestion-bucket",
    S3_TIMESTAMP_BUCKET="your-timestamp-bucket"
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
            client,table, time_stamp, S3_INGESTION_BUCKET, S3_TIMESTAMP_BUCKET
        )
