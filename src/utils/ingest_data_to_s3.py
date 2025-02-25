import logging
import pg8000
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.load_credentials_for_pg_access import (
    load_credentials_for_pg_access,
)
from src.utils.upload_time_stamp import upload_time_stamp
from src.utils.s3_data_upload import s3_data_upload

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def ingest_data_to_s3(
    s3_client,
    table_name,
    S3_INGESTION_BUCKET,
    S3_TIMESTAMP_BUCKET,
    time_stamp=None
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
            query = (
                f"SELECT * FROM {table_name} WHERE last_updated > {time_stamp};"  # noqa
            )
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

    time_stamp = upload_time_stamp(s3_client, S3_TIMESTAMP_BUCKET, table_name)

    s3_data_upload(s3_client, S3_INGESTION_BUCKET, table_name, buffer)
