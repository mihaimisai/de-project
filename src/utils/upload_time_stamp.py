from io import BytesIO
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def upload_time_stamp(client, bucket_name, table_name):
    try:
        # Define the file path in the S3 bucket
        s3_key_ingestion = f"time_stamp_{table_name}.txt"
        # Get the current date and time
        now = datetime.now()

        # Format the output to yyyy-mm-dd hh:mm:ss
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
        # Store the timestamp in an in-memory buffer
        timestamp_buffer = BytesIO()
        timestamp_buffer.write(formatted_now.encode("utf-8"))
        timestamp_buffer.seek(0)

        # Define the S3 key for the timestamp file
        s3_key_ingestion = f"time_stamp_{table_name}.txt"

        # Upload the timestamp file to S3
        client.upload_fileobj(timestamp_buffer, bucket_name, s3_key_ingestion)
        logger.info(
            f"Successfully uploaded time_stamp_{table_name}.txt file to S3 bucket '{bucket_name}'"  # noqa
        )
        return formatted_now
    except Exception as e:
        logger.error(
            f"Error uploading time_stamp_{table_name}.txt to S3 bucket: '{bucket_name}': {e}"  # noqa
        )
        raise  # Re-raise the exception so tests expecting an error will pass
