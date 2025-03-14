from .utils.s3_client import s3_client
from .utils.get_parquet_v2 import (
    get_recent_parquet_files_from_s3,
)
from .utils.insert_dataframe_in_db import (
    insert_dataframe_in_db,
)
from .utils.connect_to_dw import connect_to_db
import logging
import os


def load_handler(event, context):
    """
    Orchestrates load and storage of data
    from transformed data bucket into warehouse.

    Parameters:
        event (event object): json object - converted to empty dict
        by lambda python runtime
        context (context object): context object provided by AWS

    Returns:
        dictionary, {'statusCode': 200, "body": "Data loading complete" }
        if successful
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    conn = None
    client = s3_client()
    transformed_data_bucket = os.environ.get("transformed_data_bucket")
    try:
        conn = connect_to_db(logger)
        df_data_tables = get_recent_parquet_files_from_s3(
            bucket_name=transformed_data_bucket, client=client, logger=logger
        )

        sorted_tables = sorted(df_data_tables.items())
        for table_name, df in sorted_tables:
            logger.debug(table_name)
            insert_dataframe_in_db(conn, table_name, df, logger)

        return {"statusCode": 200, "body": "Data loading complete"}
    except Exception as e:
        logger.error(f"Error in load_handler: {e}")
        return {"statusCode": 500, "body": f"Error: {e}"}
    finally:
        if conn is not None:
            conn.close()
