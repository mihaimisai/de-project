from src.load.function.utils.s3_client import s3_client
from src.load.function.utils.get_parquet_v2 import (
    get_recent_parquet_files_from_s3,
)
from src.load.function.utils.create_table_in_db import create_table_in_db
from src.load.function.utils.insert_dataframe_in_db import (
    insert_dataframe_in_db,
)
from src.load.function.utils.connect_to_dw import connect_to_db
import logging
import os


def load_handler(event, context):
    """
    Orchestrates load and storage of data
    from processed bucket into warehouse.

    Parameters:
        event (event object): json object - converted to empty dict
        by lambda python runtime
        context (context object): context object provided by AWS

    Returns:
        dictionary, {'statusCode': 200, "body": "Data ingestion complete" }
        if successful
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    conn = None
    client = s3_client()
    load_bucket_name = os.environ.get("processed_data_bucket")
    try:
        conn = connect_to_db(logger)
        df_data_tables = get_recent_parquet_files_from_s3(
            bucket_name=load_bucket_name, client=client, logger=logger
        )

        for table_name, df in df_data_tables.items():
            table_columns = ", ".join(df.columns)
            create_table_in_db(conn, table_name, table_columns, logger)
            insert_dataframe_in_db(conn, table_name, df, logger)

        return {"statusCode": 200, "body": "Data ingestion complete"}
    except Exception as e:
        logger.error(f"Error in load_handler: {e}")
        return {"statusCode": 500, "body": f"Error: {e}"}
    finally:
        if conn is not None:
            conn.close()
