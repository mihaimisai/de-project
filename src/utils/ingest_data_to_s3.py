from .upload_time_stamp import upload_time_stamp
from .s3_data_upload import s3_data_upload
from .connect_to_db import connect_to_db, close_db
from .timestamp_data_retrival import timestamp_data_retrival


def fetch_data(conn, table_name, time_stamp, logger):
 
    if not time_stamp:
        query = f"SELECT * FROM {table_name};"

    else:
        query = f"SELECT * FROM {table_name} WHERE last_updated > '{time_stamp}';" # noqa
        
    try:
        result = conn.execute(query)
        return result.fetchall()
    except Exception as e:
        print(f"Logger type: {type(logger)}")

        logger.error('Error')
        return []


def convert_to_csv(df):
    
    return df.to_csv(index=False).encode("utf-8")

def ingest_data_to_s3(
    s3_client, logger, table_name, s3_ingestion_bucket, s3_timestamp_bucket
):
    """
    Extracts data from a PostgreSQL table and uploads it to
    the ingestion S3 bucket in Parquet format.
    Logs information at each stage for better observability.
    """
    print(type(logger))
    conn = None

    try:

        conn = connect_to_db(logger)

        time_stamp = timestamp_data_retrival(
            s3_client,
            s3_timestamp_bucket,
            table_name
        )

        df = fetch_data(conn, table_name, time_stamp, logger)
        csv_df = convert_to_csv(df)

        logger.info(f"Successfully fetched data from table: {table_name}")

        s3_data_upload(
            s3_client,
            s3_ingestion_bucket,
            table_name,
            csv_df,
            logger,
            time_stamp
        )

        upload_time_stamp(s3_client, s3_timestamp_bucket, table_name, logger)

    except Exception as e:
        logger.error(
            f"Error connecting to PostgreSQL or executing query for table '{table_name}': {e}" # noqa 501
        )
        raise Exception(e)

    finally:
        if conn:
            close_db(conn)
