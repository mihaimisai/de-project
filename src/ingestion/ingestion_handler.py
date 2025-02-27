from src.ingestion.utils.s3_client import s3_client
from src.ingestion.utils.process_all_tables import process_all_tables


def ingestion_handler(event, context):

    client = s3_client()
    process_all_tables(client)

    return {"statusCode": 200, "body": "Data ingestion complete"}
