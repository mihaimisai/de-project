from .utils.s3_client import s3_client
from .utils.process_all_tables import process_all_tables


def ingestion_handler(event, context):
    '''
    Orchestrates ingestion and storage of data from multiple tables in database. 

    Invokes s3_client and process_all_tables.

        Parameters:
            event (event object): json object - converted to empty dict by lambda python runtime
            context (context object): context object provided by AWS

        Returns:
            dictionary, {'statusCode': 200, "body": "Data ingestion complete" } if successful
    '''
    client = s3_client()
    process_all_tables(client)

    return {"statusCode": 200, "body": "Data ingestion complete"}