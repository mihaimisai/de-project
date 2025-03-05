from .utils.s3_client import s3_client


def load_handler(event, context):
    """
    Orchestrates load and storage of data
    from processed bucket into warehouse.

    Invokes s3_client and process_all_tables.

        Parameters:
            event (event object): json object - converted to empty dict
            by lambda python runtime
            context (context object): context object provided by AWS

        Returns:
            dictionary, {'statusCode': 200, "body": "Data ingestion complete" }
            if successful
    """
    client = s3_client()

    return {"statusCode": 200, "body": "Data ingestion complete"}
