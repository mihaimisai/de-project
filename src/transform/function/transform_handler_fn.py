from .utils.get_latest_files import get_latest_files
from .utils.list_s3 import list_s3
from .utils.s3_client import s3_client
import logging
import os

def transform_handler(event, context):
    
    """
    Is invoked when triggered by an event

    Invokes s3_client, files_dict and list_s3.

        Parameters:
            event (event object): json object - converted to empty dict
            by lambda python runtime
            context (context object): context object provided by AWS

        Returns:
            dictionary, {'statusCode': 200, "body": "Data transformation complete" }
            if successful
    """
    
    logger = logging.getLogger(__name__)
    logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
    
    bucket_name = os.environ.get("ingested_data_bucket")
    
    files_dict = get_latest_files(s3_client, bucket_name, logger)
    
    list_s3(s3_client, files_dict, logger, bucket_name)
    
    return {"statusCode": 200, "body": "Data transformation complete"}


