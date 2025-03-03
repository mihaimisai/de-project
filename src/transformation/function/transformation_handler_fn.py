from .utils.s3_client import s3_client
from .utils.main import main
from .list_s3 import logger


def transformation_handler(event, context):

    client = s3_client()
    main(client,
         logger=logger,
         ingested_bucket_name = "de-project-ingested-data-20250227143401632000000004t",
         transform_bucket_name="project-test-transform-bucket")


    return {"statusCode": 200, "body": "Data tranformation and upload completed"}
