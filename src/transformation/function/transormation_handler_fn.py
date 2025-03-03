from .utils.s3_client import s3_client
from .utils.main import main


def transformation_handler(event, context):

    client = s3_client()
    main(client, logger=logger, bucket="project-test-transform-bucket")


    return {"statusCode": 200, "body": "Data tranformation and upload completed"}
