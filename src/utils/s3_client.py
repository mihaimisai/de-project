import boto3


def s3_client(REGION="eu-west-2"):
    # Initialize S3 client
    client = boto3.client("s3", region_name=REGION)
    return client
