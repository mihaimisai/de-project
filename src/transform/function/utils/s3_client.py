import boto3


def s3_client(REGION="eu-west-2"):
    """
    Initializes s3 client.

        Parameters:
            region (str): AWS region (default is 'eu-west-2')

        Returns:
            s3 client
    """

    try:

        client = boto3.client("s3", region_name=REGION)

        return client

    except Exception as e:

        raise e
