def get_latest_files(client, bucket, logger):
    """
    Gets the latest files in each table folder

    Arguments:
        client: the s3 client
        bucket: the bucket from where to extract the files
        logger: to log any info or errors
    """

    try:

        objects = []
        response = client.list_objects_v2(Bucket=bucket)
        while response["IsTruncated"]:
            objects.extend(response["Contents"])
            response = client.list_objects_v2(
                Bucket=bucket,
                ContinuationToken=response["NextContinuationToken"],
            )

        objects.extend(response["Contents"])

        def get_last_modified(obj):

            return int(obj["LastModified"].strftime("%s"))

        list_objects = [
            obj["Key"]
            for obj in sorted(objects, key=get_last_modified, reverse=True)  # noqa
        ]

        files_dict = {}

        for table in list_objects[:11]:
            files_dict[table.split("/")[0]] = table

        logger.info(f"Succesfully retrieved files from bucket:{bucket}")

        return files_dict

    except Exception as e:

        logger.error(f"Error getting files from bucket:{bucket}: {e}")
        raise Exception(e)
