def get_latest_files(client, bucket, logger):
    """
    Gets the latest files in each table folder

    Arguments:
        client: the s3 client
        bucket: the bucket from where to extract the files
        logger: to log any info or errors
    """

    try:

        objs = client.list_objects_v2(Bucket=bucket)

        def get_last_modified(obj):
            return int(obj["LastModified"].strftime("%s"))

        list_objects = [
            obj["Key"]
            for obj in sorted(objs.get("Contents", []), key=get_last_modified)
        ]

        keys = [table.split("/")[0] for table in list_objects]

        files_dict = {}

        for i in range(len(keys)):
            files_dict[keys[i]] = list_objects[i]

        logger.info(f"Succesfully retrieved files from bucket:{bucket}")

        return files_dict

    except Exception as e:

        logger.error(f"Error getting files from bucket:{bucket}: {e}")
        raise Exception(e)
