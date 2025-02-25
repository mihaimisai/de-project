def timestamp_data_retrival(client, S3_TIMESTAMP_BUCKET, table_name):

    try:
        time_stamp = client.get_object(
            Bucket=S3_TIMESTAMP_BUCKET, Key=f"time_stamp_{table_name}.txt"
        )  # noqa
        content = time_stamp["Body"].read()
        time_stamp = content.decode("utf-8")
        return time_stamp
    except Exception:
        time_stamp = None
        return time_stamp
