def timestamp_data_retrival(client, s3_timestamp_bucket, table_name):

    try:
        time_stamp = client.get_object(
            Bucket=s3_timestamp_bucket, Key=f"time_stamp_{table_name}.txt"
        )
        content = time_stamp["Body"].read()
        time_stamp = content.decode("utf-8")
        return time_stamp
    except Exception:
        time_stamp = None
        return time_stamp
