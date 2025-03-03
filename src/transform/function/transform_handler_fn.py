import boto3 

def transform_handler(event, context):
    
    s3_client = boto3.client('s3')
    
    print('Event ', event)
    
    print('Context', context)
    
    all_files = s3_client.list_objects_v2(Bucket='ingestion')
    
    print(all_files)