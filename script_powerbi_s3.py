import pandas as pd
import boto3
from io import StringIO

aws_access_key_id = 'AKIAXLOEF7XPQ74PVODY'
aws_secret_access_key = '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'
aws_default_region = 'ap-southeast-1'

s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_default_region)

bucket_name = 'youtube-do-mixi'
object_key = 'do_mixi28112023103237.csv'

try:
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    csv_content = response['Body'].read().decode('utf-8')
    data = pd.read_csv(StringIO(csv_content))
    print(data)
except Exception as e:
    print(f"Error: {e}")
