import boto3
import os
import json

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def main(event, context):
    # Parse event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Read file content
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    content = response['Body'].read().decode('utf-8')
    
    # Count lines
    line_count = len(content.splitlines())
    
    # Prepare DynamoDB entry
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    table.put_item(
        Item={
            'file_id': object_key,
            'timestamp': response['LastModified'].isoformat(),
            'line_count': line_count,
        }
    )
    
    return {'statusCode': 200, 'body': json.dumps('File processed successfully.')}

