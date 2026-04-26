import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Attr

dynamodb  = boto3.resource('dynamodb', region_name='us-east-2')
table     = dynamodb.Table('Anomaly_Detector_Results')
s3_client = boto3.client('s3', region_name='us-east-2')
S3_BUCKET = 'anomaly-detector-dashboard'

def lambda_handler(event, context):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    print(f"Aggregating sources for {today}...")

    # Scan for all records from today across all three sources
    response = table.scan(FilterExpression=Attr('date').eq(today))
    items    = response.get('Items', [])

    # Handle pagination if table has more than 1MB of data
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Attr('date').eq(today),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    print(f"Found {len(items)} source records")

    if not items:
        print("No records found for today yet — workers may still be running")
        return {'statusCode': 200, 'message': 'No records yet'}

    # Write the per-source detail file for the Sources tab
    # Convert any Decimal types to strings for JSON serialization
    clean_items = json.loads(json.dumps(items, default=str))

    s3_client.put_object(
        Bucket      = S3_BUCKET,
        Key         = f"results/{today}_sources.json",
        Body        = json.dumps(clean_items, indent=2).encode('utf-8'),
        ContentType = 'application/json',
    )

    print(f"Wrote {len(items)} records to results/{today}_sources.json")
    return {'statusCode': 200, 'records': len(items), 'date': today}