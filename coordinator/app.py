import json
import boto3
import os
 
lambda_client = boto3.client('lambda', region_name='us-east-2')
WORKER_FUNCTION_NAME = 'AnomalyDetectorWorker'
 
def lambda_handler(event, context):
    cities_path = os.path.join(os.path.dirname(__file__), 'city_demo.json')
    with open(cities_path, 'r') as f:
        cities = json.load(f)
 
    print(f'Launching {len(cities)} workers in parallel...')
    launched = 0
 
    for city in cities:
        try:
            lambda_client.invoke(
                FunctionName = WORKER_FUNCTION_NAME,
                InvocationType = 'Event',
                Payload = json.dumps(city).encode('utf-8')
            )
            launched += 1
        except Exception as e:
            print(f'Failed to launch worker for {city["name"]}: {e}')
 
    print(f'Successfully launched {launched} of {len(cities)} workers.')
    return {
        'statusCode': 200,
        'message': f'Launched {launched} workers'
    }
