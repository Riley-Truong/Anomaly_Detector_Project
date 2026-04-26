import json
import boto3
import urllib.request
import time
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table    = dynamodb.Table('Anomaly_Detector_Results')


def lambda_handler(event, context):
    cities = event['cities']   # list of 10 cities
    source = event['source']   # 'forecast'
    today  = event.get('date', datetime.utcnow().strftime('%Y-%m-%d'))

    start_ms = int(time.time() * 1000)
    print(f"WorkerForecast: processing {len(cities)} cities")

    for city in cities:
        try:
            url = (f"https://api.open-meteo.com/v1/forecast"
                   f"?latitude={city['lat']}&longitude={city['lon']}"
                   f"&current_weather=true"
                   f"&daily=precipitation_sum"
                   f"&timezone=America%2FNew_York&forecast_days=1")

            data   = fetch_json(url)
            temp_c = data['current_weather']['temperature']
            temp_f = round((temp_c * 9/5) + 32, 1)
            rain   = data.get('daily', {}).get('precipitation_sum', [0])[0] or 0.0

            table.put_item(Item={
                'city':        city['name'],
                'date':        today,
                'source':      'forecast',
                'temp_f':      str(temp_f),
                'rain_in':     str(rain),
                'duration_ms': str(int(time.time() * 1000) - start_ms),
            })
            print(f"Forecast saved: {city['name']} = {temp_f}F")

        except Exception as e:
            print(f"Error on {city['name']}: {e}")

    total_ms = int(time.time() * 1000) - start_ms
    print(f"WorkerForecast done in {total_ms}ms")
    return {'statusCode': 200, 'source': source, 'cities': len(cities), 'duration_ms': total_ms}


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode('utf-8'))