import json
import boto3
import urllib.request
import time
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table    = dynamodb.Table('Anomaly_Detector_Results')


def lambda_handler(event, context):
    cities = event['cities']   # list of 10 cities
    source = event['source']   # 'archive'
    today  = event.get('date', datetime.utcnow().strftime('%Y-%m-%d'))

    start_ms = int(time.time() * 1000)
    print(f"WorkerArchive: processing {len(cities)} cities")

    for city in cities:
        try:
            now   = datetime.utcnow()
            temps = []
            rains = []

            for years_back in range(1, 6):
                past  = now.replace(year=now.year - years_back)
                start = (past - timedelta(days=3)).strftime('%Y-%m-%d')
                end   = (past + timedelta(days=3)).strftime('%Y-%m-%d')
                url   = (f"https://archive-api.open-meteo.com/v1/archive"
                         f"?latitude={city['lat']}&longitude={city['lon']}"
                         f"&start_date={start}&end_date={end}"
                         f"&daily=temperature_2m_max,precipitation_sum"
                         f"&timezone=America%2FNew_York")
                data  = fetch_json(url)

                for t in data.get('daily', {}).get('temperature_2m_max', []):
                    if t: temps.append(round((t * 9/5) + 32, 1))
                for r in data.get('daily', {}).get('precipitation_sum', []):
                    if r is not None: rains.append(r)

            avg_temp = round(sum(temps)/len(temps), 1) if temps else 0.0
            avg_rain = round(sum(rains)/len(rains), 2) if rains else 0.0

            table.put_item(Item={
                'city':        city['name'],
                'date':        today,
                'source':      'archive',
                'avg_temp_f':  str(avg_temp),
                'avg_rain_in': str(avg_rain),
                'duration_ms': str(int(time.time() * 1000) - start_ms),
            })
            print(f"Archive saved: {city['name']} avg={avg_temp}F")

        except Exception as e:
            print(f"Error on {city['name']}: {e}")

    total_ms = int(time.time() * 1000) - start_ms
    print(f"WorkerArchive done in {total_ms}ms")
    return {'statusCode': 200, 'source': source, 'cities': len(cities), 'duration_ms': total_ms}


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode('utf-8'))