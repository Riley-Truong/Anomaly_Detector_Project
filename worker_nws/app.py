import json
import boto3
import urllib.request
import time
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table    = dynamodb.Table('Anomaly_Detector_Results')

HEADERS = {'User-Agent': 'AnomalyDetector/2.0 (student-project)'}


def lambda_handler(event, context):
    cities = event['cities']   # list of 10 cities
    source = event['source']   # 'nws'
    today  = event.get('date', datetime.utcnow().strftime('%Y-%m-%d'))

    start_ms = int(time.time() * 1000)
    print(f"WorkerNWS: processing {len(cities)} cities")

    for city in cities:
        obs_temp_f  = None
        alerts      = []
        has_warning = False

        try:
            points  = fetch_json(f"https://api.weather.gov/points/{city['lat']},{city['lon']}")
            obs_url = points['properties'].get('observationStations', '')

            if obs_url:
                stations = fetch_json(obs_url)
                if stations.get('features'):
                    sid  = stations['features'][0]['properties']['stationIdentifier']
                    obs  = fetch_json(f"https://api.weather.gov/stations/{sid}/observations/latest")
                    temp_c = obs['properties']['temperature']['value']
                    if temp_c is not None:
                        obs_temp_f = round((temp_c * 9/5) + 32, 1)

            alert_data = fetch_json(f"https://api.weather.gov/alerts/active?point={city['lat']},{city['lon']}")
            for feature in alert_data.get('features', []):
                event_name = feature['properties'].get('event', '')
                alerts.append(event_name)
                if any(w in event_name.lower() for w in ['warning', 'advisory', 'watch']):
                    has_warning = True

        except Exception as e:
            print(f"NWS error for {city['name']}: {e}")

        table.put_item(Item={
            'city':        city['name'],
            'date':        today,
            'source':      'nws',
            'obs_temp_f':  str(obs_temp_f) if obs_temp_f else 'N/A',
            'alerts':      json.dumps(alerts),
            'has_warning': has_warning,
            'duration_ms': str(int(time.time() * 1000) - start_ms),
        })
        print(f"NWS saved: {city['name']} obs={obs_temp_f}F alerts={len(alerts)}")

    total_ms = int(time.time() * 1000) - start_ms
    print(f"WorkerNWS done in {total_ms}ms")
    return {'statusCode': 200, 'source': source, 'cities': len(cities), 'duration_ms': total_ms}


def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode('utf-8'))