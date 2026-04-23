import json
import boto3
import urllib.request
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('Anomaly_Detector_Results')
s3_client = boto3.client('s3', region_name='us-east-2')

S3_BUCKET = 'anomaly-detector-dashboard'
TEMP_THRESHOLD = 10.0


def lambda_handler(event, context):
    city = event['name']
    lat = event['lat']
    lon = event['lon']
    today = datetime.utcnow().strftime('%Y-%m-%d')

    print(f'Processing {city}...')
    current = get_current_weather(lat, lon)
    average = get_historical_average(lat, lon)
    diff = round(current['temp'] - average['temp'], 2)
    flagged = abs(diff) >= TEMP_THRESHOLD

    if flagged:
        direction = 'above' if diff > 0 else 'below'
        print(f'{city} is {abs(diff)} degrees {direction} normal - FLAGGED')

    table.put_item(Item={
        'city': city,
        'date': today,
        'current_temp': str(current['temp']),
        'avg_temp': str(average['temp']),
        'difference': str(diff),
        'flagged': flagged,
        'current_rain': str(current['rain']),
        'avg_rain': str(average['rain'])
    })

    update_summary_file(
        city, today,
        current['temp'], average['temp'],
        diff, flagged
    )

    return {
        'statusCode': 200,
        'city': city,
        'flagged': flagged,
        'diff': diff
    }


def update_summary_file(city, date, current_temp, avg_temp, diff, flagged):
    key = f'results/{date}.json'

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        existing = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        existing = []

    existing.append({
        'city': city,
        'date': date,
        'current_temp': current_temp,
        'avg_temp': avg_temp,
        'difference': diff,
        'flagged': flagged
    })

    s3_client.put_object(
        Bucket = S3_BUCKET,
        Key = key,
        Body = json.dumps(existing).encode('utf-8'),
        ContentType = 'application/json'
    )


def get_current_weather(lat, lon):
    url = (f'https://api.open-meteo.com/v1/forecast'
           f'?latitude={lat}&longitude={lon}'
           f'&current_weather=true'
           f'&daily=precipitation_sum'
           f'&timezone=America%2FNew_York'
           f'&forecast_days=1')
    data = fetch_json(url)
    temp_c = data['current_weather']['temperature']
    temp_f = round((temp_c * 9/5) + 32, 1)
    rain = data.get('daily', {}).get('precipitation_sum', [0])[0] or 0.0
    return {'temp': temp_f, 'rain': rain}


def get_historical_average(lat, lon):
    today = datetime.utcnow()
    temps = []
    rains = []

    for years_back in range(1, 6):
        try:
            past = today.replace(year=today.year - years_back)
            start = (past - timedelta(days=3)).strftime('%Y-%m-%d')
            end = (past + timedelta(days=3)).strftime('%Y-%m-%d')
            url = (f'https://archive-api.open-meteo.com/v1/archive'
                     f'?latitude={lat}&longitude={lon}'
                     f'&start_date={start}&end_date={end}'
                     f'&daily=temperature_2m_max,precipitation_sum'
                     f'&timezone=America%2FNew_York')
            data = fetch_json(url)

            for t in data.get('daily', {}).get('temperature_2m_max', []):
                if t is not None:
                    temps.append(round((t * 9/5) + 32, 1))
            for r in data.get('daily', {}).get('precipitation_sum', []):
                if r is not None:
                    rains.append(r)

        except Exception as e:
            print(f'Warning: could not fetch year -{years_back}: {e}')
            continue

    avg_temp = round(sum(temps) / len(temps), 1) if temps else 0.0
    avg_rain = round(sum(rains) / len(rains), 2) if rains else 0.0
    return {'temp': avg_temp, 'rain': avg_rain}


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))