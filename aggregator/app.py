import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr

dynamodb  = boto3.resource('dynamodb', region_name='us-east-2')
table     = dynamodb.Table('Anomaly_Detector_Results')
s3_client = boto3.client('s3', region_name='us-east-2')
S3_BUCKET = 'anomaly-detector-dashboard'

ATLANTA_LAT   = 33.7490
ATLANTA_LON   = -84.3880
SEARCH_RADIUS = 50000
CITY_LIMIT    = 30
TEMP_THRESHOLD = 10.0


def lambda_handler(event, context):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    print(f"Aggregating for {today}...")

    # ── Step 1: Write sources file from DynamoDB (workers tab) ──────
    response = table.scan(FilterExpression=Attr('date').eq(today))
    items    = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Attr('date').eq(today),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    print(f"Found {len(items)} DynamoDB records")

    if items:
        clean_items = json.loads(json.dumps(items, default=str))
        s3_client.put_object(
            Bucket      = S3_BUCKET,
            Key         = f"results/{today}_sources.json",
            Body        = json.dumps(clean_items, indent=2).encode('utf-8'),
            ContentType = 'application/json',
        )
        print(f"Wrote sources file: {len(items)} records")

    # ── Step 2: Fetch cities ─────────────────────────────────────────
    cities = get_cities_near_atlanta()
    print(f"Processing {len(cities)} cities for anomaly results...")

    # ── Step 3: Fetch weather and compute anomalies ──────────────────
    summary = []

    for city in cities:
        current = get_current_weather(city['lat'], city['lon'])
        average = get_historical_average(city['lat'], city['lon'])

        if current and average:
            diff    = round(current['temp_f'] - average['avg_temp_f'], 2)
            flagged = abs(diff) >= TEMP_THRESHOLD
            summary.append({
                'city':         city['name'],
                'date':         today,
                'current_temp': str(current['temp_f']),
                'avg_temp':     str(average['avg_temp_f']),
                'difference':   str(diff),
                'flagged':      flagged,
                'current_rain': str(current['rain_in']),
                'avg_rain':     str(average['avg_rain_in']),
            })
            status = 'FLAGGED' if flagged else 'normal'
            print(f"{city['name']}: {current['temp_f']}F vs avg {average['avg_temp_f']}F | diff={diff} | {status}")
        else:
            print(f"Skipping {city['name']} — weather data unavailable")

    # Sort by absolute difference descending
    summary.sort(key=lambda x: -abs(float(x['difference'])))

    # ── Step 4: Write anomaly results file (anomalies tab) ───────────
    if summary:
        s3_client.put_object(
            Bucket      = S3_BUCKET,
            Key         = f"results/{today}.json",
            Body        = json.dumps(summary, indent=2).encode('utf-8'),
            ContentType = 'application/json',
        )
        print(f"Wrote results/{today}.json with {len(summary)} cities")
    else:
        print("No anomaly data to write — all cities failed to fetch")

    return {
        'statusCode': 200,
        'date':       today,
        'cities':     len(summary),
        'sources':    len(items),
    }


# ── Weather helpers ───────────────────────────────────────────────

def get_current_weather(lat, lon):
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lon}"
           f"&current_weather=true"
           f"&daily=precipitation_sum"
           f"&timezone=America%2FNew_York&forecast_days=1")
    try:
        data   = fetch_json(url)
        temp_c = data['current_weather']['temperature']
        temp_f = round((temp_c * 9/5) + 32, 1)
        rain   = data.get('daily', {}).get('precipitation_sum', [0])[0] or 0.0
        return {'temp_f': temp_f, 'rain_in': rain}
    except Exception as e:
        print(f"Forecast error ({lat},{lon}): {e}")
        return None


def get_historical_average(lat, lon):
    now   = datetime.utcnow()
    temps = []
    rains = []
    for years_back in range(1, 6):
        try:
            past  = now.replace(year=now.year - years_back)
            start = (past - timedelta(days=3)).strftime('%Y-%m-%d')
            end   = (past + timedelta(days=3)).strftime('%Y-%m-%d')
            url   = (f"https://archive-api.open-meteo.com/v1/archive"
                     f"?latitude={lat}&longitude={lon}"
                     f"&start_date={start}&end_date={end}"
                     f"&daily=temperature_2m_max,precipitation_sum"
                     f"&timezone=America%2FNew_York")
            data  = fetch_json(url)
            for t in data.get('daily', {}).get('temperature_2m_max', []):
                if t: temps.append(round((t * 9/5) + 32, 1))
            for r in data.get('daily', {}).get('precipitation_sum', []):
                if r is not None: rains.append(r)
        except Exception as e:
            print(f"Archive error year -{years_back}: {e}")
    avg_temp = round(sum(temps)/len(temps), 1) if temps else 0.0
    avg_rain = round(sum(rains)/len(rains), 2) if rains else 0.0
    return {'avg_temp_f': avg_temp, 'avg_rain_in': avg_rain}


# ── City helpers ──────────────────────────────────────────────────

def get_cities_near_atlanta():
    query = f"""
    [out:json];
    node(around:{SEARCH_RADIUS},{ATLANTA_LAT},{ATLANTA_LON})["place"~"city|town"];
    out {CITY_LIMIT};
    """
    url = "https://overpass-api.de/api/interpreter?data=" + urllib.parse.quote(query.strip())
    try:
        req = urllib.request.Request(
            url, headers={'User-Agent': 'AnomalyDetector/2.0 (student-project)'}
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
        cities = []
        for element in data.get('elements', []):
            if 'tags' in element and 'name' in element['tags']:
                cities.append({
                    'name': element['tags']['name'],
                    'lat':  element['lat'],
                    'lon':  element['lon']
                })
        return cities if cities else get_fallback_cities()
    except Exception as e:
        print(f"Overpass failed: {e}. Using fallback.")
        return get_fallback_cities()


def get_fallback_cities():
    return [
        {'name': 'Atlanta',        'lat': 33.7490, 'lon': -84.3880},
        {'name': 'Marietta',       'lat': 33.9526, 'lon': -84.5499},
        {'name': 'Roswell',        'lat': 34.0232, 'lon': -84.3616},
        {'name': 'Alpharetta',     'lat': 34.0754, 'lon': -84.2941},
        {'name': 'Decatur',        'lat': 33.7748, 'lon': -84.2963},
        {'name': 'Smyrna',         'lat': 33.8840, 'lon': -84.5144},
        {'name': 'Sandy Springs',  'lat': 33.9304, 'lon': -84.3733},
        {'name': 'Kennesaw',       'lat': 34.0234, 'lon': -84.6155},
        {'name': 'Peachtree City', 'lat': 33.3965, 'lon': -84.5949},
        {'name': 'Duluth',         'lat': 34.0032, 'lon': -84.1447},
        {'name': 'Lawrenceville',  'lat': 33.9526, 'lon': -83.9880},
        {'name': 'Woodstock',      'lat': 34.1015, 'lon': -84.5194},
        {'name': 'Canton',         'lat': 34.2368, 'lon': -84.4913},
        {'name': 'Cumming',        'lat': 34.2073, 'lon': -84.1400},
        {'name': 'Gainesville',    'lat': 34.2979, 'lon': -83.8241},
        {'name': 'Buford',         'lat': 34.1218, 'lon': -83.9913},
        {'name': 'Cartersville',   'lat': 34.1651, 'lon': -84.7996},
        {'name': 'Douglasville',   'lat': 33.7515, 'lon': -84.7477},
        {'name': 'Newnan',         'lat': 33.3807, 'lon': -84.7997},
        {'name': 'Griffin',        'lat': 33.2468, 'lon': -84.2641},
        {'name': 'McDonough',      'lat': 33.4473, 'lon': -84.1474},
        {'name': 'Stockbridge',    'lat': 33.5440, 'lon': -84.2338},
        {'name': 'Conyers',        'lat': 33.6676, 'lon': -84.0177},
        {'name': 'Covington',      'lat': 33.5968, 'lon': -83.8602},
        {'name': 'Monroe',         'lat': 33.7954, 'lon': -83.7135},
        {'name': 'Winder',         'lat': 33.9937, 'lon': -83.7196},
        {'name': 'Jasper',         'lat': 34.4676, 'lon': -84.4285},
        {'name': 'Dallas',         'lat': 33.9237, 'lon': -84.8399},
        {'name': 'Villa Rica',     'lat': 33.7315, 'lon': -84.9177},
        {'name': 'Carrollton',     'lat': 33.5801, 'lon': -85.0766},
    ]


def fetch_json(url):
    req = urllib.request.Request(
        url, headers={'User-Agent': 'AnomalyDetector/2.0 (student-project)'}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode('utf-8'))